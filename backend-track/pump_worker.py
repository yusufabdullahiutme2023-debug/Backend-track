"""Read-only Pump.fun launch watcher; records validated creations, never trades.

Run as a dedicated persistent worker (see README). One process does, in order:

1. Subscribe to confirmed logs FIRST, then replay what was missed since the durable cursor,
   so nothing falls between the checkpoint and the stream. Two streams are available
   (``PUMP_WORKER_STREAM``): ``program`` mentions the Pump program, so every buy and sell is
   delivered; ``creations`` mentions Pump's mint-authority account, which only launch
   transactions include, so almost nothing else is delivered or billed. Each keeps its own
   cursor.
2. A logs notification already says whether a transaction can be a creation. Only those
   (plus any whose logs are missing or truncated, so a creation is never ruled out blind)
   are fetched with getTransaction and verified by the decoder. Everything else only moves
   the in-memory cursor forward, which keeps the RPC bill proportional to launches rather
   than to every buy and sell on the program.
3. A fetch that comes back empty is retried briefly in place. Only when the retries are
   exhausted does the connection restart, and the cursor never moves past an unverified
   creation.
4. Launches and the cursor are written in one Postgres transaction over ONE long-lived
   connection. Cursor progress made on skipped transactions is flushed on every heartbeat.
5. A heartbeat row (table ``pump_worker_heartbeat``, readable with
   ``python pump_worker.py --status``) and a log line report liveness and counters, and a
   stream that goes silent is torn down and rebuilt.

Provider quota still has to be watched in the provider dashboard: the stream itself is
metered by data volume, independent of anything this process fetches. That is why the
``creations`` stream exists; use it only after ``pump_stream_compare.py`` has shown on live
data that it misses nothing the ``program`` stream sees.
"""
import argparse
import asyncio
import http.client
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import Json
import websockets

from pump_discover import rpc
from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM, fetch_transaction, log_verdict, parse_launch

log = logging.getLogger('pump-worker')
# What the worker listens to. Each stream has its own cursor, because a signature taken from one
# address's history is not found in the other's. Switching streams therefore starts a new cursor
# ("monitor from now"); the old one is left untouched. A creation-only stream is quiet enough that
# silence has to last longer before it counts as a stall.
#
# ``filter_logs``: the program stream is mostly trades (and about half of them failed), so a log line
# decides which notifications are worth a getTransaction. On the creations stream the subscription
# itself is the filter: every successful notification mentions the mint authority, so each one is
# verified by the decoder and nothing depends on Pump's log text.
STREAMS = {
    'program': {'address': PUMP_PROGRAM, 'checkpoint': 'pump-fun-creations-v1', 'stall_seconds': 60.0,
                'filter_logs': True},
    'creations': {'address': MINT_AUTHORITY, 'checkpoint': 'pump-fun-creations-v1-mint-authority',
                  'stall_seconds': 300.0, 'filter_logs': False},
}
CHECKPOINT_NAME = STREAMS['program']['checkpoint']
HEALTHY_RUN_SECONDS = 60       # a connection that lived this long resets the reconnect backoff
BASE_BACKOFF_SECONDS = 1       # first reconnect delay; doubles per consecutive failure
MAX_BACKOFF_SECONDS = 60
MANUAL_BACKFILL_DELAY_SECONDS = 300   # a gap only a person can approve is re-checked this rarely
CURSOR_FLUSH_EVERY = 100       # backfill: persist cursor progress at least this often
RETRYABLE_HTTP = frozenset({408, 425, 429})


class TransactionUnavailable(RuntimeError):
    """getTransaction kept failing transiently; the cursor is left where it was."""


class ManualBackfillRequired(RuntimeError):
    """The gap since the cursor cannot be proven covered; a person has to decide what to do."""


class StreamStalled(RuntimeError):
    """The subscription is open but no notification arrived for too long."""


@dataclass(frozen=True)
class Settings:
    heartbeat_seconds: float = 15.0
    stall_seconds: float = 60.0
    backfill_concurrency: int = 4
    max_backfill_pages: int = 10          # x 1000 signatures; beyond it a person must approve
    max_queue: int = 10_000
    fetch_attempts: int = 5
    fetch_backoff: tuple = (0.25, 0.5, 1.0, 2.0)   # seconds waited between attempts
    stream: str = 'program'                        # a key of STREAMS

    @property
    def address(self):
        return STREAMS[self.stream]['address']

    @property
    def checkpoint(self):
        return STREAMS[self.stream]['checkpoint']

    @property
    def filter_logs(self):
        return STREAMS[self.stream]['filter_logs']

    @classmethod
    def from_env(cls, environ=None):
        environ = os.environ if environ is None else environ

        def read(name, default, low, high, cast):
            raw = (environ.get(name) or '').strip()
            if not raw:
                return default
            try:
                value = cast(raw)
            except ValueError:
                raise ValueError(f'{name} must be a number') from None
            if not low <= value <= high:
                raise ValueError(f'{name} must be between {low} and {high}')
            return value

        stream = (environ.get('PUMP_WORKER_STREAM') or '').strip() or cls.stream
        if stream not in STREAMS:
            raise ValueError(f'PUMP_WORKER_STREAM must be one of: {", ".join(sorted(STREAMS))}')
        return cls(
            stream=stream,
            heartbeat_seconds=read('PUMP_WORKER_HEARTBEAT_SECONDS', cls.heartbeat_seconds, 1, 300, float),
            stall_seconds=read('PUMP_WORKER_STALL_SECONDS', STREAMS[stream]['stall_seconds'], 10, 3600, float),
            backfill_concurrency=read('PUMP_WORKER_BACKFILL_CONCURRENCY', cls.backfill_concurrency, 1, 16, int),
            max_backfill_pages=read('PUMP_WORKER_MAX_BACKFILL_PAGES', cls.max_backfill_pages, 1, 500, int),
        )


def websocket_url(http_url):
    parsed = urlsplit(http_url)
    if parsed.scheme != 'https' or parsed.hostname not in (
            'mainnet.helius-rpc.com', 'atlas-mainnet.helius-rpc.com'):
        raise ValueError('Expected an HTTPS Helius Mainnet RPC endpoint')
    return urlunsplit(('wss', parsed.netloc, parsed.path, parsed.query, ''))


# --------------------------------------------------------------------------- state ------

COUNTERS = ('notifications',   # logs notifications received
            'failed',          # failed on-chain, never a launch
            'filtered',        # complete logs without a creation line, skipped without a fetch
            'candidates',      # creation lines, fetched and verified
            'unfiltered',      # logs missing/truncated, fetched because a creation can't be ruled out
            'launches',        # verified and recorded for the first time (replays not recounted)
            'malformed',       # unreadable messages
            'rpc_calls',       # RPC requests issued (a proxy for provider credits)
            'fetch_retries',   # getTransaction attempts repeated in place
            'backfill_fetches',
            'reconnects')


class WorkerState:
    """Liveness, counters and cursor progress shared by stream, backfill and heartbeat."""

    def __init__(self):
        self.started_at = datetime.now(timezone.utc)
        self.status = 'starting'
        self.stream = 'program'
        self.last_notification_at = None
        self.last_slot = None
        self.last_error = None
        self.counters = dict.fromkeys(COUNTERS, 0)
        self.cursor = None        # newest signature fully handled (possibly not persisted yet)
        self.persisted = None     # newest signature known to be committed in Postgres
        self.last_beat = 0.0      # monotonic time of the last heartbeat attempt
        self._lock = threading.Lock()

    def bump(self, name, amount=1):
        with self._lock:
            self.counters[name] += amount

    def handled(self, signature):
        self.cursor = signature

    def saved(self, signature):
        self.cursor = self.persisted = signature

    def stats(self, connects=0):
        with self._lock:
            stats = dict(self.counters)
        stats['db_connects'] = connects
        stats['stream'] = self.stream
        stats['last_error'] = self.last_error
        return stats


# ---------------------------------------------------------------------------- store ------

class Store:
    """Postgres access over ONE long-lived connection.

    A fresh connection per write costs a TCP + TLS + auth handshake every time, which at
    mainnet volume alone exceeds the time budget. A connection the server dropped (restart,
    idle timeout, network blip) is reopened once per call; every statement is idempotent,
    so that retry is safe.
    """

    def __init__(self, db_url, connect=psycopg2.connect, checkpoint=CHECKPOINT_NAME):
        self._db_url = db_url
        self._checkpoint = checkpoint
        self._connect = connect
        self._conn = None
        self._lock = threading.Lock()
        self.connects = 0

    def _connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = self._connect(self._db_url, connect_timeout=10, keepalives=1,
                                       keepalives_idle=30, keepalives_interval=10,
                                       keepalives_count=3)
            self.connects += 1
        return self._conn

    def _discard(self):
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - the connection is already unusable
                pass

    def _run(self, work):
        """Run ``work(cursor)`` in one transaction; retry once if the connection dropped."""
        with self._lock:
            for attempt in (1, 2):
                try:
                    conn = self._connection()
                    with conn:                      # commit/rollback; the connection stays open
                        with conn.cursor() as cur:
                            return work(cur)
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    self._discard()
                    if attempt == 2:
                        raise

    def close(self):
        with self._lock:
            self._discard()

    def prepare(self):
        def work(cur):
            cur.execute('''CREATE TABLE IF NOT EXISTS pump_launches (
                signature TEXT PRIMARY KEY, mint TEXT NOT NULL, pool TEXT NOT NULL,
                slot BIGINT NOT NULL, detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                evidence JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'unscored')''')
            cur.execute('''CREATE TABLE IF NOT EXISTS pump_worker_cursor (
                name TEXT PRIMARY KEY, signature TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now())''')
            cur.execute('''CREATE TABLE IF NOT EXISTS pump_worker_heartbeat (
                name TEXT PRIMARY KEY, beat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                status TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL,
                last_notification_at TIMESTAMPTZ, last_slot BIGINT,
                stats JSONB NOT NULL DEFAULT '{}'::jsonb)''')
        self._run(work)

    def cursor(self):
        def work(cur):
            cur.execute('SELECT signature FROM pump_worker_cursor WHERE name=%s',
                        (self._checkpoint,))
            row = cur.fetchone()
            return row[0] if row else None
        return self._run(work)

    def save(self, signature, launch):
        """Event + cursor commit in the SAME transaction. Duplicate notifications are safe;
        a crash before commit causes a replay instead of silent loss.

        Returns True when ``launch`` was recorded for the first time (a replay of a launch
        that is already stored returns False), so callers count genuinely new launches.
        """
        def work(cur):
            inserted = False
            if launch is not None:
                cur.execute('''INSERT INTO pump_launches (signature,mint,pool,slot,evidence)
                               VALUES (%s,%s,%s,%s,%s) ON CONFLICT (signature) DO NOTHING''',
                            (launch.signature, launch.mint, launch.pool, launch.slot,
                             Json(launch.model_dump())))
                inserted = cur.rowcount == 1
            cur.execute('''INSERT INTO pump_worker_cursor (name, signature) VALUES (%s,%s)
                           ON CONFLICT (name) DO UPDATE SET signature=EXCLUDED.signature,
                           updated_at=now()''', (self._checkpoint, signature))
            return inserted
        return self._run(work)

    def beat(self, status, started_at, last_notification_at, last_slot, stats):
        def work(cur):
            cur.execute('''INSERT INTO pump_worker_heartbeat
                               (name, status, started_at, last_notification_at, last_slot, stats)
                           VALUES (%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (name) DO UPDATE SET beat_at=now(), status=EXCLUDED.status,
                               started_at=EXCLUDED.started_at,
                               last_notification_at=EXCLUDED.last_notification_at,
                               last_slot=EXCLUDED.last_slot, stats=EXCLUDED.stats''',
                        (self._checkpoint, status, started_at, last_notification_at, last_slot,
                         Json(stats)))
        self._run(work)

    def heartbeat_status(self):
        """The freshest heartbeat of any stream, as the database sees it (ages use the DB clock).

        The newest beat belongs to whichever stream is running now, so ``--status`` needs no
        knowledge of which one that is. None when no worker ever started.
        """
        def work(cur):
            cur.execute('''SELECT h.name, h.status, h.last_slot, h.stats, h.started_at,
                                  h.last_notification_at,
                                  extract(epoch FROM now() - h.beat_at),
                                  extract(epoch FROM now() - c.updated_at)
                           FROM pump_worker_heartbeat h
                           LEFT JOIN pump_worker_cursor c ON c.name = h.name
                           ORDER BY h.beat_at DESC LIMIT 1''')
            row = cur.fetchone()
            if row is None:
                return None
            def iso(value):
                return value.isoformat() if value is not None else None
            return {'checkpoint': row[0], 'status': row[1], 'last_slot': row[2], 'stats': row[3],
                    'started_at': iso(row[4]), 'last_notification_at': iso(row[5]),
                    'heartbeat_age_seconds': float(row[6]),
                    'cursor_age_seconds': float(row[7]) if row[7] is not None else None}
        return self._run(work)


# ------------------------------------------------------------------------ rpc helpers ----

def missed_items(http_url, previous, max_pages=10, state=None, address=PUMP_PROGRAM):
    """Signature entries for ``address`` newer than the cursor, oldest-first; fail closed if the
    backlog is too big. ``address`` is the one the stream mentions, so the cursor is found in it."""
    before = None
    found = []
    for _ in range(max_pages):
        opts = {'limit': 1000, 'commitment': 'confirmed'}
        if before:
            opts['before'] = before
        if state is not None:
            state.bump('rpc_calls')
        page = rpc(http_url, 'getSignaturesForAddress', [address, opts])
        for item in page:
            if item['signature'] == previous:
                return list(reversed(found))
            found.append(item)
        if not page or len(page) < 1000:
            # Provider may have truncated history; don't silently pretend no gap.
            raise ManualBackfillRequired('Persisted cursor missing from available program history')
        before = page[-1]['signature']
    raise ManualBackfillRequired(
        f'Missed more than {max_pages * 1000:,} Pump transactions; manual backfill required')


def missed_signatures(http_url, previous, max_pages=10):
    """Return signatures oldest-first since cursor; fail closed if backlog is too big."""
    return [item['signature'] for item in missed_items(http_url, previous, max_pages)]


def _retryable(exc):
    if isinstance(exc, urllib.error.HTTPError):       # before OSError: HTTPError is one
        return exc.code in RETRYABLE_HTTP or 500 <= exc.code <= 599
    # ValueError: null result / JSON-RPC error / undecodable body; the rest: network trouble.
    return isinstance(exc, (ValueError, OSError, http.client.HTTPException))


def _retry_after(exc):
    """Seconds a rate-limited caller should wait: the provider's hint, bounded to 1-5 s."""
    try:
        wanted = float(exc.headers.get('Retry-After'))
    except (AttributeError, TypeError, ValueError):
        wanted = 1.0
    return min(5.0, max(1.0, wanted))


def fetch_with_retry(http_url, signature, settings, state=None):
    """getTransaction with a few short in-place retries before giving up.

    A freshly confirmed transaction is often not yet served by the node that answers the
    call (null result / block not available). That clears in well under a second, so
    waiting briefly beats tearing the stream down. Rate limits, 5xx and network errors get
    the same treatment; credential or request errors (4xx) do not, since retrying cannot
    fix them. Exception text is never propagated: it can embed the API key.
    """
    attempts = max(1, settings.fetch_attempts)
    for attempt in range(1, attempts + 1):
        if state is not None:
            state.bump('rpc_calls')
        try:
            return fetch_transaction(http_url, signature)
        except Exception as exc:  # noqa: BLE001 - classified below
            if not _retryable(exc):
                raise
            if attempt == attempts:
                raise TransactionUnavailable(
                    f'getTransaction failed {attempts} times ({type(exc).__name__})') from None
            if state is not None:
                state.bump('fetch_retries')
            backoff = settings.fetch_backoff
            delay = backoff[min(attempt - 1, len(backoff) - 1)] if backoff else 0
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 429:
                delay = max(delay, _retry_after(exc))      # a rate limit clears per window
            time.sleep(delay)


# -------------------------------------------------------------------------- processing ----

def classify(event, filter_logs=True):
    """What does one websocket message need? Returns ``(action, signature, slot)``.

    ``ignore`` (not a logs notification), ``malformed``, ``failed`` (failed on-chain),
    ``filtered`` (complete logs, no creation: skip), ``candidate`` (creation line) or
    ``unfiltered`` (logs missing/truncated: a creation cannot be ruled out).

    With ``filter_logs=False`` (the creations stream) the subscription already did the
    filtering, so every successful notification is a ``candidate`` whatever its logs say.
    """
    if not isinstance(event, dict) or event.get('method') != 'logsNotification':
        return 'ignore', None, None
    params = event.get('params')
    result = params.get('result') if isinstance(params, dict) else None
    value = result.get('value') if isinstance(result, dict) else None
    if not isinstance(value, dict) or not isinstance(value.get('signature'), str):
        return 'malformed', None, None
    context = result.get('context')
    slot = context.get('slot') if isinstance(context, dict) else None
    if value.get('err') is not None:
        return 'failed', value['signature'], slot
    if not filter_logs:
        return 'candidate', value['signature'], slot
    action = {'creation': 'candidate', 'unknown': 'unfiltered',
              'other': 'filtered'}[log_verdict(value.get('logs'))]
    return action, value['signature'], slot


async def flush_cursor(store, state):
    """Persist cursor progress made on transactions that were skipped without a fetch."""
    signature = state.cursor
    if signature is None or signature == state.persisted:
        return
    await asyncio.to_thread(store.save, signature, None)
    state.persisted = signature


async def beat(store, state, status=None):
    """Flush cursor progress, write the heartbeat row and log one status line.

    Monitoring must never take ingest down: failures here are logged (class name only) and
    ignored. Launch writes are separate and DO fail loudly.
    """
    if status is not None:
        state.status = status
    state.last_beat = time.monotonic()
    try:
        await flush_cursor(store, state)
    except Exception as exc:  # noqa: BLE001
        log.warning('cursor flush failed (%s)', type(exc).__name__)
    stats = state.stats(getattr(store, 'connects', 0))
    try:
        await asyncio.to_thread(store.beat, state.status, state.started_at,
                                state.last_notification_at, state.last_slot, stats)
    except Exception as exc:  # noqa: BLE001
        log.warning('heartbeat write failed (%s)', type(exc).__name__)
    log.info('heartbeat status=%s slot=%s %s', state.status, state.last_slot,
             ' '.join(f'{key}={value}' for key, value in stats.items() if key != 'last_error'))


async def process(http_url, store, signature, settings, state):
    """Verify one candidate creation and commit it together with the cursor."""
    tx = await asyncio.to_thread(fetch_with_retry, http_url, signature, settings, state)
    launch = parse_launch(tx)
    inserted = await asyncio.to_thread(store.save, signature, launch)
    state.saved(signature)
    if inserted:
        state.bump('launches')
        log.info('Verified creation signature=%s mint=%s slot=%s',
                 signature, launch.mint, launch.slot)


async def handle(raw, http_url, store, settings, state):
    try:
        event = json.loads(raw)
    except ValueError:
        state.bump('malformed')
        return
    action, signature, slot = classify(event, settings.filter_logs)
    if action == 'ignore':
        return
    if action == 'malformed':
        state.bump('malformed')
        return
    state.bump('notifications')
    state.last_notification_at = datetime.now(timezone.utc)
    if isinstance(slot, int) and (state.last_slot is None or slot > state.last_slot):
        state.last_slot = slot
    if action in ('failed', 'filtered'):
        state.bump(action)
        state.handled(signature)
        return
    state.bump('candidates' if action == 'candidate' else 'unfiltered')
    await process(http_url, store, signature, settings, state)


async def backfill(http_url, store, settings, state):
    """Replay everything since the durable cursor, oldest-first, then return.

    The signature listing carries no logs, so each successful transaction has to be
    fetched to know whether it was a creation. Transactions that failed on-chain cannot
    be launches and are skipped, and fetches run with bounded concurrency so catching
    up is faster than the program produces new transactions. Results are still applied
    strictly in order, so the cursor never passes a creation that is not recorded.
    """
    previous = await asyncio.to_thread(store.cursor)
    if not previous:
        # First-ever start: establish a documented "monitor from now" cursor.
        state.bump('rpc_calls')
        latest = await asyncio.to_thread(rpc, http_url, 'getSignaturesForAddress',
                                         [settings.address, {'limit': 1}])
        if latest:
            await asyncio.to_thread(store.save, latest[0]['signature'], None)
            state.saved(latest[0]['signature'])
        return
    state.saved(previous)
    items = await asyncio.to_thread(missed_items, http_url, previous, settings.max_backfill_pages,
                                    state, settings.address)
    if items:
        log.info('Backfilling %d signatures since the cursor', len(items))
    since_flush = 0
    width = max(1, settings.backfill_concurrency)
    for start in range(0, len(items), width):
        chunk = items[start:start + width]
        wanted = [item['signature'] for item in chunk if item.get('err') is None]
        state.bump('backfill_fetches', len(wanted))
        results = await asyncio.gather(
            *(asyncio.to_thread(fetch_with_retry, http_url, sig, settings, state)
              for sig in wanted), return_exceptions=True)
        fetched = dict(zip(wanted, results))
        for item in chunk:
            signature = item['signature']
            outcome = fetched.get(signature)
            if isinstance(outcome, BaseException):
                raise outcome           # earlier signatures are already applied, in order
            launch = parse_launch(outcome) if outcome is not None else None
            if launch is not None:
                inserted = await asyncio.to_thread(store.save, signature, launch)
                state.saved(signature)
                since_flush = 0
                if inserted:
                    state.bump('launches')
                    log.info('Verified creation signature=%s mint=%s slot=%s (backfill)',
                             signature, launch.mint, launch.slot)
            else:
                state.handled(signature)
                since_flush += 1
        if since_flush >= CURSOR_FLUSH_EVERY:
            await flush_cursor(store, state)
            since_flush = 0
        if time.monotonic() - state.last_beat >= settings.heartbeat_seconds:
            await beat(store, state)
    await flush_cursor(store, state)


async def stream(ws, http_url, store, settings, state):
    """Process live notifications until the connection fails or goes silent."""
    last_message = time.monotonic()
    next_beat = last_message + settings.heartbeat_seconds
    while True:
        wait = min(next_beat, last_message + settings.stall_seconds) - time.monotonic()
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(wait, 0.01))
        except asyncio.TimeoutError:
            raw = None
        now = time.monotonic()
        if raw is not None:
            last_message = now
            await handle(raw, http_url, store, settings, state)
        if now - last_message >= settings.stall_seconds:
            raise StreamStalled(f'No Pump.fun notification for {settings.stall_seconds:g}s')
        if now >= next_beat:
            await beat(store, state)
            next_beat = time.monotonic() + settings.heartbeat_seconds


async def watch_once(http_url, store, settings, state):
    # Subscribe FIRST, then backfill. This closes the gap between querying the
    # checkpoint and establishing the stream; duplicates are idempotent.
    state.status = 'connecting'
    state.stream = settings.stream          # the heartbeat always names the stream it is running
    async with websockets.connect(websocket_url(http_url), ping_interval=30, ping_timeout=30,
                                  max_queue=settings.max_queue) as ws:
        await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'logsSubscribe',
                                  'params': [{'mentions': [settings.address]},
                                             {'commitment': 'confirmed'}]}))
        reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        if reply.get('error') or not isinstance(reply.get('result'), int):
            raise RuntimeError('WebSocket subscription rejected')
        log.info('Pump.fun subscription established (stream=%s)', settings.stream)
        try:
            await beat(store, state, status='backfilling')
            await backfill(http_url, store, settings, state)
            await beat(store, state, status='streaming')
            await stream(ws, http_url, store, settings, state)
        finally:
            try:
                await flush_cursor(store, state)
            except Exception as exc:  # noqa: BLE001
                log.warning('final cursor flush failed (%s)', type(exc).__name__)


async def run(http_url, db_url, settings=None, store=None):
    settings = settings or Settings.from_env()
    store = store or Store(db_url, checkpoint=settings.checkpoint)
    state = WorkerState()
    state.stream = settings.stream
    await asyncio.to_thread(store.prepare)
    failures = 0
    try:
        while True:
            started = time.monotonic()
            try:
                await watch_once(http_url, store, settings, state)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # A connection that lived a while was healthy: restart the backoff ladder.
                failures = 1 if time.monotonic() - started >= HEALTHY_RUN_SECONDS else failures + 1
                blocked = isinstance(exc, ManualBackfillRequired)
                delay = (MANUAL_BACKFILL_DELAY_SECONDS if blocked
                         else min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * 2 ** (failures - 1)))
                frames = traceback.extract_tb(exc.__traceback__)
                where = frames[-1].name if frames else '?'
                state.bump('reconnects')
                # Class and function name only: exception text can embed the API key.
                state.last_error = f'{type(exc).__name__} in {where}'
                log.error('Watcher interrupted (%s); retrying in %gs', state.last_error, delay)
                if blocked:
                    log.error('The gap since the cursor cannot be proven covered. Nothing is skipped; see '
                              'the README (PUMP_WORKER_MAX_BACKFILL_PAGES) to approve a larger catch-up.')
                await beat(store, state, status='needs_manual_backfill' if blocked else 'reconnecting')
                await asyncio.sleep(delay)
    finally:
        state.status = 'stopped'
        await beat(store, state)
        store.close()


async def main():
    http_url = os.environ['SOLANA_RPC_URL']
    db_url = os.environ['DATABASE_URL']
    websocket_url(http_url)  # validate before connecting/logging
    settings = Settings.from_env()
    task = asyncio.create_task(run(http_url, db_url, settings))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, task.cancel)   # finish with a final cursor flush
        except NotImplementedError:                     # not available on every platform
            pass
    try:
        await task
    except asyncio.CancelledError:
        log.info('Stopped on signal')


def status(db_url, max_age):
    """Print the heartbeat; exit code 0 healthy, 1 stale or not streaming, 2 never started."""
    store = Store(db_url)
    try:
        row = store.heartbeat_status()
    finally:
        store.close()
    if row is None:
        print(json.dumps({'healthy': False, 'reason': 'no heartbeat recorded'}))
        return 2
    healthy = (row['heartbeat_age_seconds'] <= max_age
               and row['status'] in ('streaming', 'backfilling'))
    print(json.dumps({'healthy': healthy, **row}, indent=2))
    return 0 if healthy else 1


def cli(argv=None):
    parser = argparse.ArgumentParser(description='Pump.fun launch watcher (read-only, never trades)')
    parser.add_argument('--status', action='store_true',
                        help='print the heartbeat from Postgres and exit '
                             '(0 healthy, 1 stale or not streaming, 2 never started)')
    parser.add_argument('--max-age', type=float, default=90.0,
                        help='seconds after which a heartbeat counts as stale (default 90)')
    args = parser.parse_args(argv)
    if args.status:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            parser.error('Set DATABASE_URL privately; do not paste credentials into the command.')
        return status(db_url, args.max_age)
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
    return 0


if __name__ == '__main__':
    sys.exit(cli())
