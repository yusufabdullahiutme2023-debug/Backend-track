"""The worker's live path: stream filtering, in-place retry, backfill, heartbeat, backoff, CLI.

Unit tests drive stream()/backfill()/run() with scripted fakes. The end-to-end tests at the
bottom run the real ``watch_once``/``run`` against a local WebSocket + JSON-RPC server pair, so
the actual wire handling (subscribe, notifications, reconnect, getSignaturesForAddress paging)
is exercised without any provider.
"""
import asyncio
import contextlib
import json
import threading
import time

import pytest

import pump_worker
from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM
from pump_fakes import (BUY_LOGS, CREATE_LOGS, Done, FakeNode, FakeRpcServer, FakeStream, FakeWS,
                        MemoryStore, creation_tx, eventually, notification, plain_tx, signature)

FAILED = {'InstructionError': [0, 'Custom']}
FAST = pump_worker.Settings(heartbeat_seconds=0.05, stall_seconds=0.4, backfill_concurrency=3,
                            fetch_attempts=4, fetch_backoff=(0, 0, 0))
QUIET = pump_worker.Settings(heartbeat_seconds=600, stall_seconds=600, backfill_concurrency=3,
                             fetch_attempts=4, fetch_backoff=(0, 0, 0))


class FakeFetch:
    """Replaces pump_worker.fetch_transaction: serves txs, fails on demand, tracks concurrency."""

    def __init__(self, txs=None, unavailable=None, delays=None, delay=0.0):
        self.txs = txs or {}
        self.unavailable = dict(unavailable or {})   # signature -> how many calls fail first
        self.delays = delays or {}
        self.delay = delay
        self.calls = []
        self.inflight = self.max_inflight = 0
        self.lock = threading.Lock()

    def __call__(self, url, sig):
        with self.lock:
            self.calls.append(sig)
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            time.sleep(self.delays.get(sig, self.delay))
            with self.lock:
                if self.unavailable.get(sig, 0) > 0:
                    self.unavailable[sig] -= 1
                    raise ValueError('Transaction not available from this RPC')
            return self.txs.get(sig) or plain_tx(sig)
        finally:
            with self.lock:
                self.inflight -= 1


def install(monkeypatch, fetch):
    monkeypatch.setattr(pump_worker, 'fetch_transaction', fetch)
    return fetch


def run_stream(messages, store, state, settings=QUIET, then='done'):
    return asyncio.run(pump_worker.stream(FakeWS(messages, then), 'url', store, settings, state))


# --- stream ---------------------------------------------------------------------------------

def test_only_creation_logs_and_unreadable_logs_cost_a_fetch(monkeypatch):
    fetch = install(monkeypatch, FakeFetch({signature(5): creation_tx(signature(5))}))
    store, state = MemoryStore(), pump_worker.WorkerState()
    messages = [notification(signature(1), BUY_LOGS), notification(signature(2), BUY_LOGS),
                notification(signature(3), BUY_LOGS, err=FAILED),
                notification(signature(4), BUY_LOGS + ['Log truncated']),   # create line may be cut off
                notification(signature(5), CREATE_LOGS, slot=105),
                notification(signature(6), BUY_LOGS),
                json.dumps({'jsonrpc': '2.0', 'result': 3, 'id': 9})]       # not a notification
    with pytest.raises(Done):
        run_stream(messages, store, state)
    assert fetch.calls == [signature(4), signature(5)]
    counters = state.counters
    assert (counters['notifications'], counters['filtered'], counters['failed'],
            counters['unfiltered'], counters['candidates'], counters['launches']) == (6, 3, 1, 1, 1, 1)
    # Only the two fetched transactions were written; skipped ones cost no database round trip.
    assert [sig for sig, _ in store.saves] == [signature(4), signature(5)]
    assert store.saves[1][1].signature == signature(5)
    assert state.cursor == signature(6) and state.persisted == signature(5)
    assert state.last_slot == 105


def test_cursor_progress_on_skipped_transactions_is_flushed_by_the_heartbeat(monkeypatch):
    install(monkeypatch, FakeFetch())
    store, state = MemoryStore(), pump_worker.WorkerState()
    messages = [notification(signature(n), BUY_LOGS) for n in range(1, 6)]
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(asyncio.wait_for(pump_worker.stream(
            FakeWS(messages, 'hang'), 'url', store, FAST.__class__(**{**FAST.__dict__, 'stall_seconds': 30}),
            state), 0.4))
    assert store.saves == [(signature(5), None)]          # one batched cursor write, not five
    assert state.persisted == signature(5)
    assert len(store.beats) >= 2 and store.beats[-1]['stats']['filtered'] == 5


def test_an_empty_fetch_is_retried_in_place_without_dropping_the_stream(monkeypatch):
    sig = signature(1)
    fetch = install(monkeypatch, FakeFetch({sig: creation_tx(sig)}, unavailable={sig: 2}))
    store, state = MemoryStore(), pump_worker.WorkerState()
    with pytest.raises(Done):                                # Done = our script ended, not a failure
        run_stream([notification(sig, CREATE_LOGS)], store, state)
    assert fetch.calls == [sig] * 3 and state.counters['fetch_retries'] == 2
    assert [launch.signature for launch in store.launches] == [sig]


def test_exhausted_retries_end_the_connection_without_passing_the_creation(monkeypatch):
    fetch = install(monkeypatch, FakeFetch(unavailable={signature(3): 10 ** 6}))
    store, state = MemoryStore(), pump_worker.WorkerState()
    messages = [notification(signature(1), BUY_LOGS), notification(signature(2), BUY_LOGS),
                notification(signature(3), CREATE_LOGS)]
    with pytest.raises(pump_worker.TransactionUnavailable):
        run_stream(messages, store, state)
    assert len(fetch.calls) == QUIET.fetch_attempts
    assert store.saves == []                                 # nothing recorded for the creation ...
    assert state.cursor == signature(2)                      # ... and the cursor stops right before it


def test_a_failed_launch_write_is_fatal_and_keeps_the_cursor_back(monkeypatch):
    sig = signature(2)
    install(monkeypatch, FakeFetch({sig: creation_tx(sig)}))
    store, state = MemoryStore(), pump_worker.WorkerState()
    store.fail_saves = 1
    with pytest.raises(RuntimeError, match='database unavailable'):
        run_stream([notification(signature(1), BUY_LOGS), notification(sig, CREATE_LOGS)], store, state)
    assert state.cursor == signature(1) and store.launches == []


def test_a_silent_subscription_is_torn_down_and_heartbeats_continue_meanwhile():
    store, state = MemoryStore(), pump_worker.WorkerState()
    settings = pump_worker.Settings(heartbeat_seconds=0.05, stall_seconds=0.3)
    started = time.monotonic()
    # wait_for makes a regression fail in seconds (TimeoutError) instead of hanging the suite.
    with pytest.raises(pump_worker.StreamStalled):
        asyncio.run(asyncio.wait_for(pump_worker.stream(
            FakeWS([], 'hang'), 'url', store, settings, state), 5))
    assert 0.25 <= time.monotonic() - started < 2.0
    assert len(store.beats) >= 3                             # the heartbeat kept beating while waiting


def test_unreadable_messages_are_counted_and_skipped(monkeypatch):
    sig = signature(1)
    install(monkeypatch, FakeFetch({sig: creation_tx(sig)}))
    store, state = MemoryStore(), pump_worker.WorkerState()
    junk = ['not json at all', json.dumps({'method': 'logsNotification',
                                            'params': {'result': {'value': {}}}})]
    with pytest.raises(Done):
        run_stream(junk + [notification(sig, CREATE_LOGS)], store, state)
    assert state.counters['malformed'] == 2 and state.counters['launches'] == 1


# --- backfill -------------------------------------------------------------------------------

def listing(monkeypatch, oldest_first, failed=(), cursor='cursor'):
    """Serve a getSignaturesForAddress page: newest-first, ending at the cursor."""
    entries = [{'signature': sig, 'err': FAILED if sig in failed else None} for sig in oldest_first]
    page = list(reversed(entries)) + [{'signature': cursor, 'err': None}]
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: page)


def run_backfill(store, state=None, settings=QUIET):
    state = state or pump_worker.WorkerState()
    state.last_beat = time.monotonic()
    asyncio.run(pump_worker.backfill('url', store, settings, state))
    return state


def test_first_ever_start_only_records_a_monitor_from_now_cursor(monkeypatch):
    fetch = install(monkeypatch, FakeFetch())
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: [{'signature': 'latest'}])
    store = MemoryStore(cursor=None)
    state = run_backfill(store)
    assert store.saves == [('latest', None)] and fetch.calls == [] and state.persisted == 'latest'


def test_backfill_skips_failed_transactions_and_ends_on_the_newest_signature(monkeypatch):
    sigs = [signature(n) for n in range(1, 9)]
    fetch = install(monkeypatch, FakeFetch({sigs[1]: creation_tx(sigs[1]), sigs[5]: creation_tx(sigs[5])}))
    listing(monkeypatch, sigs, failed={sigs[2], sigs[7]})
    store = MemoryStore(cursor='cursor')
    state = run_backfill(store)
    assert sorted(fetch.calls) == sorted(sigs[i] for i in (0, 1, 3, 4, 5, 6))     # failed ones: no fetch
    assert [launch.signature for launch in store.launches] == [sigs[1], sigs[5]]
    assert store.cursor_value == sigs[7]               # newest listing entry, though it failed on-chain
    assert state.counters['backfill_fetches'] == 6 and state.counters['launches'] == 2


def test_backfill_fetches_concurrently_but_applies_results_in_order(monkeypatch):
    sigs = [signature(n) for n in range(1, 13)]
    txs = {sigs[0]: creation_tx(sigs[0]), sigs[1]: creation_tx(sigs[1])}
    fetch = install(monkeypatch, FakeFetch(txs, delays={sigs[0]: 0.15}, delay=0.02))
    listing(monkeypatch, sigs)
    store = MemoryStore(cursor='cursor')
    run_backfill(store)
    assert 1 < fetch.max_inflight <= QUIET.backfill_concurrency
    # The first launch finished LAST inside its chunk, yet is still recorded first.
    assert [launch.signature for launch in store.launches] == [sigs[0], sigs[1]]


def test_backfill_stops_at_the_first_unavailable_transaction_and_keeps_order(monkeypatch):
    sigs = [signature(n) for n in range(1, 10)]
    txs = {sigs[i]: creation_tx(sigs[i]) for i in (1, 3, 5)}        # launches at 2, 4 and 6
    install(monkeypatch, FakeFetch(txs, unavailable={sigs[4]: 10 ** 6}))   # transaction 5 never arrives
    listing(monkeypatch, sigs)
    store, state = MemoryStore(cursor='cursor'), pump_worker.WorkerState()
    with pytest.raises(pump_worker.TransactionUnavailable):
        run_backfill(store, state)
    assert [launch.signature for launch in store.launches] == [sigs[1], sigs[3]]   # 6 was fetched but NOT applied
    assert state.cursor == sigs[3] and store.cursor_value == sigs[3]


def test_backfill_persists_cursor_progress_in_batches(monkeypatch):
    sigs = [signature(n) for n in range(1, 251)]
    install(monkeypatch, FakeFetch())
    listing(monkeypatch, sigs)
    store = MemoryStore(cursor='cursor')
    run_backfill(store, settings=pump_worker.Settings(heartbeat_seconds=600, backfill_concurrency=4,
                                                      fetch_backoff=(0,)))
    assert [sig for sig, _ in store.saves] == [sigs[99], sigs[199], sigs[249]]   # every 100, then the end


def test_a_backlog_the_worker_cannot_prove_it_covered_fails_closed(monkeypatch):
    install(monkeypatch, FakeFetch())
    monkeypatch.setattr(pump_worker, 'rpc', lambda *a: [{'signature': signature(i)} for i in range(1000)])
    store = MemoryStore(cursor='cursor')
    with pytest.raises(pump_worker.ManualBackfillRequired, match='manual backfill required'):
        run_backfill(store)
    assert store.saves == []


def test_backfill_uses_the_operators_page_limit(monkeypatch):
    install(monkeypatch, FakeFetch())
    seen = []
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: (
        seen.append(1), [{'signature': signature(len(seen) * 1000 + i)} for i in range(1000)])[1])
    settings = pump_worker.Settings(heartbeat_seconds=600, max_backfill_pages=3, fetch_backoff=(0,))
    with pytest.raises(pump_worker.ManualBackfillRequired, match='Missed more than 3,000'):
        run_backfill(MemoryStore(cursor='cursor'), settings=settings)
    assert len(seen) == 3


# --- reconnect policy -----------------------------------------------------------------------

def run_failing_worker(monkeypatch, sleeps_before_stop, error):
    delays = []

    async def failing_watch(*args):
        raise error

    async def fake_sleep(delay):
        delays.append(delay)
        if len(delays) >= sleeps_before_stop:
            raise asyncio.CancelledError

    monkeypatch.setattr(pump_worker, 'watch_once', failing_watch)
    monkeypatch.setattr(pump_worker.asyncio, 'sleep', fake_sleep)
    store = MemoryStore()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(pump_worker.run('url', 'db', FAST, store=store))
    return delays, store


def test_reconnect_delay_doubles_to_a_cap_and_the_heartbeat_records_it(monkeypatch):
    delays, store = run_failing_worker(monkeypatch, 8, RuntimeError('boom'))
    assert delays == [1, 2, 4, 8, 16, 32, 60, 60]
    assert store.prepared and store.closed
    statuses = [beat['status'] for beat in store.beats]
    assert 'reconnecting' in statuses and statuses[-1] == 'stopped'
    assert store.beats[-1]['stats']['reconnects'] == 8


def test_a_connection_that_lived_long_enough_resets_the_backoff(monkeypatch):
    monkeypatch.setattr(pump_worker, 'HEALTHY_RUN_SECONDS', 0)
    delays, _ = run_failing_worker(monkeypatch, 4, RuntimeError('boom'))
    assert delays == [1, 1, 1, 1]


def test_a_gap_only_a_person_can_approve_is_rechecked_rarely_and_flagged(monkeypatch, caplog):
    with caplog.at_level('ERROR', logger='pump-worker'):
        delays, store = run_failing_worker(monkeypatch, 3, pump_worker.ManualBackfillRequired('gap'))
    assert delays == [300, 300, 300]                       # never on the doubling ladder
    assert 'needs_manual_backfill' in [beat['status'] for beat in store.beats]
    assert 'PUMP_WORKER_MAX_BACKFILL_PAGES' in caplog.text
    assert store.beats[-1]['stats']['last_error'].startswith('ManualBackfillRequired')


def test_exception_text_never_reaches_the_log_or_the_heartbeat(monkeypatch, caplog):
    secret = 'https://mainnet.helius-rpc.com/?api-key=SECRET'
    with caplog.at_level('INFO', logger='pump-worker'):
        _, store = run_failing_worker(monkeypatch, 2, RuntimeError(secret))
    assert 'SECRET' not in caplog.text and 'SECRET' not in json.dumps(store.beats)
    assert 'RuntimeError in failing_watch' in caplog.text      # class and function, nothing else
    assert store.beats[-1]['stats']['last_error'] == 'RuntimeError in failing_watch'


# --- --status -------------------------------------------------------------------------------

class StatusStore:
    row = None

    def __init__(self, url):
        pass

    def heartbeat_status(self):
        return StatusStore.row

    def close(self):
        pass


def check_status(monkeypatch, capsys, row, *args):
    StatusStore.row = row
    monkeypatch.setattr(pump_worker, 'Store', StatusStore)
    monkeypatch.setenv('DATABASE_URL', 'postgresql://user:SECRETPASSWORD@db.example/app')
    code = pump_worker.cli(['--status', *args])
    out = capsys.readouterr().out
    assert 'SECRETPASSWORD' not in out
    return code, json.loads(out)


def beat_row(status='streaming', age=3.0):
    return {'status': status, 'last_slot': 9, 'stats': {'launches': 4}, 'started_at': None,
            'last_notification_at': None, 'heartbeat_age_seconds': age, 'cursor_age_seconds': 1.0}


@pytest.mark.parametrize('status, age, extra, code', [
    ('streaming', 3.0, [], 0), ('backfilling', 3.0, [], 0),
    ('streaming', 500.0, [], 1), ('reconnecting', 3.0, [], 1), ('stopped', 3.0, [], 1),
    ('needs_manual_backfill', 3.0, [], 1),
    ('streaming', 500.0, ['--max-age', '1000'], 0),
])
def test_status_exit_codes(monkeypatch, capsys, status, age, extra, code):
    got, report = check_status(monkeypatch, capsys, beat_row(status, age), *extra)
    assert got == code and report['healthy'] == (code == 0) and report['stats'] == {'launches': 4}


def test_status_reports_a_worker_that_never_started(monkeypatch, capsys):
    code, report = check_status(monkeypatch, capsys, None)
    assert code == 2 and report == {'healthy': False, 'reason': 'no heartbeat recorded'}


def test_status_needs_a_database_url(monkeypatch, capsys):
    monkeypatch.delenv('DATABASE_URL', raising=False)
    with pytest.raises(SystemExit) as caught:
        pump_worker.cli(['--status'])
    assert caught.value.code == 2 and 'DATABASE_URL' in capsys.readouterr().err


def test_status_against_a_real_database(pg, monkeypatch, capsys):
    store = pg[0]
    store.beat('streaming', pump_worker.WorkerState().started_at, None, 42, {'launches': 1})
    monkeypatch.setattr(pump_worker, 'Store', lambda url: store)
    monkeypatch.setattr(store, 'close', lambda: None)
    monkeypatch.setenv('DATABASE_URL', 'unused')
    assert pump_worker.cli(['--status']) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['last_slot'] == 42 and report['checkpoint'] == pump_worker.CHECKPOINT_NAME


# --- end to end: real worker loop, local WebSocket + JSON-RPC servers -----------------------

E2E = pump_worker.Settings(heartbeat_seconds=0.05, stall_seconds=5, backfill_concurrency=3,
                           fetch_attempts=4, fetch_backoff=(0.0,))


async def stop(task):
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def busy_program_script(creations):
    """300 buys, 3 creations and one failed transaction, interleaved."""
    buys = [notification(signature(n), BUY_LOGS) for n in range(300)]
    return (buys[:150] + [notification(creations[0], CREATE_LOGS)] + buys[150:250]
            + [notification(creations[1], CREATE_LOGS), notification(signature(9999), BUY_LOGS, err=FAILED)]
            + buys[250:] + [notification(creations[2], CREATE_LOGS)])


def run_busy_program(monkeypatch, store, beat_shows_all):
    """Run the worker over a busy program; stop once a heartbeat reflects all 3 launches."""
    creations = [signature(1000 + n) for n in range(3)]
    node = FakeNode()
    node.txs.update({sig: creation_tx(sig) for sig in creations})
    node.null_once.add(creations[1])             # RPC lag: the first lookup of one creation is empty
    node.listing = [{'signature': 'latest', 'err': None}]
    state = pump_worker.WorkerState()
    captured = {}

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream([busy_program_script(creations)]) as stream:
                monkeypatch.setattr(pump_worker, 'websocket_url', lambda url: stream.url)
                task = asyncio.create_task(pump_worker.watch_once(rpc.url, store, E2E, state))
                try:
                    await eventually(lambda: state.counters['notifications'] == 304
                                     and state.counters['launches'] == 3)
                    await eventually(beat_shows_all)       # the heartbeat itself reports the totals
                finally:
                    await stop(task)
                captured['requests'] = stream.requests
    asyncio.run(scenario())
    return creations, node, state, captured['requests']


def test_end_to_end_a_busy_program_costs_one_fetch_per_creation(monkeypatch):
    store = MemoryStore(cursor=None)
    creations, node, state, requests = run_busy_program(
        monkeypatch, store, lambda: store.beats[-1]['stats']['launches'] == 3)
    assert [r['params'][0]['mentions'] for r in requests] == [[PUMP_PROGRAM]]      # the default stream
    assert requests[0]['params'][1] == {'commitment': 'confirmed'}
    assert store.beats[-1]['stats']['filtered'] == 300 and store.beats[-1]['status'] == 'streaming'
    assert [launch.signature for launch in store.launches] == creations
    # 304 notifications arrived; only the 3 creations were fetched (+1 retry for the laggy one).
    assert node.count('getTransaction') == 4 and state.counters['fetch_retries'] == 1
    assert state.counters['filtered'] == 300 and state.counters['failed'] == 1
    assert not any(beat['status'] == 'reconnecting' for beat in store.beats)    # never dropped the stream
    assert store.cursor_value == creations[2]


def test_end_to_end_reconnect_replays_exactly_what_was_missed(monkeypatch):
    monkeypatch.setattr(pump_worker, 'BASE_BACKOFF_SECONDS', 0.05)
    first, second, third = signature(1), signature(2), signature(3)
    node = FakeNode()
    node.txs.update({sig: creation_tx(sig) for sig in (first, second, third)})
    node.listing = [{'signature': 'c0', 'err': None}]
    store = MemoryStore(cursor='c0')
    seen = {}

    async def scenario():
        gate = asyncio.Event()
        scripts = [[notification(first, CREATE_LOGS), gate, 'close'], []]
        with FakeRpcServer(node) as rpc:
            async with FakeStream(scripts) as stream:
                monkeypatch.setattr(pump_worker, 'websocket_url', lambda url: stream.url)
                task = asyncio.create_task(pump_worker.run(rpc.url, 'db', E2E, store=store))
                try:
                    await eventually(lambda: store.cursor_value == first)      # seen live
                    assert node.count('getTransaction') == 1
                    # Two more launches confirm (one more transaction failed on-chain) while the
                    # worker is about to lose its stream; then the server drops the connection.
                    node.listing[:0] = [{'signature': third, 'err': None},
                                        {'signature': 'failed-tx', 'err': FAILED},
                                        {'signature': second, 'err': None},
                                        {'signature': first, 'err': None}]
                    gate.set()
                    await eventually(lambda: len(store.launches) == 3 and stream.connections == 2
                                     and store.beats[-1]['status'] == 'streaming')
                finally:
                    await stop(task)
                seen['connections'] = stream.connections
    asyncio.run(scenario())
    assert [launch.signature for launch in store.launches] == [first, second, third]
    assert node.count('getTransaction') == 3        # each exactly once; the failed one never fetched
    assert seen['connections'] == 2
    assert 'reconnecting' in [beat['status'] for beat in store.beats]
    assert store.cursor_value == third


def test_end_to_end_on_real_postgres_uses_one_connection_for_everything(pg, monkeypatch):
    store, admin, schema = pg
    creations, node, state, requests = run_busy_program(
        monkeypatch, store,
        lambda: ((store.heartbeat_status() or {}).get('stats') or {}).get('launches') == 3)
    rows = [row[0] for row in store._run(lambda cur: (cur.execute(
        'SELECT signature FROM pump_launches ORDER BY slot, signature'), cur.fetchall())[1])]
    assert sorted(rows) == sorted(creations)
    assert store.cursor() == creations[2]
    beat = store.heartbeat_status()
    assert beat['status'] == 'streaming' and beat['stats']['launches'] == 3
    assert beat['stats']['filtered'] == 300 and beat['stats']['db_connects'] == 1
    # 304 notifications, several heartbeats, 3 launches and their cursor writes: still ONE connect.
    assert store.connects == 1


# --- the creations stream -------------------------------------------------------------------

CREATIONS = pump_worker.Settings(heartbeat_seconds=0.05, stall_seconds=5, backfill_concurrency=3,
                                 fetch_attempts=4, fetch_backoff=(0.0,), stream='creations')


@pytest.mark.parametrize('stream, address', [('program', PUMP_PROGRAM), ('creations', MINT_AUTHORITY)])
def test_the_first_start_cursor_comes_from_the_streams_own_history(monkeypatch, stream, address):
    seen = []
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: (
        seen.append(params[0]), [{'signature': 'latest'}])[1])
    install(monkeypatch, FakeFetch())
    store = MemoryStore(cursor=None)
    run_backfill(store, settings=pump_worker.Settings(heartbeat_seconds=600, stream=stream, fetch_backoff=(0,)))
    assert seen == [address] and store.saves == [('latest', None)]


def test_backfill_reads_the_creation_history_when_the_stream_is_creations(monkeypatch):
    seen = []
    sigs = [signature(n) for n in range(1, 4)]
    page = list(reversed([{'signature': sig, 'err': None} for sig in sigs])) + [{'signature': 'cursor', 'err': None}]
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: (seen.append(params[0]), page)[1])
    install(monkeypatch, FakeFetch({sigs[1]: creation_tx(sigs[1])}))
    store = MemoryStore(cursor='cursor')
    run_backfill(store, settings=pump_worker.Settings(heartbeat_seconds=600, stream='creations', fetch_backoff=(0,)))
    assert set(seen) == {MINT_AUTHORITY} and [launch.signature for launch in store.launches] == [sigs[1]]


def test_run_gives_each_stream_its_own_cursor_and_says_which_one_in_the_heartbeat(monkeypatch):
    made = []

    class RecordingStore(MemoryStore):
        def __init__(self, db_url, checkpoint=None):
            super().__init__()
            made.append((db_url, checkpoint, self))

    async def stop_now(*args):
        raise asyncio.CancelledError
    monkeypatch.setattr(pump_worker, 'Store', RecordingStore)
    monkeypatch.setattr(pump_worker, 'watch_once', stop_now)
    for stream in ('program', 'creations'):
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(pump_worker.run('url', 'postgresql://db', pump_worker.Settings(stream=stream)))
    assert [(url, checkpoint) for url, checkpoint, _ in made] == [
        ('postgresql://db', 'pump-fun-creations-v1'), ('postgresql://db', 'pump-fun-creations-v1-mint-authority')]
    assert [store.beats[-1]['stats']['stream'] for _, _, store in made] == ['program', 'creations']


def test_end_to_end_the_creations_stream_subscribes_to_and_backfills_from_the_mint_authority(monkeypatch):
    first, second = signature(1), signature(2)
    node = FakeNode()
    node.txs.update({sig: creation_tx(sig) for sig in (first, second)})
    # The mint authority's history holds launches only. One landed while the worker was away.
    node.listing = [{'signature': first, 'err': None}, {'signature': 'cursor', 'err': None}]
    store, state, captured = MemoryStore(cursor='cursor'), pump_worker.WorkerState(), {}

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream([[notification(second, CREATE_LOGS)]]) as stream:
                monkeypatch.setattr(pump_worker, 'websocket_url', lambda url: stream.url)
                task = asyncio.create_task(pump_worker.watch_once(rpc.url, store, CREATIONS, state))
                try:
                    await eventually(lambda: len(store.launches) == 2 and store.beats[-1]['stats']['launches'] == 2)
                finally:
                    await stop(task)
                captured['requests'] = stream.requests
    asyncio.run(scenario())
    assert [r['params'][0]['mentions'] for r in captured['requests']] == [[MINT_AUTHORITY]]
    assert {address for method, address in node.calls if method == 'getSignaturesForAddress'} == {MINT_AUTHORITY}
    assert [launch.signature for launch in store.launches] == [first, second]   # backfilled one, then the live one
    assert node.count('getTransaction') == 2
    assert store.beats[-1]['stats']['stream'] == 'creations'
