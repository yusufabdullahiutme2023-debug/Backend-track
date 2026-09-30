"""Read-only Pump.fun launch watcher; records validated creations, never trades.

Run as a dedicated persistent worker. For a production deployment the heartbeat,
provider quota and missed-event metrics must be monitored before enabling alerts.
"""
import asyncio
import json
import logging
import os
import time
from urllib.parse import urlsplit, urlunsplit

import psycopg2
from psycopg2.extras import Json
import websockets

from pump_discover import rpc
from pump_replay import PUMP_PROGRAM, fetch_transaction, parse_launch

log = logging.getLogger('pump-worker')
CHECKPOINT_NAME = 'pump-fun-creations-v1'


def websocket_url(http_url):
    parsed = urlsplit(http_url)
    if parsed.scheme != 'https' or parsed.hostname not in (
            'mainnet.helius-rpc.com', 'atlas-mainnet.helius-rpc.com'):
        raise ValueError('Expected an HTTPS Helius Mainnet RPC endpoint')
    return urlunsplit(('wss', parsed.netloc, parsed.path, parsed.query, ''))


def prepare_database(db_url):
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS pump_launches (
            signature TEXT PRIMARY KEY, mint TEXT NOT NULL, pool TEXT NOT NULL,
            slot BIGINT NOT NULL, detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            evidence JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'unscored')''')
        cur.execute('''CREATE TABLE IF NOT EXISTS pump_worker_cursor (
            name TEXT PRIMARY KEY, signature TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())''')


def cursor_value(db_url):
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute('SELECT signature FROM pump_worker_cursor WHERE name=%s', (CHECKPOINT_NAME,))
        row = cur.fetchone()
        return row[0] if row else None


def save_event(db_url, signature, launch):
    # Event + cursor must commit in the SAME transaction. Duplicate notifications
    # are safe; a crash before commit causes a replay instead of silent loss.
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        if launch is not None:
            cur.execute('''INSERT INTO pump_launches (signature,mint,pool,slot,evidence)
                           VALUES (%s,%s,%s,%s,%s) ON CONFLICT (signature) DO NOTHING''',
                        (launch.signature, launch.mint, launch.pool, launch.slot,
                         Json(launch.model_dump())))
        cur.execute('''INSERT INTO pump_worker_cursor (name, signature) VALUES (%s,%s)
                       ON CONFLICT (name) DO UPDATE SET signature=EXCLUDED.signature,
                       updated_at=now()''', (CHECKPOINT_NAME, signature))


def missed_signatures(http_url, previous, max_pages=10):
    """Return signatures oldest-first since cursor; fail closed if backlog is too big."""
    before = None
    found = []
    for _ in range(max_pages):
        opts = {'limit': 1000, 'commitment': 'confirmed'}
        if before:
            opts['before'] = before
        page = rpc(http_url, 'getSignaturesForAddress', [PUMP_PROGRAM, opts])
        for item in page:
            if item['signature'] == previous:
                return list(reversed(found))
            found.append(item['signature'])
        if not page or len(page) < 1000:
            # Provider may have truncated history; don't silently pretend no gap.
            raise RuntimeError('Persisted cursor missing from available program history')
        before = page[-1]['signature']
    raise RuntimeError('Missed more than 10,000 Pump transactions; manual backfill required')


async def process(http_url, db_url, signature):
    try:
        tx = await asyncio.to_thread(fetch_transaction, http_url, signature)
    except ValueError:
        # Confirmed tx not yet indexed: do NOT advance durable cursor.
        raise RuntimeError('Transaction not yet available; retry after reconnect') from None
    launch = parse_launch(tx)
    await asyncio.to_thread(save_event, db_url, signature, launch)
    if launch:
        log.info('Verified creation signature=%s mint=%s slot=%s',
                 signature, launch.mint, launch.slot)


async def watch_once(http_url, db_url):
    # Subscribe FIRST, then backfill. This closes the gap between querying the
    # checkpoint and establishing the stream; duplicates are idempotent.
    async with websockets.connect(websocket_url(http_url), ping_interval=30,
                                  ping_timeout=30, max_queue=1000) as ws:
        await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'logsSubscribe',
                                  'params': [{'mentions': [PUMP_PROGRAM]},
                                             {'commitment': 'confirmed'}]}))
        reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
        if reply.get('error') or not isinstance(reply.get('result'), int):
            raise RuntimeError('WebSocket subscription rejected')
        log.info('Pump.fun subscription established')
        previous = await asyncio.to_thread(cursor_value, db_url)
        if previous:
            missed = await asyncio.to_thread(missed_signatures, http_url, previous)
            for signature in missed:
                await process(http_url, db_url, signature)
        else:
            # First-ever start: establish a documented "monitor from now" cursor.
            latest = await asyncio.to_thread(rpc, http_url, 'getSignaturesForAddress',
                                             [PUMP_PROGRAM, {'limit': 1}])
            if latest:
                await asyncio.to_thread(save_event, db_url, latest[0]['signature'], None)
        while True:
            event = json.loads(await ws.recv())
            if event.get('method') != 'logsNotification':
                continue
            value = event['params']['result']['value']
            if value.get('err') is not None:
                continue
            signature = value['signature']
            # Never process out of order across disconnects: backlog is replayed
            # before this message loop starts.
            await process(http_url, db_url, signature)


async def main():
    http_url = os.environ['SOLANA_RPC_URL']
    db_url = os.environ['DATABASE_URL']
    websocket_url(http_url)  # validate before connecting/logging
    prepare_database(db_url)
    while True:
        try:
            await watch_once(http_url, db_url)
        except (Exception,):
            # Never log exception text: transport errors can include API keys.
            log.exception('Watcher interrupted (exception text may contain secret)', exc_info=False)
            await asyncio.sleep(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
