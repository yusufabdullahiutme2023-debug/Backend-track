"""Bounded, read-only sample of Pump.fun creations for GitHub Actions.

This is NOT continuous monitoring. It never places trades or marks buyer coverage
complete; failures and empty samples are reported as such.
"""
import argparse
import asyncio
import json
import os
import sys
import time

import websockets
from pump_replay import PUMP_PROGRAM, creation_log, fetch_transaction, parse_launch
from pump_worker import websocket_url


async def sample(http_url, seconds=60, max_candidates=8):
    end = time.monotonic() + seconds
    report = {'mode': 'bounded_sample', 'monitor_seconds': seconds,
              'candidates': 0, 'validated_launches': [],
              'unavailable_transactions': 0, 'buyers_complete': False,
              'note': 'Sampling is not continuous and must not be used as a trading alert.'}
    async with websockets.connect(websocket_url(http_url), ping_interval=20,
                                  ping_timeout=20, max_queue=500) as ws:
        await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'logsSubscribe',
                                  'params': [{'mentions': [PUMP_PROGRAM]},
                                             {'commitment': 'confirmed'}]}))
        ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=12))
        if ack.get('error') or not isinstance(ack.get('result'), int):
            raise RuntimeError('Provider rejected logs subscription')
        seen = set()
        while time.monotonic() < end and report['candidates'] < max_candidates:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=min(5, max(.1, end-time.monotonic()))))
            except asyncio.TimeoutError:
                continue
            if msg.get('method') != 'logsNotification':
                continue
            value = msg['params']['result']['value']
            if value.get('err') is not None or not creation_log(value.get('logs', [])):
                continue
            sig = value['signature']
            if sig in seen:
                continue
            seen.add(sig)
            report['candidates'] += 1
            # Logs are only a cheap prefilter. The actual proof is the RPC
            # transaction's Pump ID, instruction discriminator and signer checks.
            try:
                launch = parse_launch(await asyncio.to_thread(fetch_transaction, http_url, sig))
            except (ValueError, KeyError, IndexError):
                report['unavailable_transactions'] += 1
                continue
            if launch:
                report['validated_launches'].append(launch.model_dump())
    return report


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='pump-report.json')
    parser.add_argument('--seconds', type=int, default=60)
    args = parser.parse_args()
    if not 10 <= args.seconds <= 90:
        parser.error('seconds must be between 10 and 90')
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        parser.error('SOLANA_RPC_URL must be a private Helius Mainnet HTTPS URL')
    try:
        report = await sample(url, args.seconds)
    except Exception:
        # urllib and websocket errors can contain a URL with an embedded key.
        print('Sample could not complete. Check provider quota and connectivity in your dashboard.',
              file=sys.stderr)
        return 1
    with open(args.output, 'w') as file:
        json.dump(report, file, indent=2)
    print('Validated launches:', len(report['validated_launches']),
          'candidates:', report['candidates'],
          'unavailable:', report['unavailable_transactions'])
    return 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
