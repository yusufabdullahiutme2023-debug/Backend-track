"""Bounded read-only Mainnet sampling; never submits transactions.

Find a recent Pump.fun creation from program history, then inspect its bonding
curve's early signatures. A sampled launch is not complete buyer coverage.
"""
import json
import os
import sys
import urllib.request
from pump_replay import PUMP_PROGRAM, fetch_transaction, parse_launch, parse_buys


def rpc(url, method, params):
    request = urllib.request.Request(url, json.dumps({
        'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params,
    }).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=20) as response:
        body = json.load(response)
    if body.get('error') or body.get('result') is None:
        raise ValueError('RPC returned an error or no result')
    return body['result']


def discover(url, limit=40):
    try:
        program_signatures = rpc(url, 'getSignaturesForAddress',
                                 [PUMP_PROGRAM, {'limit': limit, 'commitment': 'confirmed'}])
    except ValueError as exc:
        raise ValueError('program signature lookup failed') from exc
    print(f'Program signatures sampled: {len(program_signatures)}', file=sys.stderr)
    unavailable = 0
    for item in program_signatures:
        try:
            tx = fetch_transaction(url, item['signature'])
        except ValueError:
            unavailable += 1
            continue
        launch = parse_launch(tx)
        if not launch:
            continue
        try:
            curve_signatures = rpc(url, 'getSignaturesForAddress',
                                   [launch.pool, {'limit': 100, 'commitment': 'confirmed'}])
        except ValueError as exc:
            raise ValueError('bonding curve signature lookup failed') from exc
        early = [s for s in curve_signatures if launch.slot <= s['slot'] <= launch.end_slot]
        early.sort(key=lambda s: s['slot'])
        buys = []
        for candidate in early[:25]:
            buys.extend(parse_buys(fetch_transaction(url, candidate['signature']), launch))
        return {
            'launch': launch.model_dump(),
            'sampled_curve_signatures': len(early),
            'sampled_verified_buys': [b.model_dump() for b in buys],
            'buyers_complete': False,  # bounded RPC sampling is not exhaustive
        }
    raise ValueError(f'No creation in {len(program_signatures)} sampled signatures; '
                     f'{unavailable} transactions unavailable')


if __name__ == '__main__':
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        sys.exit('SOLANA_RPC_URL not configured')
    try:
        print(json.dumps(discover(url), indent=2))
    except (ValueError, KeyError, IndexError, OSError) as error:
        # Only our own explicitly generated messages are safe to print. Transport
        # exceptions and RPC payloads might contain the private provider URL.
        safe = str(error) if isinstance(error, ValueError) and str(error).startswith(
            ('program signature lookup failed', 'bonding curve signature lookup failed',
             'No creation in ')) else type(error).__name__
        print(f'Historical sample failed: {safe}; no live claim.', file=sys.stderr)
        sys.exit(1)
