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
    program_signatures = rpc(url, 'getSignaturesForAddress',
                             [PUMP_PROGRAM, {'limit': limit, 'commitment': 'confirmed'}])
    for item in program_signatures:
        tx = fetch_transaction(url, item['signature'])
        launch = parse_launch(tx)
        if not launch:
            continue
        curve_signatures = rpc(url, 'getSignaturesForAddress',
                               [launch.pool, {'limit': 100, 'commitment': 'confirmed'}])
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
    raise ValueError('No creation in bounded program sample; retry later')


if __name__ == '__main__':
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        sys.exit('SOLANA_RPC_URL not configured')
    try:
        print(json.dumps(discover(url), indent=2))
    except (ValueError, KeyError, IndexError, OSError) as error:
        # Never include error details: transport errors may contain the private URL.
        print(f'Historical sample failed ({type(error).__name__}); no live claim.', file=sys.stderr)
        sys.exit(1)
