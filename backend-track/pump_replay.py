"""Conservative Pump.fun transaction replay; read-only Solana JSON-RPC.

Use historical getTransaction responses (encoding=json, maxSupportedTransactionVersion=0).
Only explicit program instructions are accepted; an arbitrary positive token
balance or fee payer alone is insufficient evidence of a buy.
"""
import argparse
import json
import os
import sys
import urllib.request

from solana_signals import Buy, Launch

PUMP_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
DISCRIMINATORS = {
    'create': bytes([24, 30, 200, 40, 5, 28, 7, 119]),
    'create_v2': bytes([214, 144, 76, 236, 95, 139, 49, 180]),
    'buy': bytes([102, 6, 61, 18, 1, 218, 235, 234]),
}
_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def decode58(value: str) -> bytes:
    number = 0
    for char in value:
        number = number * 58 + _ALPHABET.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, 'big') if number else b''
    return b'\x00' * (len(value) - len(value.lstrip('1'))) + raw


def _parts(tx: dict):
    if not tx or tx.get('meta', {}).get('err') is not None:
        return None
    message = tx['transaction']['message']
    keys = message['accountKeys']
    keys = [k['pubkey'] if isinstance(k, dict) else k for k in keys]
    loaded = tx['meta'].get('loadedAddresses') or {}
    keys += loaded.get('writable', []) + loaded.get('readonly', [])
    signatures = tx['transaction'].get('signatures') or []
    if not signatures:
        return None
    signed = set(keys[:message['header']['numRequiredSignatures']])
    return message, keys, signatures[0], signed


def _instructions(tx: dict):
    message, keys, _, _ = _parts(tx)
    for ix in message['instructions']:
        if keys[ix['programIdIndex']] != PUMP_PROGRAM:
            continue
        try:
            data = decode58(ix['data'])
            accounts = [keys[n] for n in ix['accounts']]
        except (IndexError, KeyError, ValueError, TypeError):
            continue
        for kind, prefix in DISCRIMINATORS.items():
            if data.startswith(prefix):
                yield kind, accounts
                break


def parse_launch(tx: dict) -> Launch | None:
    parts = _parts(tx)
    if parts is None:
        return None
    _, _, signature, signed = parts
    for kind, accounts in _instructions(tx):
        # Pump official IDL: mint=0, bonding_curve=2, user=7 (create) or 5 (v2).
        user_index = 7 if kind == 'create' else 5
        if kind not in ('create', 'create_v2') or len(accounts) <= user_index:
            continue
        if accounts[0] not in signed or accounts[user_index] not in signed:
            continue
        return Launch(mint=accounts[0], pool=accounts[2], signature=signature,
                      venue='pump.fun', slot=tx['slot'], end_slot=tx['slot'] + 10)
    return None


def _balances(tx: dict, key: str, mint: str, owner: str) -> int:
    # Sum raw token balances across all token accounts owned by the signer.
    result = 0
    for b in tx['meta'].get(key) or []:
        if b.get('mint') == mint and b.get('owner') == owner:
            result += int(b['uiTokenAmount']['amount'])
    return result


def parse_buys(tx: dict, launch: Launch) -> list[Buy]:
    parts = _parts(tx)
    if parts is None or not (launch.slot <= tx['slot'] <= launch.end_slot):
        return []
    _, _, signature, signed = parts
    found = []
    # Official IDL buy: mint=2, bonding_curve=3, user=6.
    for order, (kind, accounts) in enumerate(_instructions(tx)):
        if kind != 'buy' or len(accounts) < 7:
            continue
        if accounts[2] != launch.mint or accounts[3] != launch.pool:
            continue
        owner = accounts[6]
        if owner not in signed:
            continue
        change = _balances(tx, 'postTokenBalances', launch.mint, owner) - _balances(
            tx, 'preTokenBalances', launch.mint, owner)
        if change > 0:
            found.append(Buy(wallet=owner, signature=signature, slot=tx['slot'],
                             order=order, raw_amount=change, verified=True))
    return found


def fetch_transaction(rpc_url: str, signature: str) -> dict:
    request = urllib.request.Request(rpc_url, data=json.dumps({
        'jsonrpc': '2.0', 'id': 1, 'method': 'getTransaction',
        'params': [signature, {'encoding': 'json', 'commitment': 'confirmed',
                               'maxSupportedTransactionVersion': 0}],
    }).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=20) as response:
        data = json.load(response)
    if 'error' in data:
        raise ValueError('RPC error: ' + str(data['error']))
    if data.get('result') is None:
        raise ValueError('Transaction not available from this RPC')
    return data['result']


def main():
    parser = argparse.ArgumentParser(description='Replay a Pump.fun creation and buy transaction')
    parser.add_argument('launch_signature')
    parser.add_argument('buy_signatures', nargs='*')
    args = parser.parse_args()
    rpc_url = os.environ.get('SOLANA_RPC_URL')
    if not rpc_url:
        parser.error('Set SOLANA_RPC_URL privately; do not paste an API key into the command.')
    launch = parse_launch(fetch_transaction(rpc_url, args.launch_signature))
    if launch is None:
        sys.exit('No verified Pump.fun create/create_v2 instruction in launch transaction')
    buys = [b.model_dump() for sig in args.buy_signatures
            for b in parse_buys(fetch_transaction(rpc_url, sig), launch)]
    print(json.dumps({'launch': launch.model_dump(), 'verified_buys': buys}, indent=2))
    if args.buy_signatures and not buys:
        sys.exit('No verified buy found in supplied transaction(s) for this launch')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, IndexError, OSError) as exc:
        # urllib exceptions may embed the RPC URL and thus its API key.
        # Never print network exception details in a hosted CI log.
        print(f'Replay failed ({type(exc).__name__}). Check RPC access and signatures.',
              file=sys.stderr)
        sys.exit(1)
