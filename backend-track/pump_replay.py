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
# Pump's mint-authority PDA: seeds [b'mint-authority'] under PUMP_PROGRAM. The official IDL lists it as
# account 1 of `create` and `create_v2` and of no other instruction, so every launch transaction
# mentions it and ordinary trades do not. A test re-derives the address from the seed.
MINT_AUTHORITY = 'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM'
# Discriminators and account positions come from the official Pump IDL
# (pump-fun/pump-public-docs, idl/pump.json). tests/fixtures/pump_idl_subset.json
# snapshots exactly these entries and a test pins this table to it, so a typo here
# cannot silently attribute a buy to the wrong wallet.
DISCRIMINATORS = {
    'create': bytes([24, 30, 200, 40, 5, 28, 7, 119]),
    'create_v2': bytes([214, 144, 76, 236, 95, 139, 49, 180]),
    'buy': bytes([102, 6, 61, 18, 1, 218, 235, 234]),
    # Newer buy instructions. On mainnet many curve buys use buy_exact_sol_in
    # rather than the original buy; the *_v2 pair carries an explicit quote mint.
    'buy_exact_sol_in': bytes([56, 252, 116, 8, 158, 223, 205, 95]),
    'buy_v2': bytes([184, 23, 238, 97, 103, 197, 211, 61]),
    'buy_exact_quote_in_v2': bytes([194, 171, 28, 70, 104, 77, 91, 47]),
}
# (mint, bonding_curve, user) account positions of every buy instruction we decode.
# The original pair shares one layout and the v2 pair another.
BUY_LAYOUTS = {
    'buy': (2, 3, 6),
    'buy_exact_sol_in': (2, 3, 6),
    'buy_v2': (1, 10, 13),
    'buy_exact_quote_in_v2': (1, 10, 13),
}
CREATION_LOG_LINES = ('Program log: Instruction: Create', 'Program log: Instruction: CreateV2')
_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'


def decode58(value: str) -> bytes:
    number = 0
    for char in value:
        number = number * 58 + _ALPHABET.index(char)
    raw = number.to_bytes((number.bit_length() + 7) // 8, 'big') if number else b''
    return b'\x00' * (len(value) - len(value.lstrip('1'))) + raw


def creation_log(logs) -> bool:
    """Cheap prefilter: does a logsNotification carry a Pump create/create_v2 log line?

    Logs are only a hint. The proof of a launch is always the decoded transaction.
    """
    return any(isinstance(line, str) and line.strip() in CREATION_LOG_LINES
               for line in logs or [])


def log_verdict(logs) -> str:
    """Decide from a logsNotification alone whether a transaction must be fetched.

    ``creation``  a Pump create/create_v2 line is present: fetch and verify.
    ``unknown``   logs are missing, empty, malformed or truncated, so a creation cannot
                  be ruled out: fetch and verify rather than risk a silent miss.
    ``other``     complete logs with no creation line: safe to skip without a fetch.
    """
    if not isinstance(logs, list) or not logs:
        return 'unknown'
    if creation_log(logs):
        return 'creation'
    if any(isinstance(line, str) and line.strip() == 'Log truncated' for line in logs):
        return 'unknown'
    return 'other'


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
    # Account positions per buy instruction come from the official IDL (BUY_LAYOUTS).
    # The mint, pool, signer and positive-token-delta checks below all have to pass,
    # so a layout drift fails closed (no buy) rather than naming the wrong wallet.
    for order, (kind, accounts) in enumerate(_instructions(tx)):
        layout = BUY_LAYOUTS.get(kind)
        if layout is None or len(accounts) <= max(layout):
            continue
        mint_index, pool_index, user_index = layout
        if accounts[mint_index] != launch.mint or accounts[pool_index] != launch.pool:
            continue
        owner = accounts[user_index]
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
