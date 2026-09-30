from pump_replay import PUMP_PROGRAM, DISCRIMINATORS, decode58, parse_launch, parse_buys

ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

def encode58(data):
    n = int.from_bytes(data, 'big')
    s = ''
    while n:
        n, rem = divmod(n, 58)
        s = ALPHABET[rem] + s
    return '1' * (len(data) - len(data.lstrip(b'\0'))) + s


def transaction(kind='create_v2', slot=100, change=0, failed=False):
    keys = ['M'*32, 'C'*32, 'P'*32, 'A'*32, 'G'*32, 'U'*32,
            'X'*32, 'Y'*32, PUMP_PROGRAM]
    accounts = [0, 3, 2, 4, 5, 6] if kind == 'create_v2' else [4, 5, 0, 2, 3, 7, 6]
    ix = {'programIdIndex': 8, 'accounts': accounts,
          'data': encode58(DISCRIMINATORS[kind] + b'payload')}
    return {'slot': slot, 'transaction': {'signatures': ['s'*64],
            'message': {'header': {'numRequiredSignatures': 7}, 'accountKeys': keys,
                        'instructions': [ix]}},
            'meta': {'err': 'failure' if failed else None,
                     'preTokenBalances': [{'mint': 'M'*32, 'owner': 'X'*32,
                                           'uiTokenAmount': {'amount': '100'}}],
                     'postTokenBalances': [{'mint': 'M'*32, 'owner': 'X'*32,
                                            'uiTokenAmount': {'amount': str(100 + change)}}]}}


def test_discriminators_match_anchor_hash():
    import hashlib
    for name, discriminator in DISCRIMINATORS.items():
        assert hashlib.sha256(('global:' + name).encode()).digest()[:8] == discriminator
        assert decode58(encode58(discriminator + b'payload')) == discriminator + b'payload'


def test_v2_launch_detected_from_signed_mint_and_user():
    launch = parse_launch(transaction())
    assert (launch.mint, launch.pool, launch.slot) == ('M'*32, 'P'*32, 100)
    assert parse_launch(transaction(failed=True)) is None


def test_wrong_program_and_unsigned_mint_rejected():
    tx = transaction()
    tx['transaction']['message']['header']['numRequiredSignatures'] = 0
    assert parse_launch(tx) is None
    tx = transaction()
    tx['transaction']['message']['accountKeys'][8] = 'Q'*32
    assert parse_launch(tx) is None


def test_buy_requires_positive_delta_correct_pool_and_signer():
    launch = parse_launch(transaction())
    tx = transaction('buy', slot=103, change=45)
    assert parse_buys(tx, launch)[0].raw_amount == 45
    assert parse_buys(transaction('buy', slot=103, change=-10), launch) == []
    assert parse_buys(transaction('buy', slot=120, change=45), launch) == []
    tx['transaction']['message']['header']['numRequiredSignatures'] = 2
    assert parse_buys(tx, launch) == []
    tx = transaction('buy', slot=103, change=45)
    tx['transaction']['message']['instructions'][0]['accounts'][3] = 4
    assert parse_buys(tx, launch) == []
