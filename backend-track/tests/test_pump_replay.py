import hashlib
import json
import pathlib

import pytest

from pump_replay import (BUY_LAYOUTS, DISCRIMINATORS, MINT_AUTHORITY, PUMP_PROGRAM, creation_log,
                         decode58, log_verdict, parse_buys, parse_launch)

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


# --- Decoder pinned to the official Pump IDL ------------------------------------------------
# tests/fixtures/pump_idl_subset.json snapshots the discriminators and ordered account names of
# the instructions the decoder understands (pump-fun/pump-public-docs idl/pump.json).
IDL = json.loads((pathlib.Path(__file__).parent / 'fixtures' / 'pump_idl_subset.json').read_text())
IDL_INSTRUCTIONS = IDL['instructions']
BUY_KINDS = ['buy', 'buy_exact_sol_in', 'buy_v2', 'buy_exact_quote_in_v2']


def idl_transaction(kind, slot=100, change=0, *, by_name=None):
    """Build a transaction whose single Pump instruction follows the IDL's account order.

    Every account position gets its own distinct key, so a wrong index in the decoder
    would surface as the wrong mint/pool/wallet rather than passing by coincidence.
    """
    names = IDL_INSTRUCTIONS[kind]['accounts']
    keys = [f'K{n:02d}'.ljust(32, '_') for n in range(len(names) + 1)] + [PUMP_PROGRAM]
    program_index = len(keys) - 1
    by_name = by_name or {}
    ix = {'programIdIndex': program_index, 'accounts': list(range(len(names))),
          'data': encode58(bytes(IDL_INSTRUCTIONS[kind]['discriminator']) + b'payload')}
    mint = keys[names.index('mint' if 'mint' in names else 'base_mint')]
    user = keys[names.index('user')]
    return {'slot': slot, 'transaction': {'signatures': ['s' * 64], 'message': {
                'header': {'numRequiredSignatures': len(names)}, 'accountKeys': keys,
                'instructions': [ix]}},
            'meta': {'err': None,
                     'preTokenBalances': [{'mint': mint, 'owner': user,
                                           'uiTokenAmount': {'amount': '100'}}],
                     'postTokenBalances': [{'mint': mint, 'owner': user,
                                            'uiTokenAmount': {'amount': str(100 + change)}}]}}


def test_decoder_tables_match_official_idl_snapshot():
    assert IDL['_program'] == PUMP_PROGRAM
    for kind, entry in IDL_INSTRUCTIONS.items():
        assert DISCRIMINATORS[kind] == bytes(entry['discriminator']), kind
    for kind in BUY_KINDS:
        names = IDL_INSTRUCTIONS[kind]['accounts']
        mint = names.index('mint' if 'mint' in names else 'base_mint')
        assert BUY_LAYOUTS[kind] == (mint, names.index('bonding_curve'), names.index('user')), kind
    assert set(BUY_LAYOUTS) == set(BUY_KINDS)


@pytest.mark.parametrize('kind', ['create', 'create_v2'])
def test_launch_positions_follow_the_idl_for_both_create_instructions(kind):
    names = IDL_INSTRUCTIONS[kind]['accounts']
    tx = idl_transaction(kind)
    keys = tx['transaction']['message']['accountKeys']
    launch = parse_launch(tx)
    assert launch is not None
    assert launch.mint == keys[names.index('mint')]
    assert launch.pool == keys[names.index('bonding_curve')]


@pytest.mark.parametrize('kind', BUY_KINDS)
def test_every_buy_instruction_is_decoded_to_the_signing_wallet(kind):
    launch_tx = idl_transaction('create_v2')
    launch = parse_launch(launch_tx)
    names = IDL_INSTRUCTIONS[kind]['accounts']
    mint_name = 'mint' if 'mint' in names else 'base_mint'
    # Same mint and bonding curve as the launch, wherever this instruction keeps them.
    tx = idl_transaction(kind, slot=103, change=45)
    keys = tx['transaction']['message']['accountKeys']
    keys[names.index(mint_name)] = launch.mint
    keys[names.index('bonding_curve')] = launch.pool
    user = keys[names.index('user')]
    tx['meta']['preTokenBalances'][0].update(mint=launch.mint, owner=user)
    tx['meta']['postTokenBalances'][0].update(mint=launch.mint, owner=user)
    buys = parse_buys(tx, launch)
    assert [(b.wallet, b.raw_amount, b.slot, b.verified) for b in buys] == [(user, 45, 103, True)]


@pytest.mark.parametrize('kind', BUY_KINDS)
def test_buy_variants_keep_every_safety_check(kind):
    launch = parse_launch(idl_transaction('create_v2'))
    names = IDL_INSTRUCTIONS[kind]['accounts']
    mint_name = 'mint' if 'mint' in names else 'base_mint'

    def build(**changes):
        tx = idl_transaction(kind, slot=changes.get('slot', 103), change=changes.get('change', 45))
        keys = tx['transaction']['message']['accountKeys']
        keys[names.index(mint_name)] = changes.get('mint', launch.mint)
        keys[names.index('bonding_curve')] = changes.get('pool', launch.pool)
        user = keys[names.index('user')]
        for key in ('preTokenBalances', 'postTokenBalances'):
            tx['meta'][key][0].update(mint=launch.mint, owner=user)
        if 'signers' in changes:
            tx['transaction']['message']['header']['numRequiredSignatures'] = changes['signers']
        return tx

    assert len(parse_buys(build(), launch)) == 1
    assert parse_buys(build(mint='W' * 32), launch) == []          # another token
    assert parse_buys(build(pool='W' * 32), launch) == []          # another bonding curve
    assert parse_buys(build(change=-5), launch) == []              # sold, not bought
    assert parse_buys(build(slot=launch.end_slot + 1), launch) == []   # outside the window
    assert parse_buys(build(signers=1), launch) == []              # wallet did not sign
    failed = build()
    failed['meta']['err'] = {'InstructionError': [0, 'Custom']}
    assert parse_buys(failed, launch) == []


def test_buy_v2_is_not_read_with_the_original_account_layout():
    """A v2 instruction whose accounts merely look like an original buy must not match."""
    launch = parse_launch(idl_transaction('create_v2'))
    tx = idl_transaction('buy_v2', slot=103, change=45)
    keys = tx['transaction']['message']['accountKeys']
    keys[2], keys[3], keys[6] = launch.mint, launch.pool, keys[13]   # original-layout positions
    assert parse_buys(tx, launch) == []


def test_bundled_create_and_buy_exact_sol_in_in_one_transaction():
    launch_tx = idl_transaction('create_v2')
    launch = parse_launch(launch_tx)
    buy_tx = idl_transaction('buy_exact_sol_in', slot=100, change=7)
    # Re-point the buy at the launch, then append it as a second Pump instruction.
    names = IDL_INSTRUCTIONS['buy_exact_sol_in']['accounts']
    keys = buy_tx['transaction']['message']['accountKeys']
    keys[names.index('mint')], keys[names.index('bonding_curve')] = launch.mint, launch.pool
    user = keys[names.index('user')]
    for key in ('preTokenBalances', 'postTokenBalances'):
        buy_tx['meta'][key][0].update(mint=launch.mint, owner=user)
    create_ix = launch_tx['transaction']['message']['instructions'][0]
    buy_tx['transaction']['message']['instructions'].insert(0, {
        **create_ix, 'accounts': [0] * len(create_ix['accounts'])})
    buys = parse_buys(buy_tx, launch)
    assert len(buys) == 1 and buys[0].order == 1 and buys[0].wallet == user


def test_log_prefilter_matches_only_create_and_createv2():
    assert creation_log(['Program log: Instruction: Create'])
    assert creation_log(['x', '  Program log: Instruction: CreateV2  '])
    assert not creation_log(['Program log: Instruction: CreateTokenAccount'])
    assert not creation_log(['Program log: Instruction: Buy'])
    assert not creation_log(None) and not creation_log([])


@pytest.mark.parametrize('logs, verdict', [
    (['Program log: Instruction: Buy', 'Program 6EF8 success'], 'other'),
    (['Program log: Instruction: Sell'], 'other'),
    (['Program log: Instruction: Create'], 'creation'),
    (['Program log: Instruction: CreateV2', 'Log truncated'], 'creation'),
    (['Program log: Instruction: Buy', 'Log truncated'], 'unknown'),   # the create line may be cut off
    (None, 'unknown'),                                                  # logs absent
    ([], 'unknown'),                                                    # a successful tx always logs
    ('not-a-list', 'unknown'),
])
def test_log_verdict_never_rules_out_a_creation_it_cannot_see(logs, verdict):
    assert log_verdict(logs) == verdict


# --- The mint authority: the one address only launch transactions mention ------------------------

P = 2 ** 255 - 19
D = -121665 * pow(121666, P - 2, P) % P


def on_ed25519_curve(raw):
    """True when 32 bytes decode to an ed25519 point (a PDA must NOT be one)."""
    y = int.from_bytes(raw, 'little') & ((1 << 255) - 1)
    u, v = (y * y - 1) % P, (D * y * y + 1) % P
    x_squared = u * pow(v, P - 2, P) % P
    return x_squared == 0 or pow(x_squared, (P - 1) // 2, P) == 1


def derive_pda(seeds, program):
    """Solana's find_program_address, in pure Python."""
    number = 0
    for char in program:
        number = number * 58 + ALPHABET.index(char)
    program_bytes = number.to_bytes(32, 'big')
    for bump in range(255, -1, -1):
        digest = hashlib.sha256(b''.join(seeds) + bytes([bump]) + program_bytes
                                + b'ProgramDerivedAddress').digest()
        if not on_ed25519_curve(digest):
            return encode58(digest)
    raise AssertionError('no valid bump')


def test_mint_authority_constant_is_the_pda_of_its_seed():
    # A typo in the constant would make a creations-only stream listen to an address nobody uses.
    assert derive_pda([b'mint-authority'], PUMP_PROGRAM) == MINT_AUTHORITY


def test_the_pda_helper_rejects_a_wrong_seed_or_program():
    assert derive_pda([b'mint-authority '], PUMP_PROGRAM) != MINT_AUTHORITY
    assert derive_pda([b'mint-authority'], 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL') != MINT_AUTHORITY


def test_only_launch_instructions_carry_the_mint_authority():
    # Computed over every instruction of the full IDL when the fixture was cut.
    assert IDL['mint_authority_accounts'] == {'create': 1, 'create_v2': 1}
    for kind in ('create', 'create_v2'):
        assert IDL_INSTRUCTIONS[kind]['accounts'][1] == 'mint_authority'
    for kind in BUY_KINDS:
        assert 'mint_authority' not in IDL_INSTRUCTIONS[kind]['accounts']
