"""Offline tests for bounded early-buyer evidence and honest coverage claims.

No network, no RPC, no transactions: every provider call is faked. These tests
exist to make the honesty rules executable rather than aspirational.
"""
import pytest

import pump_evidence
import pump_history
from pump_evidence import (FIRST_N_BUYERS, annotation_lines, assert_no_unproven_first_n_claim,
                           collect_evidence, coverage_proven, early_buyer_claim, finalize,
                           find_launch, redact)
from solana_signals import Launch

SECRET_URL = 'https://mainnet.helius-rpc.com/?api-key=SUPERSECRETKEY'


def launch(slot=300_000_000):
    return Launch(mint='M' * 32, pool='P' * 32, signature='s' * 64,
                  venue='pump.fun', slot=slot, end_slot=slot + 10)


def buy(wallet='W' * 32, slot=300_000_001, order=0, raw_amount=1000):
    return {'wallet': wallet, 'signature': 'b' * 64, 'slot': slot, 'order': order,
            'raw_amount': raw_amount, 'verified': True}


def sample(reached_launch_slot=True, unavailable=0, seen=2, attempted=2,
           independent=False, buys=None):
    return {'buys': buys if buys is not None else [buy()], 'pages_scanned': 2,
            'early_signatures_seen': seen, 'transactions_attempted': attempted,
            'unavailable_transactions': unavailable,
            'reached_launch_slot': reached_launch_slot,
            'independent_block_verified': independent, 'buyers_complete': False}


# ---------------------------------------------------------------- discovery

def test_find_launch_reports_skipped_and_unavailable_candidates(monkeypatch):
    page = [{'signature': 'notcreate', 'err': None}, {'signature': 'other', 'err': None},
            {'signature': 'gone', 'err': None}, {'signature': 'good', 'err': None},
            {'signature': 'failed', 'err': {'InstructionError': 0}}]
    monkeypatch.setattr(pump_evidence, 'rpc', lambda url, method, params: page)
    fetched = []

    def fetch(url, signature):
        fetched.append(signature)
        if signature == 'gone':
            raise ValueError('Transaction not available from this RPC')
        return {'signature': signature}

    monkeypatch.setattr(pump_evidence, 'fetch_transaction', fetch)
    monkeypatch.setattr(pump_evidence, 'parse_launch',
                        lambda tx: launch() if tx.get('signature') == 'good' else None)
    found, diagnostics = find_launch(SECRET_URL)
    assert found is not None and found.mint == 'M' * 32
    assert diagnostics['unavailable_transactions'] == 1
    assert diagnostics['non_creation_signatures'] == 2
    assert diagnostics['transactions_fetched'] == 4
    assert diagnostics['program_signatures_seen'] == 5
    # An errored signature is not evidence of a creation and must not be fetched.
    assert 'failed' not in fetched


def test_find_launch_returns_none_instead_of_inventing_a_launch(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'rpc',
                        lambda url, method, params: [{'signature': 'a', 'err': None}])
    monkeypatch.setattr(pump_evidence, 'fetch_transaction', lambda url, sig: {})
    monkeypatch.setattr(pump_evidence, 'parse_launch', lambda tx: None)
    found, diagnostics = find_launch(SECRET_URL)
    assert found is None
    assert diagnostics['transactions_fetched'] == 1


def test_find_launch_bounds_the_number_of_transaction_fetches(monkeypatch):
    calls = []
    monkeypatch.setattr(pump_evidence, 'rpc', lambda url, method, params:
                        [{'signature': f's{i}', 'err': None} for i in range(40)])
    monkeypatch.setattr(pump_evidence, 'fetch_transaction',
                        lambda url, sig: calls.append(sig) or {})
    monkeypatch.setattr(pump_evidence, 'parse_launch', lambda tx: None)
    found, diagnostics = find_launch(SECRET_URL, max_fetches=3)
    assert found is None
    assert len(calls) == 3
    assert diagnostics['transactions_fetched'] == 3


# ---------------------------------------------------------------- coverage

def test_perfect_pagination_alone_never_proves_coverage():
    complete = sample(reached_launch_slot=True, unavailable=0, seen=2, attempted=2)
    assert coverage_proven(complete, transactions_fetched=2) is False
    # The missing ingredient is an attestation from outside this RPC path.
    assert coverage_proven(dict(complete, independent_block_verified=True),
                           transactions_fetched=2) is True


def test_coverage_proven_requires_every_in_window_transaction_decoded():
    perfect = sample(independent=True)
    assert coverage_proven(perfect, transactions_fetched=2) is True
    assert coverage_proven(sample(independent=True, unavailable=1), transactions_fetched=1) is False
    assert coverage_proven(sample(independent=True, seen=9, attempted=2),
                           transactions_fetched=2) is False
    assert coverage_proven(sample(independent=True, reached_launch_slot=False),
                           transactions_fetched=2) is False


def test_claim_refuses_a_first_n_wording_without_proof(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys', lambda *a, **kw: sample())
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert str(FIRST_N_BUYERS) in report['claim']
    assert 'NOT the first' in report['claim']
    assert report['coverage']['coverage_proven'] is False
    assert report['coverage']['buyers_complete'] is False
    assert 'independent block data' in report['claim']


def test_claim_reports_missing_data_when_transactions_are_unavailable(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(unavailable=1, seen=3, attempted=3))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['unavailable_transactions'] == 1
    assert report['coverage']['transactions_fetched'] == 2
    assert '1 in-window transaction(s) unavailable from RPC' in report['claim']
    assert report['coverage']['transactions_attempted'] == 3
    # Everything seen was attempted, so no truncation message is expected here.
    assert 'signatures were decoded' not in report['claim']


def test_claim_reports_truncation_when_the_decode_budget_is_hit(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(seen=150, attempted=100))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['unavailable_transactions'] == 0
    assert 'only 100 of 150 in-window signatures were decoded' in report['claim']


def test_claim_flags_pagination_that_never_reached_the_launch_slot(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(reached_launch_slot=False))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['reached_launch_slot'] is False
    assert 'pagination never reached the launch slot' in report['claim']


def test_claim_wording_changes_only_when_coverage_is_proven(monkeypatch):
    proven = sample(independent=True, buys=[buy(), buy(wallet='V' * 32, order=1)])
    monkeypatch.setattr(pump_evidence, 'collect_early_buys', lambda *a, **kw: proven)
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['coverage_proven'] is True
    assert 'coverage independently verified' in report['claim']
    assert 'NOT the first' not in report['claim']
    assert report['distinct_wallet_count'] == 2


def test_unproven_affirmative_first_n_claim_is_rejected():
    report = {'claim': f'these are the first {FIRST_N_BUYERS} buyers of the launch',
              'coverage': {'coverage_proven': False}}
    with pytest.raises(AssertionError):
        assert_no_unproven_first_n_claim(report)
    # The same wording is fine once coverage is genuinely proven.
    report['coverage']['coverage_proven'] = True
    assert_no_unproven_first_n_claim(report) is None


# ------------------------------------------------- real accounting plumbing

def test_transactions_attempted_flows_through_real_collect_early_buys(monkeypatch):
    """Exercises pump_history itself so the accounting field is real, not stubbed."""
    from types import SimpleNamespace
    pages = [[{'slot': 120, 'signature': 'latest', 'err': None},
              {'slot': 106, 'signature': 'one', 'err': None},
              {'slot': 103, 'signature': 'two', 'err': None},
              {'slot': 99, 'signature': 'older', 'err': None}]]
    monkeypatch.setattr(pump_history, 'rpc', lambda url, method, args: pages[0])

    def fetch(url, signature):
        if signature == 'one':
            raise ValueError('Transaction not available from this RPC')
        return {'signature': signature}

    monkeypatch.setattr(pump_history, 'fetch_transaction', fetch)
    monkeypatch.setattr(pump_history, 'parse_buys', lambda tx, lch: [
        SimpleNamespace(model_dump=lambda sig=tx['signature']: {'wallet': 'W' * 32,
                                                                'signature': sig,
                                                                'slot': 103, 'order': 0,
                                                                'raw_amount': 5,
                                                                'verified': True})])
    result = pump_history.collect_early_buys('private', SimpleNamespace(
        pool='curve', slot=100, end_slot=110), page_size=10)
    # Slots 120 and 99 sit outside the 100..110 window, so only two are decoded.
    assert result['transactions_attempted'] == 2
    assert result['early_signatures_seen'] == 2
    assert result['unavailable_transactions'] == 1
    assert [b['signature'] for b in result['buys']] == ['two']
    assert result['reached_launch_slot'] is True
    assert result['independent_block_verified'] is False
    assert result['buyers_complete'] is False


# ---------------------------------------------------------------- annotations

def test_annotations_carry_launch_and_buy_evidence(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[buy(), buy(wallet='V' * 32, slot=300_000_002,
                                                                 order=1)]))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    lines = annotation_lines(report)
    assert lines[0].startswith('::notice::Verified launch mint=MMM')
    assert f'slot={launch().slot}' in lines[0]
    assert any('Buy 1 wallet=' in line and 'proof=buy_ix+mint+pool+signer+token_delta' in line
               for line in lines)
    assert any('Claim:' in line for line in lines)
    assert all(line.startswith('::notice::') for line in lines)


def test_annotations_are_length_and_count_bounded(monkeypatch):
    many = [buy(wallet=f'{i:032d}', slot=300_000_000 + i, order=i) for i in range(60)]
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=many, seen=60, attempted=60))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    lines = annotation_lines(report)
    assert len(lines) <= 12
    assert all(len(line) <= 480 for line in lines)
    assert any('further verified buys omitted' in line for line in lines)


def test_annotations_never_leak_the_provider_url_or_key():
    leaked = {'launch': launch().model_dump(), 'verified_buys': [buy()],
              'distinct_wallet_count': 1, 'verified_buy_count': 1,
              'claim': f'the provider was {SECRET_URL}',
              'coverage': {'coverage_proven': False, 'reached_launch_slot': False,
                           'unavailable_transactions': 0, 'missing_data': []}}
    text = '\n'.join(annotation_lines(leaked))
    assert 'SUPERSECRETKEY' not in text
    assert 'redacted' in text


def test_redact_strips_query_credentials_but_keeps_signatures():
    cleaned = redact(f'url={SECRET_URL} sig={"a" * 64}')
    assert 'SUPERSECRETKEY' not in cleaned
    assert 'a' * 64 in cleaned
