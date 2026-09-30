"""Offline tests for bounded early-buyer evidence and honest coverage claims.

No network, no RPC, no transactions: every provider call is faked. These tests
exist to make the honesty rules executable rather than aspirational.
"""
import json
import sys

import pytest

import pump_evidence
import pump_history
from pump_evidence import (BUYS_OBSERVED, COVERAGE_PROVEN, EXIT_LAUNCH_UNKNOWN,
                           FIRST_N_BUYERS, NO_BUYS_IN_WINDOW, UNKNOWN_INCOMPLETE,
                           UNKNOWN_PHRASE, annotation_lines,
                           assert_no_unproven_first_n_claim, assert_no_unproven_zero_claim,
                           collect_evidence, coverage_proven, early_buyer_claim, evaluate_candidates,
                           finalize, find_launch, launch_time_reachable,
                           launches_from_report, redact, select_report)
from solana_signals import Launch

SECRET_URL = 'https://mainnet.helius-rpc.com/?api-key=SUPERSECRETKEY'


def launch(slot=300_000_000):
    return Launch(mint='M' * 32, pool='P' * 32, signature='s' * 64,
                  venue='pump.fun', slot=slot, end_slot=slot + 10)


def buy(wallet='W' * 32, slot=300_000_001, order=0, raw_amount=1000, signature='b' * 64):
    return {'wallet': wallet, 'signature': signature, 'slot': slot, 'order': order,
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


def test_pagination_that_never_reached_the_launch_slot_is_unknown(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(reached_launch_slot=False))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['reached_launch_slot'] is False
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
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
              'evidence_status': 'buys_observed', 'buyer_count_known': False,
              'claim': f'the provider was {SECRET_URL}',
              'coverage': {'coverage_proven': False, 'reached_launch_slot': False,
                           'unavailable_transactions': 0, 'missing_data': [],
                           'early_signatures_seen': 1, 'transactions_attempted': 1,
                           'transactions_fetched': 1}}
    text = '\n'.join(annotation_lines(leaked))
    assert 'SUPERSECRETKEY' not in text
    assert 'redacted' in text


def test_redact_strips_query_credentials_but_keeps_signatures():
    cleaned = redact(f'url={SECRET_URL} sig={"a" * 64}')
    assert 'SUPERSECRETKEY' not in cleaned
    assert 'a' * 64 in cleaned


# ------------------------------------------- unknown/incomplete vs zero buyers

def test_reachability_requires_every_launch_time_transaction_to_be_readable():
    reachable, reasons = launch_time_reachable(
        {'early_signatures_seen': 2, 'transactions_attempted': 2,
         'transactions_fetched': 2, 'reached_launch_slot': True})
    assert reachable is True and reasons == []
    reachable, reasons = launch_time_reachable(
        {'early_signatures_seen': 2, 'transactions_attempted': 2,
         'transactions_fetched': 0, 'reached_launch_slot': True})
    assert reachable is False
    assert any('unavailable from the RPC' in reason for reason in reasons)


def test_unreadable_launch_window_reports_unknown_never_zero(monkeypatch):
    """The core rule: unreachable data is unknown, not a zero buyer count."""
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=3, attempted=3, unavailable=3))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
    assert report['coverage']['transactions_fetched'] == 0
    assert report['coverage']['launch_time_reachable'] is False
    assert report['buyer_count_known'] is False
    assert report['first_n_claim_allowed'] is False
    assert UNKNOWN_PHRASE in report['claim']
    assert 'number of early buyers is UNKNOWN' in report['claim']
    # It must not smuggle in a count either: no "0 distinct early buyers".
    assert '0 distinct early buyers' not in report['claim']
    assert 'zero buyers' not in report['claim'].replace('NOT "zero buyers"', '')


def test_empty_curve_history_is_unknown_not_zero(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=0, attempted=0))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
    assert report['verified_buy_count'] == 0
    assert 'provider retention' in report['claim']
    assert 'no launch-time transaction was available to decode' in report['claim']


def test_zero_decoded_buys_without_proven_coverage_is_unknown(monkeypatch):
    """Everything readable decoded fine, but no buys: still not a zero finding."""
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=2, attempted=2))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['launch_time_reachable'] is True
    assert report['verified_buy_count'] == 0
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
    assert report['coverage']['coverage_proven'] is False


def test_zero_buys_becomes_a_finding_only_with_proven_coverage(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=2, attempted=2, independent=True))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['evidence_status'] == NO_BUYS_IN_WINDOW
    assert report['buyer_count_known'] is True
    assert 'no buys in the launch window' in report['claim']
    assert 'independently verified' in report['claim']


def test_partial_sample_labels_its_count_a_lower_bound(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys', lambda *a, **kw: sample())
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['evidence_status'] == BUYS_OBSERVED
    assert report['buyer_count_known'] is False
    assert 'LOWER BOUND' in report['claim']
    assert 'at least 1 distinct early buyers' in report['claim']
    assert 'NOT the first 50 buyers' in report['claim']


def test_status_transitions_are_exhaustive(monkeypatch):
    cases = [
        (sample(buys=[], seen=0, attempted=0), UNKNOWN_INCOMPLETE),
        (sample(buys=[], seen=2, attempted=2), UNKNOWN_INCOMPLETE),
        (sample(buys=[buy()], seen=2, attempted=2), BUYS_OBSERVED),
        (sample(buys=[buy()], seen=2, attempted=2, independent=True), COVERAGE_PROVEN),
        (sample(buys=[], seen=2, attempted=2, independent=True), NO_BUYS_IN_WINDOW),
    ]
    for stub, expected in cases:
        monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                            lambda *a, _stub=stub, **kw: _stub)
        report = finalize(collect_evidence(SECRET_URL, launch()))
        assert report['evidence_status'] == expected, stub


# ----------------------------------------------------------------- claim guards

def test_zero_claim_guard_rejects_unproven_zero_wording():
    report = {'claim': 'there were zero buyers for this launch',
              'evidence_status': UNKNOWN_INCOMPLETE, 'coverage': {'coverage_proven': False}}
    with pytest.raises(AssertionError):
        assert_no_unproven_zero_claim(report)
    report['claim'] = 'no buyers participated'
    with pytest.raises(AssertionError):
        assert_no_unproven_zero_claim(report)
    # The same wording is legitimate once coverage is proven.
    report['coverage']['coverage_proven'] = True
    report['evidence_status'] = NO_BUYS_IN_WINDOW
    assert assert_no_unproven_zero_claim(report) is None


def test_disclaimers_do_not_trip_the_guards():
    """'NOT the first 50 buyers' / 'NOT "zero buyers"' are denials, not claims."""
    report = {'evidence_status': UNKNOWN_INCOMPLETE, 'coverage': {'coverage_proven': False},
              'claim': (f'{UNKNOWN_PHRASE}: buyers UNKNOWN. This is NOT "zero buyers" and '
                        'NOT the first 50 buyers.')}
    assert_no_unproven_first_n_claim(report)
    assert_no_unproven_zero_claim(report)


def test_finalize_rejects_a_bare_affirmative_zero_claim(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=0, attempted=0))
    monkeypatch.setattr(pump_evidence, 'early_buyer_claim',
                        lambda report: 'this launch had zero buyers')
    with pytest.raises(AssertionError):
        finalize(collect_evidence(SECRET_URL, launch()))


# ------------------------------------------------------------- CLI exit codes

def test_main_reports_unknown_and_exits_2_when_the_launch_is_unverifiable(
        monkeypatch, capsys, tmp_path):
    monkeypatch.setenv('SOLANA_RPC_URL', SECRET_URL)
    monkeypatch.setattr(sys, 'argv', ['pump_evidence.py', '--launch-signature', 'L' * 64,
                                      '--output', str(tmp_path / 'out.json')])

    def fetch(url, signature):
        raise ValueError('Transaction not available from this RPC')

    monkeypatch.setattr(pump_evidence, 'fetch_transaction', fetch)
    assert pump_evidence.main() == EXIT_LAUNCH_UNKNOWN
    captured = capsys.readouterr()
    assert f'::warning::Evidence status: {UNKNOWN_INCOMPLETE}' in captured.out
    assert 'launch not verifiable' in captured.out
    # No report file is written when there is nothing verified to report.
    assert not (tmp_path / 'out.json').exists()


def test_main_reports_unknown_when_no_launch_is_found(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv('SOLANA_RPC_URL', SECRET_URL)
    monkeypatch.setattr(sys, 'argv', ['pump_evidence.py',
                                      '--output', str(tmp_path / 'out.json')])
    monkeypatch.setattr(pump_evidence, 'find_launch', lambda url: (None, {'seen': 0}))
    assert pump_evidence.main() == EXIT_LAUNCH_UNKNOWN
    captured = capsys.readouterr()
    assert f'::warning::Evidence status: {UNKNOWN_INCOMPLETE}' in captured.out
    assert 'SUPERSECRETKEY' not in captured.out + captured.err


def test_main_writes_a_report_for_any_evidence_status(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv('SOLANA_RPC_URL', SECRET_URL)
    out = tmp_path / 'out.json'
    monkeypatch.setattr(sys, 'argv', ['pump_evidence.py', '--launch-signature', 'L' * 64,
                                      '--output', str(out)])
    monkeypatch.setattr(pump_evidence, 'fetch_transaction', lambda url, sig: {})
    monkeypatch.setattr(pump_evidence, 'parse_launch', lambda tx: launch())
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=0, attempted=0))
    assert pump_evidence.main() == 0
    report = json.loads(out.read_text())
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
    assert report['buyer_count_known'] is False
    captured = capsys.readouterr()
    assert f'::warning::Evidence status: {UNKNOWN_INCOMPLETE}' in captured.out


# ------------------------- reachable window with no verified buy (run #5 case)

def test_readable_window_without_buys_does_not_claim_unreachable_data(monkeypatch):
    """Regression from live run #5: reached_launch_slot=true contradicted the claim."""
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=2, attempted=2))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['evidence_status'] == UNKNOWN_INCOMPLETE
    assert report['coverage']['reached_launch_slot'] is True
    assert report['coverage']['unreachable_reasons'] == []
    assert 'launch window was readable' in report['claim']
    assert '2 in-window transaction(s) were decoded' in report['claim']
    # The old fallback contradicted reached_launch_slot and is gone for good.
    assert 'no reachable launch-time data' not in report['claim']
    assert 'zero buyers' not in report['claim'].replace('NOT "zero buyers"', '')


def test_unreachable_reason_text_is_kept_when_data_really_is_unreadable(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=0, attempted=0))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['coverage']['unreachable_reasons']
    assert 'could not be fully read' in report['claim']
    assert 'launch window was readable' not in report['claim']


def test_status_annotation_carries_the_coverage_counters(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[], seen=7, attempted=5, unavailable=2))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    status_line = annotation_lines(report)[1]
    assert 'signatures_seen=7' in status_line
    assert 'tx_attempted/fetched=5/3' in status_line
    assert 'unavailable=2' in status_line


# ------------------------------------------------------ bounded multi-launch

def test_launches_from_report_is_bounded_and_skips_malformed_entries(tmp_path):
    path = tmp_path / 'pump-report.json'
    path.write_text(json.dumps({'validated_launches':
                                [{'signature': f's{i}'} for i in range(10)] + ['junk', {}]}))
    assert launches_from_report(path, 3) == ['s0', 's1', 's2']
    assert launches_from_report(path, 0) == []


def test_evaluate_candidates_stops_at_the_first_launch_with_a_verified_buy(monkeypatch):
    evaluated = []
    monkeypatch.setattr(pump_evidence, 'fetch_transaction', lambda url, sig: {'signature': sig})
    monkeypatch.setattr(pump_evidence, 'parse_launch',
                        lambda tx: None if tx['signature'] == 'bad' else launch())
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda url, lch, **kw: evaluated.append(lch) or sample(buys=[buy()]))
    reports = evaluate_candidates('private', ['bad', 'good1', 'good2'], 4, 100)
    assert len(reports) == 1
    assert len(evaluated) == 1
    assert reports[0]['verified_buy_count'] == 1
    assert reports[0]['discovery']['signature'] == 'good1'


def test_evaluate_candidates_exhausts_the_bounded_list_when_no_buy_is_found(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'fetch_transaction', lambda url, sig: {'signature': sig})
    monkeypatch.setattr(pump_evidence, 'parse_launch', lambda tx: launch())
    monkeypatch.setattr(pump_evidence, 'collect_early_buys', lambda *a, **kw: sample(buys=[]))
    reports = evaluate_candidates('private', ['a', 'b', 'c'], 4, 100)
    assert len(reports) == 3
    assert all(r['evidence_status'] == UNKNOWN_INCOMPLETE for r in reports)


def test_select_report_prefers_buys_then_the_most_decoded_window():
    few = {'verified_buy_count': 0, 'coverage': {'transactions_fetched': 1}}
    many = {'verified_buy_count': 0, 'coverage': {'transactions_fetched': 9}}
    with_buys = {'verified_buy_count': 2, 'coverage': {'transactions_fetched': 1}}
    assert select_report([many, with_buys]) is with_buys
    assert select_report([few, many]) is many
    assert select_report([]) is None


# ------------------- bundled-with-creation vs independent (live run #6 case)

def test_buy_inside_the_creation_transaction_is_flagged_bundled(monkeypatch):
    """Regression from live run #6: the only verified buy shared the launch signature."""
    lch = launch()
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[buy(signature=lch.signature)]))
    report = finalize(collect_evidence(SECRET_URL, lch))
    assert report['verified_buys'][0]['bundled_with_creation'] is True
    assert report['bundled_buy_count'] == 1
    assert report['independent_buy_count'] == 0
    assert report['independent_wallet_count'] == 0
    assert 'ride inside the creation transaction' in report['claim']
    assert 'no independent early buyer is evidenced yet' in report['claim']
    # A bundled buy must not be sold as independent early demand.
    assert 'LOWER BOUND' in report['claim']


def test_bundled_and_independent_buys_are_never_counted_together(monkeypatch):
    lch = launch()
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[buy(signature=lch.signature),
                                                      buy(wallet='V' * 32, order=1)]))
    report = finalize(collect_evidence(SECRET_URL, lch))
    assert report['verified_buy_count'] == 2
    assert report['bundled_buy_count'] == 1
    assert report['independent_buy_count'] == 1
    assert report['independent_wallet_count'] == 1
    assert '1 of 2 verified buys are bundled inside the creation transaction' in report['claim']
    assert '1 are independent' in report['claim']


def test_all_independent_buys_produce_no_bundling_clause(monkeypatch):
    monkeypatch.setattr(pump_evidence, 'collect_early_buys', lambda *a, **kw: sample())
    report = finalize(collect_evidence(SECRET_URL, launch()))
    assert report['bundled_buy_count'] == 0
    assert report['independent_buy_count'] == 1
    assert 'bundled' not in report['claim']


def test_annotations_separate_bundled_from_independent(monkeypatch):
    lch = launch()
    monkeypatch.setattr(pump_evidence, 'collect_early_buys',
                        lambda *a, **kw: sample(buys=[buy(signature=lch.signature)]))
    report = finalize(collect_evidence(SECRET_URL, launch()))
    lines = annotation_lines(report)
    assert 'independent=0, bundled=1' in lines[1]
    assert any('bundled_with_creation=true' in line for line in lines)
    assert all(len(line) <= 480 for line in lines)
