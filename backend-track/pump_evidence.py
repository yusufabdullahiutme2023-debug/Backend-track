"""Bounded, read-only early-buyer evidence for ONE verified Pump.fun launch.

The central honesty rule: a bounded RPC sample can prove that a buy happened,
but it can almost never prove how many buyers there were. So every report
carries an explicit ``evidence_status`` instead of implying one:

``buys_observed``            at least one buy verified; the count is a LOWER BOUND
``no_buys_in_window``        zero buys AND coverage independently proven
``coverage_proven``          full coverage independently attested
``unknown_incomplete``       the launch-time window could not be read; the buyer
                             count is UNKNOWN -- never "zero buyers"

Two failure modes this module refuses to produce:

1. Reporting "zero buyers" because the RPC could not return launch-time
   transactions. Absence of data is unknown, not a negative finding.
2. Reporting "the first 50 buyers" from pagination depth. A single
   ``getSignaturesForAddress`` page is newest-first and provider retention is
   not guaranteed, so depth never proves completeness.

Findings are emitted as GitHub Actions ``::notice::`` annotations as well as
JSON, because artifact blobs live on Azure blob storage that restricted
networks cannot download while annotations stay readable through the public
check-run annotations API.

Read-only: only ``getTransaction`` / ``getSignaturesForAddress`` are issued.
No transaction is ever constructed, signed, or sent, and no trade is placed.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

from pump_discover import rpc
from pump_history import collect_early_buys
from pump_replay import PUMP_PROGRAM, fetch_transaction, parse_launch

# A "first N buyers" claim additionally requires an attestation that comes from
# OUTSIDE this RPC path (for example an independent full-block scan). Nothing in
# this module can produce that attestation, so the claim stays unavailable.
FIRST_N_BUYERS = 50

BUYS_OBSERVED = 'buys_observed'
NO_BUYS_IN_WINDOW = 'no_buys_in_window'
COVERAGE_PROVEN = 'coverage_proven'
UNKNOWN_INCOMPLETE = 'unknown_incomplete'

UNKNOWN_PHRASE = 'unknown/incomplete'
_SECRETISH = re.compile(r'(?:api[-_]?key|token|authorization)=[^\s&"]+', re.IGNORECASE)
_FIRST_N = re.compile(r'first\s+\d+\s+buyers?', re.IGNORECASE)
_ZERO_BUYERS = re.compile(r'\b(?:no|zero|0)\s+(?:early\s+)?buyers?\b', re.IGNORECASE)
# Explicit disclaimers are the opposite of an overclaim, but they contain the very
# phrases the guards look for. Strip them before searching so the guards test what
# the report asserts rather than what it denies.
_DISCLAIMERS = (
    re.compile(r'NOT\s+"?the\s+first\s+\d+\s+buyers?"?', re.IGNORECASE),
    re.compile(r'NOT\s+"?(?:no|zero|0)\s+(?:early\s+)?buyers?"?', re.IGNORECASE),
)

# Exit codes: 0 produced a report of any status, 2 the launch itself is unknown.
EXIT_REPORT = 0
EXIT_LAUNCH_UNKNOWN = 2
EXIT_USAGE = 1


def redact(text: str) -> str:
    """Strip provider credentials before anything reaches a log or annotation."""
    return _SECRETISH.sub('redacted', text)


def find_launch(url, max_program_signatures=40, max_fetches=25):
    """Locate one RPC-verified create/create_v2, reporting what was skipped.

    Returns ``(launch_or_None, diagnostics)``. A ``None`` launch is a missing
    result, never evidence that no launch happened.
    """
    diagnostics = {'program_signatures_seen': 0, 'transactions_fetched': 0,
                   'unavailable_transactions': 0, 'non_creation_signatures': 0}
    try:
        page = rpc(url, 'getSignaturesForAddress',
                   [PUMP_PROGRAM, {'limit': max_program_signatures,
                                   'commitment': 'confirmed'}])
    except ValueError as exc:
        raise ValueError('program signature lookup failed') from exc
    diagnostics['program_signatures_seen'] = len(page)
    for item in page:
        if diagnostics['transactions_fetched'] >= max_fetches:
            break
        if item.get('err') is not None:
            continue
        diagnostics['transactions_fetched'] += 1
        try:
            launch = parse_launch(fetch_transaction(url, item['signature']))
        except ValueError:
            diagnostics['unavailable_transactions'] += 1
            continue
        if launch is None:
            diagnostics['non_creation_signatures'] += 1
            continue
        return launch, diagnostics
    return None, diagnostics


def launch_time_reachable(coverage) -> tuple[bool, list[str]]:
    """Did we actually read the launch-time window, or did the RPC fail us?

    Every unmet condition is a reason the buyer count is unknown rather than a
    reason to report zero.
    """
    reasons = []
    if coverage['early_signatures_seen'] == 0:
        reasons.append('the RPC returned no bonding-curve signatures inside the '
                       'launch window (history may be beyond provider retention)')
    if coverage['transactions_attempted'] == 0:
        reasons.append('no launch-time transaction was available to decode')
    elif coverage['transactions_fetched'] == 0:
        reasons.append(f"all {coverage['transactions_attempted']} launch-time "
                       'transaction(s) attempted were unavailable from the RPC')
    if not coverage['reached_launch_slot']:
        reasons.append('pagination never reached the launch slot, so earlier '
                       'launch-time transactions may exist that we never saw')
    return (not reasons), reasons


def annotate_bundling(buys, launch_signature) -> list[dict]:
    """Flag buys that ride inside the creation transaction.

    Pump.fun creations are frequently bundled with an initial buy in the same
    transaction. That is a real, instruction-verified buy, but it is NOT
    evidence of independent early demand, and the two must never be counted
    together as if they were.
    """
    for buy in buys:
        buy['bundled_with_creation'] = buy['signature'] == launch_signature
    return buys


def coverage_proven(sample, transactions_fetched) -> bool:
    """True only when nothing in the window is unaccounted for AND an external
    attestation exists. Pagination depth by itself is deliberately insufficient.
    """
    return bool(
        sample.get('reached_launch_slot')
        and sample.get('unavailable_transactions', 0) == 0
        and transactions_fetched >= sample.get('early_signatures_seen', 0)
        and sample.get('independent_block_verified') is True
    )


def classify(report) -> str:
    """Assign the evidence status. Unknown always wins over a count."""
    coverage = report['coverage']
    reachable, reasons = launch_time_reachable(coverage)
    coverage['launch_time_reachable'] = reachable
    coverage['unreachable_reasons'] = reasons
    if not reachable:
        return UNKNOWN_INCOMPLETE
    proven = coverage['coverage_proven']
    if report['verified_buy_count'] > 0:
        return COVERAGE_PROVEN if proven else BUYS_OBSERVED
    # Zero decoded buys is only a finding when coverage is proven; otherwise the
    # buyers may simply be in transactions we never managed to read.
    return NO_BUYS_IN_WINDOW if proven else UNKNOWN_INCOMPLETE


def collect_evidence(url, launch, max_pages=4, page_size=100):
    """Collect verified early buys plus an explicit accounting of what is missing."""
    sample = collect_early_buys(url, launch, max_pages=max_pages, page_size=page_size)
    attempted = sample.get('transactions_attempted', 0)
    unavailable = sample['unavailable_transactions']
    fetched = max(attempted - unavailable, 0)
    buys = annotate_bundling(sample['buys'], launch.signature)
    independent = [b for b in buys if not b['bundled_with_creation']]
    wallets = {b['wallet'] for b in buys}
    independent_wallets = {b['wallet'] for b in independent}
    missing_data = []
    if unavailable:
        missing_data.append(f'{unavailable} in-window transaction(s) unavailable from RPC')
    if attempted > fetched + unavailable:
        missing_data.append('some in-window transactions were never fetched')
    if not sample['reached_launch_slot']:
        missing_data.append('pagination never reached the launch slot')
    if attempted < sample['early_signatures_seen']:
        missing_data.append(
            f"only {attempted} of {sample['early_signatures_seen']} in-window "
            'signatures were decoded')
    # Same-slot ordering comes from the RPC's own ordering, not from block data.
    missing_data.append('same-slot ordering not confirmed against independent block data')
    report = {
        'launch': launch.model_dump(),
        'verified_buys': buys,
        'distinct_wallets': sorted(wallets),
        'distinct_wallet_count': len(wallets),
        'verified_buy_count': len(buys),
        # Bundled buys ride inside the creation tx; independent ones do not.
        'bundled_buy_count': len(buys) - len(independent),
        'independent_buy_count': len(independent),
        'independent_wallet_count': len(independent_wallets),
        'evidence_status': None,      # set by classify()
        'buyer_count_known': False,   # only true with proven coverage
        'first_n_claim_allowed': False,
        'coverage': {
            'pages_scanned': sample['pages_scanned'],
            'early_signatures_seen': sample['early_signatures_seen'],
            'transactions_attempted': attempted,
            'transactions_fetched': fetched,
            'unavailable_transactions': unavailable,
            'reached_launch_slot': sample['reached_launch_slot'],
            'independent_block_verified': sample.get('independent_block_verified', False),
            'buyers_complete': False,
            'coverage_proven': coverage_proven(sample, fetched),
            'launch_time_reachable': None,
            'unreachable_reasons': [],
            'missing_data': missing_data,
        },
        'claim': None,
        'read_only': True,
        'note': 'Bounded read-only RPC sample. Not a trading signal.',
    }
    return report


def early_buyer_claim(report) -> str:
    """Human-readable claim that cannot overstate coverage."""
    coverage = report['coverage']
    status = report['evidence_status']
    count = report['distinct_wallet_count']
    buys = report['verified_buys']
    slots = sorted({b['slot'] for b in buys})
    span = f'slots {slots[0]}-{slots[-1]}' if slots else 'no slots observed'

    if status == UNKNOWN_INCOMPLETE:
        reasons = coverage['unreachable_reasons']
        if reasons:
            return (f'{UNKNOWN_PHRASE}: the launch-time transactions could not be fully read, '
                    f'so the number of early buyers is UNKNOWN. This is NOT "zero buyers" and '
                    f'NOT a first-{FIRST_N_BUYERS} buyer list. Verified buys in the reachable '
                    f'subset: {len(buys)}. Reasons: ' + '; '.join(reasons))
        # The window WAS readable; we simply found no verified buy in it, and
        # coverage is unproven. Saying "no reachable data" here would contradict
        # reached_launch_slot, and saying "zero buyers" would be a false negative.
        return (f'{UNKNOWN_PHRASE}: the launch window was readable and '
                f"{coverage['transactions_attempted']} in-window transaction(s) were decoded, "
                f'but no verified buy was found among them. Coverage is not proven, so the '
                f'number of early buyers is UNKNOWN — buys may sit in transactions that were '
                f'never decoded or in same-slot transactions we cannot order. This is NOT '
                f'"zero buyers" and NOT a first-{FIRST_N_BUYERS} buyer list.')
    if status == COVERAGE_PROVEN:
        return (f'{count} distinct early buyers verified across {len(buys)} buy transactions '
                f'({span}); coverage independently verified')
    if status == NO_BUYS_IN_WINDOW:
        return (f'no buys in the launch window ({span}); coverage independently verified, '
                f'so this is a finding rather than missing data')
    bundled = report.get('bundled_buy_count', 0)
    independent_n = report.get('independent_buy_count', 0)
    if independent_n == 0 and bundled:
        bundling = (f' All {bundled} verified buy transaction(s) ride inside the creation '
                    'transaction itself (bundled with create), so no independent early '
                    'buyer is evidenced yet.')
    elif bundled:
        bundling = (f' {bundled} of {len(buys)} verified buys are bundled inside the '
                    f'creation transaction; {independent_n} are independent.')
    else:
        bundling = ''
    return (f'at least {count} distinct early buyers observed across {len(buys)} verified buy '
            f'transactions ({span}); this count is a LOWER BOUND because coverage is '
            f'incomplete, so it is NOT the first {FIRST_N_BUYERS} buyers.{bundling} Missing: '
            + ('; '.join(coverage['missing_data']) or 'no coverage evidence recorded'))


def finalize(report) -> dict:
    """Attach status and claim, then refuse to publish an unprovable claim."""
    report['evidence_status'] = classify(report)
    coverage = report['coverage']
    coverage['buyers_complete'] = coverage['coverage_proven']
    report['buyer_count_known'] = coverage['coverage_proven']
    report['first_n_claim_allowed'] = coverage['coverage_proven']
    report['claim'] = early_buyer_claim(report)
    assert_no_unproven_first_n_claim(report)
    assert_no_unproven_zero_claim(report)
    return report


def _strip_disclaimers(text: str) -> str:
    for pattern in _DISCLAIMERS:
        text = pattern.sub('', text)
    return text


def assert_no_unproven_first_n_claim(report) -> None:
    """Guard rail: a first-N phrasing may only exist when coverage is proven."""
    if report['coverage'].get('coverage_proven'):
        return
    claim = _strip_disclaimers(str(report.get('claim', '')))
    if _FIRST_N.search(claim):
        raise AssertionError(f'refusing to publish unproven buyer coverage: {claim!r}')


def assert_no_unproven_zero_claim(report) -> None:
    """Guard rail: an unknown window must never be worded as "zero buyers"."""
    if report.get('evidence_status') != UNKNOWN_INCOMPLETE:
        return
    claim = _strip_disclaimers(str(report.get('claim', '')))
    if _ZERO_BUYERS.search(claim):
        raise AssertionError(
            f'refusing to report missing launch-time data as zero buyers: {claim!r}')


def annotation_lines(report, max_lines=12, max_chars=480) -> list[str]:
    """Render findings as Actions annotations (retrievable via the check-run API)."""
    launch = report['launch']
    coverage = report['coverage']
    status = report['evidence_status']
    # Unknown gets a warning so an incomplete run is impossible to skim past.
    status_level = 'warning' if status == UNKNOWN_INCOMPLETE else 'notice'
    lines = [
        f"::notice::Verified launch mint={launch['mint']} pool={launch['pool']} "
        f"slot={launch['slot']} signature={launch['signature']}",
        f"::{status_level}::Evidence status: {status}; buyer_count_known="
        f"{str(report['buyer_count_known']).lower()}; verified_buys="
        f"{report['verified_buy_count']} (independent="
        f"{report.get('independent_buy_count', 0)}, bundled="
        f"{report.get('bundled_buy_count', 0)}); signatures_seen="
        f"{coverage['early_signatures_seen']}; tx_attempted/fetched="
        f"{coverage['transactions_attempted']}/{coverage['transactions_fetched']}; "
        f"reached_launch_slot={str(coverage['reached_launch_slot']).lower()}; unavailable="
        f"{coverage['unavailable_transactions']}",
        f"::{status_level}::Claim: {report['claim']}",
    ]
    # Reserve room for the "omitted" trailer so the bound is never exceeded.
    omitted = max(report['verified_buy_count'] - (max_lines - len(lines) - 1), 0)
    budget = max_lines - 1 if omitted else max_lines
    for index, buy in enumerate(report['verified_buys'], start=1):
        if len(lines) >= budget:
            break
        lines.append(
            f"::notice::Buy {index} wallet={buy['wallet']} slot={buy['slot']} "
            f"order={buy['order']} raw_amount={buy['raw_amount']} "
            f"signature={buy['signature']} "
            f"bundled_with_creation={str(buy.get('bundled_with_creation', False)).lower()} "
            f"proof=buy_ix+mint+pool+signer+token_delta")
    if omitted:
        lines.append(f'::notice::{omitted} further verified buys omitted from '
                     'annotations; see artifact JSON')
    return [redact(line)[:max_chars] for line in lines]


def launches_from_report(path, limit) -> list[str]:
    """Bounded list of already-validated launch signatures from a sampler report."""
    with open(path) as handle:
        data = json.load(handle)
    signatures = []
    for item in (data.get('validated_launches') or [])[:max(limit, 0)]:
        signature = item.get('signature') if isinstance(item, dict) else None
        if signature:
            signatures.append(signature)
    return signatures


def evaluate_candidates(url, signatures, max_pages, page_size):
    """Evaluate launches in order, stopping at the first with a verified buy.

    A brand-new launch often has no decodable buy yet, so giving up after one
    candidate reports unknown/incomplete far more often than the data warrants.
    Bounded by the caller-supplied signature list.
    """
    reports = []
    for signature in signatures:
        try:
            launch = parse_launch(fetch_transaction(url, signature))
        except ValueError:
            continue
        if launch is None:
            continue
        report = finalize(collect_evidence(url, launch, max_pages=max_pages,
                                           page_size=page_size))
        report['discovery'] = {'source': 'candidate signature', 'signature': signature}
        reports.append(report)
        if report['verified_buy_count'] > 0:
            break
    return reports


def select_report(reports) -> dict | None:
    """Prefer a report with verified buys; otherwise the most informative one."""
    if not reports:
        return None
    for report in reports:
        if report['verified_buy_count'] > 0:
            return report
    return max(reports, key=lambda r: (r['coverage']['transactions_fetched'],
                                       r['verified_buy_count']))


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Read-only early-buyer evidence for one verified Pump.fun launch')
    parser.add_argument('--launch-signature', default=None,
                        help='Known create/create_v2 signature to validate')
    parser.add_argument('--from-report', default=None,
                        help='Sampler report JSON to draw candidate launches from')
    parser.add_argument('--max-launches', type=int, default=3,
                        help='Bound on how many candidate launches to evaluate')
    parser.add_argument('--max-pages', type=int, default=4)
    parser.add_argument('--page-size', type=int, default=100)
    parser.add_argument('--output', default='pump-evidence.json')
    parser.add_argument('--no-annotations', action='store_true')
    args = parser.parse_args()
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        parser.error('Set SOLANA_RPC_URL privately; never paste a key on the command line.')
    try:
        signatures = ([args.launch_signature] if args.launch_signature
                      else launches_from_report(args.from_report, args.max_launches)
                      if args.from_report else [])
        if signatures:
            reports = evaluate_candidates(url, signatures[:args.max_launches],
                                          args.max_pages, args.page_size)
            report = select_report(reports)
            if report is None:
                raise ValueError(
                    f'none of {len(signatures)} candidate signature(s) verified as a '
                    'Pump.fun creation')
            report['launches_tried'] = len(reports)
            report['launches_with_verified_buys'] = sum(
                1 for r in reports if r['verified_buy_count'] > 0)
        else:
            launch, diagnostics = find_launch(url)
            if launch is None:
                raise ValueError('no verified Pump.fun creation in the bounded sample: '
                                 + json.dumps(diagnostics))
            report = finalize(collect_evidence(url, launch, max_pages=args.max_pages,
                                               page_size=args.page_size))
            report['discovery'] = diagnostics
    except ValueError as exc:
        # The launch itself is unknown. Say so explicitly rather than exiting
        # with a bare failure that could be read as "no launch happened".
        print(f'::warning::Evidence status: {UNKNOWN_INCOMPLETE}; launch not verifiable '
              f'({redact(str(exc))})')
        print(redact(f'Launch verification failed: {exc}'), file=sys.stderr)
        return EXIT_LAUNCH_UNKNOWN
    except (KeyError, IndexError, OSError) as exc:
        safe = str(exc) if isinstance(exc, (KeyError, IndexError)) else type(exc).__name__
        print(f'::warning::Evidence status: {UNKNOWN_INCOMPLETE}; buyer history unreadable')
        print(redact(f'Evidence collection failed: {safe}'), file=sys.stderr)
        return EXIT_LAUNCH_UNKNOWN
    with open(args.output, 'w') as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps(report, indent=2))
    if not args.no_annotations:
        for line in annotation_lines(report):
            print(line)
    return EXIT_REPORT


if __name__ == '__main__':
    raise SystemExit(main())
