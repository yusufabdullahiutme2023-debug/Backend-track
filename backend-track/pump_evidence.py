"""Bounded, read-only early-buyer evidence for ONE verified Pump.fun launch.

This module exists because two things are easy to get wrong:

1. Calling a bounded RPC sample "the first 50 buyers". It is not. A single
   ``getSignaturesForAddress`` page is newest-first and provider retention is
   not guaranteed, so pagination depth alone never proves completeness.
2. Publishing evidence only as a workflow artifact. Artifact blobs live on
   Azure blob storage, which restricted networks (and this repository's own
   sandbox) cannot reach. Findings are therefore also emitted as GitHub
   Actions ``::notice::`` annotations, which stay readable through the
   public check-run annotations API.

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
_SECRETISH = re.compile(r'(?:api[-_]?key|token|authorization)=[^\s&"]+', re.IGNORECASE)


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


def collect_evidence(url, launch, max_pages=4, page_size=100):
    """Collect verified early buys plus an explicit accounting of what is missing."""
    sample = collect_early_buys(url, launch, max_pages=max_pages, page_size=page_size)
    attempted = sample.get('transactions_attempted', 0)
    unavailable = sample['unavailable_transactions']
    fetched = max(attempted - unavailable, 0)
    buys = sample['buys']
    wallets = {b['wallet'] for b in buys}
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
    return {
        'launch': launch.model_dump(),
        'verified_buys': buys,
        'distinct_wallets': sorted(wallets),
        'distinct_wallet_count': len(wallets),
        'verified_buy_count': len(buys),
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
            'missing_data': missing_data,
        },
        'claim': None,  # filled below; never a first-N claim without proof
        'read_only': True,
        'note': 'Bounded read-only RPC sample. Not a trading signal.',
    }


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


def early_buyer_claim(report) -> str:
    """Human-readable claim that cannot overstate coverage."""
    coverage = report['coverage']
    count = report['distinct_wallet_count']
    buys = report['verified_buys']
    slots = sorted({b['slot'] for b in buys})
    span = f'slots {slots[0]}-{slots[-1]}' if slots else 'no slots observed'
    if coverage['coverage_proven']:
        return (f'{count} distinct early buyers verified across {len(buys)} buy '
                f'transactions ({span}); coverage independently verified')
    reasons = '; '.join(coverage['missing_data']) or 'no coverage evidence recorded'
    return (f'{count} distinct early buyers observed across {len(buys)} verified buy '
            f'transactions ({span}); coverage INCOMPLETE so this is NOT the first '
            f'{FIRST_N_BUYERS} buyers. Missing: {reasons}')


def finalize(report) -> dict:
    """Attach the claim and refuse to publish an unproven first-N claim."""
    report['claim'] = early_buyer_claim(report)
    report['coverage']['buyers_complete'] = report['coverage']['coverage_proven']
    assert_no_unproven_first_n_claim(report)
    return report


def assert_no_unproven_first_n_claim(report) -> None:
    """Guard rail: a first-N phrasing may only exist when coverage is proven."""
    coverage = report['coverage']
    if coverage.get('coverage_proven'):
        return
    claim = str(report.get('claim', ''))
    # "NOT the first 50 buyers" is an explicit disclaimer and is allowed; a bare
    # affirmative first-N claim is not.
    affirmative = re.search(r'(?<!NOT the )first\s+\d+\s+buyers?', claim, re.IGNORECASE)
    if affirmative:
        raise AssertionError(
            f'refusing to publish unproven buyer coverage: {claim!r}')


def annotation_lines(report, max_lines=12, max_chars=480) -> list[str]:
    """Render findings as Actions annotations (retrievable via the check-run API)."""
    launch = report['launch']
    coverage = report['coverage']
    lines = [
        f"::notice::Verified launch mint={launch['mint']} pool={launch['pool']} "
        f"slot={launch['slot']} signature={launch['signature']}",
        f"::notice::Early buyers: {report['distinct_wallet_count']} distinct wallets / "
        f"{report['verified_buy_count']} verified buys; coverage_proven="
        f"{str(coverage['coverage_proven']).lower()}; reached_launch_slot="
        f"{str(coverage['reached_launch_slot']).lower()}; unavailable="
        f"{coverage['unavailable_transactions']}",
        f"::notice::Claim: {report['claim']}",
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
            f"signature={buy['signature']} proof=buy_ix+mint+pool+signer+token_delta")
    if omitted:
        lines.append(f"::notice::{omitted} further verified buys omitted from "
                     'annotations; see artifact JSON')
    return [redact(line)[:max_chars] for line in lines]


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Read-only early-buyer evidence for one verified Pump.fun launch')
    parser.add_argument('--launch-signature', default=None,
                        help='Optional known create/create_v2 signature')
    parser.add_argument('--max-pages', type=int, default=4)
    parser.add_argument('--page-size', type=int, default=100)
    parser.add_argument('--output', default='pump-evidence.json')
    parser.add_argument('--no-annotations', action='store_true')
    args = parser.parse_args()
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        parser.error('Set SOLANA_RPC_URL privately; never paste a key on the command line.')
    try:
        if args.launch_signature:
            launch = parse_launch(fetch_transaction(url, args.launch_signature))
            diagnostics = {'source': 'supplied signature'}
            if launch is None:
                raise ValueError('supplied signature is not a verified Pump.fun creation')
        else:
            launch, diagnostics = find_launch(url)
            if launch is None:
                raise ValueError(
                    'no verified Pump.fun creation found in the bounded sample: '
                    + json.dumps(diagnostics))
        report = finalize(collect_evidence(url, launch, max_pages=args.max_pages,
                                           page_size=args.page_size))
        report['discovery'] = diagnostics
    except (ValueError, KeyError, IndexError, OSError) as exc:
        # Transport errors can embed the provider URL and its API key.
        safe = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        print(redact(f'Evidence collection failed: {safe}'), file=sys.stderr)
        return 1
    with open(args.output, 'w') as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps(report, indent=2))
    if not args.no_annotations:
        for line in annotation_lines(report):
            print(line)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
