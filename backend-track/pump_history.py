"""Bounded backwards pagination of bonding-curve history, with honest coverage.

RPC getSignaturesForAddress is newest first; a single page is NOT the first
100 signatures after launch. Never claim completeness unless we reach launch.
"""
from pump_discover import rpc
from pump_replay import fetch_transaction, parse_buys


def collect_early_buys(url, launch, max_pages=8, page_size=100):
    cursor = None
    early = {}
    reached_launch = False
    pages = 0
    for _ in range(max_pages):
        opts = {'limit': page_size, 'commitment': 'confirmed'}
        if cursor:
            opts['before'] = cursor
        page = rpc(url, 'getSignaturesForAddress', [launch.pool, opts])
        pages += 1
        if not page:
            reached_launch = True
            break
        for item in page:
            if launch.slot <= item['slot'] <= launch.end_slot and item.get('err') is None:
                early[item['signature']] = item['slot']
        # We can stop once history crosses the launch boundary. A short page
        # also indicates exhaustion, but provider retention may be incomplete.
        if min(item['slot'] for item in page) < launch.slot:
            reached_launch = True
            break
        if len(page) < page_size:
            reached_launch = True
            break
        if cursor == page[-1]['signature']:
            break  # malfunctioning provider pagination, never infinite loop
        cursor = page[-1]['signature']
    ordered = sorted(early, key=lambda s: (early[s], s))
    buys = []
    missing = 0
    for signature in ordered[:100]:
        try:
            buys.extend(parse_buys(fetch_transaction(url, signature), launch))
        except ValueError:
            missing += 1
    # Even reaching launch doesn't prove no missing transactions / same-slot
    # ordering. This flag represents only pagination coverage, not full buyers.
    return {'buys': [b.model_dump() for b in buys], 'pages_scanned': pages,
            'early_signatures_seen': len(ordered), 'unavailable_transactions': missing,
            'reached_launch_slot': reached_launch,
            'buyers_complete': False}  # full coverage needs independent block verification
