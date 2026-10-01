"""Bounded, read-only live comparison of two Pump.fun log subscriptions.

The worker can listen to the whole Pump program (every buy and sell is delivered, and the
provider bills streamed bytes) or only to Pump's mint-authority account (almost only launches).
The second is far cheaper, but only if it never misses a launch the first one sees. This tool
opens both at once on live data for a fixed window and answers, with one number each:

  1. missed          launches the program stream delivered that the creations stream did not
  2. prefilter       launches the creations stream delivered whose logs the worker's create/
                     create_v2 log filter would have skipped (a hole in the filter)
  3. volume          bytes per notification and the projected monthly credits of each stream
  4. unknown logs    how often logs are missing or truncated (those cost an extra fetch)
  5. lookup tables   whether the filter still matches when the authority account is resolved
                     through an address lookup table instead of being listed in the message

It is hard-capped in time AND in streamed bytes, so the spend has a known ceiling. It issues
``logsSubscribe`` twice plus a few ``getTransaction`` calls, never builds or sends a transaction,
and never prints the provider URL (exception text can embed it, so only classes are reported).
"""
import argparse
import asyncio
import json
import math
import os
import sys
import time
from collections import Counter

import websockets

from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM, log_verdict
from pump_worker import Settings, fetch_with_retry, websocket_url

BYTES_PER_UNIT = 100_000          # Helius meters 2 credits per 0.1 MB of uncompressed streamed data
CREDITS_PER_UNIT = 2              # (if the provider means MiB the true cost is about 5% lower)
SECONDS_PER_MONTH = 30 * 86_400
MARGIN_SLOTS = 2                  # ignore launches within this many slots of either stream's edges
MIN_LAUNCHES = 50                 # below this a clean result cannot rule out a few percent of misses
LIMITS = {'seconds': (30, 900), 'max_megabytes': (1, 60), 'sample': (0, 60)}
DEFAULTS = {'seconds': 300, 'max_megabytes': 40, 'sample': 25}


class Tap:
    """Everything one subscription delivered during the window."""

    def __init__(self, name, address):
        self.name, self.address = name, address
        self.bytes = 0
        self.notifications = 0
        self.malformed = 0
        self.records = {}                 # signature -> {'slot', 'verdict', 'at'}
        self.first_slot = self.last_slot = None
        self.started = self.ended = None
        self.error = None

    def take(self, raw, now):
        """Count one received frame and remember what it announced."""
        self.bytes += len(raw.encode()) if isinstance(raw, str) else len(raw)
        try:
            event = json.loads(raw)
        except ValueError:
            self.malformed += 1
            return
        if not isinstance(event, dict) or event.get('method') != 'logsNotification':
            return                                    # not something this tool counts
        try:
            result = event['params']['result']
            value = result['value']
            signature, slot = value['signature'], result['context']['slot']
        except (KeyError, TypeError):
            self.malformed += 1
            return
        if not isinstance(slot, int) or not isinstance(signature, str):
            self.malformed += 1
            return
        self.notifications += 1
        self.first_slot = slot if self.first_slot is None else min(self.first_slot, slot)
        self.last_slot = slot if self.last_slot is None else max(self.last_slot, slot)
        if signature not in self.records:
            verdict = 'failed' if value.get('err') is not None else log_verdict(value.get('logs'))
            self.records[signature] = {'slot': slot, 'verdict': verdict, 'at': now}


async def listen(http_url, tap, stop, budget_bytes):
    """Feed ``tap`` until ``stop`` is set. A failure ends the whole experiment and is reported."""
    try:
        async with websockets.connect(websocket_url(http_url), ping_interval=20, ping_timeout=20,
                                      max_queue=5_000) as ws:
            await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'logsSubscribe',
                                      'params': [{'mentions': [tap.address]},
                                                 {'commitment': 'confirmed'}]}))
            ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            if ack.get('error') or not isinstance(ack.get('result'), int):
                raise RuntimeError('subscription rejected')
            tap.started = time.monotonic()
            while not stop.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.25)   # short, so a stop is noticed quickly
                except asyncio.TimeoutError:
                    continue
                tap.take(raw, time.monotonic())
                if tap.bytes >= budget_bytes:
                    stop.set()
    except Exception as exc:  # noqa: BLE001 - never report text: it can embed the provider URL
        tap.error = type(exc).__name__
        stop.set()
    finally:
        tap.ended = time.monotonic()


async def collect(http_url, seconds, max_bytes):
    """Run both subscriptions until the time or the byte budget runs out."""
    stop = asyncio.Event()
    program, creations = Tap('program', PUMP_PROGRAM), Tap('creations', MINT_AUTHORITY)

    async def timer():
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        stop.set()
    await asyncio.gather(listen(http_url, program, stop, max_bytes),
                         listen(http_url, creations, stop, max_bytes), timer())
    stopped_by = 'byte budget' if max(program.bytes, creations.bytes) >= max_bytes else (
        'error' if (program.error or creations.error) else 'time')
    return program, creations, stopped_by


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1)] if ordered else None


def summarize(tap):
    seconds = (tap.ended - tap.started) if tap.started is not None else 0.0
    credits = tap.bytes / BYTES_PER_UNIT * CREDITS_PER_UNIT
    return {
        'address': tap.address, 'notifications': tap.notifications, 'unique_signatures': len(tap.records),
        'malformed': tap.malformed, 'bytes': tap.bytes,
        'average_bytes': round(tap.bytes / tap.notifications) if tap.notifications else None,
        'seconds': round(seconds, 1), 'per_second': round(tap.notifications / seconds, 2) if seconds else None,
        'verdicts': dict(Counter(record['verdict'] for record in tap.records.values())),
        'credits_in_window': round(credits, 1),
        'credits_per_month': round(credits / seconds * SECONDS_PER_MONTH) if seconds else None,
        'error': tap.error,
    }


def compare(program, creations):
    """What the two streams agree and disagree on, inside the slot range both were live for."""
    if program.first_slot is None or creations.first_slot is None:
        return {'window': None, 'launches': 0, 'missed': [], 'prefilter_skips': [],
                'prefilter_skips_by_side': {'program': 0, 'creations': 0}, 'unknown_on_program': [],
                'unknown_on_creations': [], 'creations_only': [], 'creations_failed_attempts': 0,
                'creations_successful': 0, 'latency_seconds': None}
    lo = max(program.first_slot, creations.first_slot) + MARGIN_SLOTS
    hi = min(program.last_slot, creations.last_slot) - MARGIN_SLOTS

    def inside(record):
        return lo <= record['slot'] <= hi
    launches = {sig for sig, r in program.records.items() if r['verdict'] == 'creation' and inside(r)}
    missed = sorted(launches - set(creations.records), key=lambda sig: program.records[sig]['slot'])
    successful = {sig for sig, r in creations.records.items() if r['verdict'] != 'failed' and inside(r)}
    failed = sum(1 for r in creations.records.values() if r['verdict'] == 'failed' and inside(r))
    # The filter is applied to whatever the worker's own stream delivers, so check both views of a launch.
    skipped_creations_side = {sig for sig in successful if creations.records[sig]['verdict'] == 'other'}
    skipped_program_side = {sig for sig in successful
                            if sig in program.records and program.records[sig]['verdict'] == 'other'}
    on_both = [sig for sig in creations.records if sig in program.records and
               inside(creations.records[sig]) and inside(program.records[sig])]
    latency = [creations.records[sig]['at'] - program.records[sig]['at'] for sig in on_both]
    return {
        'window': {'first_slot': lo, 'last_slot': hi},
        'launches': len(launches),
        'missed': missed,
        # successful launches (they mention the authority) whose logs the worker's filter would skip
        'prefilter_skips': sorted(skipped_creations_side | skipped_program_side),
        'prefilter_skips_by_side': {'program': len(skipped_program_side), 'creations': len(skipped_creations_side)},
        # ... and launches whose logs are missing/truncated: the worker fetches those, so they are not misses
        'unknown_on_program': sorted(sig for sig in successful
                                     if sig in program.records and program.records[sig]['verdict'] == 'unknown'),
        'unknown_on_creations': sorted(sig for sig in successful if creations.records[sig]['verdict'] == 'unknown'),
        # successful on the creations stream, never delivered by the program stream at all
        'creations_only': sorted(sig for sig in successful if sig not in program.records),
        'creations_successful': len(successful),
        'creations_failed_attempts': failed,
        'latency_seconds': ({'matched': len(latency), 'median': round(percentile(latency, 0.5), 3),
                             'p95': round(percentile(latency, 0.95), 3), 'min': round(min(latency), 3),
                             'max': round(max(latency), 3)} if latency else None),
    }


def authority_position(tx):
    """Where Pump's mint authority appears in a getTransaction result."""
    message = tx['transaction']['message']
    static = [key['pubkey'] if isinstance(key, dict) else key for key in message['accountKeys']]
    loaded = tx['meta'].get('loadedAddresses') or {}
    resolved = list(loaded.get('writable') or []) + list(loaded.get('readonly') or [])
    in_static, in_loaded = MINT_AUTHORITY in static, MINT_AUTHORITY in resolved
    return 'both' if in_static and in_loaded else 'static' if in_static else 'loaded' if in_loaded else 'absent'


def choose_sample(program, creations, result, count):
    """Every missed launch first, then launches spread evenly across the window."""
    chosen = list(result['missed'][:count])
    pool = sorted((sig for sig, r in program.records.items() if r['verdict'] == 'creation'
                   and result['window'] and result['window']['first_slot'] <= r['slot'] <= result['window']['last_slot']
                   and sig not in chosen), key=lambda sig: program.records[sig]['slot'])
    room = count - len(chosen)
    if room > 0 and pool:
        step = max(1, len(pool) // room)
        chosen += pool[::step][:room]
    return chosen


def inspect_sample(http_url, signatures, delivered):
    """Fetch each signature once (bounded) and record where the authority account appears.

    Fetching is the only RPC use; every fetch is a few credits at most. ``delivered`` is the set
    of signatures the creations stream delivered.
    """
    settings = Settings(fetch_attempts=3, fetch_backoff=(0.5, 1.0))
    where, delivered_where, failures, versions = Counter(), Counter(), 0, Counter()
    for signature in signatures:
        try:
            tx = fetch_with_retry(http_url, signature, settings)
            position = authority_position(tx)
        except Exception:  # noqa: BLE001 - class-only reporting, text can embed the URL
            failures += 1
            continue
        where[position] += 1
        versions[str(tx.get('version', 'legacy'))] += 1
        if signature in delivered:
            delivered_where[position] += 1
    return {'sampled': len(signatures), 'fetch_failed': failures, 'authority_listed': dict(where),
            'delivered_by_creations_stream': dict(delivered_where), 'transaction_versions': dict(versions)}


def assess(report):
    """A plain-language reading of the numbers, including what the sample could not show."""
    program, creations, compared = report['streams']['program'], report['streams']['creations'], report['compare']
    if program['error'] or creations['error']:
        return 'inconclusive: a subscription failed (' + ', '.join(
            f"{name} stream {report['streams'][name]['error']}" for name in ('program', 'creations')
            if report['streams'][name]['error']) + ')'
    launches = compared['launches']
    if launches < MIN_LAUNCHES:
        return (f'inconclusive: only {launches} launches in the window; fewer than {MIN_LAUNCHES} cannot '
                'rule out a miss rate of a few percent. Run again with a longer window or larger byte budget.')
    problems = []
    if compared['missed']:
        problems.append(f"the creations stream missed {len(compared['missed'])} of {launches} launches")
    if compared['prefilter_skips']:
        problems.append(f"the log filter would skip {len(compared['prefilter_skips'])} real launches "
                        f"({compared['prefilter_skips_by_side']})")
    lookup = report['lookup_tables']
    if lookup['authority_listed'].get('absent'):
        problems.append(f"{lookup['authority_listed']['absent']} sampled launches do not include the authority at all")
    if problems:
        return 'do not enable the creations stream: ' + '; '.join(problems)
    notes = []
    if compared['creations_only']:
        notes.append(f"{len(compared['creations_only'])} successful transactions reached only the creations stream "
                     '(window edges or a program-stream gap; not a problem for the narrow stream)')
    loaded = lookup['authority_listed'].get('loaded', 0)
    if loaded:
        notes.append(f"{loaded} sampled launches resolved the authority through a lookup table and were still "
                     f"delivered ({lookup['delivered_by_creations_stream'].get('loaded', 0)} of {loaded})")
    else:
        notes.append('no sampled launch used a lookup table for the authority, so that case is untested')
    return (f'no misses in {launches} launches (95% upper bound on the miss rate about {3 / launches:.1%}); '
            'the creations stream looks safe to try. ' + '; '.join(notes) + '.')


def build_report(program, creations, stopped_by, requested, lookup, rpc_calls_estimate):
    result = compare(program, creations)
    streams = {'program': summarize(program), 'creations': summarize(creations)}
    spend = (streams['program']['credits_in_window'] + streams['creations']['credits_in_window']
             + 2 + rpc_calls_estimate)          # 1 credit per connection, ~1 per fetch
    report = {'request': requested, 'stopped_by': stopped_by, 'streams': streams, 'compare': result,
              'lookup_tables': lookup, 'estimated_credits_spent': round(spend)}
    report['assessment'] = assess(report)
    return report


def annotation_lines(report):
    """At most eight short GitHub Actions annotations (public, so no provider details)."""
    streams, compared, lookup = report['streams'], report['compare'], report['lookup_tables']
    program, creations = streams['program'], streams['creations']

    def megabytes(tap):
        return f"{tap['bytes'] / 1e6:.1f} MB"
    lines = [('notice', f"Window: {program['seconds']:g}s, stopped by {report['stopped_by']}, slots "
                        f"{(compared['window'] or {}).get('first_slot')}-{(compared['window'] or {}).get('last_slot')}; "
                        f"estimated credits spent about {report['estimated_credits_spent']}"),
             ('notice', f"Program stream: {program['notifications']} notifications ({program['per_second']}/s), "
                        f"{megabytes(program)}, average {program['average_bytes']} bytes, about "
                        f"{program['credits_per_month']} credits/month at this rate; verdicts {program['verdicts']}"),
             ('notice', f"Creations stream: {creations['notifications']} notifications ({creations['per_second']}/s), "
                        f"{megabytes(creations)}, average {creations['average_bytes']} bytes, about "
                        f"{creations['credits_per_month']} credits/month at this rate")]
    missed = compared['missed']
    lines.append(('warning' if missed else 'notice',
                  f"Launches on the program stream: {compared['launches']}; missed by the creations stream: "
                  f"{len(missed)}" + (f" (first: {', '.join(missed[:3])})" if missed else '')))
    lines.append(('warning' if (compared['prefilter_skips'] or compared['creations_only']) else 'notice',
                  f"Creations stream extras: failed attempts {compared['creations_failed_attempts']}, successful "
                  f"launches absent from the program stream {len(compared['creations_only'])}, launches the log "
                  f"filter would skip {len(compared['prefilter_skips'])}, with missing/truncated logs "
                  f"{len(compared['unknown_on_creations'])}"))
    if compared['latency_seconds']:
        lines.append(('notice', 'Arrival of the same launch, creations minus program stream (seconds): '
                                + json.dumps(compared['latency_seconds'])))
    lines.append(('notice', f"Lookup tables: sampled {lookup['sampled']} launches (fetch failures "
                            f"{lookup['fetch_failed']}); authority listed {lookup['authority_listed']}, delivered by "
                            f"the creations stream {lookup['delivered_by_creations_stream']}, versions "
                            f"{lookup['transaction_versions']}"))
    lines.append(('warning' if report['assessment'].startswith(('do not', 'inconclusive')) else 'notice',
                  'Assessment: ' + report['assessment']))
    return [f'::{level}::{text[:900]}' for level, text in lines]


def summary_markdown(report):
    """The same findings as a small table for the Actions run page."""
    streams, compared, lookup = report['streams'], report['compare'], report['lookup_tables']
    program, creations = streams['program'], streams['creations']
    rows = [('notifications', program['notifications'], creations['notifications']),
            ('megabytes streamed', f"{program['bytes'] / 1e6:.1f}", f"{creations['bytes'] / 1e6:.1f}"),
            ('average bytes per notification', program['average_bytes'], creations['average_bytes']),
            ('credits per month at this rate (estimate)', program['credits_per_month'], creations['credits_per_month'])]
    lines = ['### Pump.fun log subscriptions: whole program vs mint authority (bounded, read-only)', '',
             f"**Assessment:** {report['assessment']}", '', '| | program stream | creations stream |', '|---|---|---|']
    lines += [f'| {name} | {a} | {b} |' for name, a, b in rows]
    lines += ['', f"- Launches on the program stream: {compared['launches']}; missed by the creations stream: "
                  f"{len(compared['missed'])}",
              f"- The log filter would skip {len(compared['prefilter_skips'])} real launches "
              f"({compared['prefilter_skips_by_side']}); logs missing or truncated on the creations stream: "
              f"{len(compared['unknown_on_creations'])}",
              f"- Lookup tables: {lookup['sampled']} launches sampled, authority listed {lookup['authority_listed']}, "
              f"delivered by the creations stream {lookup['delivered_by_creations_stream']}",
              f"- Stopped by: {report['stopped_by']}; estimated credits spent: {report['estimated_credits_spent']}", '']
    return '\n'.join(lines)


def read_request(path):
    """Parse ``key=value`` lines (seconds, max_megabytes, sample; others ignored if they start with # or run=)."""
    request = dict(DEFAULTS)
    with open(path) as handle:
        for number, line in enumerate(handle, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, _, value = line.partition('=')
            key, value = key.strip(), value.strip()
            if key == 'run':
                continue                      # a counter people bump to start another comparison
            if key not in LIMITS:
                raise ValueError(f'{path}:{number}: unknown setting {key!r}')
            try:
                number_value = int(value)
            except ValueError:
                raise ValueError(f'{path}:{number}: {key} must be a whole number') from None
            low, high = LIMITS[key]
            if not low <= number_value <= high:
                raise ValueError(f'{path}:{number}: {key} must be between {low} and {high}')
            request[key] = number_value
    return request


async def run(http_url, request):
    program, creations, stopped_by = await collect(http_url, request['seconds'], request['max_megabytes'] * 1_000_000)
    result = compare(program, creations)
    chosen = choose_sample(program, creations, result, request['sample']) if request['sample'] else []
    lookup = await asyncio.to_thread(inspect_sample, http_url, chosen, set(creations.records))
    return build_report(program, creations, stopped_by, request, lookup, len(chosen))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Compare Pump.fun log subscriptions on live data (read-only)')
    parser.add_argument('--request', help='file with seconds=, max_megabytes=, sample= lines')
    parser.add_argument('--seconds', type=int)
    parser.add_argument('--max-megabytes', type=int, dest='max_megabytes')
    parser.add_argument('--sample', type=int)
    parser.add_argument('--output', default='stream-compare.json')
    parser.add_argument('--annotate', action='store_true', help='print GitHub Actions annotations')
    parser.add_argument('--summary', help='append a markdown summary to this file ($GITHUB_STEP_SUMMARY)')
    args = parser.parse_args(argv)
    try:
        request = read_request(args.request) if args.request else dict(DEFAULTS)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    for key in LIMITS:
        value = getattr(args, key)
        if value is not None:
            low, high = LIMITS[key]
            if not low <= value <= high:
                parser.error(f'{key} must be between {low} and {high}')
            request[key] = value
    http_url = os.environ.get('SOLANA_RPC_URL')
    if not http_url:
        parser.error('Set SOLANA_RPC_URL privately; do not paste an API key into the command.')
    try:
        websocket_url(http_url)
    except ValueError:
        parser.error('SOLANA_RPC_URL must be an HTTPS Helius Mainnet endpoint.')
    try:
        report = asyncio.run(run(http_url, request))
    except Exception as exc:  # noqa: BLE001 - class only: text can embed the provider URL
        print(f'Comparison could not complete ({type(exc).__name__}). Check provider quota and connectivity.',
              file=sys.stderr)
        return 1
    with open(args.output, 'w') as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps({key: report[key] for key in ('stopped_by', 'estimated_credits_spent', 'assessment')}, indent=2))
    if args.annotate:
        print('\n'.join(annotation_lines(report)))
    if args.summary:
        with open(args.summary, 'a') as handle:
            handle.write(summary_markdown(report))
    failed = report['streams']['program']['error'] or report['streams']['creations']['error']
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
