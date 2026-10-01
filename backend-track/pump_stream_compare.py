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
  6. skips           for every successful transaction the log filter would skip: is it a launch
                     after all (does the decoder accept it)? which instructions and programs ran?

``streams=creations`` runs only the cheap subscription (about 15 credits per 200 s) to investigate
questions 2, 4, 5 and 6 without paying for the whole-program stream.

It is hard-capped in time AND in streamed bytes, so the spend has a known ceiling. It issues
``logsSubscribe`` twice plus a few ``getTransaction`` calls, never builds or sends a transaction,
and never prints the provider URL (exception text can embed it, so only classes are reported).
"""
import argparse
import asyncio
import json
import math
import os
import re
import sys
import time
from collections import Counter

import websockets

from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM, log_verdict, parse_launch
from pump_worker import Settings, fetch_with_retry, websocket_url

BYTES_PER_UNIT = 100_000          # Helius meters 2 credits per 0.1 MB of uncompressed streamed data
CREDITS_PER_UNIT = 2              # (if the provider means MiB the true cost is about 5% lower)
SECONDS_PER_MONTH = 30 * 86_400
MARGIN_SLOTS = 2                  # ignore launches within this many slots of either stream's edges
MIN_LAUNCHES = 50                 # below this a clean result cannot rule out a few percent of misses
LIMITS = {'seconds': (30, 900), 'max_megabytes': (1, 60), 'sample': (0, 60)}
CHOICES = {'streams': ('both', 'creations')}      # `creations` skips the expensive whole-program stream
DEFAULTS = {'seconds': 300, 'max_megabytes': 40, 'sample': 25, 'streams': 'both'}
SKIPS_DIAGNOSED = 5                               # how many log-filter skips get a full diagnosis
MAX_ANNOTATIONS = 9                               # GitHub drops annotations beyond ten per step


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


async def collect(http_url, seconds, max_bytes, streams='both'):
    """Run the subscriptions until the time or the byte budget runs out."""
    stop = asyncio.Event()
    program, creations = Tap('program', PUMP_PROGRAM), Tap('creations', MINT_AUTHORITY)

    async def timer():
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
        stop.set()
    listeners = [listen(http_url, creations, stop, max_bytes)]
    if streams == 'both':                      # the whole-program stream is where the credits go
        listeners.append(listen(http_url, program, stop, max_bytes))
    await asyncio.gather(*listeners, timer())
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


def programs_invoked(tx):
    """Every program the transaction invoked, top-level or inner, as a sorted list of addresses."""
    message = tx['transaction']['message']
    keys = [key['pubkey'] if isinstance(key, dict) else key for key in message['accountKeys']]
    loaded = tx['meta'].get('loadedAddresses') or {}
    keys += list(loaded.get('writable') or []) + list(loaded.get('readonly') or [])
    instructions = list(message.get('instructions') or [])
    for inner in tx['meta'].get('innerInstructions') or []:
        instructions += inner.get('instructions') or []
    return sorted({keys[ix['programIdIndex']] for ix in instructions})


INSTRUCTION_LOG = re.compile(r'^Program log: Instruction: (\w+)$')


def instruction_names(tx, limit=12):
    """The instruction names the programs logged, in order (a launch logs Create or CreateV2)."""
    names = []
    for line in tx['meta'].get('logMessages') or []:
        match = INSTRUCTION_LOG.match(line.strip())
        if match and match.group(1) not in names:
            names.append(match.group(1))
    return names[:limit]


def diagnose(tx, signature):
    """Why would the log filter skip this transaction, and is it a launch after all?"""
    logs = tx['meta'].get('logMessages') or []
    return {'signature': signature, 'decoder_accepts': parse_launch(tx) is not None,
            'instructions': instruction_names(tx), 'programs': programs_invoked(tx),
            'authority': authority_position(tx), 'version': str(tx.get('version', 'legacy')),
            'log_lines': len(logs), 'log_head': [line[:110] for line in logs[:8]]}


def probe(creations):
    """What the creations stream shows on its own (used when the program stream is not run)."""
    if creations.first_slot is None:
        return {'window': None, 'successful': 0, 'failed_attempts': 0, 'skips': [], 'unknown': []}
    lo, hi = creations.first_slot + MARGIN_SLOTS, creations.last_slot - MARGIN_SLOTS
    inside = {sig: r for sig, r in creations.records.items() if lo <= r['slot'] <= hi}
    successful = {sig: r for sig, r in inside.items() if r['verdict'] != 'failed'}
    return {'window': {'first_slot': lo, 'last_slot': hi}, 'successful': len(successful),
            'failed_attempts': len(inside) - len(successful),
            'skips': sorted(sig for sig, r in successful.items() if r['verdict'] == 'other'),
            'unknown': sorted(sig for sig, r in successful.items() if r['verdict'] == 'unknown')}


def choose_sample(priority, candidates, count):
    """The priority signatures first (misses, filter skips), then others spread evenly in order."""
    chosen = list(dict.fromkeys(priority))[:count]
    pool = [sig for sig in candidates if sig not in chosen]
    room = count - len(chosen)
    if room > 0 and pool:
        chosen += pool[::max(1, len(pool) // room)][:room]
    return chosen


def inspect_sample(http_url, signatures, delivered, diagnose_for=()):
    """Fetch each signature once (bounded); record where the authority appears and whether the
    decoder accepts it as a launch, and fully diagnose those in ``diagnose_for``.

    Fetching is the only RPC use, at most a few credits per signature. ``delivered`` is the set of
    signatures the creations stream delivered (None when the program stream was not run).
    """
    settings = Settings(fetch_attempts=3, fetch_backoff=(0.5, 1.0))
    where, delivered_where, versions = Counter(), Counter(), Counter()
    decoded = failures = 0
    details = []
    for signature in signatures:
        try:
            tx = fetch_with_retry(http_url, signature, settings)
            position = authority_position(tx)
            accepted = parse_launch(tx) is not None
            if signature in diagnose_for and len(details) < SKIPS_DIAGNOSED:
                details.append(diagnose(tx, signature))
        except Exception:  # noqa: BLE001 - class-only reporting, text can embed the URL
            failures += 1
            continue
        where[position] += 1
        decoded += accepted
        versions[str(tx.get('version', 'legacy'))] += 1
        if delivered is not None and signature in delivered:
            delivered_where[position] += 1
    return ({'sampled': len(signatures), 'fetch_failed': failures, 'authority_listed': dict(where),
             'delivered_by_creations_stream': dict(delivered_where) if delivered is not None else None,
             'transaction_versions': dict(versions), 'decoded_as_launch': decoded,
             'not_a_launch': sum(where.values()) - decoded}, details)


def filter_sentence(skips, details):
    """The program-mode log filter's finding, in words (the creations stream does not use that filter)."""
    if not skips:
        return ''
    accepted = [d for d in details if d['decoder_accepts']]
    inspected = f" Of the {len(details)} inspected, {len(accepted)} decode as launches" if details else ''
    return (f" Separately, the program-mode log filter would skip {len(skips)} successful transactions that mention "
            f"the authority.{inspected}"
            + (' (real launches the program stream would miss)' if accepted else ' (none is a launch)' if details else '')
            + '; the creations stream does not use that filter.')


def assess(report):
    """A plain-language reading of the numbers, including what the sample could not show."""
    streams = report['streams']
    errors = [f"{name} stream {streams[name]['error']}" for name in ('program', 'creations') if streams[name]['error']]
    if errors:
        return 'inconclusive: a subscription failed (' + ', '.join(errors) + ')'
    lookup, details = report['lookup_tables'], report.get('filter_skip_details') or []
    decoded = lookup.get('decoded_as_launch')
    decode_note = (f" {decoded} of {lookup['sampled'] - lookup['fetch_failed']} sampled transactions decode as launches."
                   if decoded is not None and lookup['sampled'] else '')
    if report.get('mode') == 'creations':
        probed = report['probe']
        return (f"creations-only probe: {probed['successful']} successful transactions mention the authority "
                f"({probed['failed_attempts']} failed attempts).{decode_note}"
                + (filter_sentence(probed['skips'], details) or ' None would be skipped by the log filter.'))
    compared = report['compare']
    launches = compared['launches']
    if launches < MIN_LAUNCHES:
        return (f'inconclusive: only {launches} launches in the window; fewer than {MIN_LAUNCHES} cannot '
                'rule out a miss rate of a few percent. Run again with a longer window or larger byte budget.')
    problems = []
    if compared['missed']:
        problems.append(f"the creations stream missed {len(compared['missed'])} of {launches} launches")
    if lookup['authority_listed'].get('absent'):
        problems.append(f"{lookup['authority_listed']['absent']} sampled launches do not include the authority at all")
    if problems:
        return 'do not enable the creations stream: ' + '; '.join(problems) + '.' + filter_sentence(
            compared['prefilter_skips'], details)
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
            'the creations stream looks safe to try. ' + '; '.join(notes) + '.' + decode_note
            + filter_sentence(compared['prefilter_skips'], details))


def build_report(mode, program, creations, stopped_by, requested, lookup, details, rpc_calls_estimate):
    streams = {'program': summarize(program), 'creations': summarize(creations)}
    spend = (streams['program']['credits_in_window'] + streams['creations']['credits_in_window']
             + (2 if mode == 'both' else 1) + rpc_calls_estimate)       # 1 credit per connection, ~1 per fetch
    report = {'mode': mode, 'request': requested, 'stopped_by': stopped_by, 'streams': streams,
              'lookup_tables': lookup, 'filter_skip_details': details, 'estimated_credits_spent': round(spend)}
    report['compare' if mode == 'both' else 'probe'] = compare(program, creations) if mode == 'both' else probe(creations)
    report['assessment'] = assess(report)
    return report


def describe_skip(detail):
    names = ','.join(detail['instructions']) or 'none'
    programs = ','.join(p[:6] for p in detail['programs'])
    return (f"{detail['signature'][:12]}.. decoder_accepts_launch={detail['decoder_accepts']} instructions=[{names}] "
            f"programs=[{programs}] authority={detail['authority']} version={detail['version']} "
            f"log_lines={detail['log_lines']}")


def annotation_lines(report):
    """At most nine short GitHub Actions annotations (public, so no provider details)."""
    streams, lookup = report['streams'], report['lookup_tables']
    program, creations = streams['program'], streams['creations']
    both = report.get('mode', 'both') == 'both'
    window = (report['compare'] if both else report['probe'])['window'] or {}

    def megabytes(tap):
        return f"{tap['bytes'] / 1e6:.1f} MB"
    lines = [('notice', f"Window ({'both streams' if both else 'creations stream only'}): {creations['seconds']:g}s, "
                        f"stopped by {report['stopped_by']}, slots {window.get('first_slot')}-{window.get('last_slot')}; "
                        f"estimated credits spent about {report['estimated_credits_spent']}")]
    if both:
        lines.append(('notice', f"Program stream: {program['notifications']} notifications ({program['per_second']}/s), "
                                f"{megabytes(program)}, average {program['average_bytes']} bytes, about "
                                f"{program['credits_per_month']} credits/month at this rate; verdicts {program['verdicts']}"))
    lines.append(('notice', f"Creations stream: {creations['notifications']} notifications ({creations['per_second']}/s), "
                            f"{megabytes(creations)}, average {creations['average_bytes']} bytes, about "
                            f"{creations['credits_per_month']} credits/month at this rate; verdicts {creations['verdicts']}"))
    if both:
        compared = report['compare']
        missed = compared['missed']
        lines.append(('warning' if missed else 'notice',
                      f"Launches on the program stream: {compared['launches']}; missed by the creations stream: "
                      f"{len(missed)}" + (f" (first: {', '.join(missed[:3])})" if missed else '')))
        lines.append(('warning' if (compared['prefilter_skips'] or compared['creations_only']) else 'notice',
                      f"Creations stream extras: failed attempts {compared['creations_failed_attempts']}, successful "
                      f"transactions absent from the program stream {len(compared['creations_only'])}, the log filter "
                      f"would skip {len(compared['prefilter_skips'])} ({compared['prefilter_skips_by_side']}), with "
                      f"missing/truncated logs {len(compared['unknown_on_creations'])}"))
        if compared['latency_seconds']:
            lines.append(('notice', 'Arrival of the same launch, creations minus program stream (seconds): '
                                    + json.dumps(compared['latency_seconds'])))
    else:
        probed = report['probe']
        lines.append(('warning' if probed['skips'] else 'notice',
                      f"Probe: {probed['successful']} successful transactions, {probed['failed_attempts']} failed "
                      f"attempts; the log filter would skip {len(probed['skips'])}; missing/truncated logs "
                      f"{len(probed['unknown'])}"))
    lines.append(('notice', f"Lookup tables and decoding: sampled {lookup['sampled']} (fetch failures "
                            f"{lookup['fetch_failed']}); authority listed {lookup['authority_listed']}"
                            + (f", delivered by the creations stream {lookup['delivered_by_creations_stream']}" if both else '')
                            + f"; decoded as launches {lookup['decoded_as_launch']}, not launches {lookup['not_a_launch']}; "
                              f"versions {lookup['transaction_versions']}"))
    # GitHub shows at most ten annotations per step. The assessment is the line that matters most, so it
    # is reserved first and the skip diagnoses (also in the summary and the JSON) share what is left.
    room = max(0, min(3, MAX_ANNOTATIONS - 1 - len(lines)))
    for detail in report['filter_skip_details'][:room]:
        lines.append(('warning', 'Log-filter skip: ' + describe_skip(detail)))
    lines.append(('warning' if report['assessment'].startswith(('do not', 'inconclusive')) else 'notice',
                  'Assessment: ' + report['assessment']))
    return [f'::{level}::{text[:900]}' for level, text in lines]


def summary_markdown(report):
    """The same findings as a small table for the Actions run page."""
    streams, lookup = report['streams'], report['lookup_tables']
    program, creations = streams['program'], streams['creations']
    both = report.get('mode', 'both') == 'both'
    rows = [('notifications', program['notifications'], creations['notifications']),
            ('megabytes streamed', f"{program['bytes'] / 1e6:.1f}", f"{creations['bytes'] / 1e6:.1f}"),
            ('average bytes per notification', program['average_bytes'], creations['average_bytes']),
            ('credits per month at this rate (estimate)', program['credits_per_month'], creations['credits_per_month'])]
    lines = ['### Pump.fun log subscriptions: whole program vs mint authority (bounded, read-only)', '',
             f"**Assessment:** {report['assessment']}", '', '| | program stream | creations stream |', '|---|---|---|']
    lines += [f'| {name} | {a if both else "not run"} | {b} |' for name, a, b in rows]
    if both:
        compared = report['compare']
        lines += ['', f"- Launches on the program stream: {compared['launches']}; missed by the creations stream: "
                      f"{len(compared['missed'])}",
                  f"- The log filter would skip {len(compared['prefilter_skips'])} successful transactions "
                  f"({compared['prefilter_skips_by_side']}); logs missing or truncated on the creations stream: "
                  f"{len(compared['unknown_on_creations'])}"]
    else:
        probed = report['probe']
        lines += ['', f"- Successful transactions: {probed['successful']}; failed attempts: {probed['failed_attempts']}; "
                      f"the log filter would skip {len(probed['skips'])}"]
    lines.append(f"- Lookup tables and decoding: {lookup['sampled']} sampled, authority listed "
                 f"{lookup['authority_listed']}, decoded as launches {lookup['decoded_as_launch']}, "
                 f"not launches {lookup['not_a_launch']}")
    lines += [f"- Log-filter skip: `{describe_skip(detail)}`" for detail in report['filter_skip_details']]
    lines += [f"- Stopped by: {report['stopped_by']}; estimated credits spent: {report['estimated_credits_spent']}", '']
    return '\n'.join(lines)


def read_request(path):
    """Parse ``key=value`` lines: seconds, max_megabytes, sample, streams (and a ``run=`` counter)."""
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
            if key in CHOICES:
                if value not in CHOICES[key]:
                    raise ValueError(f"{path}:{number}: {key} must be one of: {', '.join(CHOICES[key])}")
                request[key] = value
                continue
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
    mode = request.get('streams', 'both')
    program, creations, stopped_by = await collect(http_url, request['seconds'], request['max_megabytes'] * 1_000_000, mode)
    if mode == 'both':
        result = compare(program, creations)
        window = result['window']
        in_window = sorted((sig for sig, r in program.records.items() if r['verdict'] == 'creation' and window
                            and window['first_slot'] <= r['slot'] <= window['last_slot']),
                           key=lambda sig: program.records[sig]['slot'])
        priority = list(result['missed']) + list(result['prefilter_skips'])
        delivered, skips = set(creations.records), set(result['prefilter_skips'])
    else:
        result = probe(creations)
        window = result['window']
        in_window = sorted((sig for sig, r in creations.records.items() if r['verdict'] != 'failed' and window
                            and window['first_slot'] <= r['slot'] <= window['last_slot']),
                           key=lambda sig: creations.records[sig]['slot'])
        priority = list(result['skips']) + list(result['unknown'])
        delivered, skips = None, set(result['skips'])
    chosen = choose_sample(priority, in_window, request['sample']) if request['sample'] else []
    lookup, details = await asyncio.to_thread(inspect_sample, http_url, chosen, delivered, skips)
    return build_report(mode, program, creations, stopped_by, request, lookup, details, len(chosen))


def main(argv=None):
    parser = argparse.ArgumentParser(description='Compare Pump.fun log subscriptions on live data (read-only)')
    parser.add_argument('--request', help='file with seconds=, max_megabytes=, sample=, streams= lines')
    parser.add_argument('--seconds', type=int)
    parser.add_argument('--max-megabytes', type=int, dest='max_megabytes')
    parser.add_argument('--sample', type=int)
    parser.add_argument('--streams', choices=CHOICES['streams'])
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
    if args.streams:
        request['streams'] = args.streams
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
    print(json.dumps({key: report[key] for key in ('mode', 'stopped_by', 'estimated_credits_spent', 'assessment')}, indent=2))
    if args.annotate:
        print('\n'.join(annotation_lines(report)))
    if args.summary:
        with open(args.summary, 'a') as handle:
            handle.write(summary_markdown(report))
    failed = report['streams']['creations']['error'] or (report['mode'] == 'both' and report['streams']['program']['error'])
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
