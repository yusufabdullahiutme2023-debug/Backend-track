"""The live stream comparison tool: its arithmetic, its verdicts, and its behaviour on the wire.

Everything runs against fakes. The end-to-end tests drive the real WebSocket code through a local
server that serves different traffic to the two subscriptions, so the byte counting, the slot
window, the miss detection and the lookup-table diagnosis are all exercised without a provider.
"""
import asyncio
import json
import pathlib
import time

import pytest
import yaml

import pump_stream_compare as compare_tool
import pump_worker
from pump_fakes import (BUY_LOGS, CREATE_LOGS, FakeNode, FakeRpcServer, FakeStream, creation_tx,
                        notification, plain_tx, signature)
from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM

FAILED = {'InstructionError': [0, 'Custom']}


def tap_with(address, *messages):
    tap = compare_tool.Tap('t', address)
    for index, message in enumerate(messages):
        tap.take(message, float(index))
    return tap


# --- one subscription's bookkeeping ---------------------------------------------------------

def test_tap_counts_every_byte_but_only_notifications():
    ascii_note = notification('a' * 64, BUY_LOGS, slot=5)
    unicode_note = notification('b' * 64, ['Program log: h\u00e9llo'], slot=6).replace('\\u00e9', '\u00e9')   # a real é
    ack = json.dumps({'jsonrpc': '2.0', 'result': 3, 'id': 1})
    tap = tap_with(PUMP_PROGRAM, ascii_note, unicode_note, ack)
    assert tap.bytes == len(ascii_note.encode()) + len(unicode_note.encode()) + len(ack.encode())
    assert len(unicode_note.encode()) > len(unicode_note)          # multi-byte characters are billed as bytes
    assert tap.notifications == 2 and tap.malformed == 0
    assert (tap.first_slot, tap.last_slot) == (5, 6)


def test_tap_keeps_the_first_sighting_and_classifies_what_it_saw():
    sig = 'c' * 64
    tap = compare_tool.Tap('t', PUMP_PROGRAM)
    tap.take(notification(sig, CREATE_LOGS, slot=10), 1.0)
    tap.take(notification(sig, BUY_LOGS, slot=11), 2.0)            # a duplicate delivery changes nothing
    tap.take(notification('d' * 64, BUY_LOGS, err=FAILED, slot=12), 3.0)
    tap.take(notification('e' * 64, BUY_LOGS, slot=13), 4.0)
    tap.take(notification('f' * 64, None, slot=14), 5.0)
    tap.take(notification('g' * 64, BUY_LOGS + ['Log truncated'], slot=15), 6.0)
    verdicts = {sig: record['verdict'] for sig, record in tap.records.items()}
    assert verdicts == {sig: 'creation', 'd' * 64: 'failed', 'e' * 64: 'other', 'f' * 64: 'unknown', 'g' * 64: 'unknown'}
    assert tap.records[sig]['at'] == 1.0 and tap.notifications == 6


@pytest.mark.parametrize('frame', ['not json', '[]', json.dumps({'method': 'logsNotification', 'params': {}}),
                                   json.dumps({'method': 'logsNotification', 'params': {'result': {
                                       'context': {'slot': 'x'}, 'value': {'signature': 's', 'err': None}}}})])
def test_tap_counts_unreadable_frames_without_crashing(frame):
    tap = tap_with(PUMP_PROGRAM, frame)
    assert tap.notifications == 0 and tap.bytes == len(frame) and tap.malformed == (0 if frame == '[]' else 1)


# --- comparison -----------------------------------------------------------------------------

def stream_pair(program_notes, creation_notes):
    return (tap_with(PUMP_PROGRAM, *program_notes), tap_with(MINT_AUTHORITY, *creation_notes))


def test_a_launch_the_creations_stream_missed_is_reported_inside_the_common_window():
    # Both streams were live from slot 100 to 130; launches land at 110..114 and #12 never reaches the
    # creations stream. (Edge frames: a trade on the program stream, a failed attempt on the other.)
    launches = [notification(signature(10 + n), CREATE_LOGS, slot=110 + n) for n in range(5)]
    trades = [notification(signature(50 + n), BUY_LOGS, slot=100 + 2 * n) for n in range(15)]
    program = [notification(signature(98), BUY_LOGS, slot=100)] + trades + launches + [
        notification(signature(99), BUY_LOGS, slot=130)]
    creations = ([notification(signature(97), CREATE_LOGS, err=FAILED, slot=100)]
                 + [launches[0], launches[1], launches[3], launches[4]]
                 + [notification(signature(96), CREATE_LOGS, err=FAILED, slot=130)])
    result = compare_tool.compare(*stream_pair(program, creations))
    assert result['window'] == {'first_slot': 102, 'last_slot': 128}
    assert result['launches'] == 5 and result['missed'] == [signature(12)]
    assert result['creations_failed_attempts'] == 0        # the two failed frames sit on the window's edges


def test_launches_outside_the_common_window_are_not_counted_as_misses():
    program, creations = stream_pair(
        [notification(signature(1), CREATE_LOGS, slot=100), notification(signature(2), CREATE_LOGS, slot=150),
         notification(signature(3), CREATE_LOGS, slot=200)],
        [notification(signature(9), CREATE_LOGS, slot=140), notification(signature(8), CREATE_LOGS, slot=160)])
    result = compare_tool.compare(program, creations)
    # The creations stream only existed for slots 140-160, so launches at 100 and 200 say nothing about it.
    assert result['window'] == {'first_slot': 142, 'last_slot': 158}
    assert result['launches'] == 1 and result['missed'] == [signature(2)]       # slot 150 IS inside, and IS missing
    assert signature(1) not in result['missed'] and signature(3) not in result['missed']


def test_the_log_filter_is_checked_from_both_sides():
    shared = signature(1)
    program, creations = stream_pair(
        [notification(signature(0), BUY_LOGS, slot=100), notification(shared, BUY_LOGS, slot=110),
         notification(signature(2), CREATE_LOGS, slot=111), notification(signature(3), CREATE_LOGS, slot=112),
         notification(signature(4), BUY_LOGS, slot=130)],
        [notification(signature(0), BUY_LOGS, slot=100), notification(shared, CREATE_LOGS, slot=110),
         notification(signature(2), BUY_LOGS, slot=111),          # program side saw a creation line, creations side did not
         notification(signature(3), CREATE_LOGS + ['Log truncated'], slot=112),
         notification(signature(5), None, slot=115), notification(signature(6), CREATE_LOGS, slot=116),
         notification(signature(7), CREATE_LOGS, slot=129)])
    result = compare_tool.compare(program, creations)
    # The window is slots 102-127, so signature(0) (slot 100) and signature(7) (slot 129) are edge frames.
    # signature(1): the program stream's own logs show no creation line -> program-mode filter would skip it.
    # signature(2): the creations stream's logs lack the line -> creations-mode filter would skip it.
    assert result['window'] == {'first_slot': 102, 'last_slot': 127}
    assert result['prefilter_skips'] == sorted([shared, signature(2)])
    assert result['prefilter_skips_by_side'] == {'program': 1, 'creations': 1}
    # signature(3) is truncated but still carries the create line, so it is a candidate, not unknown;
    # signature(5) has no logs at all, which the worker answers by fetching it.
    assert result['unknown_on_creations'] == [signature(5)]
    assert result['creations_only'] == sorted([signature(5), signature(6)])
    assert result['creations_failed_attempts'] == 0


def test_failed_attempts_and_arrival_latency():
    program = compare_tool.Tap('p', PUMP_PROGRAM)
    creations = compare_tool.Tap('c', MINT_AUTHORITY)
    for n in range(10):
        program.take(notification(signature(n), CREATE_LOGS, slot=100 + n), 10.0 + n)
        creations.take(notification(signature(n), CREATE_LOGS, slot=100 + n), 10.0 + n + (0.5 if n % 2 else 0.1))
    creations.take(notification(signature(50), CREATE_LOGS, err=FAILED, slot=105), 20.0)
    result = compare_tool.compare(program, creations)
    assert result['creations_failed_attempts'] == 1
    latency = result['latency_seconds']
    assert latency['matched'] == 6 and latency['min'] == 0.1 and latency['max'] == 0.5
    assert latency['median'] in (0.1, 0.5)


def test_no_data_yields_an_empty_but_complete_result():
    result = compare_tool.compare(compare_tool.Tap('p', PUMP_PROGRAM), compare_tool.Tap('c', MINT_AUTHORITY))
    assert result['window'] is None and result['launches'] == 0 and result['latency_seconds'] is None


# --- where the authority appears ------------------------------------------------------------

@pytest.mark.parametrize('where', ['static', 'loaded', 'absent'])
def test_authority_position_for_each_way_a_launch_can_list_it(where):
    assert compare_tool.authority_position(creation_tx(signature(1), authority=where)) == where


def test_authority_position_understands_both_key_encodings_and_both_places():
    tx = creation_tx(signature(1), authority='static')
    tx['transaction']['message']['accountKeys'] = [{'pubkey': key} for key in tx['transaction']['message']['accountKeys']]
    assert compare_tool.authority_position(tx) == 'static'
    tx['meta']['loadedAddresses'] = {'writable': [MINT_AUTHORITY], 'readonly': []}
    assert compare_tool.authority_position(tx) == 'both'


# --- sampling -------------------------------------------------------------------------------

def test_the_sample_starts_with_the_priority_signatures_then_spreads_across_the_rest():
    ordered = [signature(n) for n in range(40)]
    priority = [signature(31), signature(7), signature(31)]                   # a repeat is inspected once
    sample = compare_tool.choose_sample(priority, ordered, 8)
    assert sample[:2] == [signature(31), signature(7)] and len(sample) == 8 and len(set(sample)) == 8
    spread = [ordered.index(sig) for sig in sample[2:]]
    assert spread == sorted(spread) and spread[-1] - spread[0] > 20            # spread out, not clustered
    assert compare_tool.choose_sample(priority, ordered, 0) == []
    assert compare_tool.choose_sample(priority, ordered, 1) == [signature(31)]  # priority is never crowded out
    assert compare_tool.choose_sample([], [], 5) == []


# --- the verdict ----------------------------------------------------------------------------

def report(launches=80, missed=(), skips=(), only=(), lookup=None, errors=(None, None), details=()):
    return {'mode': 'both', 'filter_skip_details': list(details),
            'streams': {'program': {'error': errors[0]}, 'creations': {'error': errors[1]}},
            'compare': {'launches': launches, 'missed': list(missed), 'prefilter_skips': list(skips),
                        'prefilter_skips_by_side': {'program': len(skips), 'creations': 0},
                        'creations_only': list(only)},
            'lookup_tables': lookup or {'authority_listed': {'static': 20}, 'delivered_by_creations_stream': {'static': 20}}}


def test_a_clean_run_is_called_safe_to_try_with_the_statistical_limit_and_the_untested_case():
    text = compare_tool.assess(report(launches=100))
    assert text.startswith('no misses in 100 launches') and 'about 3.0%' in text
    assert 'looks safe to try' in text and 'no sampled launch used a lookup table' in text and 'untested' in text


def test_a_run_that_saw_lookup_tables_says_how_they_fared():
    lookup = {'authority_listed': {'static': 18, 'loaded': 4}, 'delivered_by_creations_stream': {'static': 18, 'loaded': 4},
              'decoded_as_launch': 22, 'not_a_launch': 0, 'sampled': 23, 'fetch_failed': 1}
    text = compare_tool.assess(report(lookup=lookup))
    assert '4 sampled launches resolved the authority through a lookup table and were still delivered (4 of 4)' in text
    assert '22 of 22 sampled transactions decode as launches' in text
    assert 'untested' not in text


@pytest.mark.parametrize('changes, expected', [
    ({'missed': ['a', 'b']}, 'missed 2 of 80 launches'),
    ({'lookup': {'authority_listed': {'absent': 3}, 'delivered_by_creations_stream': {}}}, '3 sampled launches do not include the authority'),
])
def test_a_problem_with_the_creations_stream_blocks_the_recommendation(changes, expected):
    text = compare_tool.assess(report(**changes))
    assert text.startswith('do not enable the creations stream') and expected in text


def skip_detail(accepts):
    return {'signature': 'x' * 64, 'decoder_accepts': accepts, 'instructions': ['Buy'], 'programs': ['6EF8rr'],
            'authority': 'loaded', 'version': '0', 'log_lines': 30, 'log_head': []}


def test_a_log_filter_hole_is_the_program_streams_problem_not_the_creations_streams():
    # The creations stream verifies every successful notification with the decoder, so a filter
    # skip is reported separately and does not block it.
    text = compare_tool.assess(report(skips=['a'], details=[skip_detail(True)]))
    assert text.startswith('no misses in 80 launches') and 'looks safe to try' in text
    assert 'program-mode log filter would skip 1 successful transactions' in text
    assert 'Of the 1 inspected, 1 decode as launches (real launches the program stream would miss)' in text
    assert 'the creations stream does not use that filter' in text
    harmless = compare_tool.assess(report(skips=['a'], details=[skip_detail(False)]))
    assert 'Of the 1 inspected, 0 decode as launches (none is a launch)' in harmless
    assert compare_tool.assess(report()).count('log filter') == 0               # nothing to say when nothing was skipped
    blocked = compare_tool.assess(report(missed=['m'], skips=['a'], details=[skip_detail(True)]))
    assert blocked.startswith('do not enable') and 'program-mode log filter would skip 1' in blocked


def test_too_few_launches_is_inconclusive_not_clean():
    text = compare_tool.assess(report(launches=12))
    assert text.startswith('inconclusive') and 'only 12 launches' in text


def test_a_failed_subscription_is_inconclusive_and_named():
    assert 'creations stream RuntimeError' in compare_tool.assess(report(errors=(None, 'RuntimeError')))


def test_launches_only_the_narrow_stream_delivered_are_a_note_not_a_problem():
    text = compare_tool.assess(report(only=['x', 'y']))
    assert text.startswith('no misses') and '2 successful transactions reached only the creations stream' in text


# --- the request file -----------------------------------------------------------------------

def test_request_file_parsing(tmp_path):
    path = tmp_path / 'request'
    path.write_text('# a comment\n\nrun=7\nseconds=120\nmax_megabytes = 20\nsample=0\n')
    assert compare_tool.read_request(path) == {'seconds': 120, 'max_megabytes': 20, 'sample': 0, 'streams': 'both'}
    path.write_text('run=1\n')
    assert compare_tool.read_request(path) == compare_tool.DEFAULTS


@pytest.mark.parametrize('text, message', [
    ('seconds=5\n', 'seconds must be between 30 and 900'), ('seconds=9000\n', 'seconds must be between'),
    ('max_megabytes=500\n', 'max_megabytes must be between 1 and 60'), ('sample=-1\n', 'sample must be between'),
    ('seconds=soon\n', 'must be a whole number'), ('speed=fast\n', "unknown setting 'speed'"),
    ('streams=program\n', 'streams must be one of: both, creations'), ('streams=\n', 'streams must be one of'),
])
def test_request_file_cannot_raise_the_ceiling_or_smuggle_settings(tmp_path, text, message):
    path = tmp_path / 'request'
    path.write_text(text)
    with pytest.raises(ValueError, match=message):
        compare_tool.read_request(path)


def test_the_cheap_mode_can_be_requested(tmp_path):
    path = tmp_path / 'request'
    path.write_text('streams = creations\nseconds=600\n')
    assert compare_tool.read_request(path)['streams'] == 'creations'


def test_the_largest_request_is_still_a_bounded_spend():
    top = compare_tool.LIMITS['max_megabytes'][1] * 1e6 / compare_tool.BYTES_PER_UNIT * compare_tool.CREDITS_PER_UNIT
    assert top <= 1200                     # the hard ceiling of what any request file can authorise per stream


# --- on the wire ----------------------------------------------------------------------------

def traffic(launches=70, miss=(), authority_for=None):
    """Program-stream traffic: two trades around every launch. The creations stream gets the
    launches except those in ``miss``. Returns (program_script, creations_script, node)."""
    node = FakeNode()
    program, creations = [notification(signature(9000), BUY_LOGS, slot=990)], []
    for n in range(launches):
        slot, sig = 1000 + n * 3, signature(n)
        program += [notification(signature(5000 + 2 * n), BUY_LOGS, slot=slot - 1), notification(sig, CREATE_LOGS, slot=slot),
                    notification(signature(5001 + 2 * n), BUY_LOGS, slot=slot + 1)]
        if n not in miss:
            creations.append(notification(sig, CREATE_LOGS, slot=slot))
        node.txs[sig] = creation_tx(sig, slot, authority=(authority_for or (lambda n: 'static'))(n))
    program.append(notification(signature(9001), BUY_LOGS, slot=1000 + launches * 3 + 10))
    creations += [notification(signature(8000), CREATE_LOGS, err=FAILED, slot=1010)]       # a failed attempt
    return program, creations, node


def collect_live(monkeypatch, program, creations, node, request=None, reject=()):
    request = request or {'seconds': 0.3, 'max_megabytes': 40, 'sample': 6}

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={PUMP_PROGRAM: program, MINT_AUTHORITY: creations}, reject=reject) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                return await compare_tool.run(rpc.url, request)
    return asyncio.run(scenario())


def test_end_to_end_a_clean_comparison(monkeypatch):
    program, creations, node = traffic(authority_for=lambda n: 'loaded' if n % 7 == 0 else 'static')
    result = collect_live(monkeypatch, program, creations, node)
    streams, compared = result['streams'], result['compare']
    assert streams['program']['bytes'] == sum(len(m.encode()) for m in program)
    assert streams['creations']['bytes'] == sum(len(m.encode()) for m in creations)
    assert streams['program']['notifications'] == len(program) and streams['creations']['notifications'] == len(creations)
    assert streams['program']['average_bytes'] == round(streams['program']['bytes'] / len(program))
    assert compared['missed'] == [] and compared['launches'] >= 60 and compared['creations_failed_attempts'] == 1
    assert result['stopped_by'] == 'time' and result['assessment'].startswith('no misses in')
    lookup = result['lookup_tables']
    assert lookup['sampled'] == 6 and lookup['fetch_failed'] == 0
    assert sum(lookup['authority_listed'].values()) == 6
    assert lookup['delivered_by_creations_stream'] == lookup['authority_listed']        # everything sampled was delivered
    assert lookup['decoded_as_launch'] == 6 and lookup['not_a_launch'] == 0             # the decoder accepts every launch
    assert result['mode'] == 'both' and result['filter_skip_details'] == []
    assert node.count('getTransaction') == 6                                            # the only RPC spend
    json.dumps(result)                                                                  # the report is serializable


def test_end_to_end_misses_caused_by_lookup_tables_are_found_and_explained(monkeypatch):
    missed = {10, 20}
    program, creations, node = traffic(miss=missed, authority_for=lambda n: 'loaded' if n in missed else 'static')
    result = collect_live(monkeypatch, program, creations, node)
    assert result['compare']['missed'] == [signature(10), signature(20)]
    assert result['assessment'].startswith('do not enable the creations stream')
    lookup = result['lookup_tables']
    # The two launches the narrow stream never delivered are exactly the two that list the
    # authority only through a lookup table: the tool names the cause.
    assert lookup['authority_listed']['loaded'] == 2
    assert lookup['delivered_by_creations_stream'].get('loaded', 0) == 0
    assert lookup['delivered_by_creations_stream']['static'] == lookup['authority_listed']['static']


def test_end_to_end_a_launch_without_the_authority_is_flagged(monkeypatch):
    program, creations, node = traffic(miss={30}, authority_for=lambda n: 'absent' if n == 30 else 'static')
    result = collect_live(monkeypatch, program, creations, node)
    assert 'do not include the authority at all' in result['assessment']


def test_end_to_end_the_byte_budget_stops_the_run_early(monkeypatch):
    program, creations, node = traffic()
    started = time.monotonic()
    tapped = asyncio.run(_collect_only(monkeypatch, program, creations, node, max_bytes=3000, seconds=30))
    assert time.monotonic() - started < 10                                              # not the 30 s window
    assert tapped[2] == 'byte budget' and tapped[0].bytes >= 3000
    assert tapped[0].bytes < 3000 + 2 * max(len(m) for m in program)                    # overshoot is one frame, not a flood


async def _collect_only(monkeypatch, program, creations, node, max_bytes, seconds):
    with FakeRpcServer(node) as rpc:
        async with FakeStream(by_address={PUMP_PROGRAM: program, MINT_AUTHORITY: creations}) as stream:
            monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
            return await compare_tool.collect(rpc.url, seconds, max_bytes)


def test_end_to_end_a_rejected_subscription_is_reported_by_class_only(monkeypatch):
    program, creations, node = traffic()
    result = collect_live(monkeypatch, program, creations, node, reject={MINT_AUTHORITY})
    assert result['streams']['creations']['error'] == 'RuntimeError' and result['stopped_by'] == 'error'
    assert result['assessment'].startswith('inconclusive: a subscription failed (creations stream RuntimeError')


def test_provider_error_text_never_reaches_the_report_or_the_annotations(monkeypatch):
    def leaking(url):
        raise ValueError('https://mainnet.helius-rpc.com/?api-key=SECRET')
    program, creations, node = traffic()
    monkeypatch.setattr(compare_tool, 'websocket_url', leaking)
    result = asyncio.run(compare_tool.run('url', {'seconds': 0.3, 'max_megabytes': 5, 'sample': 0}))
    text = json.dumps(result) + '\n'.join(compare_tool.annotation_lines(result))
    assert 'SECRET' not in text and 'api-key' not in text
    assert result['streams']['program']['error'] == 'ValueError'


def test_annotations_are_few_short_and_public(monkeypatch):
    program, creations, node = traffic(miss={10})
    lines = compare_tool.annotation_lines(collect_live(monkeypatch, program, creations, node))
    assert 6 <= len(lines) <= 9
    assert all(line.startswith(('::notice::', '::warning::')) and len(line) <= 912 for line in lines)
    assert any(line.startswith('::warning::') and 'missed by the creations stream: 1' in line for line in lines)
    assert any('Assessment: do not enable' in line for line in lines)
    assert not any('api-key' in line.lower() for line in lines)


# --- command line ---------------------------------------------------------------------------

def run_cli(monkeypatch, argv, node, program, creations, tmp_path):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://mainnet.helius-rpc.com/?api-key=SECRETKEY')
    monkeypatch.setitem(compare_tool.LIMITS, 'seconds', (0, 900))

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={PUMP_PROGRAM: program, MINT_AUTHORITY: creations}) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                monkeypatch.setenv('SOLANA_RPC_URL', rpc.url)
                return await asyncio.to_thread(compare_tool.main, argv + ['--output', str(tmp_path / 'out.json')])
    return asyncio.run(scenario())


def test_cli_writes_the_report_prints_annotations_and_never_the_url(monkeypatch, tmp_path, capsys):
    program, creations, node = traffic()
    code = run_cli(monkeypatch, ['--seconds', '1', '--sample', '3', '--annotate'], node, program, creations, tmp_path)
    out = capsys.readouterr().out
    saved = json.loads((tmp_path / 'out.json').read_text())
    assert code == 0 and saved['assessment'].startswith('no misses') and '::notice::' in out
    assert 'api-key' not in out and 'api-key' not in json.dumps(saved) and 'SECRETKEY' not in out


def test_cli_exits_nonzero_when_a_stream_failed(monkeypatch, tmp_path):
    program, creations, node = traffic()
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://mainnet.helius-rpc.com/?api-key=SECRETKEY')
    monkeypatch.setitem(compare_tool.LIMITS, 'seconds', (0, 900))

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={PUMP_PROGRAM: program, MINT_AUTHORITY: creations}, reject={PUMP_PROGRAM}) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                monkeypatch.setenv('SOLANA_RPC_URL', rpc.url)
                return await asyncio.to_thread(compare_tool.main, ['--seconds', '1', '--sample', '0',
                                                                    '--output', str(tmp_path / 'o.json')])
    assert asyncio.run(scenario()) == 1


@pytest.mark.parametrize('argv, message', [(['--seconds', '5'], 'seconds must be between 30 and 900'),
                                           (['--max-megabytes', '999'], 'max_megabytes must be between'),
                                           (['--sample', '99'], 'sample must be between')])
def test_cli_refuses_values_beyond_the_ceiling(monkeypatch, capsys, argv, message):
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://mainnet.helius-rpc.com/?api-key=SECRETKEY')
    with pytest.raises(SystemExit) as caught:
        compare_tool.main(argv)
    err = capsys.readouterr().err
    assert caught.value.code == 2 and message in err and 'SECRETKEY' not in err


def test_cli_needs_a_helius_endpoint_and_does_not_echo_a_bad_one(monkeypatch, capsys):
    monkeypatch.delenv('SOLANA_RPC_URL', raising=False)
    with pytest.raises(SystemExit):
        compare_tool.main([])
    assert 'Set SOLANA_RPC_URL privately' in capsys.readouterr().err
    monkeypatch.setenv('SOLANA_RPC_URL', 'https://evil.example/?api-key=SECRETKEY')
    with pytest.raises(SystemExit):
        compare_tool.main([])
    err = capsys.readouterr().err
    assert 'HTTPS Helius Mainnet endpoint' in err and 'SECRETKEY' not in err


def test_the_tool_reuses_the_workers_fetch_policy_and_endpoint_check():
    assert compare_tool.websocket_url is pump_worker.websocket_url
    assert compare_tool.fetch_with_retry is pump_worker.fetch_with_retry


def test_summary_markdown_carries_the_verdict_and_the_numbers(monkeypatch):
    program, creations, node = traffic(miss={10})
    text = compare_tool.summary_markdown(collect_live(monkeypatch, program, creations, node))
    assert '**Assessment:** do not enable' in text and '| notifications |' in text
    assert 'missed by the creations stream: 1' in text and 'api-key' not in text.lower()


def test_cli_appends_the_summary_where_asked(monkeypatch, tmp_path):
    program, creations, node = traffic()
    summary = tmp_path / 'summary.md'
    summary.write_text('earlier step\n')
    run_cli(monkeypatch, ['--seconds', '1', '--sample', '2', '--summary', str(summary)], node, program, creations, tmp_path)
    assert summary.read_text().startswith('earlier step\n### Pump.fun log subscriptions')


# --- the workflow's guarantees ----------------------------------------------------------------

ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / '.github' / 'workflows' / 'stream-compare.yml'
BRANCH = 'arena/01a0f47f-backend-track'


@pytest.fixture(scope='module')
def workflow():
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def triggers(workflow):
    return workflow.get('on') or workflow.get(True)          # PyYAML reads the key `on` as boolean True


def test_the_workflow_only_runs_on_request_and_never_on_a_schedule(workflow):
    on = triggers(workflow)
    assert set(on) == {'workflow_dispatch', 'push'}
    assert on['push']['branches'] == [BRANCH]
    # The request file is the ONLY path that can start a push run: editing the tool or the workflow cannot.
    assert on['push']['paths'] == ['backend-track/stream_compare.request']


def test_the_workflow_is_read_only_bounded_and_shares_the_sampling_lock(workflow):
    assert workflow['permissions'] == {'contents': 'read'}
    assert workflow['concurrency']['group'] == 'bounded-solana-sampling'     # never alongside the research sampler
    assert workflow['concurrency']['cancel-in-progress'] is False
    assert workflow['jobs']['compare']['timeout-minutes'] <= 15


def test_the_secret_is_used_in_exactly_one_step_and_never_echoed(workflow):
    text = WORKFLOW_PATH.read_text()
    assert text.count('secrets.') == 1
    steps = workflow['jobs']['compare']['steps']
    holders = [step for step in steps if 'secrets.' in json.dumps(step)]
    assert len(holders) == 1 and holders[0]['env'] == {'SOLANA_RPC_URL': '${{ secrets.HELIUS_RPC_URL }}'}
    command = holders[0]['run']
    assert 'echo "$SOLANA_RPC_URL"' not in command and 'set -x' not in command
    assert 'pump_stream_compare.py --request stream_compare.request' in command and '--annotate' in command
    assert holders[0]['working-directory'] == 'backend-track'


def test_the_report_is_kept_even_when_the_comparison_fails(workflow):
    upload = [step for step in workflow['jobs']['compare']['steps'] if 'upload-artifact' in step.get('uses', '')][0]
    assert upload['if'] == 'always()' and upload['with']['retention-days'] == 7


def test_a_committed_request_file_stays_within_the_agreed_spend():
    path = ROOT / 'backend-track' / 'stream_compare.request'
    if not path.exists():
        pytest.skip('no request file committed')
    request = compare_tool.read_request(path)
    ceiling = request['max_megabytes'] * 1e6 / compare_tool.BYTES_PER_UNIT * compare_tool.CREDITS_PER_UNIT
    assert ceiling <= 800                    # the figure agreed with the owner: a hard cap of ~800 stream credits
    assert request['seconds'] <= 600


# --- diagnosing a transaction the log filter would skip ---------------------------------------

def with_logs(tx, *lines):
    tx['meta']['logMessages'] = list(lines)
    return tx


def test_programs_invoked_counts_inner_instructions_and_lookup_table_programs():
    tx = creation_tx(signature(1), authority='loaded')
    tx['transaction']['message']['instructions'].append({'programIdIndex': 0, 'accounts': [], 'data': ''})
    tx['meta']['innerInstructions'] = [{'index': 0, 'instructions': [{'programIdIndex': 9, 'accounts': [], 'data': ''}]}]
    programs = compare_tool.programs_invoked(tx)
    assert PUMP_PROGRAM in programs and MINT_AUTHORITY in programs      # index 9 is the lookup-table address
    assert programs == sorted(set(programs)) and signature(1)[:0] == ''


def test_instruction_names_come_only_from_exact_instruction_log_lines():
    tx = with_logs(creation_tx(signature(1)),
                   'Program 6EF8rr invoke [1]', 'Program log: Instruction: CreateV2', 'Program log: Instruction: CreateV2',
                   'Program log: Instruction: InitializeMint2', 'Program log: not an instruction line',
                   'Program log: Instruction: Buy extra words', ' Program log: Instruction: Sell ')
    assert compare_tool.instruction_names(tx) == ['CreateV2', 'InitializeMint2', 'Sell']
    assert compare_tool.instruction_names(creation_tx(signature(1))) == []              # no logs at all
    many = with_logs(creation_tx(signature(1)), *[f'Program log: Instruction: Step{n}' for n in range(30)])
    assert len(compare_tool.instruction_names(many)) == 12


def test_diagnose_says_whether_the_decoder_accepts_the_transaction_and_what_ran():
    launch = with_logs(creation_tx(signature(1), authority='loaded'), *[f'line {n} ' + 'x' * 200 for n in range(20)])
    detail = compare_tool.diagnose(launch, signature(1))
    assert detail['decoder_accepts'] is True and detail['authority'] == 'loaded' and detail['version'] == 'legacy'
    assert detail['log_lines'] == 20 and len(detail['log_head']) == 8 and all(len(line) <= 110 for line in detail['log_head'])
    plain = compare_tool.diagnose(with_logs(plain_tx(signature(2)), 'Program log: Instruction: Transfer'), signature(2))
    assert plain['decoder_accepts'] is False and plain['instructions'] == ['Transfer'] and plain['authority'] == 'absent'
    assert 'decoder_accepts_launch=True' in compare_tool.describe_skip(detail) and len(compare_tool.describe_skip(detail)) < 400


# --- the creations-only probe -----------------------------------------------------------------

def test_probe_summarises_what_the_creations_stream_shows_alone():
    notes = ([notification(signature(n), CREATE_LOGS, slot=100 + n) for n in range(1, 9)]
             + [notification(signature(20), BUY_LOGS, slot=104), notification(signature(21), None, slot=105),
                notification(signature(22), CREATE_LOGS, err=FAILED, slot=106)])
    creations = tap_with(MINT_AUTHORITY, *notes)
    result = compare_tool.probe(creations)
    assert result['window'] == {'first_slot': 103, 'last_slot': 106}      # slots 101-108 seen, minus the 2-slot margin
    # Inside the window: launches 3-6, the two odd successful transactions, and the one failed attempt.
    assert result['successful'] == 6 and result['failed_attempts'] == 1
    assert result['skips'] == [signature(20)] and result['unknown'] == [signature(21)]
    assert compare_tool.probe(compare_tool.Tap('c', MINT_AUTHORITY))['window'] is None


def traffic_with_filter_holes():
    """Creations-stream traffic: launches, failed attempts, and two successful notifications whose logs
    show no Create line: one is a real launch by the decoder's standard, the other is not a launch."""
    node = FakeNode()
    notes = [notification(signature(9000), CREATE_LOGS, err=FAILED, slot=995)]
    for n in range(40):
        slot, sig = 1000 + n * 3, signature(n)
        notes.append(notification(sig, CREATE_LOGS, slot=slot))
        node.txs[sig] = with_logs(creation_tx(sig, slot, authority='loaded' if n % 4 else 'static'),
                                  'Program log: Instruction: CreateV2')
    hole, odd = signature(500), signature(501)
    node.txs[hole] = with_logs(creation_tx(hole, 1050), 'Program log: Instruction: Mystery')
    node.txs[odd] = with_logs(plain_tx(odd, 1060), 'Program log: Instruction: Transfer')
    notes += [notification(hole, BUY_LOGS, slot=1050), notification(odd, BUY_LOGS, slot=1060),
              notification(signature(9001), CREATE_LOGS, slot=1200)]
    return notes, node, hole, odd


def probe_live(monkeypatch, notes, node, request):
    captured = {}

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={MINT_AUTHORITY: notes}) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                result = await compare_tool.run(rpc.url, request)
                captured['requests'] = stream.requests
                return result
    return asyncio.run(scenario()), captured['requests']


def test_end_to_end_the_cheap_probe_never_opens_the_program_stream_and_diagnoses_filter_skips(monkeypatch):
    notes, node, hole, odd = traffic_with_filter_holes()
    result, requests = probe_live(monkeypatch, notes, node, {'seconds': 0.3, 'max_megabytes': 5, 'sample': 3, 'streams': 'creations'})
    assert [r['params'][0]['mentions'] for r in requests] == [[MINT_AUTHORITY]]       # the expensive stream was never opened
    assert result['mode'] == 'creations' and 'compare' not in result
    assert result['streams']['program']['notifications'] == 0 and result['streams']['program']['bytes'] == 0
    assert result['probe']['skips'] == sorted([hole, odd])
    details = {d['signature']: d for d in result['filter_skip_details']}
    assert set(details) == {hole, odd}                                  # skips are inspected even with a sample of 3
    assert details[hole]['decoder_accepts'] is True and details[hole]['instructions'] == ['Mystery']
    assert details[odd]['decoder_accepts'] is False and details[odd]['instructions'] == ['Transfer']
    lookup = result['lookup_tables']
    assert lookup['sampled'] == 3 and lookup['delivered_by_creations_stream'] is None   # nothing to compare against
    assert lookup['decoded_as_launch'] == 2 and lookup['not_a_launch'] == 1
    assert 'creations-only probe' in result['assessment'] and '(real launches the program stream would miss)' in result['assessment']
    assert result['estimated_credits_spent'] < 10                                           # the whole point of this mode
    assert node.count('getTransaction') == 3


def test_the_cheap_probe_annotations_name_each_skip_and_stay_bounded(monkeypatch):
    notes, node, hole, odd = traffic_with_filter_holes()
    result, _ = probe_live(monkeypatch, notes, node, {'seconds': 0.3, 'max_megabytes': 5, 'sample': 4, 'streams': 'creations'})
    lines = compare_tool.annotation_lines(result)
    assert len(lines) <= 9 and all(len(line) <= 912 for line in lines)
    skips = [line for line in lines if 'Log-filter skip:' in line]
    assert len(skips) == 2 and all(line.startswith('::warning::') for line in skips)
    assert any(f'{hole[:12]}.. decoder_accepts_launch=True' in line for line in skips)
    assert any('Probe: ' in line for line in lines) and not any('Program stream:' in line for line in lines)
    summary = compare_tool.summary_markdown(result)
    assert '| not run |' in summary and f'`{hole[:12]}..' in summary and 'api-key' not in summary.lower()


def test_a_probe_with_no_skips_says_so(monkeypatch):
    node = FakeNode()
    notes = []
    for n in range(20):
        sig = signature(n)
        notes.append(notification(sig, CREATE_LOGS, slot=1000 + n * 3))
        node.txs[sig] = creation_tx(sig, 1000 + n * 3)
    result, _ = probe_live(monkeypatch, notes, node, {'seconds': 0.3, 'max_megabytes': 5, 'sample': 2, 'streams': 'creations'})
    assert result['probe']['skips'] == [] and result['filter_skip_details'] == []
    assert 'None would be skipped by the log filter.' in result['assessment']


def test_cli_runs_the_cheap_mode_from_a_flag(monkeypatch, tmp_path, capsys):
    notes, node, hole, odd = traffic_with_filter_holes()
    monkeypatch.setitem(compare_tool.LIMITS, 'seconds', (0, 900))

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={MINT_AUTHORITY: notes}) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                monkeypatch.setenv('SOLANA_RPC_URL', rpc.url)
                return await asyncio.to_thread(compare_tool.main, [
                    '--streams', 'creations', '--seconds', '1', '--sample', '3', '--annotate',
                    '--output', str(tmp_path / 'probe.json')])
    code = asyncio.run(scenario())
    saved = json.loads((tmp_path / 'probe.json').read_text())
    out = capsys.readouterr().out
    assert code == 0 and saved['mode'] == 'creations' and '"mode": "creations"' in out and 'Log-filter skip:' in out


def test_the_cheap_mode_fails_only_when_its_own_stream_fails(monkeypatch, tmp_path):
    node = FakeNode()
    monkeypatch.setitem(compare_tool.LIMITS, 'seconds', (0, 900))

    async def scenario():
        with FakeRpcServer(node) as rpc:
            async with FakeStream(by_address={MINT_AUTHORITY: []}, reject={MINT_AUTHORITY}) as stream:
                monkeypatch.setattr(compare_tool, 'websocket_url', lambda url: stream.url)
                monkeypatch.setenv('SOLANA_RPC_URL', rpc.url)
                return await asyncio.to_thread(compare_tool.main, ['--streams', 'creations', '--seconds', '1', '--sample', '0',
                                                                    '--output', str(tmp_path / 'o.json')])
    assert asyncio.run(scenario()) == 1


def worst_case_report(mode, skips):
    """A report with every optional annotation present, to prove the assessment is never cut."""
    stream = {'address': 'a', 'notifications': 9, 'unique_signatures': 9, 'malformed': 0, 'bytes': 9000, 'average_bytes': 1000,
              'seconds': 100.0, 'per_second': 0.09, 'verdicts': {'creation': 9}, 'credits_in_window': 0.2,
              'credits_per_month': 1234, 'error': None}
    detail = {'signature': 'z' * 64, 'decoder_accepts': True, 'instructions': ['Step%d' % n for n in range(12)],
              'programs': ['P%05d' % n for n in range(12)], 'authority': 'loaded', 'version': '0', 'log_lines': 99, 'log_head': []}
    report = {'mode': mode, 'request': {}, 'stopped_by': 'time', 'streams': {'program': dict(stream), 'creations': dict(stream)},
              'lookup_tables': {'sampled': 25, 'fetch_failed': 0, 'authority_listed': {'loaded': 22, 'static': 3},
                                'delivered_by_creations_stream': {'loaded': 22, 'static': 3} if mode == 'both' else None,
                                'transaction_versions': {'0': 25}, 'decoded_as_launch': 25, 'not_a_launch': 0},
              'filter_skip_details': [dict(detail, signature=chr(97 + n) * 64) for n in range(skips)],
              'estimated_credits_spent': 840, 'assessment': 'do not enable the creations stream: ' + 'because ' * 200}
    if mode == 'both':
        report['compare'] = {'window': {'first_slot': 1, 'last_slot': 2}, 'launches': 99, 'missed': [], 'prefilter_skips': ['s'] * skips,
                             'prefilter_skips_by_side': {'program': skips, 'creations': 0}, 'unknown_on_creations': [],
                             'creations_only': [], 'creations_failed_attempts': 0,
                             'latency_seconds': {'matched': 5, 'median': 0.01, 'p95': 0.02, 'min': 0.0, 'max': 0.03}}
    else:
        report['probe'] = {'window': {'first_slot': 1, 'last_slot': 2}, 'successful': 9, 'failed_attempts': 0,
                           'skips': ['s'] * skips, 'unknown': []}
    return report


@pytest.mark.parametrize('mode', ['both', 'creations'])
@pytest.mark.parametrize('skips', [0, 1, 3, 5])
def test_the_assessment_is_never_cut_however_many_skips_were_diagnosed(mode, skips):
    lines = compare_tool.annotation_lines(worst_case_report(mode, skips))
    assert len(lines) <= compare_tool.MAX_ANNOTATIONS and all(len(line) <= 912 for line in lines)
    assert 'Assessment:' in lines[-1] and lines[-1].startswith('::warning::')
    shown = sum('Log-filter skip:' in line for line in lines)
    assert shown == min(skips, 1 if mode == 'both' else 3)        # what fits beside the fixed lines, never more
