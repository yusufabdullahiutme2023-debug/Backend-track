"""Tests for the read-only Bitquery sample profiler.

Only synthetic rows are used: no real vendor data is committed. The table is built
with the schema decoded from the real file's footer (Symbol, Fungible and Block_Time
are strings, flags are int8), not the vendor's documentation table.
"""
import io
import json
import urllib.error
from pathlib import Path

import pytest
import yaml

import pump_dataset as ds

WORKFLOW = Path(__file__).resolve().parents[2] / '.github/workflows/bitquery-profile.yml'
WSOL = 'So11111111111111111111111111111111111111112'


def row(n, method, mint, slot, success=1, trunk=1, **extra):
    """One synthetic file row. Migration rows leave the token metadata empty, as the real file does."""
    creation = method in ds.CREATE_METHODS
    base = {
        ds.SIGNATURE: f'sig{n:02d}'.ljust(88, 'x'), ds.SIGNER: f'signer{n}',
        ds.METHOD: method, ds.MINT: mint,
        ds.NAME: f'Token {n}' if creation else None,
        ds.CREATORS: [f'signer{n}'] if creation else None,
        ds.SYMBOL: f'T{n}' if creation else None, ds.DECIMALS: 6,
        ds.FUNGIBLE: 'true' if creation else None,
        ds.URI: f'https://example.invalid/{n}' if creation else None,
        ds.QUOTE_MINT: WSOL, ds.QUOTE_NAME: 'Wrapped Solana', ds.SLOT: slot,
        ds.TIME: f'2026-07-01T00:00:{n:02d}.000000Z', ds.SUCCESS: success, ds.TRUNK: trunk,
    }
    base.update(extra)
    return base


ROWS = [
    row(1, 'create_v2', 'M1', 100),
    row(2, 'create_v2', 'M2', 101),
    row(3, 'create', 'M3', 102),                       # legacy method still counts as a launch
    row(4, 'create_v2', 'M4', 103, success=0),         # failed transaction
    row(5, 'create_v2', 'M5', 104, trunk=0),           # block not on the canonical chain
    row(6, 'migrate_v2', 'M1', 100),                   # graduated in the same slot
    row(7, 'migrate', 'M2', 400),
    row(8, 'migrate_v2', 'M2', 405),                   # second successful row for the same mint
    row(9, 'migrate', 'M3', 300, success=0),           # failed migration must not count
    row(10, 'migrate', 'M9', 500),                     # migrated; its creation is not in this file
]


@pytest.fixture
def parquet_bytes():
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    schema = pa.schema([
        (ds.SIGNATURE, pa.string()), (ds.SIGNER, pa.string()), (ds.METHOD, pa.string()),
        (ds.MINT, pa.string()), (ds.NAME, pa.string()), (ds.CREATORS, pa.list_(pa.string())),
        (ds.SYMBOL, pa.string()), (ds.DECIMALS, pa.int32()), (ds.FUNGIBLE, pa.string()),
        (ds.URI, pa.string()), (ds.QUOTE_MINT, pa.string()), (ds.QUOTE_NAME, pa.string()),
        (ds.SLOT, pa.int64()), (ds.TIME, pa.string()), (ds.SUCCESS, pa.int8()),
        (ds.TRUNK, pa.int8()),
    ])

    def build(rows=ROWS):
        sink = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression='zstd')
        return sink.getvalue()
    return build


def test_column_constants_cover_the_documented_sixteen_columns():
    assert len(ds.COLUMNS) == len(set(ds.COLUMNS)) == 16


def test_only_successful_canonical_rows_are_good():
    assert ds.is_good(ROWS[0])
    assert not ds.is_good(ROWS[3])   # failed
    assert not ds.is_good(ROWS[4])   # off trunk
    assert not ds.is_good({ds.SUCCESS: None, ds.TRUNK: 1})


def test_profile_counts_launches_and_migrations_from_good_rows_only(parquet_bytes):
    report = ds.profile(parquet_bytes())
    assert not [name for name, section in report.items() if 'error' in section]
    assert report['file']['rows'] == 10 and report['file']['missing_columns'] == []
    assert report['methods']['by_method'] == {
        'create_v2': 4, 'migrate': 3, 'migrate_v2': 2, 'create': 1}
    assert report['creations']['rows'] == 5
    assert report['creations']['good_rows'] == 3          # failed + off-trunk excluded
    assert report['creations']['distinct_good_mints'] == 3
    assert report['migrations']['rows'] == 5
    assert report['migrations']['good_rows'] == 4          # the failed migrate is excluded
    assert report['migrations']['distinct_good_mints'] == 3


def test_schema_section_reports_the_file_types_not_the_docs(parquet_bytes):
    schema = ds.profile(parquet_bytes())['schema']
    assert schema[ds.TIME] == 'string'
    assert schema[ds.SYMBOL] == 'string' and schema[ds.FUNGIBLE] == 'string'
    assert schema[ds.SUCCESS] == 'int8' and schema[ds.TRUNK] == 'int8'


def test_funnel_joins_creation_to_graduation_by_mint(parquet_bytes):
    funnel = ds.profile(parquet_bytes())['funnel']
    assert funnel['created_mints'] == 3 and funnel['migrated_mints'] == 3
    assert funnel['created_and_migrated_in_file'] == 2          # M1 and M2
    assert funnel['migrated_without_creation_in_file'] == 1     # M9
    assert funnel['migration_before_creation'] == 0
    # M2's earliest good migration is slot 400 against creation at slot 101.
    assert funnel['lag_buckets'] == {'0 (same slot)': 1, '151-1500 (<10 min)': 1}
    assert {e['mint']: e['lag_slots'] for e in funnel['examples']} == {'M1': 0, 'M2': 299}


def test_repeated_successful_migrations_are_surfaced(parquet_bytes):
    repeats = ds.profile(parquet_bytes())['migration_repeats']
    assert repeats['repeated_mints'] == 1
    assert repeats['method_mix_of_repeats'] == {'migrate+migrate_v2': 1}
    assert repeats['slot_span_of_repeats']['max'] == 5


def test_consumer_check_flags_unusable_candidates(parquet_bytes):
    check = ds.profile(parquet_bytes())['consumer_check']
    assert check['candidates'] == 5 and check['unusable_candidates'] == 2
    assert check['unusable_in_first_four'] == 1                 # row 4 is a failed create
    assert check['first_four'][3] == {'signature': 'sig04' + 'x' * 11, 'success': 0, 'on_trunk': 1}


def test_repeated_signature_is_reported(parquet_bytes):
    duplicate = dict(ROWS[0], **{ds.SLOT: 103, ds.TRUNK: 0})   # same tx seen on a fork
    keys = ds.profile(parquet_bytes(ROWS + [duplicate]))['keys']
    assert keys['signatures_with_multiple_rows'] == 1 and keys['max_rows_per_signature'] == 2
    assert keys['repeated_signature_examples'][0]['signature'] == ROWS[0][ds.SIGNATURE]


def test_a_failing_section_is_reported_and_does_not_hide_the_others(parquet_bytes, monkeypatch):
    def boom(rows, raw, pfile, table):
        raise KeyError('surprise column')
    monkeypatch.setattr(ds, 'SECTIONS', (('boom', boom), ('methods', ds.methods_section)))
    report = ds.profile(parquet_bytes())
    assert report['boom'] == {'error': "KeyError: 'surprise column'"}
    assert report['methods']['by_method']['create_v2'] == 4


def test_schema_drift_is_visible_not_fatal(parquet_bytes):
    pa = pytest.importorskip('pyarrow')
    pq = pytest.importorskip('pyarrow.parquet')
    sink = io.BytesIO()
    pq.write_table(pa.table({ds.SIGNATURE: ['s'], ds.METHOD: ['create_v2'], 'Surprise': [1]}), sink)
    report = ds.profile(sink.getvalue())
    assert report['file']['unexpected_columns'] == ['Surprise']
    assert ds.SLOT in report['file']['missing_columns']
    assert report['creations']['good_rows'] == 0   # no success/trunk flags: nothing is trusted


def test_fetch_reads_a_local_file_and_enforces_the_budget(tmp_path):
    path = tmp_path / 'sample.parquet'
    path.write_bytes(b'x' * 20)
    assert ds.fetch(str(path), max_bytes=20) == b'x' * 20
    with pytest.raises(ValueError, match='byte budget'):
        ds.fetch(str(path), max_bytes=19)


class FakeResponse:
    def __init__(self, size):
        self.headers = {'Content-Length': str(size)}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_availability_uses_head_requests_and_counts_refusals():
    seen = []

    def opener(request, timeout):
        seen.append((request.get_method(), request.full_url))
        if request.full_url.endswith(('2026-07-01.parquet', '2026-07-02.parquet')):
            return FakeResponse(1234)
        raise urllib.error.HTTPError(request.full_url, 403, 'Forbidden', {}, io.BytesIO(b''))
    result = ds.availability('2026-06-30', '2026-07-03', opener=opener)
    assert result['public_days'] == {'2026-07-01': 1234, '2026-07-02': 1234}
    assert result['refused_by_status'] == {'403': 2}
    assert {method for method, _ in seen} == {'HEAD'}
    assert all(url.startswith(ds.BASE) for _, url in seen)


def test_availability_rejects_backwards_or_huge_windows():
    with pytest.raises(ValueError):
        ds.availability('2026-07-03', '2026-07-01', opener=None)
    with pytest.raises(ValueError):
        ds.availability('2025-01-01', '2026-12-31', opener=None)


def test_annotation_slices_are_small_and_reassemble_exactly():
    report = {'big': {'text': 'a' * 7000}, 'small': {'n': 1}}
    slices = ds.annotation_slices(report, size=3000)
    assert [title for title, _ in slices] == ['big 1/3', 'big 2/3', 'big 3/3', 'small 1/1']
    assert all(len(text) <= 3000 for _, text in slices)
    assert json.loads(''.join(text for title, text in slices if title.startswith('big'))) == report['big']


def test_annotations_are_escaped_onto_single_lines(capsys):
    ds.emit_part({'x': {'note': '100% sure\nsecond line'}}, part=1, parts=1)
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1 and lines[0].startswith('::notice title=x 1/1::')
    assert '\n' not in lines[0]
    assert ds._escape('a%b\nc') == 'a%25b%0Ac'
    assert ds._escape('a:b,c', prop=True) == 'a%3Ab%2Cc'


def test_steps_share_the_annotation_budget_and_overflow_is_announced(capsys):
    report = {f's{i:02d}': {'i': i} for i in range(35)}
    for part in (1, 2, 3):
        ds.emit_part(report, part=part, parts=3)
    out = capsys.readouterr().out.splitlines()
    notices = [line for line in out if line.startswith('::notice')]
    assert len(notices) == 30 and len({line for line in notices}) == 30
    assert out[-1].startswith('::warning::5 annotation slices did not fit')


def test_main_profiles_a_file_and_reports_failures_as_annotations(parquet_bytes, tmp_path, capsys):
    source, target = tmp_path / 'in.parquet', tmp_path / 'out.json'
    source.write_bytes(parquet_bytes())
    assert ds.main(['profile', str(source), '--output', str(target)]) == 0
    assert json.loads(target.read_text())['file']['rows'] == 10
    capsys.readouterr()
    assert ds.main(['profile', str(tmp_path / 'missing.parquet'), '--output', str(target)]) == 1
    assert capsys.readouterr().out.startswith('::error::Profile failed: FileNotFoundError')


def test_annotate_subcommand_reads_a_saved_profile(parquet_bytes, tmp_path, capsys):
    source, target = tmp_path / 'in.parquet', tmp_path / 'out.json'
    source.write_bytes(parquet_bytes())
    ds.main(['profile', str(source), '--output', str(target)])
    capsys.readouterr()
    assert ds.main(['annotate', str(target), '--part', '1', '--parts', '3']) == 0
    assert capsys.readouterr().out.count('::notice title=') >= 10


# --- the workflow that runs this on GitHub's network -----------------------------------

SPEC = yaml.safe_load(WORKFLOW.read_text())
JOB = SPEC['jobs']['profile']


def test_workflow_is_read_only_secretless_and_bounded():
    text = WORKFLOW.read_text()
    assert SPEC['permissions'] == {'contents': 'read'}
    assert JOB['timeout-minutes'] <= 5
    assert 'secrets.' not in text and 'pull_request_target' not in text
    assert 'SOLANA_RPC_URL' not in text and 'HELIUS' not in text   # never touches the provider


def test_workflow_mirrors_the_profile_into_three_annotation_steps():
    runs = [step['run'] for step in JOB['steps'] if 'annotate' in step.get('run', '')]
    assert sorted(runs) == [
        f'python pump_dataset.py annotate pump-dataset-profile.json --part {n} --parts 3'
        for n in (1, 2, 3)]
    triggers = SPEC.get('on', SPEC.get(True))
    assert 'workflow_dispatch' in triggers
