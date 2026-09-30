"""Read-only profile of Bitquery's public Pump.fun creation/migration files.

The Parquet files are a third-party candidate list, not proof of on-chain
events: every signature still needs independent RPC validation before it is
trusted. This module only inspects a file. It talks to no RPC provider, needs
no key, and signs or sends nothing.

Column types are read from the file itself because the vendor's documentation
table disagrees with it (for example Block_Time is a string, not a datetime).

    python pump_dataset.py profile [PATH_OR_URL] [--scan START:END] [--output FILE]
    python pump_dataset.py annotate FILE --part 1 --parts 3

``annotate`` mirrors the profile into GitHub Actions annotations, because
artifact blobs are unreachable from restricted networks and annotations are not.
"""
import argparse
import datetime
import hashlib
import json
import sys
import urllib.error
import urllib.request
from collections import Counter

BASE = ('https://bitquery-blockchain-dataset.s3.us-east-1.amazonaws.com/'
        'solana/pumpfun_creation_migrations/')
SAMPLE = BASE + '2026-07-01.parquet'
MAX_BYTES = 50_000_000
CREATE_METHODS = ('create', 'create_v2')
MIGRATE_METHODS = ('migrate', 'migrate_v2')

SIGNATURE = 'Transaction_Signature'
SIGNER = 'Transaction_Signer'
METHOD = 'Instruction_Program_Method'
MINT = 'Pool_Market_BaseCurrency_MintAddress'
NAME = 'Pool_Market_BaseCurrency_Name'
CREATORS = 'Pool_Market_BaseCurrency_TokenCreators_Address'
SYMBOL = 'Pool_Market_BaseCurrency_Symbol'
DECIMALS = 'Pool_Market_BaseCurrency_Decimals'
FUNGIBLE = 'Pool_Market_BaseCurrency_Fungible'
URI = 'Pool_Market_BaseCurrency_Uri'
QUOTE_MINT = 'Pool_Market_QuoteCurrency_MintAddress'
QUOTE_NAME = 'Pool_Market_QuoteCurrency_Name'
SLOT = 'Block_Slot'
TIME = 'Block_Time'
SUCCESS = 'Transaction_Result_Success'
TRUNK = 'Indexing_OnTrunk'
USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
COLUMNS = (SIGNATURE, SIGNER, METHOD, MINT, NAME, CREATORS, SYMBOL, DECIMALS,
           FUNGIBLE, URI, QUOTE_MINT, QUOTE_NAME, SLOT, TIME, SUCCESS, TRUNK)

ANNOTATION_CHARS = 3000   # keep each annotation far below the API's 64 KB limit
ANNOTATIONS_PER_STEP = 10  # Actions keeps at most ten notices per step


def fetch(source=SAMPLE, max_bytes=MAX_BYTES):
    """Return the raw bytes of a local path or HTTPS URL, refusing oversized input."""
    if source.startswith('https://'):
        with urllib.request.urlopen(source, timeout=30) as response:
            data = response.read(max_bytes + 1)
    else:
        with open(source, 'rb') as file:
            data = file.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ValueError(f'Source exceeds the {max_bytes} byte budget')
    return data


def is_good(row):
    """A row Bitquery marks as a successful transaction on the canonical chain."""
    return row.get(SUCCESS) == 1 and row.get(TRUNK) == 1


def _counts(values, top=None):
    return {str(key): count for key, count in Counter(values).most_common(top)}


def _spread(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None

    def at(fraction):
        return values[min(len(values) - 1, int(fraction * len(values)))]
    return {'n': len(values), 'min': values[0], 'p50': at(.5), 'p90': at(.9),
            'max': values[-1]}


def _with_method(rows, methods, good_only=True):
    return [r for r in rows if r.get(METHOD) in methods and (is_good(r) or not good_only)]


# --- profile sections: each takes (rows, raw, parquet_file, table) -> JSON-able ---

def file_section(rows, raw, pfile, table):
    meta = pfile.metadata
    group = meta.row_group(0) if meta.num_row_groups else None
    names = list(table.column_names)
    return {
        'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
        'rows': meta.num_rows, 'row_groups': meta.num_row_groups,
        'created_by': meta.created_by,
        'compression': sorted({group.column(i).compression
                               for i in range(group.num_columns)}) if group else [],
        'missing_columns': [c for c in COLUMNS if c not in names],
        'unexpected_columns': [c for c in names if c not in COLUMNS],
        'column_order_matches_docs': names == list(COLUMNS),
    }


def schema_section(rows, raw, pfile, table):
    return {f.name: f'{f.type}' + ('' if f.nullable else ' NOT NULL') for f in table.schema}


def methods_section(rows, raw, pfile, table):
    cross = Counter((r.get(METHOD), r.get(SUCCESS), r.get(TRUNK)) for r in rows)
    return {
        'by_method': _counts(r.get(METHOD) for r in rows),
        'method|success|on_trunk': {f'{m}|{s}|{t}': n for (m, s, t), n in sorted(
            cross.items(), key=lambda item: (-item[1], str(item[0])))},
    }


def keys_section(rows, raw, pfile, table):
    per_signature = Counter(r.get(SIGNATURE) for r in rows)
    repeated = [s for s, n in per_signature.items() if n > 1]
    detail = []
    for signature in sorted(repeated, key=str)[:3]:
        detail.append({'signature': signature, 'rows': [
            [r.get(METHOD), r.get(MINT), r.get(SLOT)] for r in rows
            if r.get(SIGNATURE) == signature]})
    return {
        'rows': len(rows), 'distinct_signatures': len(per_signature),
        'signatures_with_multiple_rows': len(repeated),
        'max_rows_per_signature': max(per_signature.values(), default=0),
        'signature_length': _counts(len(s) for s in per_signature if isinstance(s, str)),
        'repeated_signature_examples': detail,
    }


def nulls_section(rows, raw, pfile, table):
    out = {}
    for method in sorted({r.get(METHOD) for r in rows}, key=str):
        group = [r for r in rows if r.get(METHOD) == method]
        blanks = {}
        for column in table.column_names:
            missing = sum(1 for r in group if r.get(column) is None)
            empty = sum(1 for r in group if r.get(column) in ('', []))
            if missing or empty:
                blanks[column] = {'null': missing, 'empty': empty}
        out[str(method)] = {'rows': len(group), 'blank_columns': blanks}
    return out


def ranges_section(rows, raw, pfile, table):
    slots = [r.get(SLOT) for r in rows if r.get(SLOT) is not None]
    times = [r.get(TIME) for r in rows if r.get(TIME) is not None]
    return {
        'slot_min': min(slots, default=None), 'slot_max': max(slots, default=None),
        'distinct_slots': len(set(slots)),
        'sorted_by_slot': all(a <= b for a, b in zip(slots, slots[1:])),
        'time_python_types': _counts(type(t).__name__ for t in times),
        'time_min': min(times, default=None), 'time_max': max(times, default=None),
        'time_first_rows': times[:3],
    }


def creations_section(rows, raw, pfile, table):
    every = _with_method(rows, CREATE_METHODS, good_only=False)
    good = _with_method(rows, CREATE_METHODS)
    mints = [r.get(MINT) for r in good if r.get(MINT)]
    creators = [r.get(CREATORS) for r in good]
    return {
        'rows': len(every), 'good_rows': len(good), 'distinct_good_mints': len(set(mints)),
        'mint_length': _counts(len(m) for m in mints),
        'mint_ends_with_pump': sum(1 for m in mints if m.endswith('pump')),
        'decimals': _counts(r.get(DECIMALS) for r in good),
        'fungible': _counts(r.get(FUNGIBLE) for r in good),
        'quote_mint': _counts((r.get(QUOTE_MINT) for r in good), 5),
        'quote_name': _counts((r.get(QUOTE_NAME) for r in good), 5),
        'quote_by_method': _counts(f'{r.get(METHOD)}|{r.get(QUOTE_NAME)}' for r in good),
        'name_length': _spread(len(r[NAME]) for r in good if r.get(NAME)),
        'symbol_length': _spread(len(r[SYMBOL]) for r in good if r.get(SYMBOL)),
        'uri_prefix': _counts((r[URI][:30] for r in good if r.get(URI)), 6),
        'creator_count': _counts(len(c) if c is not None else None for c in creators),
        'signer_is_first_creator': sum(1 for r in good if (r.get(CREATORS) or [None])[0]
                                       == r.get(SIGNER)),
        'signer_among_creators': sum(1 for r in good if r.get(SIGNER) in (r.get(CREATORS) or [])),
    }


def migrations_section(rows, raw, pfile, table):
    every = _with_method(rows, MIGRATE_METHODS, good_only=False)
    good = _with_method(rows, MIGRATE_METHODS)
    return {
        'rows': len(every), 'good_rows': len(good),
        'good_by_method': _counts(r.get(METHOD) for r in good),
        'distinct_good_mints': len({r.get(MINT) for r in good if r.get(MINT)}),
        'mint_populated': sum(1 for r in good if r.get(MINT)),
        'name_populated': sum(1 for r in good if r.get(NAME)),
        'symbol_populated': sum(1 for r in good if r.get(SYMBOL)),
        'uri_populated': sum(1 for r in good if r.get(URI)),
        'creators_populated': sum(1 for r in good if r.get(CREATORS)),
        'decimals': _counts(r.get(DECIMALS) for r in good),
        'quote_mint': _counts((r.get(QUOTE_MINT) for r in good), 5),
        'quote_name': _counts((r.get(QUOTE_NAME) for r in good), 5),
        'signer': _counts((r.get(SIGNER) for r in good), 5),
    }


def _lag_bucket(lag):
    for limit, label in ((0, '0 (same slot)'), (150, '1-150 (<1 min)'), (1500, '151-1500 (<10 min)'),
                         (9000, '1501-9000 (<1 h)')):
        if lag <= limit:
            return label
    return '>9000 (>1 h)'


def funnel_section(rows, raw, pfile, table):
    """Launch-to-graduation pairs visible *within this one file* only."""
    created, migrated = {}, {}
    for row in rows:
        mint, slot = row.get(MINT), row.get(SLOT)
        if not is_good(row) or not mint or slot is None:
            continue
        target = (created if row.get(METHOD) in CREATE_METHODS
                  else migrated if row.get(METHOD) in MIGRATE_METHODS else None)
        if target is not None and (mint not in target or slot < target[mint][SLOT]):
            target[mint] = row
    both = sorted(set(created) & set(migrated))
    lags = [migrated[m][SLOT] - created[m][SLOT] for m in both]
    return {
        'created_mints': len(created), 'migrated_mints': len(migrated),
        'created_and_migrated_in_file': len(both),
        'migrated_without_creation_in_file': len(set(migrated) - set(created)),
        'migration_before_creation': sum(1 for lag in lags if lag < 0),
        'lag_slots': _spread(lags),
        'lag_buckets': _counts(_lag_bucket(lag) for lag in lags),
        'examples': [{'mint': m, 'create_signature': created[m][SIGNATURE],
                      'migrate_signature': migrated[m][SIGNATURE],
                      'migrate_method': migrated[m][METHOD],
                      'lag_slots': migrated[m][SLOT] - created[m][SLOT]} for m in both[:3]],
    }


def migration_repeats_section(rows, raw, pfile, table):
    """Successful migration rows grouped by mint: duplicates, or separate events?"""
    by_mint = {}
    for row in _with_method(rows, MIGRATE_METHODS):
        if row.get(MINT):
            by_mint.setdefault(row[MINT], []).append(row)
    repeated = {m: v for m, v in by_mint.items() if len(v) > 1}
    return {
        'rows_per_mint': _counts(len(v) for v in by_mint.values()),
        'repeated_mints': len(repeated),
        'method_mix_of_repeats': _counts('+'.join(sorted(str(r.get(METHOD)) for r in v))
                                         for v in repeated.values()),
        'slot_span_of_repeats': _spread(max(r[SLOT] for r in v) - min(r[SLOT] for r in v)
                                        for v in repeated.values()),
        'repeats_with_one_signer': sum(1 for v in repeated.values()
                                       if len({r.get(SIGNER) for r in v}) == 1),
        'repeats_with_one_quote': sum(1 for v in repeated.values()
                                      if len({r.get(QUOTE_MINT) for r in v}) == 1),
        'examples': [{'mint': m, 'rows': [[r.get(METHOD), r.get(SLOT), str(r.get(SIGNATURE))[:12],
                                          r.get(QUOTE_NAME)] for r in sorted(v, key=lambda r: r[SLOT])]}
                     for m, v in sorted(repeated.items())[:3]],
    }


def hourly_section(rows, raw, pfile, table):
    hours = Counter(str(r.get(TIME))[:13] for r in _with_method(rows, CREATE_METHODS))
    return dict(sorted(hours.items()))


def samples_section(rows, raw, pfile, table):
    def pick(test, count=2):
        return [r for r in rows if test(r)][:count]
    return {
        'creation': pick(lambda r: r.get(METHOD) in CREATE_METHODS and is_good(r)),
        'migration': pick(lambda r: r.get(METHOD) in MIGRATE_METHODS and is_good(r)),
        'usdc_quote_creation': pick(lambda r: r.get(METHOD) in CREATE_METHODS and is_good(r)
                                    and r.get(QUOTE_MINT) == USDC_MINT),
        'failed_transaction': pick(lambda r: r.get(SUCCESS) != 1),
        'off_trunk': pick(lambda r: r.get(TRUNK) != 1),
    }


def consumer_check_section(rows, raw, pfile, table):
    """How pump_historical_sample.validate() would treat this file today.

    It takes the first four create/create_v2 rows (file order) with a plausible
    signature and never looks at the success / canonical-chain columns.
    """
    candidates = [r for r in rows if r.get(METHOD) in CREATE_METHODS
                  and isinstance(r.get(SIGNATURE), str) and len(r[SIGNATURE]) >= 64]
    bad = [r for r in candidates if not is_good(r)]
    return {
        'candidates': len(candidates), 'unusable_candidates': len(bad),
        'first_four': [{'signature': r[SIGNATURE][:16], 'success': r.get(SUCCESS),
                        'on_trunk': r.get(TRUNK)} for r in candidates[:4]],
        'unusable_in_first_four': sum(1 for r in candidates[:4] if not is_good(r)),
    }


SECTIONS = (
    ('file', file_section), ('schema', schema_section), ('methods', methods_section),
    ('keys', keys_section), ('ranges', ranges_section), ('nulls', nulls_section),
    ('creations', creations_section), ('migrations', migrations_section),
    ('funnel', funnel_section), ('migration_repeats', migration_repeats_section),
    ('hourly', hourly_section),
    ('consumer_check', consumer_check_section), ('samples', samples_section),
)


def profile(raw):
    """Profile Parquet bytes. A failing section is reported, never hidden."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    pfile = pq.ParquetFile(pa.BufferReader(raw))
    table = pfile.read()
    rows = table.to_pylist()
    report = {}
    for name, section in SECTIONS:
        try:
            report[name] = section(rows, raw, pfile, table)
        except Exception as error:  # a schema surprise must not hide the other sections
            report[name] = {'error': f'{type(error).__name__}: {str(error)[:200]}'}
    return report


def availability(start, end, opener=urllib.request.urlopen):
    """Which daily files are publicly downloadable? HEAD requests only."""
    day, last = datetime.date.fromisoformat(start), datetime.date.fromisoformat(end)
    if last < day or (last - day).days > 400:
        raise ValueError('scan window must be between 1 and 401 days')
    public, refused = {}, Counter()
    while day <= last:
        request = urllib.request.Request(f'{BASE}{day.isoformat()}.parquet', method='HEAD')
        try:
            with opener(request, timeout=15) as response:
                public[day.isoformat()] = int(response.headers.get('Content-Length') or 0)
        except urllib.error.HTTPError as error:
            refused[error.code] += 1
        day += datetime.timedelta(days=1)
    return {'window': [start, end], 'public_days': public,
            'refused_by_status': {str(k): v for k, v in refused.items()}}


# --- GitHub Actions annotation mirror -----------------------------------------

def _escape(text, *, prop=False):
    text = text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
    return text.replace(':', '%3A').replace(',', '%2C') if prop else text


def annotation_slices(report, size=ANNOTATION_CHARS):
    """One compact JSON document per section, split into annotation-sized slices."""
    slices = []
    for name, section in report.items():
        text = json.dumps(section, sort_keys=True, separators=(',', ':'), default=str)
        pieces = [text[i:i + size] for i in range(0, len(text), size)] or ['']
        slices.extend((f'{name} {n}/{len(pieces)}', piece) for n, piece in enumerate(pieces, 1))
    return slices


def emit_part(report, part, parts):
    """Print one step's share of the annotations (Actions caps notices per step)."""
    slices = annotation_slices(report)
    first = (part - 1) * ANNOTATIONS_PER_STEP
    for title, text in slices[first:first + ANNOTATIONS_PER_STEP]:
        print(f'::notice title={_escape(title, prop=True)}::{_escape(text)}')
    hidden = len(slices) - parts * ANNOTATIONS_PER_STEP
    if part == parts and hidden > 0:
        print(f'::warning::{hidden} annotation slices did not fit; read pump-dataset-profile.json instead')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('profile', help='profile a local path or https URL')
    run.add_argument('source', nargs='?', default=SAMPLE)
    run.add_argument('--scan', metavar='START:END',
                     help='also HEAD-check which daily files in this date range are public')
    run.add_argument('--output', default='pump-dataset-profile.json')
    mirror = commands.add_parser('annotate', help='mirror a profile into Actions annotations')
    mirror.add_argument('report')
    mirror.add_argument('--part', type=int, required=True)
    mirror.add_argument('--parts', type=int, default=3)
    args = parser.parse_args(argv)
    try:
        if args.command == 'annotate':
            if not 1 <= args.part <= args.parts:
                parser.error('--part must be between 1 and --parts')
            with open(args.report) as file:
                emit_part(json.load(file), args.part, args.parts)
            return 0
        report = profile(fetch(args.source))
        if args.scan:
            start, _, end = args.scan.partition(':')
            report['availability'] = availability(start, end)
        with open(args.output, 'w') as file:
            json.dump(report, file, indent=2, default=str)
        print('Profiled', report['file'].get('rows'), 'rows;',
              sum(1 for s in report.values() if 'error' in s), 'section errors')
        return 0
    except (ValueError, OSError, ImportError) as error:
        # Print only the exception type and our own messages, never raw payloads.
        print(f'::error::Profile failed: {type(error).__name__}: {str(error)[:160]}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
