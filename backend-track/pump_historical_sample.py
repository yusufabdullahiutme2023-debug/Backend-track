"""One-off read-only historical validation against a public creation dataset.

The sample is a third-party candidate list, not proof of on-chain events. Each
candidate must be independently validated using Helius getTransaction.
"""
import json
import os
import sys
import urllib.request
from pump_replay import fetch_transaction, parse_launch, parse_buys

SAMPLE = ('https://bitquery-blockchain-dataset.s3.us-east-1.amazonaws.com/'
          'solana/pumpfun_creation_migrations/2026-07-01.parquet')


def validate(rpc_url):
    import pyarrow.parquet as pq
    with urllib.request.urlopen(SAMPLE, timeout=30) as response:
        data = response.read(25_000_001)
    if len(data) > 25_000_000:
        raise ValueError('Public sample exceeds 25MB budget')
    import pyarrow as pa
    table = pq.read_table(pa.BufferReader(data))
    print('Sample rows:', table.num_rows, 'columns:', table.column_names, file=sys.stderr)
    if not {'Transaction_Signature', 'Instruction_Program_Method'} <= set(table.column_names):
        raise ValueError('Public sample lacks required provenance columns')
    candidates = table.select(['Transaction_Signature', 'Instruction_Program_Method']).to_pylist()
    sampled = 0
    for row in candidates:
        if row['Instruction_Program_Method'] not in ('create', 'create_v2'):
            continue
        sig = row['Transaction_Signature']
        if not isinstance(sig, str) or len(sig) < 64:
            continue
        sampled += 1
        if sampled > 6:
            break
        try:
            launch = parse_launch(fetch_transaction(rpc_url, sig))
        except ValueError:
            continue
        if launch is None:
            continue
        # Historical candidate has been independently verified on-chain.
        # Do not infer first buyers from the creation signature alone.
        return {'validated_launch': launch.model_dump(), 'creation_candidates_checked': sampled,
                'buyer_validation': 'not yet validated', 'source': 'public dataset + RPC'}
    raise ValueError(f'No RPC-verified launch among {sampled} historical candidates')


if __name__ == '__main__':
    url = os.environ.get('SOLANA_RPC_URL')
    if not url:
        sys.exit('SOLANA_RPC_URL not configured')
    try:
        print(json.dumps(validate(url), indent=2))
    except (ValueError, KeyError, IndexError, OSError) as error:
        # Do not print raw exceptions: transport failures may embed provider URL.
        safe = str(error) if isinstance(error, ValueError) and str(error).startswith(
            ('Public sample ', 'No RPC-verified launch')) else type(error).__name__
        print(f'Historical validation failed: {safe}', file=sys.stderr)
        sys.exit(1)
