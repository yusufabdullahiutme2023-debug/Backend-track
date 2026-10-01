import os
import uuid

import psycopg2
import pytest

import pump_worker


@pytest.fixture
def pg():
    """A pump_worker.Store on a real Postgres inside a throwaway schema.

    Skips when DATABASE_URL is unset or unreachable. Everything happens in a schema created
    for the test and dropped afterwards, so real ``pump_*`` tables are never touched even if
    DATABASE_URL points at live data. Yields ``(store, admin_connection, schema_name)``.
    """
    url = os.environ.get('DATABASE_URL', '')
    if not url:
        pytest.skip('DATABASE_URL not set')
    try:
        admin = psycopg2.connect(url, connect_timeout=2)
    except psycopg2.Error:
        pytest.skip('DATABASE_URL does not reach a Postgres server')
    admin.autocommit = True
    schema = 'pump_test_' + uuid.uuid4().hex[:10]
    with admin.cursor() as cur:
        cur.execute(f'CREATE SCHEMA {schema}')

    def connect(dsn, **kwargs):
        return psycopg2.connect(dsn, options=f'-c search_path={schema}', **kwargs)

    store = pump_worker.Store(url, connect=connect)
    store.prepare()
    try:
        yield store, admin, schema
    finally:
        store.close()
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA {schema} CASCADE')
        admin.close()
