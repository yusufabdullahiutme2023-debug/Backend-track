"""The worker's Postgres layer: one long-lived connection, transparent reconnect, atomic writes.

Part one drives the Store through a fake driver to pin the connection behaviour exactly.
Part two runs it against a real Postgres (the ``pg`` fixture in conftest.py: it skips unless
DATABASE_URL reaches one, and works inside a throwaway schema).
"""
import json
import time
from datetime import datetime, timezone

import psycopg2
import pytest

import pump_worker
from pump_fakes import creation_tx, signature
from pump_replay import parse_launch


# --- fake driver ----------------------------------------------------------------------------

class FakeCursor:
    rowcount = 1

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if self.conn.break_on_execute:
            self.conn.break_on_execute = False
            self.conn.closed = 2
            raise psycopg2.OperationalError('server closed the connection unexpectedly')
        self.conn.statements.append((' '.join(sql.split()), params))

    def fetchone(self):
        return self.conn.row


class FakeConnection:
    def __init__(self, break_on_execute=False, row=None):
        self.closed = 0
        self.statements = []
        self.commits = 0
        self.rollbacks = 0
        self.break_on_execute = break_on_execute
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commits += 1
        else:
            self.rollbacks += 1
        return False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = 1


class Factory:
    def __init__(self, *connections):
        self.queue = list(connections)
        self.made = []
        self.kwargs = []

    def __call__(self, url, **kwargs):
        conn = self.queue.pop(0) if self.queue else FakeConnection()
        self.made.append(conn)
        self.kwargs.append(kwargs)
        return conn


def test_many_writes_share_one_connection():
    factory = Factory()
    store = pump_worker.Store('postgresql://x', connect=factory)
    for n in range(50):
        store.save(signature(n), None)
    assert len(factory.made) == 1 and store.connects == 1
    assert factory.made[0].commits == 50 and factory.made[0].rollbacks == 0


def test_connection_options_bound_every_wait():
    factory = Factory()
    pump_worker.Store('postgresql://x', connect=factory).cursor()
    options = factory.kwargs[0]
    assert options['connect_timeout'] == 10 and options['keepalives'] == 1
    assert options['keepalives_idle'] and options['keepalives_interval'] and options['keepalives_count']


def test_launch_and_cursor_commit_in_one_transaction():
    factory = Factory()
    store = pump_worker.Store('postgresql://x', connect=factory)
    launch = parse_launch(creation_tx(signature(1)))
    store.save(signature(1), launch)
    conn = factory.made[0]
    assert conn.commits == 1
    assert [sql.split(' (')[0] for sql, _ in conn.statements] == [
        'INSERT INTO pump_launches', 'INSERT INTO pump_worker_cursor']


def test_cursor_only_save_writes_no_launch_row():
    factory = Factory()
    pump_worker.Store('postgresql://x', connect=factory).save(signature(1), None)
    assert [sql.split(' (')[0] for sql, _ in factory.made[0].statements] == ['INSERT INTO pump_worker_cursor']


def test_dropped_connection_is_reopened_once_and_the_write_retried():
    broken = FakeConnection(break_on_execute=True)
    factory = Factory(broken)
    store = pump_worker.Store('postgresql://x', connect=factory)
    store.save(signature(1), None)
    healthy = factory.made[1]
    assert store.connects == 2 and broken.closed and broken.rollbacks == 1
    assert healthy.commits == 1 and len(healthy.statements) == 1


def test_a_second_failure_propagates_and_the_next_call_starts_clean():
    factory = Factory(FakeConnection(break_on_execute=True), FakeConnection(break_on_execute=True))
    store = pump_worker.Store('postgresql://x', connect=factory)
    with pytest.raises(psycopg2.OperationalError):
        store.save(signature(1), None)
    store.save(signature(2), None)               # a fresh third connection works
    assert store.connects == 3 and factory.made[2].commits == 1


def test_non_connection_errors_do_not_trigger_a_reconnect():
    factory = Factory()
    store = pump_worker.Store('postgresql://x', connect=factory)

    def work(cur):
        raise ValueError('application bug')
    with pytest.raises(ValueError):
        store._run(work)
    assert store.connects == 1 and factory.made[0].rollbacks == 1 and not factory.made[0].closed


def test_cursor_reads_through_the_same_connection_and_close_releases_it():
    factory = Factory(FakeConnection(row=('abc',)))
    store = pump_worker.Store('postgresql://x', connect=factory)
    assert store.cursor() == 'abc'
    factory.made[0].row = None
    assert store.cursor() is None and store.connects == 1
    store.close()
    assert factory.made[0].closed


# --- real Postgres --------------------------------------------------------------------------

def query(store, sql, params=None):
    def work(cur):
        cur.execute(sql, params)
        return cur.fetchall()
    return store._run(work)


def test_prepare_is_idempotent_and_creates_the_three_tables(pg):
    store, admin, schema = pg
    store.prepare()
    store.prepare()
    names = {row[0] for row in query(store, 'SELECT table_name FROM information_schema.tables '
                                            'WHERE table_schema = %s', (schema,))}
    assert names == {'pump_launches', 'pump_worker_cursor', 'pump_worker_heartbeat'}


def test_cursor_round_trip_and_replacement(pg):
    store = pg[0]
    assert store.cursor() is None
    store.save('first', None)
    assert store.cursor() == 'first'
    store.save('second', None)
    assert store.cursor() == 'second'
    assert query(store, 'SELECT count(*) FROM pump_worker_cursor') == [(1,)]


def test_launch_is_stored_once_with_the_cursor_even_when_replayed(pg):
    store = pg[0]
    launch = parse_launch(creation_tx(signature(7), slot=555))
    assert store.save(signature(7), launch) is True      # newly recorded
    assert store.save(signature(7), launch) is False     # duplicate notification: not new
    assert store.save(signature(8), None) is False       # cursor-only write
    rows = query(store, 'SELECT signature, mint, pool, slot, status, evidence FROM pump_launches')
    assert len(rows) == 1
    sig, mint, pool, slot, status, evidence = rows[0]
    assert (sig, mint, pool, slot, status) == (signature(7), launch.mint, launch.pool, 555, 'unscored')
    assert evidence == json.loads(json.dumps(launch.model_dump()))
    assert store.cursor() == signature(8)


def test_a_failed_write_rolls_back_and_the_connection_stays_usable(pg):
    store = pg[0]

    def work(cur):
        cur.execute("INSERT INTO pump_worker_cursor (name, signature) VALUES ('half', 'written')")
        raise RuntimeError('crash between the two statements')
    with pytest.raises(RuntimeError):
        store._run(work)
    assert query(store, "SELECT count(*) FROM pump_worker_cursor WHERE name = 'half'") == [(0,)]
    assert store.connects == 1                   # a rollback is not a reconnect


def test_one_backend_serves_every_call(pg):
    store = pg[0]
    pids = {query(store, 'SELECT pg_backend_pid()')[0][0] for _ in range(25)}
    store.save('x', None)
    store.beat('streaming', datetime.now(timezone.utc), None, 1, {})
    assert len(pids) == 1 and store.connects == 1


def test_recovers_transparently_after_the_server_kills_the_connection(pg):
    store, admin, _ = pg
    pid = query(store, 'SELECT pg_backend_pid()')[0][0]
    with admin.cursor() as cur:
        cur.execute('SELECT pg_terminate_backend(%s)', (pid,))
        for _ in range(100):                      # termination is asynchronous
            cur.execute('SELECT count(*) FROM pg_stat_activity WHERE pid = %s', (pid,))
            if cur.fetchone()[0] == 0:
                break
            time.sleep(0.02)
    store.save('after-kill', None)               # first use of the dead connection fails, retry succeeds
    assert store.cursor() == 'after-kill' and store.connects == 2


def test_heartbeat_is_one_row_that_is_overwritten_and_aged_by_the_database_clock(pg):
    store = pg[0]
    assert store.heartbeat_status() is None
    started = datetime.now(timezone.utc)
    store.beat('backfilling', started, None, None, {'launches': 0, 'last_error': None})
    store.beat('streaming', started, started, 123, {'launches': 2, 'last_error': 'X in y'})
    assert query(store, 'SELECT count(*) FROM pump_worker_heartbeat') == [(1,)]
    row = store.heartbeat_status()
    assert row['status'] == 'streaming' and row['last_slot'] == 123
    assert row['stats'] == {'launches': 2, 'last_error': 'X in y'}
    assert 0 <= row['heartbeat_age_seconds'] < 5 and row['cursor_age_seconds'] is None
    store.save('cursor', None)
    assert 0 <= store.heartbeat_status()['cursor_age_seconds'] < 5


def test_streams_keep_separate_cursors_and_the_newest_heartbeat_wins(pg):
    program = pg[0]
    creations = pump_worker.Store(program._db_url, connect=program._connect,
                                  checkpoint=pump_worker.STREAMS['creations']['checkpoint'])
    started = datetime.now(timezone.utc)
    try:
        program.save('program-signature', None)
        assert creations.cursor() is None                      # a fresh stream starts from "now", not from the other's cursor
        creations.save('mint-authority-signature', None)
        assert program.cursor() == 'program-signature' and creations.cursor() == 'mint-authority-signature'

        program.beat('streaming', started, None, 1, {'stream': 'program'})
        time.sleep(0.05)
        creations.beat('backfilling', started, None, 2, {'stream': 'creations'})
        newest = program.heartbeat_status()                     # --status needs no idea which stream runs
        assert newest['checkpoint'] == pump_worker.STREAMS['creations']['checkpoint']
        assert newest['status'] == 'backfilling' and newest['stats'] == {'stream': 'creations'}
        assert newest['cursor_age_seconds'] is not None          # joined to ITS cursor, not the other one's

        time.sleep(0.05)
        program.beat('streaming', started, None, 3, {'stream': 'program'})
        assert creations.heartbeat_status()['checkpoint'] == pump_worker.CHECKPOINT_NAME
        assert query(program, 'SELECT count(*) FROM pump_worker_heartbeat') == [(2,)]
    finally:
        creations.close()
