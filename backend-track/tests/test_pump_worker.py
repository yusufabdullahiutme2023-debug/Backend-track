import http.client
import io
import json
import urllib.error

import pytest

import pump_worker
from pump_replay import MINT_AUTHORITY, PUMP_PROGRAM
from pump_fakes import BUY_LOGS, CREATE_LOGS, notification, plain_tx, signature


def test_websocket_endpoint_never_logs_or_mutates_key():
    assert pump_worker.websocket_url('https://mainnet.helius-rpc.com/?api-key=private') == \
        'wss://mainnet.helius-rpc.com/?api-key=private'
    with pytest.raises(ValueError):
        pump_worker.websocket_url('https://evil.example/?api-key=private')


def test_backfill_since_checkpoint_is_oldest_first(monkeypatch):
    calls = []
    def fake_rpc(url, method, params):
        calls.append(params)
        return [{'signature': 'newest'}, {'signature': 'middle'}, {'signature': 'previous'}]
    monkeypatch.setattr(pump_worker, 'rpc', fake_rpc)
    assert pump_worker.missed_signatures('private', 'previous') == ['middle', 'newest']
    assert calls[0][0] == pump_worker.PUMP_PROGRAM


def test_backfill_fails_when_cursor_not_found(monkeypatch):
    monkeypatch.setattr(pump_worker, 'rpc', lambda *a: [{'signature': 'unrelated'}])
    with pytest.raises(RuntimeError, match='cursor missing'):
        pump_worker.missed_signatures('private', 'previous')


# --- Settings -------------------------------------------------------------------------------

def test_settings_defaults_and_env_overrides():
    defaults = pump_worker.Settings.from_env({})
    assert (defaults.heartbeat_seconds, defaults.stall_seconds, defaults.backfill_concurrency) == (15.0, 60.0, 4)
    assert defaults.max_backfill_pages == 10
    tuned = pump_worker.Settings.from_env({'PUMP_WORKER_HEARTBEAT_SECONDS': '5',
                                           'PUMP_WORKER_STALL_SECONDS': '30',
                                           'PUMP_WORKER_BACKFILL_CONCURRENCY': '8',
                                           'PUMP_WORKER_MAX_BACKFILL_PAGES': '60'})
    assert (tuned.heartbeat_seconds, tuned.stall_seconds, tuned.backfill_concurrency) == (5.0, 30.0, 8)
    assert tuned.max_backfill_pages == 60
    assert pump_worker.Settings.from_env({'PUMP_WORKER_STALL_SECONDS': '  '}).stall_seconds == 60.0


@pytest.mark.parametrize('name, value', [
    ('PUMP_WORKER_BACKFILL_CONCURRENCY', '0'), ('PUMP_WORKER_BACKFILL_CONCURRENCY', '99'),
    ('PUMP_WORKER_STALL_SECONDS', 'soon'), ('PUMP_WORKER_STALL_SECONDS', '1'),
    ('PUMP_WORKER_HEARTBEAT_SECONDS', '0'), ('PUMP_WORKER_HEARTBEAT_SECONDS', '9999'),
    ('PUMP_WORKER_MAX_BACKFILL_PAGES', '0'), ('PUMP_WORKER_MAX_BACKFILL_PAGES', '501'),
])
def test_settings_reject_values_that_would_break_the_worker(name, value):
    with pytest.raises(ValueError, match=name):
        pump_worker.Settings.from_env({name: value})


# --- Message classification -----------------------------------------------------------------

@pytest.mark.parametrize('message, action', [
    ({'jsonrpc': '2.0', 'result': 7, 'id': 1}, 'ignore'),              # subscription ack
    (['not', 'an', 'object'], 'ignore'),
    ({'method': 'slotNotification', 'params': {}}, 'ignore'),
    (json.loads(notification('s', BUY_LOGS, err={'InstructionError': [0, 'x']})), 'failed'),
    (json.loads(notification('s', BUY_LOGS)), 'filtered'),
    (json.loads(notification('s', CREATE_LOGS)), 'candidate'),
    (json.loads(notification('s', None)), 'unfiltered'),               # logs absent
    (json.loads(notification('s', BUY_LOGS + ['Log truncated'])), 'unfiltered'),
    ({'method': 'logsNotification', 'params': {'result': {'value': {'err': None}}}}, 'malformed'),
    ({'method': 'logsNotification', 'params': 'garbage'}, 'malformed'),
])
def test_classify_decides_from_logs_alone(message, action):
    assert pump_worker.classify(message)[0] == action


def test_classify_returns_signature_and_slot():
    assert pump_worker.classify(json.loads(notification('abc', CREATE_LOGS, slot=77))) == ('candidate', 'abc', 77)


# --- getTransaction retry policy ------------------------------------------------------------

FAST = pump_worker.Settings(fetch_attempts=4, fetch_backoff=(0.25, 0.5, 1.0))


def http_error(code):
    return urllib.error.HTTPError('https://mainnet.helius-rpc.com/?api-key=SECRET', code, 'x', {}, io.BytesIO())


def flaky(monkeypatch, failures, result=None):
    calls = []

    def fake(url, sig):
        calls.append(sig)
        if len(calls) <= len(failures):
            raise failures[len(calls) - 1]
        return result if result is not None else plain_tx(sig)
    monkeypatch.setattr(pump_worker, 'fetch_transaction', fake)
    slept = []
    monkeypatch.setattr(pump_worker.time, 'sleep', slept.append)
    return calls, slept


def test_empty_response_is_retried_in_place_with_backoff(monkeypatch):
    calls, slept = flaky(monkeypatch, [ValueError('null'), ValueError('null')])
    state = pump_worker.WorkerState()
    tx = pump_worker.fetch_with_retry('url', 'sig', FAST, state)
    assert tx['transaction']['signatures'] == ['sig']
    assert len(calls) == 3 and slept == [0.25, 0.5]
    assert state.counters['rpc_calls'] == 3 and state.counters['fetch_retries'] == 2


def test_backoff_holds_at_its_last_step_when_attempts_outnumber_it(monkeypatch):
    calls, slept = flaky(monkeypatch, [ValueError('x')] * 4)
    settings = pump_worker.Settings(fetch_attempts=5, fetch_backoff=(0.25, 0.5, 1.0))
    pump_worker.fetch_with_retry('url', 'sig', settings)
    assert slept == [0.25, 0.5, 1.0, 1.0]


def test_giving_up_raises_without_leaking_exception_text(monkeypatch):
    flaky(monkeypatch, [ValueError('https://mainnet.helius-rpc.com/?api-key=SECRET')] * 10)
    with pytest.raises(pump_worker.TransactionUnavailable) as caught:
        pump_worker.fetch_with_retry('url', 'sig', FAST)
    assert 'SECRET' not in str(caught.value) and 'api-key' not in str(caught.value)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize('failure', [
    http_error(429), http_error(503), http_error(500), http_error(408),
    urllib.error.URLError('connection reset'), TimeoutError(), ConnectionResetError(),
    http.client.IncompleteRead(b''), json.JSONDecodeError('bad body', '', 0),
])
def test_transient_failures_are_retried(monkeypatch, failure):
    calls, _ = flaky(monkeypatch, [failure])
    assert pump_worker.fetch_with_retry('url', 'sig', FAST)
    assert len(calls) == 2


def test_a_rate_limit_waits_for_the_providers_window_not_just_milliseconds(monkeypatch):
    def limited(hint):
        error = urllib.error.HTTPError('https://mainnet.helius-rpc.com/?api-key=SECRET', 429, 'x',
                                       {'Retry-After': hint} if hint is not None else {}, io.BytesIO())
        calls, slept = flaky(monkeypatch, [error])
        pump_worker.fetch_with_retry('url', 'sig', FAST)
        return slept
    assert limited('2') == [2.0]          # the provider's hint
    assert limited(None) == [1.0]         # no hint: at least a second, longer than the 0.25 s backoff
    assert limited('600') == [5.0]        # an absurd hint is bounded
    assert limited('soon') == [1.0]       # an unreadable hint falls back
    calls, slept = flaky(monkeypatch, [http_error(503)])
    pump_worker.fetch_with_retry('url', 'sig', FAST)
    assert slept == [0.25]                # a plain server error keeps the short backoff


@pytest.mark.parametrize('code', [400, 401, 403, 404])
def test_request_and_credential_errors_are_not_retried(monkeypatch, code):
    calls, slept = flaky(monkeypatch, [http_error(code)] * 5)
    with pytest.raises(urllib.error.HTTPError):
        pump_worker.fetch_with_retry('url', 'sig', FAST)
    assert len(calls) == 1 and slept == []


# --- Backfill listing -----------------------------------------------------------------------

def test_missed_items_keep_the_failure_flag_and_the_old_contract(monkeypatch):
    page = [{'signature': 'c', 'err': None}, {'signature': 'b', 'err': {'InstructionError': [0, 'x']}},
            {'signature': 'a', 'err': None}, {'signature': 'cursor', 'err': None}]
    monkeypatch.setattr(pump_worker, 'rpc', lambda *a: page)
    items = pump_worker.missed_items('private', 'cursor')
    assert [i['signature'] for i in items] == ['a', 'b', 'c']
    assert [i['err'] is None for i in items] == [True, False, True]
    assert pump_worker.missed_signatures('private', 'cursor') == ['a', 'b', 'c']   # failed ones included, as before


def test_missed_items_page_through_history_and_count_rpc_calls(monkeypatch):
    first = [{'signature': f'n{i}'} for i in range(1000, 0, -1)]
    second = [{'signature': 'n0'}, {'signature': 'cursor'}]
    seen = []

    def fake_rpc(url, method, params):
        seen.append(params[1].get('before'))
        return second if params[1].get('before') else first
    monkeypatch.setattr(pump_worker, 'rpc', fake_rpc)
    state = pump_worker.WorkerState()
    items = pump_worker.missed_items('private', 'cursor', state=state)
    assert [i['signature'] for i in items][:2] == ['n0', 'n1'] and items[-1]['signature'] == 'n1000'
    assert len(items) == 1001 and seen == [None, 'n1'] and state.counters['rpc_calls'] == 2


def test_backlog_beyond_ten_pages_fails_closed(monkeypatch):
    monkeypatch.setattr(pump_worker, 'rpc', lambda *a: [{'signature': signature(i)} for i in range(1000)])
    with pytest.raises(pump_worker.ManualBackfillRequired, match='Missed more than 10,000 .*manual backfill required'):
        pump_worker.missed_items('private', 'never-seen')


def test_the_page_limit_is_the_operators_decision(monkeypatch):
    calls = []

    def pages(url, method, params):
        calls.append(params[1].get('before'))
        # 25 full pages, then the cursor: reachable only if the operator allows 26 pages.
        if len(calls) == 26:
            return [{'signature': 'newest-of-last-page'}, {'signature': 'cursor'}]
        return [{'signature': f'p{len(calls)}-{i}'} for i in range(1000)]
    monkeypatch.setattr(pump_worker, 'rpc', pages)
    with pytest.raises(pump_worker.ManualBackfillRequired, match='Missed more than 2,000'):
        pump_worker.missed_items('private', 'cursor', max_pages=2)
    assert len(calls) == 2                                   # stopped exactly at the limit
    calls.clear()
    items = pump_worker.missed_items('private', 'cursor', max_pages=26)
    assert len(items) == 25 * 1000 + 1 and len(calls) == 26


def test_a_cursor_missing_from_provider_history_needs_a_person(monkeypatch):
    monkeypatch.setattr(pump_worker, 'rpc', lambda *a: [{'signature': 'unrelated'}])
    with pytest.raises(pump_worker.ManualBackfillRequired, match='cursor missing'):
        pump_worker.missed_items('private', 'previous')


# --- Stream choice ---------------------------------------------------------------------------

def test_the_program_stream_is_the_default_and_keeps_its_old_cursor_name():
    settings = pump_worker.Settings.from_env({})
    assert settings.stream == 'program' and settings.address == PUMP_PROGRAM
    assert settings.checkpoint == pump_worker.CHECKPOINT_NAME == 'pump-fun-creations-v1'
    assert settings.stall_seconds == 60.0


def test_the_creations_stream_listens_to_the_mint_authority_with_its_own_cursor():
    settings = pump_worker.Settings.from_env({'PUMP_WORKER_STREAM': 'creations'})
    assert settings.address == MINT_AUTHORITY
    assert settings.checkpoint != pump_worker.CHECKPOINT_NAME           # a signature from one history
    assert settings.stall_seconds == 300.0                              # is not found in the other
    explicit = pump_worker.Settings.from_env({'PUMP_WORKER_STREAM': ' creations ', 'PUMP_WORKER_STALL_SECONDS': '90'})
    assert explicit.stream == 'creations' and explicit.stall_seconds == 90.0


@pytest.mark.parametrize('value', ['everything', 'Program', 'mint-authority'])
def test_an_unknown_stream_is_rejected_at_startup(value):
    with pytest.raises(ValueError, match='PUMP_WORKER_STREAM must be one of: creations, program'):
        pump_worker.Settings.from_env({'PUMP_WORKER_STREAM': value})


def test_history_is_read_from_the_address_the_stream_mentions(monkeypatch):
    seen = []
    monkeypatch.setattr(pump_worker, 'rpc', lambda url, method, params: (
        seen.append(params[0]), [{'signature': 'newest'}, {'signature': 'cursor'}])[1])
    pump_worker.missed_items('private', 'cursor')
    pump_worker.missed_items('private', 'cursor', address=MINT_AUTHORITY)
    assert seen == [PUMP_PROGRAM, MINT_AUTHORITY]


def test_a_stream_filtered_by_its_subscription_trusts_it_instead_of_the_log_text():
    buy_only = json.loads(notification('s', BUY_LOGS))                  # successful, no creation line
    assert pump_worker.classify(buy_only)[0] == 'filtered'              # the program stream skips it
    assert pump_worker.classify(buy_only, filter_logs=False)[0] == 'candidate'
    for logs in (None, [], BUY_LOGS + ['Log truncated'], CREATE_LOGS):
        assert pump_worker.classify(json.loads(notification('s', logs)), filter_logs=False)[0] == 'candidate'
    failed = json.loads(notification('s', CREATE_LOGS, err={'InstructionError': [0, 'x']}))
    assert pump_worker.classify(failed, filter_logs=False)[0] == 'failed'     # failures are never launches
    assert pump_worker.classify({'jsonrpc': '2.0', 'result': 1}, filter_logs=False)[0] == 'ignore'


def test_only_the_creations_stream_skips_the_log_filter():
    assert pump_worker.Settings.from_env({}).filter_logs is True
    assert pump_worker.Settings.from_env({'PUMP_WORKER_STREAM': 'creations'}).filter_logs is False


def test_the_default_retry_budget_covers_the_lag_seen_on_live_data():
    # Two live runs left 4-5% of just-announced launches unavailable after ~1.5 s of retrying.
    settings = pump_worker.Settings()
    waited = sum(settings.fetch_backoff[:settings.fetch_attempts - 1])
    assert settings.fetch_attempts == 6 and waited == pytest.approx(6.75)
    assert len(settings.fetch_backoff) >= settings.fetch_attempts - 1          # every wait is explicit
