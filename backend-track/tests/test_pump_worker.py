from types import SimpleNamespace
import pytest
import pump_worker


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
