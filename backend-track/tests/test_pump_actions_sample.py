import asyncio
import json
from types import SimpleNamespace
import pump_actions_sample as module


def test_logs_prefilter_is_not_generic_create():
    assert module.creation_log(['Program log: Instruction: CreateV2'])
    assert module.creation_log(['Program log: Instruction: Create'])
    assert not module.creation_log(['Program log: Instruction: Buy',
                                    'Program log: Instruction: CreateTokenAccount'])
    assert not module.creation_log(['Program log: Instruction: Buy'])


def test_sample_requires_real_transaction_validation(monkeypatch):
    class WS:
        def __init__(self):
            self.messages = [json.dumps({'result': 1}), json.dumps({
                'method': 'logsNotification',
                'params': {'result': {'value': {'signature': 'sig', 'err': None,
                    'logs': ['Program log: Instruction: CreateV2']}}}})]
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def send(self, msg): assert 'logsSubscribe' in msg
        async def recv(self): return self.messages.pop(0)
    monkeypatch.setattr(module.websockets, 'connect', lambda *a, **kw: WS())
    monkeypatch.setattr(module, 'fetch_transaction', lambda *a: {})
    monkeypatch.setattr(module, 'parse_launch', lambda tx: None)
    report = asyncio.run(module.sample('https://mainnet.helius-rpc.com/?api-key=redacted',
                                       seconds=10, max_candidates=1))
    assert report['candidates'] == 1
    assert report['validated_launches'] == []
    assert report['buyers_complete'] is False
