from types import SimpleNamespace
import pump_history


def test_pagination_reaches_launch_and_does_not_claim_full_coverage(monkeypatch):
    calls = []
    pages = [
        [{'slot': 120, 'signature': 'latest', 'err': None},
         {'slot': 106, 'signature': 'one', 'err': None}],
        [{'slot': 103, 'signature': 'two', 'err': None},
         {'slot': 99, 'signature': 'older', 'err': None}],
    ]
    def rpc(url, method, args):
        calls.append(args[1])
        return pages[len(calls)-1]
    monkeypatch.setattr(pump_history, 'rpc', rpc)
    monkeypatch.setattr(pump_history, 'fetch_transaction', lambda url, sig: {'signature': sig})
    monkeypatch.setattr(pump_history, 'parse_buys', lambda tx, launch: [
        SimpleNamespace(model_dump=lambda: {'signature': tx['signature']})])
    result = pump_history.collect_early_buys('private', SimpleNamespace(pool='curve', slot=100,
                                                 end_slot=110), page_size=2)
    assert calls[1]['before'] == 'one'
    assert [b['signature'] for b in result['buys']] == ['two', 'one']
    assert result['reached_launch_slot'] is True
    assert result['buyers_complete'] is False


def test_stops_at_page_cap(monkeypatch):
    monkeypatch.setattr(pump_history, 'rpc', lambda url, method, args: [
        {'slot': 110, 'signature': 'one', 'err': None}])
    monkeypatch.setattr(pump_history, 'fetch_transaction', lambda *a: {})
    monkeypatch.setattr(pump_history, 'parse_buys', lambda *a: [])
    result = pump_history.collect_early_buys('private', SimpleNamespace(pool='curve', slot=100,
                                                 end_slot=110), max_pages=1, page_size=1)
    assert not result['reached_launch_slot']
    assert not result['buyers_complete']
