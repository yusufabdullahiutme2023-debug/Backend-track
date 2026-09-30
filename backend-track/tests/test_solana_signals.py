from solana_signals import (EvidenceBundle, Launch, Buy, FundingEdge, CexLabel,
                            Performance, score_bundle, trace_paths)

W = 'W' * 32
F = 'F' * 32
C = 'C' * 32
M = 'M' * 32
P = 'P' * 32
S = 'S' * 64


def fixture(**changes):
    data = dict(
        launch=Launch(mint=M, pool=P, signature=S, venue='test', slot=100, end_slot=110),
        buys=[Buy(wallet=W, signature='buy', slot=105, order=0, raw_amount=5, verified=True)],
        edges=[FundingEdge(source=C, destination=F, signature='fund1', slot=90, lamports=10**9, verified=True),
               FundingEdge(source=F, destination=W, signature='fund2', slot=99, lamports=10**8, verified=True)],
        labels=[CexLabel(address=C, exchange='test exchange', source='manual verification', verified=True)],
        performance=[Performance(wallet=W, token='old', multiple=55, closed_slot=80,
                                 evidence_signature='sale', verified=True)],
        buyers_complete=True, funding_complete=[W], pnl_complete=[W]
    )
    data.update(changes)
    return EvidenceBundle(**data)


def test_verified_path_and_realized_prebuy_pnl():
    result = score_bundle(fixture())
    assert result['status'] == 'complete'
    assert result['cex_funded_50x_count'] == 1
    assert result['independent_clusters'] == 1
    assert result['qualified_wallets'][0]['exchange_path']['transfer_signatures'] == ['fund2', 'fund1']


def test_future_funding_cannot_qualify():
    bundle = fixture(edges=[FundingEdge(source=C, destination=W, signature='future',
                                        slot=106, lamports=10**9, verified=True)])
    assert score_bundle(bundle)['cex_funded_50x_count'] == 0


def test_future_or_unverified_pnl_cannot_qualify():
    for slot, verified in [(106, True), (80, False)]:
        bundle = fixture(performance=[Performance(wallet=W, token='old', multiple=100,
                                                  closed_slot=slot, evidence_signature='sale',
                                                  verified=verified)])
        assert score_bundle(bundle)['cex_funded_50x_count'] == 0


def test_unverified_edges_and_labels_are_not_trusted():
    bundle = fixture(edges=[FundingEdge(source=C, destination=W, signature='unknown',
                                        slot=90, lamports=10**9, verified=False)])
    assert score_bundle(bundle)['cex_funded_count'] == 0
    bundle = fixture(labels=[CexLabel(address=C, exchange='test', source='unknown', verified=False)])
    assert score_bundle(bundle)['cex_funded_count'] == 0


def test_missing_coverage_is_partial_not_false():
    bundle = fixture(edges=[], funding_complete=[], buyers_complete=False)
    result = score_bundle(bundle)
    assert result['status'] == 'partial'
    assert result['unknown_count'] == 1


def test_cycle_terminates_and_invalid_launch_window_rejected():
    edges = fixture().edges + [FundingEdge(source=W, destination=F, signature='cycle',
                                            slot=89, lamports=100, verified=True)]
    assert len(trace_paths(W, 105, edges, {C: fixture().labels[0]})) == 1
    import pytest
    with pytest.raises(ValueError):
        Launch(mint=M, pool=P, signature=S, venue='test', slot=100, end_slot=201)
