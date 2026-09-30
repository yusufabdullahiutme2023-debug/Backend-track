"""Read-only, evidence-driven Solana launch scoring.

This module does NOT discover pools or infer swaps from mint-address history. An
upstream chain indexer must provide decoded, verified buy and transfer evidence.
Missing evidence is unknown, never a negative finding.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from math import log10
from pydantic import BaseModel, Field, model_validator


class Launch(BaseModel):
    mint: str = Field(min_length=32, max_length=44)
    pool: str = Field(min_length=32, max_length=44)
    signature: str = Field(min_length=64, max_length=128)
    venue: str = Field(min_length=1, max_length=40)
    slot: int = Field(ge=0)
    end_slot: int = Field(ge=0)

    @model_validator(mode="after")
    def valid_window(self):
        if self.end_slot < self.slot or self.end_slot > self.slot + 100:
            raise ValueError("end_slot must be within 100 slots of launch")
        return self


class Buy(BaseModel):
    wallet: str
    signature: str
    slot: int = Field(ge=0)
    order: int = Field(ge=0)  # index within slot, supplied by indexer
    raw_amount: int = Field(gt=0)
    verified: bool = False  # venue-specific decoder verified actual recipient + direction


class FundingEdge(BaseModel):
    source: str
    destination: str
    signature: str
    slot: int = Field(ge=0)
    lamports: int = Field(gt=0)
    verified: bool = False  # decoded system transfer, including inner instructions


class CexLabel(BaseModel):
    address: str
    exchange: str
    source: str  # label provenance
    verified: bool = False


class Performance(BaseModel):
    wallet: str
    token: str
    multiple: float = Field(ge=0, allow_inf_nan=False)
    closed_slot: int = Field(ge=0)  # realized, completed BEFORE buyer's buy
    evidence_signature: str
    verified: bool = False


class EvidenceBundle(BaseModel):
    launch: Launch
    buys: list[Buy] = Field(default_factory=list, max_length=2000)
    edges: list[FundingEdge] = Field(default_factory=list, max_length=10000)
    labels: list[CexLabel] = Field(default_factory=list, max_length=1000)
    performance: list[Performance] = Field(default_factory=list, max_length=10000)
    buyers_complete: bool = False
    funding_complete: list[str] = Field(default_factory=list)
    pnl_complete: list[str] = Field(default_factory=list)


def trace_paths(wallet: str, before_slot: int, edges: list[FundingEdge],
                labels: dict[str, CexLabel], max_hops: int = 5) -> list[dict]:
    """Chronological backwards graph search; explicit transfer evidence only."""
    incoming: dict[str, list[FundingEdge]] = defaultdict(list)
    for edge in edges:
        if edge.verified and edge.source != edge.destination:
            incoming[edge.destination].append(edge)
    paths = []
    queue = [(wallet, before_slot, [wallet], [])]
    while queue:
        current, cutoff, nodes, evidence = queue.pop(0)
        if len(nodes) > 1 and current in labels and labels[current].verified:
            paths.append({"exchange": labels[current].exchange, "hops": len(evidence),
                          "chain": nodes, "transfer_signatures": evidence,
                          "label_source": labels[current].source})
            continue
        if len(evidence) >= max_hops:
            continue
        # Bound branching; favor substantial transfers, but retain multiple candidates.
        candidates = [e for e in incoming[current] if e.slot < cutoff and e.source not in nodes]
        candidates.sort(key=lambda e: (-e.lamports, -e.slot, e.signature))
        for edge in candidates[:10]:
            queue.append((edge.source, edge.slot, nodes + [edge.source],
                          evidence + [edge.signature]))
    return sorted(paths, key=lambda p: (p["hops"], p["exchange"]))


def score_bundle(bundle: EvidenceBundle) -> dict:
    launch = bundle.launch
    buys = sorted((b for b in bundle.buys if b.verified and
                   launch.slot <= b.slot <= launch.end_slot),
                  key=lambda b: (b.slot, b.order, b.signature))
    first = {}
    for buy in buys:
        if len(first) >= 50:
            break
        first.setdefault(buy.wallet, buy)
    labels = {l.address: l for l in bundle.labels if l.verified}
    completed_funding = set(bundle.funding_complete)
    completed_pnl = set(bundle.pnl_complete)
    qualified = []
    unknown = 0
    cex_count = 0
    for wallet, buy in first.items():
        paths = trace_paths(wallet, buy.slot, bundle.edges, labels)
        if not paths:
            if wallet not in completed_funding:
                unknown += 1
            continue
        cex_count += 1
        pnl = [p for p in bundle.performance if p.wallet == wallet and p.verified
               and p.closed_slot < buy.slot and p.multiple >= 50]
        if not pnl:
            if wallet not in completed_pnl:
                unknown += 1
            continue
        best = max(pnl, key=lambda p: p.multiple)
        qualified.append({"wallet": wallet, "exchange_path": paths[0],
                          "best_multiple": best.multiple, "best_token": best.token,
                          "pnl_signature": best.evidence_signature, "buy_signature": buy.signature})
    # Conservative clustering: shared immediate funder = one cluster. This is not
    # identity attribution; more sophisticated linkage requires independent validation.
    clusters = {q["exchange_path"]["chain"][1] if len(q["exchange_path"]["chain"]) > 2
                else q["wallet"] for q in qualified}
    score = sum(log10(q["best_multiple"]) /
                (1 + 0.2 * q["exchange_path"]["hops"]) for q in qualified)
    status = "complete" if bundle.buyers_complete and unknown == 0 else "partial"
    return {"mint": launch.mint, "pool": launch.pool, "launch_signature": launch.signature,
            "launch_slot": launch.slot, "first_buyer_count": len(first),
            "cex_funded_count": cex_count, "cex_funded_50x_count": len(qualified),
            "independent_clusters": len(clusters), "unknown_count": unknown,
            "score": round(score, 3), "status": status, "qualified_wallets": qualified,
            "scored_at": datetime.now(timezone.utc).isoformat()}
