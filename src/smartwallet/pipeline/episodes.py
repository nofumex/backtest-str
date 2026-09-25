from __future__ import annotations

import bisect
import hashlib
import json
from collections import Counter, defaultdict
from typing import Any

from ..pipeline.entity_context import historical_entity_context, historical_wallet_context
from ..storage import Storage
from ..normalize import STABLE_TOKEN_IDS, asset_key


class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _counterparties(evidence: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for leg in evidence.get("sends") or []:
        if isinstance(leg, dict) and leg.get("to_addr"):
            out.add(str(leg["to_addr"]).lower())
    for leg in evidence.get("receives") or []:
        if isinstance(leg, dict) and leg.get("from_addr"):
            out.add(str(leg["from_addr"]).lower())
    return out


def _nearest_event(indexes: list[int], events: list[dict[str, Any]], ts: int, gap: int) -> int | None:
    times = [events[i]["ts"] for i in indexes]
    p = bisect.bisect_left(times, ts)
    candidates = []
    if p < len(indexes): candidates.append(indexes[p])
    if p > 0: candidates.append(indexes[p - 1])
    candidates = [i for i in candidates if abs(events[i]["ts"] - ts) <= gap]
    return min(candidates, key=lambda i: abs(events[i]["ts"] - ts)) if candidates else None



def _action_token(row: dict[str, Any]) -> str:
    token = str(row["action_type"])
    if row.get("cex_id"):
        token += f"@cex:{row['cex_id']}"
    elif row.get("project_id"):
        token += f"@project:{row['project_id']}"
    elif row.get("cate_id"):
        token += f"@cate:{row['cate_id']}"
    return token


def behavior_patterns(rows: list[dict[str, Any]]) -> list[str]:
    """Hierarchical sequence patterns used by the statistical layer.

    Pattern prefixes keep broad and specific hypotheses separate. Each episode contributes at
    most once to a pattern even if the same action/bigram repeats inside the episode.
    """
    tokens = [_action_token(r) for r in rows]
    patterns: set[str] = set()
    for t in tokens:
        patterns.add(f"ACTION:{t}")
    for n, prefix in ((2, "BIGRAM"), (3, "TRIGRAM")):
        for i in range(0, max(0, len(tokens) - n + 1)):
            patterns.add(f"{prefix}:" + ">".join(tokens[i:i+n]))
    if len(tokens) >= 2:
        patterns.add(f"ENDPOINT:{tokens[0]}>{tokens[-1]}")
    if 1 <= len(tokens) <= 6:
        patterns.add("FULL:" + ">".join(tokens))
    return sorted(patterns)



def _tx_enrichments(storage: Storage, tx_hash: str | None, chain: str) -> dict[str, Any]:
    if not tx_hash:
        return {}
    rows = storage.fetchall(
        "SELECT source,payload_json FROM tx_enrichment WHERE tx_hash=? AND chain=? ORDER BY source",
        (tx_hash, chain),
    )
    out: dict[str, Any] = {}
    for row in rows:
        try:
            out[str(row["source"])] = json.loads(row["payload_json"])
        except Exception:
            continue
    return out


def build_episodes(storage: Storage, entity_id: str, gap_seconds: int, max_events: int = 50) -> list[dict[str, Any]]:
    events = storage.fetchall("SELECT * FROM wallet_events WHERE entity_id=? ORDER BY ts,event_id", (entity_id,))
    if not events:
        return []
    for e in events:
        e["evidence"] = json.loads(e["evidence_json"])
    dsu = DSU(len(events))
    by_wallet: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(events):
        by_wallet[e["wallet"].lower()].append(i)

    # Same-wallet temporal continuity, bounded by a fixed episode window.
    # This avoids one giant multi-year component for continuously active market-maker wallets.
    for idxs in by_wallet.values():
        if not idxs:
            continue
        anchor = idxs[0]
        prev = idxs[0]
        for cur in idxs[1:]:
            if events[cur]["ts"] - events[anchor]["ts"] <= gap_seconds:
                dsu.union(prev, cur)
            else:
                anchor = cur
            prev = cur

    # Confirmed own-wallet transfer continuity: only known entity wallets can bridge components.
    owned = set(by_wallet)
    for i, e in enumerate(events):
        for cp in _counterparties(e["evidence"]):
            if cp in owned and cp != e["wallet"].lower():
                j = _nearest_event(by_wallet[cp], events, e["ts"], gap_seconds)
                if j is not None:
                    dsu.union(i, j)

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for i, e in enumerate(events):
        groups[dsu.find(i)].append(e)

    out: list[dict[str, Any]] = []
    bounded_groups: list[list[dict[str, Any]]] = []
    for component in groups.values():
        component.sort(key=lambda x: (x["ts"], x["event_id"]))
        for i in range(0, len(component), max_events):
            bounded_groups.append(component[i:i + max_events])

    arkham_chain = {"eth":"ethereum", "arb":"arbitrum_one", "op":"optimism", "matic":"polygon", "avax":"avalanche"}
    for rows in bounded_groups:
        start_ts, end_ts = rows[0]["ts"], rows[-1]["ts"]
        motif = ">".join(_action_token(r) for r in rows)
        patterns = behavior_patterns(rows)
        asset_weights: Counter[str] = Counter()
        for r in rows:
            evidence = r.get("evidence") or {}
            for side, sign in (("sends", 1.0), ("receives", 1.0)):
                for leg in evidence.get(side) or []:
                    token_id = str(leg.get("token_id") or "").lower()
                    if token_id in STABLE_TOKEN_IDS:
                        continue
                    key = asset_key(r["chain"], token_id)
                    if key:
                        amount = abs(float(leg.get("amount") or 0) * float(leg.get("price") or 0))
                        asset_weights[key] += amount or 1.0
            if r.get("primary_asset_key") and not str(r.get("primary_token_id") or "").lower() in STABLE_TOKEN_IDS:
                asset_weights[r["primary_asset_key"]] += float(r.get("usd_value") or 0.0) or 1.0
        primary_asset = asset_weights.most_common(1)[0][0] if asset_weights else None
        wallets = sorted({r["wallet"] for r in rows})
        episode_id = hashlib.sha256(json.dumps([entity_id, [r["event_id"] for r in rows]], separators=(",", ":")).encode()).hexdigest()
        chains = Counter(r["chain"] for r in rows)
        context_chain = arkham_chain.get(chains.most_common(1)[0][0], chains.most_common(1)[0][0])
        episode = {
            "episode_id": episode_id,
            "entity_id": entity_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "wallets": wallets,
            "event_ids": [r["event_id"] for r in rows],
            "motif": motif,
            "primary_asset_key": primary_asset,
            "target_asset_key": primary_asset,
            "gross_usd": sum(float(r.get("usd_value") or 0.0) for r in rows),
            "evidence": {
                "patterns": patterns,
                "events": [
                    {
                        "ts": r["ts"], "wallet": r["wallet"], "chain": r["chain"], "tx_hash": r.get("tx_hash"),
                        "action_type": r["action_type"], "cate_id": r.get("cate_id"), "cex_id": r.get("cex_id"),
                        "project_id": r.get("project_id"), "usd_value": r.get("usd_value"),
                        "primary_asset_key": r.get("primary_asset_key"), "evidence": r["evidence"],
                        "tx_enrichments": _tx_enrichments(storage, r.get("tx_hash"), r["chain"]),
                    }
                    for r in rows
                ],
                "entity_context_pre_event": historical_entity_context(storage, entity_id, context_chain, start_ts),
                "wallet_context_pre_event": {
                    w: historical_wallet_context(storage, entity_id, w, context_chain, start_ts) for w in wallets
                },
            },
        }
        storage.save_episode(episode)
        out.append(episode)
    return out
