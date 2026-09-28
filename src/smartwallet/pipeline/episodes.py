from __future__ import annotations

import bisect
import hashlib
import json
from collections import Counter, defaultdict
from typing import Any

from ..pipeline.entity_context import _nearest_at_or_before
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
    if len(tokens) > 2:
        patterns.add(f"ENDPOINT:{tokens[0]}>{tokens[-1]}")
    if 4 <= len(tokens) <= 6:
        patterns.add("FULL:" + ">".join(tokens))
    return sorted(patterns)



EPISODE_BUILD_VERSION = "2.0-windowed-bulk-context"


def dirty_windows(storage: Storage, run_id: str, entity_id: str, gap_seconds: int,
                  *, limit: int = 5000) -> list[tuple[int, int]]:
    """Cluster dirty event timestamps into independent rebuild windows."""
    rows = storage.fetchall(
        """SELECT e.ts FROM run_events r JOIN wallet_events e USING(event_id)
           WHERE r.run_id=? AND e.entity_id=? AND r.built=0 ORDER BY e.ts LIMIT ?""",
        (run_id, entity_id, limit),
    )
    if not rows:
        return []
    clusters: list[list[int]] = []
    for row in rows:
        ts = int(row["ts"])
        if not clusters or ts - clusters[-1][1] > gap_seconds * 2:
            clusters.append([ts, ts])
        else:
            clusters[-1][1] = ts
    return [(start - gap_seconds, end + gap_seconds) for start, end in clusters]


def _preload_context(storage: Storage, entity_id: str, events: list[dict[str, Any]]):
    enrichments: dict[tuple[str, str], dict[str, Any]] = defaultdict(dict)
    tx_hashes = sorted({str(row["tx_hash"]) for row in events if row.get("tx_hash")})
    for start in range(0, len(tx_hashes), 400):
        chunk = tx_hashes[start:start + 400]
        placeholders = ",".join("?" for _ in chunk)
        for row in storage.fetchall(
            f"SELECT tx_hash,chain,source,payload_json FROM tx_enrichment WHERE tx_hash IN ({placeholders})",
            chunk,
        ):
            try:
                enrichments[(row["tx_hash"], row["chain"])][row["source"]] = json.loads(row["payload_json"])
            except (TypeError, ValueError):
                pass
    entity_payloads = {}
    for row in storage.fetchall(
        """SELECT kind,payload_json FROM entity_snapshots WHERE entity_id=? AND source='arkham'
           AND kind IN ('history','flow','volume') ORDER BY captured_at""", (entity_id,),
    ):
        try:
            entity_payloads[row["kind"]] = json.loads(row["payload_json"])
        except (TypeError, ValueError):
            pass
    wallets = sorted({str(row["wallet"]).lower() for row in events})
    wallet_payloads: dict[tuple[str, str], Any] = {}
    for start in range(0, len(wallets), 400):
        chunk = wallets[start:start + 400]
        placeholders = ",".join("?" for _ in chunk)
        for row in storage.fetchall(
            f"""SELECT address,source,payload_json FROM wallet_positions WHERE entity_id=?
                AND address IN ({placeholders}) AND source IN ('arkham.address_history','arkham.address_flow')
                ORDER BY captured_at""", [entity_id, *chunk],
        ):
            try:
                wallet_payloads[(row["address"].lower(), row["source"])] = json.loads(row["payload_json"])
            except (TypeError, ValueError):
                pass
    return enrichments, entity_payloads, wallet_payloads


def _context_at(payloads: dict[str, Any], chain: str, ts: int) -> dict[str, Any]:
    out = {}
    for kind, payload in payloads.items():
        rows = payload.get(chain) if isinstance(payload, dict) else None
        point = _nearest_at_or_before(rows, ts) if isinstance(rows, list) else None
        if point:
            out[kind] = point
    return out


def build_episodes(
    storage: Storage,
    entity_id: str,
    gap_seconds: int,
    max_events: int = 50,
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Build episodes, optionally for one dirty time window.

    Incremental callers expand the dirty interval by ``gap_seconds`` before calling this
    function. Existing overlapping episodes are replaced after the new set is prepared.
    """
    # Pull complete pre-existing episodes touching the dirty window into the rebuild.
    # This keeps episode membership exact at both gap boundaries without rescanning the entity.
    if run_id and start_ts is not None and end_ts is not None:
        for _ in range(8):
            boundary = storage.fetchone(
                """SELECT MIN(e.start_ts) first_ts,MAX(e.end_ts) last_ts
                   FROM run_episodes r JOIN episodes e USING(episode_id)
                   WHERE r.run_id=? AND e.entity_id=? AND e.end_ts>=? AND e.start_ts<=?""",
                (run_id, entity_id, start_ts, end_ts),
            )
            if not boundary or boundary["first_ts"] is None:
                break
            expanded_start = min(start_ts, int(boundary["first_ts"]))
            expanded_end = max(end_ts, int(boundary["last_ts"]))
            if (expanded_start, expanded_end) == (start_ts, end_ts):
                break
            start_ts, end_ts = expanded_start, expanded_end
    sql = "SELECT * FROM wallet_events WHERE entity_id=?"
    params: list[Any] = [entity_id]
    if run_id:
        sql += " AND event_id IN (SELECT event_id FROM run_events WHERE run_id=?)"
        params.append(run_id)
    if start_ts is not None:
        sql += " AND ts>=?"
        params.append(int(start_ts))
    if end_ts is not None:
        sql += " AND ts<=?"
        params.append(int(end_ts))
    sql += " ORDER BY ts,event_id"
    window_start, window_end = start_ts, end_ts
    events = storage.fetchall(sql, params)
    if not events:
        return []
    for e in events:
        e["evidence"] = json.loads(e["evidence_json"])
    enrichments, entity_payloads, wallet_payloads = _preload_context(storage, entity_id, events)
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
        episode_start, episode_end = rows[0]["ts"], rows[-1]["ts"]
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
        entity_context = _context_at(entity_payloads, context_chain, episode_start)
        wallet_context = {}
        for wallet in wallets:
            payloads = {
                "history": wallet_payloads.get((wallet.lower(), "arkham.address_history"), {}),
                "flow": wallet_payloads.get((wallet.lower(), "arkham.address_flow"), {}),
            }
            value = _context_at(payloads, context_chain, episode_start)
            if value:
                wallet_context[wallet] = value
        episode = {
            "episode_id": episode_id,
            "entity_id": entity_id,
            "start_ts": episode_start,
            "end_ts": episode_end,
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
                        "tx_enrichments": enrichments.get((r.get("tx_hash"), r["chain"]), {}),
                    }
                    for r in rows
                ],
                "entity_context_pre_event": entity_context,
                "wallet_context_pre_event": wallet_context,
            },
        }
        out.append(episode)
    replaced: list[str] = []
    if run_id:
        overlap_sql = """SELECT e.episode_id FROM run_episodes r JOIN episodes e USING(episode_id)
                         WHERE r.run_id=? AND e.entity_id=?"""
        overlap_params: list[Any] = [run_id, entity_id]
        if window_start is not None:
            overlap_sql += " AND e.end_ts>=?"
            overlap_params.append(int(window_start))
        if window_end is not None:
            overlap_sql += " AND e.start_ts<=?"
            overlap_params.append(int(window_end))
        replaced = [row["episode_id"] for row in storage.fetchall(overlap_sql, overlap_params)]
    current_inputs = {
        ep["episode_id"]: hashlib.sha256(json.dumps(ep["event_ids"], separators=(",", ":")).encode()).hexdigest()
        for ep in out
    }
    previous = {}
    episode_ids = list(current_inputs)
    for offset in range(0, len(episode_ids), 400):
        chunk = episode_ids[offset:offset + 400]
        placeholders = ",".join("?" for _ in chunk)
        for row in storage.fetchall(
            f"""SELECT artifact_id,version,input_hash FROM artifact_provenance
                WHERE artifact_type='episode' AND artifact_id IN ({placeholders})""", chunk,
        ):
            previous[row["artifact_id"]] = (row["version"], row["input_hash"])
    changed = [episode_id for episode_id, input_hash in current_inputs.items()
               if previous.get(episode_id) != (EPISODE_BUILD_VERSION, input_hash)]
    storage.save_episodes(out, run_id=run_id, replace_episode_ids=replaced)
    with storage.conn() as db:
        if run_id:
            for offset in range(0, len(changed), 400):
                chunk = changed[offset:offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                db.execute(
                    f"DELETE FROM primary_analysis_observations WHERE run_id=? AND episode_id IN ({placeholders})",
                    [run_id, *chunk],
                )
        db.executemany(
            """INSERT INTO artifact_provenance(artifact_type,artifact_id,version,input_hash,updated_at)
               VALUES('episode',?,?,?,CURRENT_TIMESTAMP) ON CONFLICT(artifact_type,artifact_id) DO UPDATE SET
               version=excluded.version,input_hash=excluded.input_hash,updated_at=excluded.updated_at""",
            [(ep["episode_id"], EPISODE_BUILD_VERSION, current_inputs[ep["episode_id"]]) for ep in out],
        )
    if run_id:
        with storage.conn() as db:
            update = """UPDATE run_events SET built=1 WHERE run_id=? AND event_id IN
                        (SELECT event_id FROM wallet_events WHERE entity_id=?"""
            update_params: list[Any] = [run_id, entity_id]
            if window_start is not None:
                update += " AND ts>=?"
                update_params.append(int(window_start))
            if window_end is not None:
                update += " AND ts<=?"
                update_params.append(int(window_end))
            db.execute(update + ")", update_params)
    return out
