from __future__ import annotations

import asyncio
import bisect
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..llm import FreeLLMClient
from ..normalize import STABLE_TOKEN_IDS, asset_key
from ..storage import Storage, canonical_json, utc_now_iso


CLASSIFIER_NAME = "deterministic_intent"
CLASSIFIER_VERSION = "1.0"
INTENT_LABELS = (
    "buy", "accumulation", "sell", "distribution", "inventory_rebalance",
    "internal_rebalance", "bridge_reposition", "liquidity_management",
    "asset_rotation", "unknown",
)

_STABLE_MARKERS = ("usdc", "usdt", "dai", "tusd", "busd", "usde", "usds", "frax", "stable")
_BRIDGE_MARKERS = ("bridge", "stargate", "across", "hop protocol", "wormhole", "layerzero", "synapse", "celer")
_LIQUIDITY_MARKERS = ("liquidity", "liquidity pool", " lp ", "lp_", "_lp", "pool deposit", "pool withdrawal")
_SUCCESS_MARKERS = ("success", "succeeded", "completed", "complete", "done", "filled")


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value or "")
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _num(value: Any) -> float:
    try:
        number = float(value or 0.0)
        return number if math.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _norm_address(value: Any) -> str:
    return str(value or "").strip().lower()


def _compact_text(value: Any, *, limit: int = 20_000) -> str:
    """Flatten structured metadata for deterministic marker detection only."""
    try:
        text = canonical_json(value).lower()
    except Exception:
        text = str(value).lower()
    return f" {text[:limit]} "


def _leg_asset(chain: str, leg: dict[str, Any]) -> str | None:
    return asset_key(chain, str(leg.get("token_id") or ""))


def _is_stable_leg(chain: str, leg: dict[str, Any], tokens: dict[str, Any]) -> bool:
    token_id = str(leg.get("token_id") or "").lower()
    if token_id in STABLE_TOKEN_IDS:
        return True
    token = tokens.get(token_id) or tokens.get(leg.get("token_id")) or {}
    descriptor = " ".join(str(x or "").lower() for x in (
        token_id, leg.get("symbol"), token.get("symbol") if isinstance(token, dict) else None,
        token.get("name") if isinstance(token, dict) else None,
    ))
    return any(marker in descriptor for marker in _STABLE_MARKERS)


def _leg_value(leg: dict[str, Any]) -> float:
    value = abs(_num(leg.get("amount")) * _num(leg.get("price")))
    return value if value else 1.0


def _successful_bridge_enrichment(enrichments: dict[str, Any]) -> bool:
    for source, payload in enrichments.items():
        text = _compact_text(payload, limit=8_000)
        if "error" in text and not any(word in text for word in _SUCCESS_MARKERS):
            continue
        if any(marker in text for marker in _BRIDGE_MARKERS) and any(word in text for word in _SUCCESS_MARKERS):
            return True
        # Status endpoints are queried for large transactions. Their presence alone is not evidence.
        if source in {"lifi_status", "rubic_status"} and any(word in text for word in _SUCCESS_MARKERS):
            return True
    return False


@dataclass
class ClassificationIndex:
    """All classifier inputs preloaded in a constant number of SQL queries."""

    events: dict[str, dict[str, Any]]
    enrichments: dict[tuple[str, str], dict[str, Any]]
    owned_wallets: set[str]
    wallet_rows: dict[str, list[dict[str, Any]]]
    wallet_times: dict[str, list[int]]
    wallet_prefixes: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    @classmethod
    def load(cls, storage: Storage, entity_id: str, run_id: str | None = None) -> "ClassificationIndex":
        sql = "SELECT * FROM wallet_events WHERE entity_id=?"
        params: list[Any] = [entity_id]
        if run_id:
            sql += " AND event_id IN (SELECT event_id FROM run_events WHERE run_id=?)"
            params.append(run_id)
        rows = storage.fetchall(sql + " ORDER BY wallet,ts,event_id", params)
        events = {row["event_id"]: row for row in rows}
        wallet_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            wallet_rows[_norm_address(row["wallet"])].append(row)
        enrichments: dict[tuple[str, str], dict[str, Any]] = defaultdict(dict)
        if rows:
            enrich_sql = """SELECT t.tx_hash,t.chain,t.source,t.payload_json FROM tx_enrichment t
                WHERE EXISTS(SELECT 1 FROM wallet_events e WHERE e.entity_id=?
                  AND e.tx_hash=t.tx_hash AND e.chain=t.chain)"""
            for item in storage.fetchall(enrich_sql, (entity_id,)):
                enrichments[(item["tx_hash"], item["chain"])][item["source"]] = _json(item["payload_json"], {})
        owned = {
            _norm_address(row["address"])
            for row in storage.fetchall("SELECT DISTINCT address FROM wallets WHERE entity_id=?", (entity_id,))
        }
        wallet_prefixes: dict[str, list[dict[str, Any]]] = {}
        for wallet, values in wallet_rows.items():
            gross = 0.0
            actions: Counter[str] = Counter()
            cexes: Counter[str] = Counter()
            projects: Counter[str] = Counter()
            prefixes = []
            for row in values:
                gross += _num(row.get("usd_value"))
                actions[row.get("action_type")] += 1
                if row.get("cex_id"):
                    cexes[row["cex_id"]] += 1
                if row.get("project_id"):
                    projects[row["project_id"]] += 1
                prefixes.append({
                    "gross_usd": gross, "action_counts": dict(actions),
                    "cex_counts": dict(cexes), "project_counts": dict(projects),
                })
            wallet_prefixes[wallet] = prefixes
        return cls(
            events=events,
            enrichments=dict(enrichments),
            owned_wallets=owned,
            wallet_rows=dict(wallet_rows),
            wallet_times={wallet: [int(row["ts"]) for row in values] for wallet, values in wallet_rows.items()},
            wallet_prefixes=wallet_prefixes,
        )

    def event_payloads(self, episode: dict[str, Any], evidence: dict[str, Any]) -> list[dict[str, Any]]:
        embedded = evidence.get("events") if isinstance(evidence, dict) else None
        if isinstance(embedded, list) and embedded:
            values = [dict(item) for item in embedded if isinstance(item, dict)]
        else:
            values = []
            for event_id in _json(episode.get("event_ids_json"), []):
                row = self.events.get(str(event_id))
                if not row:
                    continue
                values.append({**row, "evidence": _json(row.get("evidence_json"), {})})
        for event in values:
            event["evidence"] = _json(event.get("evidence") or event.get("evidence_json"), {})
            key = (event.get("tx_hash"), event.get("chain"))
            if not event.get("tx_enrichments") and key in self.enrichments:
                event["tx_enrichments"] = self.enrichments[key]
        return values

    def pre_event_profiles(self, wallets: Iterable[str], cutoff_ts: int) -> dict[str, Any]:
        profiles: dict[str, Any] = {}
        for raw_wallet in wallets:
            wallet = _norm_address(raw_wallet)
            rows = self.wallet_rows.get(wallet, [])
            n = bisect.bisect_left(self.wallet_times.get(wallet, []), cutoff_ts)
            prefix = self.wallet_prefixes.get(wallet, [])[n - 1] if n else {}
            profiles[wallet] = {
                "event_count": n,
                "gross_usd": prefix.get("gross_usd", 0.0),
                "action_counts": prefix.get("action_counts", {}),
                "cex_counts": prefix.get("cex_counts", {}),
                "project_counts": prefix.get("project_counts", {}),
            }
        return profiles


def extract_intent_features(episode: dict[str, Any], index: ClassificationIndex) -> dict[str, Any]:
    evidence = _json(episode.get("evidence_json") or episode.get("evidence"), {})
    events = index.event_payloads(episode, evidence)
    wallets = [_norm_address(x) for x in _json(episode.get("wallets_json") or episode.get("wallets"), [])]
    actions: Counter[str] = Counter()
    projects: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    cexes: Counter[str] = Counter()
    send_assets: Counter[str] = Counter()
    receive_assets: Counter[str] = Counter()
    stable_sent = stable_received = nonstable_sent = nonstable_received = 0.0
    own_transfers = external_sends = external_receives = 0
    recipients: set[str] = set()
    send_count = receive_count = 0
    bridge_evidence: list[str] = []
    liquidity_evidence: list[str] = []

    for event in events:
        action = str(event.get("action_type") or "UNKNOWN").upper()
        actions[action] += 1
        if event.get("project_id"):
            projects[str(event["project_id"])] += 1
        if event.get("cate_id"):
            categories[str(event["cate_id"])] += 1
        if event.get("cex_id"):
            cexes[str(event["cex_id"])] += 1
        event_evidence = _json(event.get("evidence"), {})
        tokens = event_evidence.get("tokens") if isinstance(event_evidence.get("tokens"), dict) else {}
        chain = str(event.get("chain") or "")
        wallet = _norm_address(event.get("wallet"))
        for side in ("sends", "receives"):
            legs = event_evidence.get(side) or []
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                value = _leg_value(leg)
                stable = _is_stable_leg(chain, leg, tokens)
                key = _leg_asset(chain, leg) or str(leg.get("token_id") or "unknown").lower()
                if side == "sends":
                    send_count += 1
                    send_assets[key] += value
                    stable_sent += value if stable else 0.0
                    nonstable_sent += 0.0 if stable else value
                    counterparty = _norm_address(leg.get("to_addr") or event_evidence.get("other_addr"))
                    if counterparty:
                        recipients.add(counterparty)
                    if counterparty and counterparty in index.owned_wallets and counterparty != wallet:
                        own_transfers += 1
                    else:
                        external_sends += 1
                else:
                    receive_count += 1
                    receive_assets[key] += value
                    stable_received += value if stable else 0.0
                    nonstable_received += 0.0 if stable else value
                    counterparty = _norm_address(leg.get("from_addr") or event_evidence.get("other_addr"))
                    if counterparty and counterparty in index.owned_wallets and counterparty != wallet:
                        own_transfers += 1
                    else:
                        external_receives += 1
        metadata = " ".join(str(x or "").lower() for x in (
            event.get("action_type"), event.get("project_id"), event.get("cate_id"),
            _compact_text(event_evidence.get("lookup") or {}, limit=2_000),
        ))
        if any(marker in metadata for marker in _BRIDGE_MARKERS):
            bridge_evidence.append("bridge marker in project/category metadata")
        if _successful_bridge_enrichment(_json(event.get("tx_enrichments"), {})):
            bridge_evidence.append("successful cross-chain tx enrichment")
        if any(marker in f" {metadata} " for marker in _LIQUIDITY_MARKERS) or action.startswith("LP_"):
            liquidity_evidence.append("liquidity marker in action/project/category")

    target = episode.get("target_asset_key") or episode.get("primary_asset_key")
    target_sent = _num(send_assets.get(str(target))) if target else 0.0
    target_received = _num(receive_assets.get(str(target))) if target else 0.0
    entity_context = evidence.get("entity_context_pre_event") or {}
    wallet_context = evidence.get("wallet_context_pre_event") or {}
    historical_profiles = index.pre_event_profiles(wallets, int(episode.get("start_ts") or 0))
    return {
        "event_count": len(events), "actions": dict(actions), "motif": episode.get("motif"),
        "cex_ids": dict(cexes), "project_ids": dict(projects), "category_ids": dict(categories),
        "send_count": send_count, "receive_count": receive_count,
        "stable_sent_value": stable_sent, "stable_received_value": stable_received,
        "nonstable_sent_value": nonstable_sent, "nonstable_received_value": nonstable_received,
        "send_assets": dict(send_assets), "receive_assets": dict(receive_assets),
        "target_asset_key": target, "target_sent_value": target_sent, "target_received_value": target_received,
        "own_wallet_transfer_count": own_transfers, "external_send_count": external_sends,
        "external_receive_count": external_receives,
        "unique_external_recipients": len({x for x in recipients if x and x not in index.owned_wallets}),
        "bridge_evidence": sorted(set(bridge_evidence)), "liquidity_evidence": sorted(set(liquidity_evidence)),
        "entity_context_pre_event_available": bool(entity_context),
        "wallet_context_pre_event_wallets": sum(bool(v) for v in wallet_context.values()) if isinstance(wallet_context, dict) else 0,
        "wallet_history_pre_event": historical_profiles,
    }


def classify_intent_features(features: dict[str, Any]) -> dict[str, Any]:
    """Conservative deterministic rules. Close/conflicting scores resolve to unknown."""
    actions = Counter(features["actions"])
    stable_out, stable_in = _num(features["stable_sent_value"]), _num(features["stable_received_value"])
    risk_out, risk_in = _num(features["nonstable_sent_value"]), _num(features["nonstable_received_value"])
    own = int(features["own_wallet_transfer_count"])
    external_out, external_in = int(features["external_send_count"]), int(features["external_receive_count"])
    send_assets = {key for key, value in features["send_assets"].items() if _num(value) > 0}
    receive_assets = {key for key, value in features["receive_assets"].items() if _num(value) > 0}
    scores: Counter[str] = Counter()
    reasons: dict[str, list[str]] = defaultdict(list)

    def add(label: str, score: float, reason: str) -> None:
        scores[label] += score
        reasons[label].append(reason)

    if features["bridge_evidence"]:
        add("bridge_reposition", 8.0, "; ".join(features["bridge_evidence"]))
    if features["liquidity_evidence"]:
        add("liquidity_management", 7.0, "; ".join(features["liquidity_evidence"]))
    if own:
        if own >= external_out + external_in:
            add("internal_rebalance", 8.0, f"{own} transfer leg(s) connect known entity wallets")
        else:
            add("internal_rebalance", 4.0, f"{own} own-wallet transfer leg(s) mixed with external flow")
    if stable_out > 0 and risk_in > 0:
        add("buy", 6.0, "stablecoin sent while non-stable asset received")
    if risk_out > 0 and stable_in > 0:
        add("sell", 6.0, "non-stable asset sent while stablecoin received")
    if risk_out > 0 and risk_in > 0 and not stable_out and not stable_in and send_assets != receive_assets:
        add("asset_rotation", 6.0, "different non-stable assets sent and received")

    cex_in = actions["CEX_INTERACTION_IN"]
    cex_out = actions["CEX_INTERACTION_OUT"]
    bidirectional = sum(count for action, count in actions.items() if "BIDIRECTIONAL" in action or "EXCHANGE_LIKE" in action)
    if cex_in and risk_in > 0 and not risk_out:
        add("accumulation", 4.5, "non-stable asset received from a labeled CEX")
    if cex_out and risk_out > 0 and not risk_in:
        add("sell", 4.5, "non-stable asset sent to a labeled CEX")
    if risk_in > 0 and not risk_out and not stable_out and external_in:
        add("accumulation", 3.0, "one-way external receipt of non-stable inventory")
    if risk_out > 0 and not risk_in and not stable_in and external_out:
        recipients = int(features["unique_external_recipients"])
        if recipients >= 2:
            add("distribution", 4.5, f"non-stable inventory distributed to {recipients} external recipients")
        else:
            add("distribution", 2.0, "one-way external non-stable transfer is weak distribution evidence")
    if bidirectional and not features["bridge_evidence"] and not features["liquidity_evidence"]:
        add("inventory_rebalance", 3.0, "bidirectional/exchange-like action without directional settlement evidence")
    if risk_in > 0 and risk_out > 0 and send_assets == receive_assets:
        add("inventory_rebalance", 4.0, "same inventory assets moved in both directions")

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    top_label, top_score = ranked[0] if ranked else ("unknown", 0.0)
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    margin = top_score - second_score
    ambiguous = top_score < 3.0 or (second_score >= 3.0 and margin < 1.5)
    if ambiguous:
        label = "unknown"
        if not ranked:
            decision_reasons = ["no strong deterministic directional, bridge, liquidity, or own-wallet evidence"]
            confidence = 0.80
        else:
            competing = ", ".join(f"{name}={score:g}" for name, score in ranked[:3])
            decision_reasons = [f"ambiguous deterministic evidence ({competing}); conservative unknown"]
            confidence = 0.45
    else:
        label = top_label
        decision_reasons = reasons[top_label]
        confidence = min(0.99, 0.62 + 0.045 * top_score + 0.035 * min(margin, 4.0))
    return {
        "label": label, "confidence": round(confidence, 4), "reason": "; ".join(decision_reasons),
        "evidence": decision_reasons, "features": features,
        "candidate_scores": {name: score for name, score in ranked},
        "classifier": CLASSIFIER_NAME, "classifier_version": CLASSIFIER_VERSION, "network_calls": 0,
    }


def classify_episode_deterministic(episode: dict[str, Any], index: ClassificationIndex) -> dict[str, Any]:
    return classify_intent_features(extract_intent_features(episode, index))


async def _bounded_map(items: list[Any], worker, concurrency: int) -> list[Any]:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(item):
        async with sem:
            try:
                return await worker(item)
            except Exception as exc:
                return exc
    return await asyncio.gather(*(one(item) for item in items))


async def classify_wallet_roles(
    storage: Storage, llm: FreeLLMClient, entity_id: str, *, concurrency: int = 4, force: bool = False,
) -> int:
    """Legacy opt-in LLM enrichment. It is never called by the normal pipeline."""
    where = "" if force else " AND r.wallet IS NULL"
    wallets = storage.fetchall(
        f"""SELECT DISTINCT w.address FROM wallets w
            LEFT JOIN wallet_roles r ON r.entity_id=w.entity_id AND r.wallet=w.address
            WHERE w.entity_id=? AND w.address LIKE '0x%' {where} ORDER BY w.address""", (entity_id,),
    )
    feature_index = ClassificationIndex.load(storage, entity_id)
    items = []
    for row in wallets:
        payload = feature_index.pre_event_profiles([row["address"]], 2**62)[_norm_address(row["address"])]
        payload.update(entity_id=entity_id, wallet=row["address"])
        if payload["event_count"]:
            items.append((row["address"], payload))

    async def worker(item):
        wallet, payload = item
        result = await llm.classify_wallet_role(payload)
        storage.save_wallet_role(entity_id, wallet, result["role"], result["confidence"], result["role_probabilities"], result["evidence"])
        return wallet
    results = await _bounded_map(items, worker, concurrency)
    failures = [result for result in results if isinstance(result, Exception)]
    if failures:
        print(f"[optional LLM] {entity_id}: wallet-role failures={len(failures)}", file=sys.stderr, flush=True)
    return len(results) - len(failures)


async def classify_episodes(
    storage: Storage, llm: FreeLLMClient | None, entity_id: str, *, force: bool = False,
    concurrency: int = 4, start_ts: int | None = None, end_ts: int | None = None,
    limit: int | None = None, run_id: str | None = None, llm_fallback: bool = False,
) -> int:
    """Classify locally with constant-query preload and one transactional batch write."""
    sql = "SELECT * FROM episodes WHERE entity_id=?"
    params: list[Any] = [entity_id]
    if run_id:
        sql += " AND episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)"
        params.append(run_id)
    if not force:
        sql += " AND intent_label IS NULL"
    if start_ts is not None:
        sql += " AND start_ts>=?"
        params.append(int(start_ts))
    if end_ts is not None:
        sql += " AND start_ts<=?"
        params.append(int(end_ts))
    sql += " ORDER BY start_ts,episode_id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    rows = storage.fetchall(sql, params)
    if not rows:
        return 0
    index = await asyncio.to_thread(ClassificationIndex.load, storage, entity_id, run_id)
    results = [classify_episode_deterministic(row, index) for row in rows]

    if llm_fallback and llm is not None:
        unknown_indexes = [i for i, result in enumerate(results) if result["label"] == "unknown"]

        async def enrich(i: int):
            try:
                results[i]["optional_llm_enrichment"] = await llm.classify_episode({
                    "deterministic_result": results[i],
                    "instruction": "Optional enrichment only; deterministic output is already accepted.",
                })
            except Exception as exc:
                results[i]["optional_llm_enrichment_error"] = type(exc).__name__
            return i
        await _bounded_map(unknown_indexes, enrich, concurrency)

    now = utc_now_iso()
    episode_ids = [row["episode_id"] for row in rows]
    with storage.conn() as db:
        db.executemany(
            "UPDATE episodes SET intent_label=?,intent_json=?,intent_confidence=?,classified_at=? WHERE episode_id=?",
            [(result["label"], canonical_json(result), result["confidence"], now, row["episode_id"])
             for row, result in zip(rows, results)],
        )
        db.executemany(
            """INSERT INTO artifact_provenance(artifact_type,artifact_id,version,input_hash,updated_at)
               VALUES('classification',?,?,?,?) ON CONFLICT(artifact_type,artifact_id) DO UPDATE SET
               version=excluded.version,input_hash=excluded.input_hash,updated_at=excluded.updated_at""",
            [(row["episode_id"], CLASSIFIER_VERSION,
              hashlib.sha256(canonical_json([row.get("motif"), row.get("evidence_json"), CLASSIFIER_VERSION]).encode()).hexdigest(), now)
             for row in rows],
        )
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='analysis_observations'").fetchone():
            for start in range(0, len(episode_ids), 400):
                chunk = episode_ids[start:start + 400]
                placeholders = ",".join("?" for _ in chunk)
                if run_id:
                    db.execute(f"DELETE FROM analysis_observations WHERE run_id=? AND episode_id IN ({placeholders})",
                               [run_id, *chunk])
                else:
                    db.execute(f"DELETE FROM analysis_observations WHERE episode_id IN ({placeholders})", chunk)
    return len(rows)
