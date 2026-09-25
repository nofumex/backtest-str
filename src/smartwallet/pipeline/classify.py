from __future__ import annotations

import asyncio
import json
import sys
from collections import Counter
from typing import Any

from ..llm import FreeLLMClient
from ..storage import Storage


def wallet_feature_payload(storage: Storage, entity_id: str, wallet: str, cutoff_ts: int | None = None) -> dict[str, Any]:
    if cutoff_ts is None:
        rows = storage.fetchall(
            "SELECT action_type,chain,cex_id,project_id,usd_value,ts FROM wallet_events WHERE entity_id=? AND wallet=? ORDER BY ts",
            (entity_id, wallet),
        )
    else:
        rows = storage.fetchall(
            "SELECT action_type,chain,cex_id,project_id,usd_value,ts FROM wallet_events WHERE entity_id=? AND wallet=? AND ts<? ORDER BY ts",
            (entity_id, wallet, cutoff_ts),
        )
    actions = Counter(r["action_type"] for r in rows)
    chains = Counter(r["chain"] for r in rows)
    cex = Counter(r["cex_id"] for r in rows if r.get("cex_id"))
    projects = Counter(r["project_id"] for r in rows if r.get("project_id"))
    return {
        "entity_id": entity_id,
        "wallet": wallet,
        "event_count": len(rows),
        "first_ts": rows[0]["ts"] if rows else None,
        "last_ts": rows[-1]["ts"] if rows else None,
        "gross_usd": sum(float(r.get("usd_value") or 0.0) for r in rows),
        "action_counts": dict(actions),
        "chain_counts": dict(chains),
        "cex_counts": dict(cex),
        "project_counts": dict(projects),
    }


async def _bounded_map(items: list[Any], worker, concurrency: int) -> list[Any]:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(item):
        async with sem:
            try:
                return await worker(item)
            except Exception as exc:  # preserve resumability; failed items remain unclassified
                return exc

    return await asyncio.gather(*(one(item) for item in items))


async def classify_wallet_roles(
    storage: Storage,
    llm: FreeLLMClient,
    entity_id: str,
    *,
    concurrency: int = 4,
    force: bool = False,
) -> int:
    if force:
        wallets = storage.fetchall(
            "SELECT DISTINCT address FROM wallets WHERE entity_id=? AND address LIKE '0x%' ORDER BY address",
            (entity_id,),
        )
    else:
        wallets = storage.fetchall(
            """SELECT DISTINCT w.address
               FROM wallets w
               LEFT JOIN wallet_roles r ON r.entity_id=w.entity_id AND r.wallet=w.address
               WHERE w.entity_id=? AND w.address LIKE '0x%' AND r.wallet IS NULL
               ORDER BY w.address""",
            (entity_id,),
        )

    items = []
    for row in wallets:
        payload = wallet_feature_payload(storage, entity_id, row["address"])
        if payload["event_count"]:
            items.append((row["address"], payload))

    async def worker(item):
        wallet, payload = item
        result = await llm.classify_wallet_role(payload)
        storage.save_wallet_role(
            entity_id, wallet, result["role"], result["confidence"],
            result["role_probabilities"], result["evidence"],
        )
        return wallet

    results = await _bounded_map(items, worker, concurrency)
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        print(f"[LLM] {entity_id}: wallet-role failures={len(failures)}; left unclassified for resume", file=sys.stderr, flush=True)
    return len(results) - len(failures)


async def classify_episodes(
    storage: Storage,
    llm: FreeLLMClient,
    entity_id: str,
    *,
    force: bool = False,
    concurrency: int = 4,
) -> int:
    sql = "SELECT * FROM episodes WHERE entity_id=?"
    params: list[Any] = [entity_id]
    if not force:
        sql += " AND intent_label IS NULL"
    sql += " ORDER BY start_ts"
    rows = storage.fetchall(sql, params)

    async def worker(row):
        evidence = json.loads(row["evidence_json"])
        pre_event_profiles = {
            wallet: wallet_feature_payload(storage, entity_id, wallet, cutoff_ts=int(row["start_ts"]))
            for wallet in json.loads(row["wallets_json"])
        }
        payload = {
            "entity_id": entity_id,
            "start_ts": row["start_ts"],
            "end_ts": row["end_ts"],
            "wallet_pre_event_profiles": pre_event_profiles,
            "motif": row["motif"],
            "gross_usd": row["gross_usd"],
            "primary_asset_key": row["primary_asset_key"],
            "pre_event_evidence": evidence,
        }
        result = await llm.classify_episode(payload)
        storage.update_episode_intent(row["episode_id"], result["label"], result, result["confidence"])
        return row["episode_id"]

    results = await _bounded_map(rows, worker, concurrency)
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        print(f"[LLM] {entity_id}: episode failures={len(failures)}; left unclassified for resume", file=sys.stderr, flush=True)
    return len(results) - len(failures)
