from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

from ..config import load_entities
from ..runtime import Runtime
from .backfill import backfill_evm_wallet, collect_solana_wallet, parse_date
from .backtest import run_backtest, write_report
from .classify import classify_episodes, classify_wallet_roles
from .discovery import discover_entity, snapshot_entity_context
from .enrich import enrich_evm_event, snapshot_stargate_wallet
from .episodes import build_episodes
from .market import MarketLabeler, snapshot_live_derivatives
from .wallet_context import snapshot_evm_wallet_context
from ..progress import ProgressDashboard


async def bounded_gather(coros: list[Any], limit: int) -> list[Any]:
    sem = asyncio.Semaphore(limit)
    async def run(c):
        async with sem:
            return await c
    return await asyncio.gather(*(run(c) for c in coros), return_exceptions=True)


async def discover_all(rt: Runtime, entity_config: str = "config/entities.yaml") -> list[dict[str, Any]]:
    entities = load_entities(entity_config)
    results = await bounded_gather(
        [discover_entity(rt.arkham, rt.storage, e["id"], e["name"]) for e in entities],
        min(rt.settings.concurrency, len(entities)),
    )
    out = []
    for e, r in zip(entities, results):
        if isinstance(r, Exception):
            out.append({"entity_id": e["id"], "error": str(r)})
        else:
            out.append(r)
    return out


async def snapshot_all(rt: Runtime, entity_config: str = "config/entities.yaml") -> list[dict[str, Any]]:
    entities = load_entities(entity_config)
    results = await bounded_gather([snapshot_entity_context(rt.arkham, rt.storage, e["id"]) for e in entities], min(3, rt.settings.concurrency))
    return [{"entity_id": e["id"], "ok": not isinstance(r, Exception), "error": str(r) if isinstance(r, Exception) else None} for e, r in zip(entities, results)]


async def backfill_all(rt: Runtime, *, from_date: str, max_wallets: int | None = None, max_pages: int | None = None) -> list[dict[str, Any]]:
    min_ts = parse_date(from_date)
    rows = rt.storage.fetchall("SELECT entity_id,address,chain,source FROM wallets ORDER BY entity_id,address")
    # One address can have multiple provenance rows. Backfill it once per entity.
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        unique[(row["entity_id"], row["address"])] = row
    items = list(unique.values())
    if max_wallets:
        per_entity: dict[str, int] = {}
        capped = []
        for item in items:
            n = per_entity.get(item["entity_id"], 0)
            if n < max_wallets:
                capped.append(item)
                per_entity[item["entity_id"]] = n + 1
        items = capped
    coros = []
    labels = []
    for row in items:
        address = row["address"]
        entity_id = row["entity_id"]
        if address.startswith("0x"):
            labels.append((entity_id, address, "evm"))
            coros.append(backfill_evm_wallet(rt.debank, rt.storage, entity_id=entity_id, address=address, min_timestamp=min_ts, max_pages=max_pages))
        elif row["chain"] == "solana":
            labels.append((entity_id, address, "solana"))
            coros.append(collect_solana_wallet(rt.jupiter, rt.storage, entity_id=entity_id, address=address, max_pages=max_pages))
    results = await bounded_gather(coros, rt.settings.concurrency)
    out = []
    for label, result in zip(labels, results):
        if isinstance(result, Exception):
            out.append({"entity_id": label[0], "address": label[1], "family": label[2], "error": str(result)})
        else:
            out.append(result)
    return out


async def snapshot_wallet_contexts(rt: Runtime, *, max_wallets: int | None = None, max_tokens: int = 5) -> list[dict[str, Any]]:
    rows = rt.storage.fetchall("SELECT DISTINCT entity_id,address FROM wallets WHERE address LIKE '0x%' ORDER BY entity_id,address")
    if max_wallets:
        rows = rows[:max_wallets]
    coros = [
        snapshot_evm_wallet_context(rt.arkham, rt.oklink, rt.gecko, rt.storage, entity_id=r["entity_id"], address=r["address"], max_tokens=max_tokens)
        for r in rows
    ]
    results = await bounded_gather(coros, rt.settings.concurrency)
    return [r if not isinstance(r, Exception) else {"error": str(r)} for r in results]


async def enrich_events(rt: Runtime, *, min_usd: float = 0.0, max_events: int | None = None) -> list[dict[str, Any]]:
    rows = rt.storage.fetchall(
        "SELECT * FROM wallet_events WHERE tx_hash IS NOT NULL AND COALESCE(usd_value,0)>=? ORDER BY usd_value DESC",
        (min_usd,),
    )
    if max_events:
        rows = rows[:max_events]
    coros = [enrich_evm_event(rt.storage, rt.oklink, rt.arkham, rt.rpc, rt.bridges, row, bridge_min_usd=rt.settings.bridge_check_min_usd) for row in rows]
    results = await bounded_gather(coros, rt.settings.concurrency)
    return [r if not isinstance(r, Exception) else {"error": str(r)} for r in results]


async def stargate_context(rt: Runtime, *, from_date: str, to_date: str | None = None, max_wallets: int | None = None) -> int:
    start = parse_date(from_date)
    end = parse_date(to_date) if to_date else int(datetime.now(timezone.utc).timestamp())
    rows = rt.storage.fetchall("SELECT DISTINCT entity_id,address FROM wallets WHERE address LIKE '0x%' ORDER BY entity_id,address")
    if max_wallets:
        rows = rows[:max_wallets]
    coros = [snapshot_stargate_wallet(rt.bridges, rt.storage, entity_id=r["entity_id"], address=r["address"], start_ts=start, end_ts=end) for r in rows]
    await bounded_gather(coros, rt.settings.concurrency)
    return len(coros)


async def build_all_episodes(rt: Runtime, entity_config: str = "config/entities.yaml") -> dict[str, int]:
    out = {}
    for e in load_entities(entity_config):
        out[e["id"]] = len(build_episodes(rt.storage, e["id"], rt.settings.episode_gap_seconds, rt.settings.max_episode_events))
    return out


async def classify_all(rt: Runtime, entity_config: str = "config/entities.yaml", *, classify_roles: bool = True, force: bool = False) -> dict[str, dict[str, int]]:
    out = {}
    for e in load_entities(entity_config):
        roles = await classify_wallet_roles(rt.storage, rt.llm, e["id"], concurrency=min(4, rt.settings.concurrency), force=force) if classify_roles else 0
        episodes = await classify_episodes(rt.storage, rt.llm, e["id"], force=force, concurrency=min(4, rt.settings.concurrency))
        out[e["id"]] = {"roles": roles, "episodes": episodes}
    return out


async def label_all_markets(rt: Runtime, *, max_episodes: int | None = None) -> int:
    rows = rt.storage.fetchall("SELECT * FROM episodes WHERE primary_asset_key IS NOT NULL ORDER BY start_ts")
    if max_episodes:
        rows = rows[:max_episodes]
    labeler = MarketLabeler(rt.llama, rt.storage)
    result = await labeler.label_episodes(rows, concurrency=rt.settings.concurrency)
    return result["labels"]


async def run_all(
    rt: Runtime,
    *,
    from_date: str,
    entity_config: str = "config/entities.yaml",
    max_wallets: int | None = None,
    max_pages: int | None = None,
    enrich_min_usd: float = 100000.0,
    max_enrich_events: int | None = None,
    max_market_episodes: int | None = None,
    min_backtest_n: int = 5,
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    started = time.monotonic()
    dashboard = ProgressDashboard()
    stage_number = 0
    async def stage(name: str, awaitable):
        nonlocal stage_number
        stage_number += 1
        stage_started = time.monotonic()
        dashboard.update(name, stage_number)
        print(f"[run-all] START {name}", flush=True)
        value = await awaitable
        elapsed = time.monotonic() - stage_started
        print(f"[run-all] DONE  {name} ({elapsed:.1f}s, total {time.monotonic() - started:.1f}s)", flush=True)
        return value
    with dashboard:
      result["discovery"] = await stage("discovery", discover_all(rt, entity_config))
      result["entity_context"] = await stage("entity_context", snapshot_all(rt, entity_config))
      result["backfill"] = await stage("backfill", backfill_all(rt, from_date=from_date, max_wallets=max_wallets, max_pages=max_pages))
      result["wallet_context"] = await stage("wallet_context", snapshot_wallet_contexts(rt, max_wallets=max_wallets))
      result["stargate_wallets"] = await stage("stargate_context", stargate_context(rt, from_date=from_date, max_wallets=max_wallets))
      result["enrichment"] = await stage("enrichment", enrich_events(rt, min_usd=enrich_min_usd, max_events=max_enrich_events))
      result["episodes"] = await stage("episodes", build_all_episodes(rt, entity_config))
      result["classification"] = await stage("classification", classify_all(rt, entity_config))
      result["market_labels"] = await stage("market_labels", label_all_markets(rt, max_episodes=max_market_episodes))
      await stage("live_derivatives", snapshot_live_derivatives(rt.okx, rt.storage))
    print("[run-all] START backtest", flush=True)
    run_id, frame = run_backtest(rt.storage, min_n=min_backtest_n)
    print("[run-all] DONE  backtest", flush=True)
    result["backtest_run_id"] = run_id
    result["reports"] = write_report(rt.storage, run_id, frame)
    print("[run-all] DONE  reports", flush=True)
    return result
