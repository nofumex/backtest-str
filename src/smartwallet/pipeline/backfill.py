from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any

from ..normalize import normalize_debank_event
from ..providers import DeBankProvider, JupiterProvider
from ..storage import Storage
from ..storage import utc_now_iso


def parse_date(value: str) -> int:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _chain_ids(chains: list[Any]) -> list[str]:
    out: list[str] = []
    for item in chains:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict) and item.get("id"):
            out.append(str(item["id"]))
    return list(dict.fromkeys(out))


async def backfill_evm_wallet(
    debank: DeBankProvider,
    storage: Storage,
    *,
    entity_id: str,
    address: str,
    min_timestamp: int,
    max_timestamp: int | None = None,
    max_pages: int | None = None,
) -> dict[str, Any]:
    chains_raw = await debank.used_chains(address)
    chains = _chain_ids(chains_raw)
    max_timestamp = int(max_timestamp or storage.run_bounds[1])
    count = 0
    per_chain: dict[str, int] = {}
    chain_errors: dict[str, str] = {}
    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=max(1, min(64, len(chains) + 1)))

    async def collect_chain(chain: str) -> tuple[str, int]:
        coverage = storage.fetchone(
            "SELECT * FROM wallet_history_coverage WHERE entity_id=? AND address=? AND chain=?",
            (entity_id, address.lower(), chain),
        )
        if (coverage and coverage["state"] == "complete"
                and int(coverage["covered_from"]) <= min_timestamp
                and int(coverage["covered_to"]) >= max_timestamp):
            cached = storage.fetchone(
                """SELECT COUNT(*) n FROM wallet_events WHERE entity_id=? AND wallet=? AND chain=?
                   AND ts BETWEEN ? AND ?""",
                (entity_id, address.lower(), chain, min_timestamp, max_timestamp),
            )
            return chain, int(cached["n"] if cached else 0)
        cursor = int(coverage["cursor"] or 0) if coverage and int(coverage["covered_to"] or 0) >= max_timestamp else 0
        pages = int(coverage["pages"] or 0) if cursor else 0
        pages_this_run = 0
        emitted = 0
        completed = False
        with storage.conn() as db:
            db.execute(
                """INSERT INTO wallet_history_coverage(entity_id,address,chain,covered_from,covered_to,cursor,pages,state,updated_at)
                   VALUES(?,?,?,?,?,?,?,'scanning',?) ON CONFLICT(entity_id,address,chain) DO UPDATE SET
                   covered_to=MAX(covered_to,excluded.covered_to),state='scanning',updated_at=excluded.updated_at""",
                (entity_id, address.lower(), chain, int(coverage["covered_from"] or 0) if coverage else 0,
                 max_timestamp, cursor, pages, utc_now_iso()),
            )
        while True:
            if max_pages is not None and pages_this_run >= max_pages:
                break
            page = await debank.history_page(address, chain, start_time=cursor, page_count=20)
            rows = page.get("history_list") or []
            if not rows:
                completed = True
                break
            dictionaries = {key: page.get(key) or {} for key in
                            ("cate_dict", "cex_dict", "memo_dict", "project_dict", "token_dict")}
            oldest = min((int(float(row.get("time_at") or 0)) for row in rows if row.get("time_at")), default=0)
            events = []
            for row in rows:
                event = normalize_debank_event(entity_id, address, chain, row, dictionaries)
                if event["ts"] > 0:
                    events.append(event)
                    if min_timestamp <= event["ts"] <= max_timestamp:
                        emitted += 1
            storage.save_events(events)
            pages += 1
            pages_this_run += 1
            if not oldest or (cursor and oldest >= cursor):
                completed = True
            cursor = oldest or cursor
            with storage.conn() as db:
                db.execute(
                    """INSERT INTO wallet_history_coverage(entity_id,address,chain,covered_from,covered_to,cursor,pages,state,updated_at)
                       VALUES(?,?,?,?,?,?,?,'scanning',?) ON CONFLICT(entity_id,address,chain) DO UPDATE SET
                       covered_to=MAX(covered_to,excluded.covered_to),cursor=excluded.cursor,pages=excluded.pages,
                       state='scanning',updated_at=excluded.updated_at""",
                    (entity_id, address.lower(), chain, int(coverage["covered_from"] or 0) if coverage else 0,
                     max_timestamp, cursor, pages, utc_now_iso()),
                )
            if completed or cursor <= min_timestamp:
                completed = True
                break
        if completed:
            with storage.conn() as db:
                db.execute(
                    """UPDATE wallet_history_coverage SET covered_from=CASE WHEN covered_from=0 THEN ? ELSE MIN(covered_from,?) END,
                       covered_to=MAX(covered_to,?),cursor=?,state='complete',updated_at=?
                       WHERE entity_id=? AND address=? AND chain=?""",
                    (min_timestamp, min_timestamp, max_timestamp, cursor, utc_now_iso(),
                     entity_id, address.lower(), chain),
                )
        return chain, emitted

    async def worker():
        while True:
            chain = await queue.get()
            try:
                if chain is None:
                    return
                try:
                    key, value = await collect_chain(chain)
                    per_chain[key] = value
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    chain_errors[chain] = f"{type(exc).__name__}: {exc}"[:500]
                    with suppress(Exception):
                        with storage.conn() as db:
                            db.execute(
                                """UPDATE wallet_history_coverage SET state='failed',updated_at=?
                                   WHERE entity_id=? AND address=? AND chain=?""",
                                (utc_now_iso(), entity_id, address.lower(), chain),
                            )
            finally:
                queue.task_done()

    worker_count = max(1, min(len(chains), storage.settings.concurrency, 8))
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    for chain in chains:
        await queue.put(chain)
    for _ in workers:
        await queue.put(None)
    try:
        await queue.join()
        await asyncio.gather(*workers)
    finally:
        for task in workers:
            if not task.done():
                task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    count = sum(per_chain.values())
    return {
        "entity_id": entity_id, "address": address, "chains": chains, "events": count,
        "per_chain": per_chain, "chain_errors": chain_errors,
        "context_error": (f"{len(chain_errors)}/{len(chains)} chain backfills failed: "
                          + "; ".join(f"{chain}: {error}" for chain, error in sorted(chain_errors.items())))
                         if chain_errors else None,
    }


async def collect_solana_wallet(jupiter: JupiterProvider, storage: Storage, *, entity_id: str, address: str, max_pages: int | None = None) -> dict[str, Any]:
    """Collect documented Jupiter wallet surfaces without inventing undocumented field schemas."""
    positions, transfers, swaps, trades = await asyncio.gather(
        jupiter.positions(address),
        jupiter.transfers(address),
        jupiter.swap_transactions(address),
        jupiter.user_trades(address),
    )
    storage.save_position(entity_id, address, "solana", "jupiter.positions", positions)
    storage.save_position(entity_id, address, "solana", "jupiter.transfers", transfers)
    storage.save_position(entity_id, address, "solana", "jupiter.swap_transactions", swaps)
    storage.save_position(entity_id, address, "solana", "jupiter.user_trades", trades)

    activity_count = 0
    activities: list[dict[str, Any]] = []
    async for row in jupiter.iter_activities(address, product="SWAP", max_pages=max_pages):
        activities.append(row)
        activity_count += 1
    storage.save_position(entity_id, address, "solana", "jupiter.activities.SWAP", {"histories": activities})
    return {"entity_id": entity_id, "address": address, "swap_activities": activity_count}
