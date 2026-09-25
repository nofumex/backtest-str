from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from ..normalize import normalize_debank_event
from ..providers import DeBankProvider, JupiterProvider
from ..storage import Storage


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
    max_pages: int | None = None,
) -> dict[str, Any]:
    chains_raw = await debank.used_chains(address)
    chains = _chain_ids(chains_raw)
    count = 0
    per_chain: dict[str, int] = {}
    pending: list[dict[str, Any]] = []

    positions = await debank.positions(address)
    storage.save_position(entity_id, address.lower(), "evm", "debank.portfolio_project_list", positions)

    for chain in chains:
        n = 0
        async for row, dictionaries in debank.iter_history(address, chain, min_timestamp=min_timestamp, page_count=20, max_pages=max_pages):
            event = normalize_debank_event(entity_id, address, chain, row, dictionaries)
            if event["ts"] <= 0:
                continue
            pending.append(event)
            n += 1
            count += 1
            if len(pending) >= 500:
                storage.save_events(pending)
                pending.clear()
        per_chain[chain] = n
    storage.save_events(pending)
    return {"entity_id": entity_id, "address": address, "chains": chains, "events": count, "per_chain": per_chain}


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
