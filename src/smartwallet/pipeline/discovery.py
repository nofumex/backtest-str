from __future__ import annotations

import asyncio
from typing import Any

from ..providers import ArkhamProvider
from ..storage import Storage


def infer_address_family(address: str) -> str:
    if address.startswith("0x") and len(address) == 42:
        return "evm"
    if address.startswith(("bc1", "1", "3")):
        return "bitcoin"
    return "solana"


def _entity_from_address_result(row: dict[str, Any]) -> str | None:
    ent = row.get("arkhamEntity")
    return str(ent.get("id")) if isinstance(ent, dict) and ent.get("id") else None


async def discover_entity(arkham: ArkhamProvider, storage: Storage, entity_id: str, name: str) -> dict[str, Any]:
    entity, summary = await asyncio.gather(arkham.entity(entity_id), arkham.entity_summary(entity_id))
    storage.upsert_entity(entity_id, entity.get("name") or name, summary, entity)

    top_task = arkham.top_address(entity_id)
    search_task = arkham.search(name, entities=5, addresses=15)
    hyper_perp_task = arkham.hypercore_perp(entity_id)
    hyper_spot_task = arkham.hypercore_spot(entity_id)
    sol_task = arkham.solana_entity_subaccounts(entity_id)
    top, search, hyper_perp, hyper_spot, sol_rows = await asyncio.gather(
        top_task, search_task, hyper_perp_task, hyper_spot_task, sol_task
    )

    if top:
        storage.upsert_wallet(entity_id, top, infer_address_family(top), "arkham.entity_top_address", {"topAddress": True})

    for row in search.get("arkhamAddresses") or []:
        if not isinstance(row, dict) or _entity_from_address_result(row) != entity_id:
            continue
        address = row.get("address")
        if not address:
            continue
        storage.upsert_wallet(entity_id, str(address), str(row.get("chain") or infer_address_family(str(address))), "arkham.intelligence_search", row)

    for source, payload in (("arkham.hypercore_perp", hyper_perp), ("arkham.hypercore_spot", hyper_spot)):
        for address in payload.get("addresses") or []:
            if isinstance(address, str):
                storage.upsert_wallet(entity_id, address, "hypercore", source, {"hypercore": True})
        storage.save_entity_snapshot(entity_id, "arkham", source.split(".")[-1], payload)

    # The documented Solana entity subaccount response is an array. Live responses expose
    # ownerAddress on each balance row; unique owners are the entity wallet seeds we persist.
    for row in sol_rows:
        if not isinstance(row, dict):
            continue
        owner = row.get("ownerAddress")
        if owner:
            storage.upsert_wallet(entity_id, str(owner), "solana", "arkham.solana_subaccounts", {"balance_row": row})

    return {
        "entity_id": entity_id,
        "name": entity.get("name") or name,
        "num_addresses": summary.get("numAddresses"),
        "top_address": top,
        "search_addresses": len([r for r in search.get("arkhamAddresses") or [] if isinstance(r, dict) and _entity_from_address_result(r) == entity_id]),
        "hypercore_addresses": len(set((hyper_perp.get("addresses") or []) + (hyper_spot.get("addresses") or []))),
        "solana_owner_addresses": len({r.get("ownerAddress") for r in sol_rows if isinstance(r, dict) and r.get("ownerAddress")}),
    }


async def snapshot_entity_context(arkham: ArkhamProvider, storage: Storage, entity_id: str) -> None:
    tasks = {
        "balances": arkham.balances(entity_id, cheap=False),
        "history": arkham.history(entity_id),
        "flow": arkham.flow(entity_id),
        "volume": arkham.volume(entity_id),
        "loans": arkham.loans(entity_id),
        "hypercore_summary": arkham.hypercore_summary(entity_id),
        "hypercore_portfolio": arkham.hypercore_portfolio(entity_id),
        "recent_swaps_24h": arkham.swaps_recent(entity_id, flow="all", time_last="24h", limit=50, offset=0),
    }
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for kind, value in zip(tasks, results):
        if isinstance(value, Exception):
            storage.save_entity_snapshot(entity_id, "error", kind, {"error": str(value)})
        else:
            storage.save_entity_snapshot(entity_id, "arkham", kind, value)
