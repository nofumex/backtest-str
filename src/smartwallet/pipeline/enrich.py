from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from ..normalize import DEBANK_TO_OKLINK_CHAIN
from ..providers import ArkhamProvider, BridgeProvider, OKLinkProvider, RPCProvider
from ..storage import Storage


EVM_RPC_CHAIN = {
    "eth": "ethereum",
    "bsc": "bsc",
    "arb": "arbitrum",
    "op": "optimism",
    "base": "base",
    "matic": "polygon",
    "avax": "avalanche",
}


async def enrich_evm_event(
    storage: Storage,
    oklink: OKLinkProvider,
    arkham: ArkhamProvider,
    rpc: RPCProvider,
    bridges: BridgeProvider,
    event: dict[str, Any],
    *,
    bridge_min_usd: float,
) -> dict[str, Any]:
    tx_hash = event.get("tx_hash")
    chain = event.get("chain")
    if not tx_hash or chain not in EVM_RPC_CHAIN:
        return {"tx_hash": tx_hash, "skipped": True}

    tasks: dict[str, Any] = {
        "rpc_receipt": rpc.evm(EVM_RPC_CHAIN[chain], "eth_getTransactionReceipt", [tx_hash]),
        "arkham_tx": arkham.tx(tx_hash),
    }
    ok_chain = DEBANK_TO_OKLINK_CHAIN.get(chain)
    if ok_chain:
        tasks["oklink_detail"] = oklink.tx_detail(ok_chain, tx_hash)
        tasks["oklink_logs"] = oklink.tx_logs(ok_chain, tx_hash, limit=20, offset=0)

    if float(event.get("usd_value") or 0.0) >= bridge_min_usd:
        tasks["lifi_status"] = bridges.lifi_status(tx_hash)
        tasks["rubic_status"] = bridges.rubic_status(tx_hash)

    names = list(tasks)
    values = await asyncio.gather(*tasks.values(), return_exceptions=True)
    output: dict[str, Any] = {}
    for name, value in zip(names, values):
        if isinstance(value, Exception):
            output[name] = {"error": str(value)}
        else:
            output[name] = value
            storage.save_enrichment(tx_hash, chain, name, value)
    return output


async def snapshot_stargate_wallet(bridges: BridgeProvider, storage: Storage, *, entity_id: str, address: str, start_ts: int, end_ts: int) -> None:
    start = datetime.fromtimestamp(start_ts, timezone.utc).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(end_ts, timezone.utc).isoformat().replace("+00:00", "Z")
    payload = await bridges.stargate_transfer_volume(address, start, end)
    storage.save_position(entity_id, address, "evm", "stargate.transfer_volume", payload)
