from __future__ import annotations

import hashlib
import json
from typing import Any


DEBANK_TO_DEFILLAMA_CHAIN = {
    "eth": "ethereum",
    "bsc": "bsc",
    "arb": "arbitrum",
    "op": "optimism",
    "base": "base",
    "matic": "polygon",
    "avax": "avax",
}

NATIVE_ASSET_KEYS = {
    ("eth", "eth"): "coingecko:ethereum",
    ("arb", "eth"): "coingecko:ethereum",
    ("op", "eth"): "coingecko:ethereum",
    ("base", "eth"): "coingecko:ethereum",
    ("bsc", "bnb"): "coingecko:binancecoin",
    ("matic", "matic"): "coingecko:matic-network",
    ("avax", "avax"): "coingecko:avalanche-2",
}

DEBANK_TO_OKLINK_CHAIN = {
    "eth": "eth",
    "bsc": "bsc",
    "arb": "arbitrum",
    "op": "optimism",
    "base": "base",
    "matic": "polygon",
    "avax": "avalanche",
}

STABLE_TOKEN_IDS = {
    "0x0000000000085d4780b73119b644ae5ecd22b376",  # TrueUSD
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0x0000000000000000000000000000000000000000",
}


def _usd_leg(leg: dict[str, Any]) -> float:
    try:
        return abs(float(leg.get("amount") or 0.0) * float(leg.get("price") or 0.0))
    except (TypeError, ValueError):
        return 0.0


def asset_key(chain: str, token_id: str | None) -> str | None:
    if not token_id:
        return None
    tid = str(token_id).lower()
    native = NATIVE_ASSET_KEYS.get((chain, tid))
    if native:
        return native
    mapped = DEBANK_TO_DEFILLAMA_CHAIN.get(chain)
    if mapped and tid.startswith("0x"):
        return f"{mapped}:{tid}"
    return None


def deterministic_action(row: dict[str, Any]) -> str:
    """Conservative factual action class; this is evidence, not human intent."""
    sends = row.get("sends") or []
    receives = row.get("receives") or []
    if row.get("token_approve"):
        return "APPROVAL"
    if row.get("cex_id"):
        if sends and receives:
            return "CEX_INTERACTION_BIDIRECTIONAL"
        if sends:
            return "CEX_INTERACTION_OUT"
        if receives:
            return "CEX_INTERACTION_IN"
        return "CEX_INTERACTION"
    if row.get("project_id"):
        if sends and receives:
            return "DEFI_INTERACTION_BIDIRECTIONAL"
        if sends:
            return "DEFI_INTERACTION_OUT"
        if receives:
            return "DEFI_INTERACTION_IN"
        return "DEFI_INTERACTION"
    if sends and receives:
        return "ASSET_EXCHANGE_LIKE"
    if sends:
        return "TRANSFER_OUT"
    if receives:
        return "TRANSFER_IN"
    return "CONTRACT_INTERACTION"


def normalize_debank_event(entity_id: str, wallet: str, chain: str, row: dict[str, Any], dictionaries: dict[str, Any]) -> dict[str, Any]:
    sends = [x for x in (row.get("sends") or []) if isinstance(x, dict)]
    receives = [x for x in (row.get("receives") or []) if isinstance(x, dict)]
    legs = [("send", x) for x in sends] + [("receive", x) for x in receives]
    best_kind = None
    best_leg: dict[str, Any] | None = None
    best_usd = 0.0
    send_usd = sum(_usd_leg(x) for x in sends)
    receive_usd = sum(_usd_leg(x) for x in receives)
    gross_usd = max(send_usd, receive_usd)
    for kind, leg in legs:
        usd = _usd_leg(leg)
        token = str(leg.get("token_id") or "").lower()
        if token not in STABLE_TOKEN_IDS and usd > best_usd:
            best_usd, best_leg, best_kind = usd, leg, kind

    tx = row.get("tx") if isinstance(row.get("tx"), dict) else {}
    tx_hash = str(row.get("id") or tx.get("id") or "") or None
    idx = row.get("idx")
    try:
        ts = int(float(row.get("time_at") or 0))
    except (TypeError, ValueError):
        ts = 0
    stable = json.dumps([entity_id, wallet.lower(), chain, tx_hash, idx, ts], separators=(",", ":"))
    event_id = hashlib.sha256(stable.encode()).hexdigest()
    primary_token = str(best_leg.get("token_id")) if best_leg and best_leg.get("token_id") else None

    evidence = {
        "cate_id": row.get("cate_id"),
        "cex_id": row.get("cex_id"),
        "project_id": row.get("project_id"),
        "other_addr": row.get("other_addr"),
        "sends": sends,
        "receives": receives,
        "tx": tx,
        "token_approve": row.get("token_approve"),
        "lookup": {
            "cate": (dictionaries.get("cate_dict") or {}).get(row.get("cate_id")) if row.get("cate_id") else None,
            "cex": (dictionaries.get("cex_dict") or {}).get(row.get("cex_id")) if row.get("cex_id") else None,
            "project": (dictionaries.get("project_dict") or {}).get(row.get("project_id")) if row.get("project_id") else None,
        },
        "primary_leg_direction": best_kind,
    }

    return {
        "event_id": event_id,
        "entity_id": entity_id,
        "wallet": wallet.lower(),
        "chain": chain,
        "tx_hash": tx_hash,
        "event_index": idx if isinstance(idx, int) else None,
        "ts": ts,
        "source": "debank.history",
        "action_type": deterministic_action(row),
        "cate_id": row.get("cate_id"),
        "cex_id": row.get("cex_id"),
        "project_id": row.get("project_id"),
        "usd_value": gross_usd,
        "primary_token_id": primary_token,
        "primary_asset_key": asset_key(chain, primary_token),
        "evidence": evidence,
        "raw": row,
    }
