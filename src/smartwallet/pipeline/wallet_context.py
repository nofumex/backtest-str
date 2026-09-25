from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from ..normalize import DEBANK_TO_OKLINK_CHAIN
from ..providers import ArkhamProvider, GeckoTerminalProvider, OKLinkProvider
from ..storage import Storage


DEBANK_TO_GECKO_NETWORK = {
    "eth": "ethereum",
    "bsc": "bsc",
    "arb": "arbitrum",
    "op": "optimism",
    "base": "base",
    "matic": "polygon",
    "avax": "avalanche",
}


async def snapshot_evm_wallet_context(
    arkham: ArkhamProvider,
    oklink: OKLinkProvider,
    gecko: GeckoTerminalProvider,
    storage: Storage,
    *,
    entity_id: str,
    address: str,
    max_tokens: int = 5,
) -> dict[str, Any]:
    """Snapshot documented indexed/address context without assuming undocumented response rows.

    OKLink address-level routes expose object responses but Hub intentionally does not publish
    their nested row schema. We therefore preserve those responses verbatim. GeckoTerminal's
    token-pools endpoint documents a `data` array but not individual pool fields; those are also
    preserved verbatim rather than guessed.
    """
    rows = storage.fetchall(
        "SELECT chain,primary_token_id,usd_value FROM wallet_events WHERE entity_id=? AND wallet=? ORDER BY ts",
        (entity_id, address.lower()),
    )
    chains = sorted({str(r["chain"]) for r in rows if r.get("chain")})
    saved = 0
    errors: list[dict[str, str]] = []

    arkham_calls = {
        "arkham.address_balances": arkham.address_balances(address),
        "arkham.address_history": arkham.address_history(address),
        "arkham.address_flow": arkham.address_flow(address),
        "arkham.address_loans": arkham.address_loans(address),
    }
    arkham_names = list(arkham_calls)
    arkham_values = await asyncio.gather(*arkham_calls.values(), return_exceptions=True)
    for name, value in zip(arkham_names, arkham_values):
        if isinstance(value, Exception):
            errors.append({"source": name, "error": str(value)})
            continue
        storage.save_position(entity_id, address.lower(), "evm", name, value)
        saved += 1

    provenance = storage.fetchall(
        "SELECT source FROM wallets WHERE entity_id=? AND address=?",
        (entity_id, address.lower()),
    )
    if any(str(r.get("source") or "").startswith("arkham.hypercore") for r in provenance):
        hyper_calls = {
            "arkham.hypercore_account_perp": arkham.hypercore_account_perp(address),
            "arkham.hypercore_account_spot": arkham.hypercore_account_spot(address),
            "arkham.hypercore_account_portfolio": arkham.hypercore_account_portfolio(address),
            "arkham.hypercore_account_summary": arkham.hypercore_account_summary(address),
        }
        hyper_names = list(hyper_calls)
        hyper_values = await asyncio.gather(*hyper_calls.values(), return_exceptions=True)
        for name, value in zip(hyper_names, hyper_values):
            if isinstance(value, Exception):
                errors.append({"source": name, "error": str(value)})
                continue
            storage.save_position(entity_id, address.lower(), "hypercore", name, value)
            saved += 1

    for debank_chain in chains:
        ok_chain = DEBANK_TO_OKLINK_CHAIN.get(debank_chain)
        if ok_chain:
            calls = {
                "oklink.address_transactions": oklink.address_transactions(ok_chain, address, limit=20, offset=0, nonzero_value=False),
                "oklink.token_transfers": oklink.token_transfers(ok_chain, address, limit=20, offset=0),
                "oklink.internal_transactions": oklink.internal_transactions(ok_chain, address, limit=20, offset=0),
                "oklink.defi_protocols": oklink.defi_protocols(ok_chain, address),
            }
            names = list(calls)
            values = await asyncio.gather(*calls.values(), return_exceptions=True)
            for name, value in zip(names, values):
                if isinstance(value, Exception):
                    errors.append({"source": name, "error": str(value)})
                    continue
                storage.save_position(entity_id, address.lower(), debank_chain, name, value)
                saved += 1

    # Choose token contracts only from already-normalized DeBank evidence. No token metadata or
    # pool-address fields are guessed here.
    token_weights: dict[tuple[str, str], float] = defaultdict(float)
    for r in rows:
        token = str(r.get("primary_token_id") or "").lower()
        chain = str(r.get("chain") or "")
        if token.startswith("0x") and chain in DEBANK_TO_GECKO_NETWORK:
            token_weights[(chain, token)] += float(r.get("usd_value") or 0.0)
    top_tokens = sorted(token_weights, key=lambda k: token_weights[k], reverse=True)[:max_tokens]
    for chain, token in top_tokens:
        try:
            payload = await gecko.token_pools(DEBANK_TO_GECKO_NETWORK[chain], token, page=1)
            storage.save_position(entity_id, address.lower(), chain, f"geckoterminal.token_pools:{token}", payload)
            saved += 1
        except Exception as exc:
            errors.append({"source": "geckoterminal.token_pools", "error": str(exc)})

    return {
        "entity_id": entity_id,
        "address": address.lower(),
        "chains": chains,
        "top_token_pool_snapshots": len(top_tokens),
        "snapshots_saved": saved,
        "errors": errors,
    }
