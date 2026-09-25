from __future__ import annotations

from typing import Any

from ..http import HubClient


# Hub /docs/rpc explicitly lists these EVM read methods. We keep a project-level
# allowlist narrower than Hub's full 31-method EVM allowlist.
EVM_READ_METHODS = {
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
    "eth_getLogs",
    "eth_getBlockByNumber",
    "eth_getCode",
    "eth_call",
}

SOLANA_READ_METHODS = {
    "getSignaturesForAddress",
    "getTransaction",
    "getTokenAccountsByOwner",
    "getBalance",
}


class RPCProvider:
    def __init__(self, hub: HubClient):
        self.hub = hub

    async def evm(self, chain: str, method: str, params: list[Any]) -> Any:
        if method not in EVM_READ_METHODS:
            raise ValueError(f"EVM method not enabled by this project: {method}")
        return await self.hub.rpc(chain, method, params)

    async def solana(self, method: str, params: list[Any]) -> Any:
        if method not in SOLANA_READ_METHODS:
            raise ValueError(f"Solana method not enabled by this project: {method}")
        return await self.hub.rpc("solana", method, params)
