from __future__ import annotations

from typing import Any

from .base import BaseProvider


OKLINK_CHAINS = {
    "btc", "eth", "x1", "xlayer", "sol", "solana", "tron", "bsc", "base", "sui", "apt", "aptos",
    "arbitrum", "optimism", "polygon", "avax", "avalanche", "zksync", "ton", "gravity",
}


class OKLinkProvider(BaseProvider):
    @staticmethod
    def _chain(chain: str) -> str:
        if chain not in OKLINK_CHAINS:
            raise ValueError(f"Unsupported OKLink chain slug from documented enum: {chain}")
        return chain

    async def address_transactions(self, chain: str, address: str, *, limit: int = 20, offset: int = 0, nonzero_value: bool = False) -> dict[str, Any]:
        r = await self.call(
            "oklink.address_transactions",
            path_params={"chain": self._chain(chain), "address": address},
            query={"limit": limit, "offset": offset, "nonzeroValue": str(nonzero_value).lower()},
            allow_upstream_error=True,
        )
        return r.data if isinstance(r.data, dict) else {}

    async def internal_transactions(self, chain: str, address: str, *, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        r = await self.call("oklink.internal_transactions", path_params={"chain": self._chain(chain), "address": address}, query={"limit": limit, "offset": offset}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def token_transfers(self, chain: str, address: str, *, limit: int = 20, offset: int = 0) -> dict[str, Any]:
        r = await self.call("oklink.token_transfers", path_params={"chain": self._chain(chain), "address": address}, query={"limit": limit, "offset": offset}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def defi_protocols(self, chain: str, address: str) -> dict[str, Any]:
        r = await self.call("oklink.defi_protocols", path_params={"chain": self._chain(chain), "address": address}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def tx_logs(self, chain: str, tx_hash: str, *, limit: int = 20, offset: int = 0) -> Any:
        r = await self.call("oklink.tx_logs", path_params={"chain": self._chain(chain), "txHash": tx_hash}, query={"limit": limit, "offset": offset}, allow_upstream_error=True)
        return r.data

    async def tx_detail(self, chain: str, tx_hash: str) -> Any:
        r = await self.call("oklink.tx_detail", path_params={"chain": self._chain(chain), "hash": tx_hash}, allow_upstream_error=True)
        return r.data
