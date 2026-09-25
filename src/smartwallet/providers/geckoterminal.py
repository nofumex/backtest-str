from __future__ import annotations

from typing import Any

from .base import BaseProvider


class GeckoTerminalProvider(BaseProvider):
    async def search_pools(self, query: str, *, page: int = 1) -> dict[str, Any]:
        r = await self.call("gecko.search_pools", query={"query": query, "page": page}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def token_pools(self, network: str, token_address: str, *, page: int = 1) -> dict[str, Any]:
        r = await self.call("gecko.token_pools", path_params={"network": network, "token_address": token_address}, query={"page": page}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def ohlcv(self, network: str, pool_address: str, timeframe: str, *, aggregate: int | None = None, limit: int | None = None) -> dict[str, Any]:
        if timeframe not in {"day", "hour", "minute", "second"}:
            raise ValueError("GeckoTerminal timeframe must be day|hour|minute|second per Hub docs")
        q: dict[str, Any] = {}
        if aggregate is not None:
            q["aggregate"] = aggregate
        if limit is not None:
            q["limit"] = limit
        r = await self.call("gecko.ohlcv", path_params={"network": network, "pool_address": pool_address, "timeframe": timeframe}, query=q, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}
