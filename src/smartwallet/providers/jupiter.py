from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from .base import BaseProvider


class JupiterProvider(BaseProvider):
    async def activities_page(self, address: str, *, product: str = "SWAP", page: int = 1) -> dict[str, Any]:
        if product not in {"SWAP", "PERPS"}:
            raise ValueError("Only SWAP and PERPS are live-confirmed by Hub documentation")
        r = await self.call("jupiter.activities", path_params={"address": address}, query={"product": product, "page": page}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def iter_activities(self, address: str, *, product: str = "SWAP", max_pages: int | None = None) -> AsyncIterator[dict[str, Any]]:
        page = 1
        seen: set[str] = set()
        while True:
            if max_pages is not None and page > max_pages:
                return
            data = await self.activities_page(address, product=product, page=page)
            rows = data.get("histories") or []
            for row in rows:
                key = str(row.get("signature") or row.get("txHash") or row.get("id") or row)
                if key in seen:
                    continue
                seen.add(key)
                yield row
            if not data.get("hasMoreData"):
                return
            page += 1

    async def swap_transactions(self, address: str) -> dict[str, Any]:
        r = await self.call("jupiter.swap_transactions", path_params={"address": address}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def positions(self, address: str, platforms: str | None = None) -> dict[str, Any]:
        q = {"platforms": platforms} if platforms else {}
        r = await self.call("jupiter.positions", path_params={"address": address}, query=q, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def transfers(self, address: str) -> dict[str, Any]:
        r = await self.call("jupiter.transfers", path_params={"address": address}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def user_trades(self, addresses: list[str] | str) -> dict[str, Any]:
        value = addresses if isinstance(addresses, str) else ",".join(addresses)
        r = await self.call("jupiter.user_trades", query={"addresses": value}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}
