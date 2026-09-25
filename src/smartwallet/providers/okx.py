from __future__ import annotations

from typing import Any

from .base import BaseProvider


class OKXMarketProvider(BaseProvider):
    async def history_candles(self, inst_id: str, *, bar: str = "1m", limit: int = 100, before: str | None = None, after: str | None = None) -> list[Any]:
        q: dict[str, Any] = {"instId": inst_id, "bar": bar, "limit": limit}
        if before is not None:
            q["before"] = before
        if after is not None:
            q["after"] = after
        r = await self.call("okx.history_candles", query=q, allow_upstream_error=True)
        return r.data if isinstance(r.data, list) else []

    async def funding_rate(self, inst_id: str) -> list[Any]:
        r = await self.call("okx.funding_rate", query={"instId": inst_id}, allow_upstream_error=True)
        return r.data if isinstance(r.data, list) else []

    async def open_interest(self, *, inst_type: str, inst_id: str | None = None, uly: str | None = None) -> list[Any]:
        q: dict[str, Any] = {"instType": inst_type}
        if inst_id:
            q["instId"] = inst_id
        if uly:
            q["uly"] = uly
        r = await self.call("okx.open_interest", query=q, allow_upstream_error=True)
        return r.data if isinstance(r.data, list) else []
