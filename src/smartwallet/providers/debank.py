from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from .base import BaseProvider


class DeBankProvider(BaseProvider):
    async def used_chains(self, address: str) -> list[Any]:
        r = await self.call("debank.used_chains", query={"id": address}, allow_upstream_error=True)
        if isinstance(r.data, dict):
            return r.data.get("chains") or []
        return []

    async def history_page(self, address: str, chain: str, *, start_time: int = 0, page_count: int = 20, token_id: str | None = None) -> dict[str, Any]:
        q: dict[str, Any] = {"user_addr": address, "chain": chain, "start_time": start_time, "page_count": page_count}
        if token_id:
            q["token_id"] = token_id
        r = await self.call("debank.history", query=q, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def iter_history(self, address: str, chain: str, *, min_timestamp: int = 0, page_count: int = 20, max_pages: int | None = None) -> AsyncIterator[tuple[dict[str, Any], dict[str, Any]]]:
        """Yield (history row, page dictionaries) newest -> oldest.

        Pagination exactly follows Hub documentation: next start_time is the oldest
        history_list[].time_at from the previous page.
        """
        cursor = 0
        page_no = 0
        seen: set[tuple[str, Any]] = set()
        while True:
            page_no += 1
            if max_pages is not None and page_no > max_pages:
                return
            page = await self.history_page(address, chain, start_time=cursor, page_count=page_count)
            rows = page.get("history_list") or []
            if not rows:
                return
            oldest: int | None = None
            emitted = 0
            dictionaries = {k: page.get(k) or {} for k in ("cate_dict", "cex_dict", "memo_dict", "project_dict", "token_dict")}
            for row in rows:
                try:
                    ts = int(float(row.get("time_at", 0)))
                except (TypeError, ValueError):
                    ts = 0
                if oldest is None or (ts and ts < oldest):
                    oldest = ts
                row_id = (str(row.get("id", "")), row.get("idx"))
                if row_id in seen:
                    continue
                seen.add(row_id)
                if ts and ts < min_timestamp:
                    continue
                emitted += 1
                yield row, dictionaries
            if oldest is None or oldest <= min_timestamp:
                return
            if cursor and oldest >= cursor:
                return
            cursor = oldest
            if emitted == 0 and min_timestamp:
                return

    async def positions(self, address: str) -> list[dict[str, Any]]:
        r = await self.call("debank.positions", query={"user_addr": address}, allow_upstream_error=True)
        return r.data if isinstance(r.data, list) else []
