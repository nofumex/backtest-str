from __future__ import annotations

from typing import Any

from .base import BaseProvider


class ArkhamProvider(BaseProvider):
    async def entity(self, entity_id: str) -> dict[str, Any]:
        r = await self.call("arkham.entity", path_params={"entity_id": entity_id})
        return r.data or {}

    async def entity_summary(self, entity_id: str) -> dict[str, Any]:
        r = await self.call("arkham.entity_summary", path_params={"entity_id": entity_id})
        return r.data or {}

    async def search(self, query: str, *, entities: int = 5, addresses: int = 15) -> dict[str, Any]:
        r = await self.call("arkham.search", query={"query": query, "arkhamEntities": entities, "arkhamAddresses": addresses})
        return r.data or {}

    async def address_enriched_all(self, address: str) -> dict[str, Any]:
        r = await self.call("arkham.address_enriched_all", path_params={"address": address})
        return r.data or {}

    async def top_address(self, entity_id: str) -> str | None:
        r = await self.call("arkham.entity_top_address", path_params={"entity_id": entity_id})
        return str(r.data) if r.data else None

    async def balances(self, entity_id: str, *, cheap: bool = True) -> dict[str, Any]:
        r = await self.call("arkham.entity_balances", path_params={"entity_id": entity_id}, query={"cheap": str(cheap).lower()})
        return r.data or {}

    async def address_balances(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.address_balances", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def address_flow(self, address: str, *, time_last: str | None = None) -> dict[str, Any]:
        q = {"timeLast": time_last} if time_last else {}
        return (await self.call("arkham.address_flow", path_params={"address": address}, query=q, allow_upstream_error=True)).data or {}

    async def address_history(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.address_history", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def address_loans(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.address_loans", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def solana_entity_subaccounts(self, entity_id: str) -> list[dict[str, Any]]:
        r = await self.call("arkham.solana_entity_subaccounts", path_params={"entities": entity_id}, query={"pricingId": "solana"}, allow_upstream_error=True)
        return r.data if isinstance(r.data, list) else []

    async def flow(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.entity_flow", path_params={"entity_id": entity_id})).data or {}

    async def history(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.entity_history", path_params={"entity_id": entity_id})).data or {}

    async def volume(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.entity_volume", path_params={"entity_id": entity_id})).data or {}

    async def loans(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.entity_loans", path_params={"entity_id": entity_id})).data or {}

    async def hypercore_account_perp(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_account_perp", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def hypercore_account_spot(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_account_spot", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def hypercore_account_portfolio(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_account_portfolio", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def hypercore_account_summary(self, address: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_account_summary", path_params={"address": address}, allow_upstream_error=True)).data or {}

    async def hypercore_perp(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_perp", path_params={"entity_id": entity_id})).data or {}

    async def hypercore_spot(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_spot", path_params={"entity_id": entity_id})).data or {}

    async def hypercore_portfolio(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_portfolio", path_params={"entity_id": entity_id})).data or {}

    async def hypercore_summary(self, entity_id: str) -> dict[str, Any]:
        return (await self.call("arkham.hypercore_summary", path_params={"entity_id": entity_id})).data or {}

    async def swaps_recent(self, base: str, *, flow: str = "all", time_last: str = "24h", limit: int = 50, offset: int = 0) -> dict[str, Any]:
        # Only parameters documented for GET /swaps are used. Historical timeGte/timeLte are intentionally not used.
        r = await self.call("arkham.swaps", query={"base": base, "flow": flow, "timeLast": time_last, "limit": limit, "offset": offset})
        return r.data or {}

    async def tx(self, tx_hash: str) -> dict[str, Any]:
        return (await self.call("arkham.tx", path_params={"hash": tx_hash}, allow_upstream_error=True)).data or {}
