from __future__ import annotations

from typing import Any

from .base import BaseProvider


class BridgeProvider(BaseProvider):
    async def lifi_status(self, tx_hash: str) -> dict[str, Any]:
        r = await self.call("lifi.status", query={"txHash": tx_hash}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def rubic_status(self, tx_hash: str) -> dict[str, Any]:
        r = await self.call("rubic.status", query={"srcTxHash": tx_hash}, allow_upstream_error=True)
        return r.data if isinstance(r.data, dict) else {}

    async def stargate_transfer_volume(self, address: str, start_iso: str, end_iso: str) -> dict[str, Any]:
        r = await self.call(
            "stargate.transfer_volume",
            query={"srcAddress": address, "start": start_iso, "end": end_iso},
            allow_upstream_error=True,
        )
        return r.data if isinstance(r.data, dict) else {}
