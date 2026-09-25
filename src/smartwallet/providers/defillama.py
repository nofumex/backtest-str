from __future__ import annotations

from typing import Any

from .base import BaseProvider


class DefiLlamaProvider(BaseProvider):
    async def chains(self) -> list[str]:
        r = await self.call("defillama.chains", allow_upstream_error=True)
        return [str(x) for x in r.data] if isinstance(r.data, list) else []

    async def historical_prices(self, timestamp: int | str, coins: list[str] | tuple[str, ...] | str) -> dict[str, Any]:
        coin_value = coins if isinstance(coins, str) else ",".join(coins)
        r = await self.call(
            "defillama.historical_price",
            path_params={"timestamp": str(timestamp), "coins": coin_value},
            allow_upstream_error=True,
        )
        return r.data if isinstance(r.data, dict) else {}
