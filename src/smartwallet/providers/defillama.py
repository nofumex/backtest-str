from __future__ import annotations

import json
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

    async def batch_historical(self, points: dict[str, list[int]]) -> dict[str, Any]:
        r = await self.call(
            "defillama.batch_historical",
            query={"coins": json.dumps(
                {coin: sorted({int(ts) for ts in timestamps}) for coin, timestamps in points.items()},
                separators=(",", ":"),
            )},
            allow_upstream_error=True,
        )
        return r.data if isinstance(r.data, dict) else {}

    async def price_chart(self, coins: list[str] | tuple[str, ...] | str, **options: Any) -> dict[str, Any]:
        coin_value = coins if isinstance(coins, str) else ",".join(coins)
        r = await self.call(
            "defillama.price_chart", path_params={"coins": coin_value},
            query={key: value for key, value in options.items() if value is not None},
            allow_upstream_error=True,
        )
        return r.data if isinstance(r.data, dict) else {}
