from __future__ import annotations

import math
import asyncio
from statistics import pstdev
from typing import Any

from ..providers import DefiLlamaProvider, OKXMarketProvider
from ..storage import Storage


DEFAULT_HORIZONS = (300, 3600, 21600, 86400, 259200)
BTC = "coingecko:bitcoin"
ETH = "coingecko:ethereum"
PRE_EVENT_STEP = 21600  # 6h; four intervals across the prior 24h


def extract_price(payload: dict[str, Any], coin: str) -> float | None:
    coins = payload.get("coins") if isinstance(payload, dict) else None
    if not isinstance(coins, dict):
        return None
    value = coins.get(coin)
    if isinstance(value, dict):
        try:
            return float(value["price"])
        except (KeyError, TypeError, ValueError):
            return None
    return None


def ret(p0: float | None, p1: float | None) -> float | None:
    if p0 is None or p1 is None or p0 == 0:
        return None
    return p1 / p0 - 1.0


def _volatility_proxy(prices: list[float | None]) -> float | None:
    clean = [p for p in prices if p is not None and p > 0]
    if len(clean) != len(prices) or len(clean) < 3:
        return None
    log_returns = [math.log(clean[i] / clean[i - 1]) for i in range(1, len(clean))]
    return float(pstdev(log_returns)) if len(log_returns) > 1 else 0.0


def _regime(btc_24h: float | None) -> str:
    # A modeling threshold, not an upstream field. It only buckets pre-event information.
    if btc_24h is None:
        return "unknown"
    if btc_24h >= 0.02:
        return "risk_on"
    if btc_24h <= -0.02:
        return "risk_off"
    return "neutral"


class MarketLabeler:
    def __init__(self, llama: DefiLlamaProvider, storage: Storage):
        self.llama = llama
        self.storage = storage
        self._cache: dict[tuple[int, tuple[str, ...]], dict[str, Any]] = {}
        self._inflight: dict[tuple[int, tuple[str, ...]], asyncio.Task] = {}

    async def prices(self, ts: int, coins: list[str]) -> dict[str, Any]:
        uniq = tuple(dict.fromkeys(coins))
        key = (int(ts), uniq)
        if key not in self._cache:
            cached = self.storage.market_price_get(int(ts), uniq)
            if cached is not None:
                self.llama.hub.metrics["cache_hit"] = self.llama.hub.metrics.get("cache_hit", 0) + 1
                self._cache[key] = cached
            else:
                task = self._inflight.get(key)
                if task is None:
                    async def fetch():
                        value = await self.llama.historical_prices(int(ts), list(uniq))
                        if value:
                            self.storage.market_price_put(int(ts), uniq, value)
                        return value
                    task = self._inflight[key] = asyncio.create_task(fetch())
                try:
                    self._cache[key] = await task
                finally:
                    self._inflight.pop(key, None)
        return self._cache[key]

    async def label_episodes(self, episodes: list[dict[str, Any]], horizons: tuple[int, ...] = DEFAULT_HORIZONS, concurrency: int = 8, progress=None) -> dict[str, int]:
        sem = asyncio.Semaphore(max(1, concurrency))
        async def one(ep):
            async with sem:
                try:
                    n = await self.label_episode(ep, horizons)
                    if progress: progress(success=n, failed=0)
                    return n, False
                except Exception:
                    if progress: progress(success=0, failed=1)
                    return 0, True
        results = await asyncio.gather(*(one(ep) for ep in episodes))
        return {"labels": sum(n for n, _ in results), "failed": sum(f for _, f in results)}

    async def save_pre_event_context(self, episode: dict[str, Any]) -> dict[str, Any]:
        start = int(episode["start_ts"])
        timestamps = [start - PRE_EVENT_STEP * i for i in range(4, -1, -1)]
        payloads = await asyncio.gather(*(self.prices(ts, [BTC, ETH]) for ts in timestamps))
        btc_prices = [extract_price(p, BTC) for p in payloads]
        eth_prices = [extract_price(p, ETH) for p in payloads]
        btc_24h = ret(btc_prices[0], btc_prices[-1])
        eth_24h = ret(eth_prices[0], eth_prices[-1])
        row = {
            "episode_id": episode["episode_id"],
            "source": "defillama.historical_price",
            "btc_return_24h": btc_24h,
            "eth_return_24h": eth_24h,
            "btc_volatility_proxy": _volatility_proxy(btc_prices),
            "eth_volatility_proxy": _volatility_proxy(eth_prices),
            "regime": _regime(btc_24h),
            "payload": {"timestamps": timestamps, "prices": payloads},
        }
        self.storage.save_episode_market_context(row)
        return row

    async def label_episode(self, episode: dict[str, Any], horizons: tuple[int, ...] = DEFAULT_HORIZONS) -> int:
        asset = episode.get("target_asset_key") or episode.get("primary_asset_key")
        if not asset:
            return 0
        await self.save_pre_event_context(episode)
        coins = [asset, BTC, ETH]
        start = int(episode["start_ts"])
        p0_payload = await self.prices(start, coins)
        p0 = extract_price(p0_payload, asset)
        btc0 = extract_price(p0_payload, BTC)
        eth0 = extract_price(p0_payload, ETH)
        n = 0
        endpoints = await asyncio.gather(*(self.prices(start + horizon, coins) for horizon in horizons))
        for horizon, end_payload in zip(horizons, endpoints):
            p1 = extract_price(end_payload, asset)
            btc1 = extract_price(end_payload, BTC)
            eth1 = extract_price(end_payload, ETH)
            asset_r = ret(p0, p1)
            btc_r = ret(btc0, btc1)
            eth_r = ret(eth0, eth1)
            row = {
                "episode_id": episode["episode_id"],
                "asset_key": asset,
                "horizon_seconds": horizon,
                "source": "defillama.historical_price",
                "price_start": p0,
                "price_end": p1,
                "simple_return": asset_r,
                "btc_return": btc_r,
                "eth_return": eth_r,
                "excess_vs_btc": asset_r - btc_r if asset_r is not None and btc_r is not None else None,
                "excess_vs_eth": asset_r - eth_r if asset_r is not None and eth_r is not None else None,
                "payload": {"start": p0_payload, "end": end_payload},
            }
            if asset_r is not None:
                self.storage.save_market_label(row)
                n += 1
        return n


async def snapshot_live_derivatives(okx: OKXMarketProvider, storage: Storage, instruments: tuple[str, ...] = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")) -> None:
    from ..storage import canonical_json, utc_now_iso
    captured = utc_now_iso()
    with storage.conn() as db:
        for inst in instruments:
            funding = await okx.funding_rate(inst)
            oi = await okx.open_interest(inst_type="SWAP", inst_id=inst)
            db.execute("INSERT OR REPLACE INTO market_snapshots(captured_at,source,instrument,kind,payload_json) VALUES(?,?,?,?,?)", (captured, "okx", inst, "funding_rate", canonical_json(funding)))
            db.execute("INSERT OR REPLACE INTO market_snapshots(captured_at,source,instrument,kind,payload_json) VALUES(?,?,?,?,?)", (captured, "okx", inst, "open_interest", canonical_json(oi)))
