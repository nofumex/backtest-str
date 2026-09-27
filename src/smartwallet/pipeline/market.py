from __future__ import annotations

import math
import time
import asyncio
from statistics import pstdev
from typing import Any

from ..providers import DefiLlamaProvider, OKXMarketProvider
from ..storage import Storage
from ..http import request_context


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
            request_context.set((ep["entity_id"],None))
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
        eid, start, n = episode["episode_id"], int(episode["start_ts"]), 0
        with self.storage.conn() as db:
            db.executemany("INSERT OR IGNORE INTO market_horizons(episode_id,horizon_seconds) VALUES(?,?)", [(eid,h) for h in horizons])
        for horizon in horizons:
            state = self.storage.fetchone("SELECT * FROM market_horizons WHERE episode_id=? AND horizon_seconds=?", (eid,horizon))
            if state["state"] in ("success", "permanently unavailable") or state["next_retry"] > time.time():
                continue
            cached = self.storage.fetchone("SELECT 1 FROM market_labels WHERE episode_id=? AND asset_key=? AND horizon_seconds=? AND simple_return IS NOT NULL", (eid,asset,horizon))
            reason, next_retry, attempts = None, 0, state["attempts"]
            if cached:
                status = "success"
            elif not asset:
                status, reason = "permanently unavailable", "unknown asset"
            elif start + horizon > time.time():
                status, reason, next_retry = "pending", "horizon has not matured", start + horizon
            else:
                attempts += 1
                try:
                    coins = [asset, BTC, ETH]
                    p0, p1 = await asyncio.gather(self.prices(start, coins), self.prices(start+horizon, coins))
                    r = ret(extract_price(p0,asset), extract_price(p1,asset))
                    if r is None:
                        status, reason = "permanently unavailable", "provider has no historical price"
                    else:
                        br, er = ret(extract_price(p0,BTC),extract_price(p1,BTC)), ret(extract_price(p0,ETH),extract_price(p1,ETH))
                        self.storage.save_market_label(dict(episode_id=eid,asset_key=asset,horizon_seconds=horizon,source="defillama.historical_price",price_start=extract_price(p0,asset),price_end=extract_price(p1,asset),simple_return=r,btc_return=br,eth_return=er,excess_vs_btc=r-br if br is not None else None,excess_vs_eth=r-er if er is not None else None,payload={"start":p0,"end":p1}))
                        status, n = "success", n+1
                except Exception as exc:
                    reason = str(exc)[:250]
                    status = "permanently unavailable" if attempts >= 3 else "retryable failure"
                    if attempts >= 3:
                        reason = "retry budget exhausted: " + reason
                    else:
                        next_retry = time.time() + min(60, 2**attempts)
            with self.storage.conn() as db:
                db.execute("UPDATE market_horizons SET state=?,reason=?,attempts=?,next_retry=? WHERE episode_id=? AND horizon_seconds=?", (status,reason,attempts,next_retry,eid,horizon))
        # Context is independent of horizon completeness.
        if not self.storage.fetchone("SELECT 1 FROM episode_market_context WHERE episode_id=?", (eid,)):
            try:
                await self.save_pre_event_context(episode)
            except Exception:
                pass
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
