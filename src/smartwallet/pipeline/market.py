from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from statistics import pstdev
from typing import Any, Iterable

from ..http import request_context
from ..providers import DefiLlamaProvider, OKXMarketProvider
from ..storage import Storage, canonical_json, utc_now_iso


DEFAULT_HORIZONS = (300, 3600, 21600, 86400, 259200)
BTC = "coingecko:bitcoin"
ETH = "coingecko:ethereum"
PRE_EVENT_STEP = 21600
PRICE_SOURCE = "defillama.batch_historical"
EXACT_RESOLUTION = 1
BATCH_POINT_LIMIT = 250  # stays comfortably below common proxy URL-length limits
POINT_COIN_LIMIT = 100


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
    if btc_24h is None:
        return "unknown"
    if btc_24h >= 0.02:
        return "risk_on"
    if btc_24h <= -0.02:
        return "risk_off"
    return "neutral"


def _chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _batch_chunks(points: set[tuple[str, int]], maximum: int = BATCH_POINT_LIMIT) -> list[dict[str, list[int]]]:
    chunks: list[dict[str, list[int]]] = []
    current: dict[str, list[int]] = defaultdict(list)
    count = 0
    for asset, timestamp in sorted(points):
        if count >= maximum:
            chunks.append(dict(current))
            current, count = defaultdict(list), 0
        current[asset].append(timestamp)
        count += 1
    if current:
        chunks.append(dict(current))
    return chunks


def _parse_batch(payload: dict[str, Any]) -> dict[tuple[str, int], float]:
    output: dict[tuple[str, int], float] = {}
    coins = payload.get("coins", payload) if isinstance(payload, dict) else {}
    if not isinstance(coins, dict):
        return output
    for asset, value in coins.items():
        series = value.get("prices") if isinstance(value, dict) else value
        if isinstance(series, dict):
            series = [{"timestamp": key, "price": price} for key, price in series.items()]
        if not isinstance(series, list):
            continue
        for item in series:
            if not isinstance(item, dict):
                continue
            try:
                timestamp = int(float(item.get("timestamp") or item.get("time") or item.get("ts")))
                price = float(item["price"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(price):
                output[(str(asset), timestamp)] = price
    return output


class MarketLabeler:
    """Run-level market-point planner backed by a persistent exact time-series cache."""

    def __init__(self, llama: DefiLlamaProvider, storage: Storage):
        self.llama = llama
        self.storage = storage
        self._cache: dict[tuple[int, tuple[str, ...]], dict[str, Any]] = {}
        self._inflight: dict[tuple[int, tuple[str, ...]], asyncio.Task] = {}

    async def prices(self, ts: int, coins: list[str]) -> dict[str, Any]:
        """Compatibility point API with in-process singleflight and legacy cache."""
        uniq = tuple(dict.fromkeys(coins))
        key = (int(ts), uniq)
        if key not in self._cache:
            cached = self.storage.market_price_get(int(ts), uniq)
            if cached is not None:
                hub = getattr(self.llama, "hub", None)
                if hub is not None:
                    hub.metrics["cache_hit"] = hub.metrics.get("cache_hit", 0) + 1
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

    def _load_cache(self, points: set[tuple[str, int]]) -> dict[tuple[str, int], dict[str, Any]]:
        if not points:
            return {}
        assets = sorted({asset for asset, _ in points})
        low, high = min(ts for _, ts in points), max(ts for _, ts in points)
        result: dict[tuple[str, int], dict[str, Any]] = {}
        for asset_chunk in _chunks(assets, 400):
            placeholders = ",".join("?" for _ in asset_chunk)
            rows = self.storage.fetchall(
                f"""SELECT asset_key,timestamp,price,status,reason,source,resolution,expires_at
                    FROM market_time_series WHERE asset_key IN ({placeholders})
                      AND timestamp BETWEEN ? AND ? ORDER BY fetched_at""",
                [*asset_chunk, low, high],
            )
            for row in rows:
                key = (row["asset_key"], int(row["timestamp"]))
                if key in points:
                    result[key] = row
        return result

    def _store_cache(self, rows: list[tuple[str, int, float | None, str, str | None]]) -> None:
        if not rows:
            return
        now = utc_now_iso()
        with self.storage.conn() as db:
            db.executemany(
                """INSERT INTO market_time_series(asset_key,timestamp,bucket,price,source,fetched_at,resolution,status,reason,expires_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(asset_key,timestamp,resolution,source) DO UPDATE SET
                   price=excluded.price,fetched_at=excluded.fetched_at,status=excluded.status,
                   reason=excluded.reason,expires_at=excluded.expires_at""",
                [(asset, ts, ts, price, PRICE_SOURCE, now, EXACT_RESOLUTION, status, reason,
                  0 if status == "success" else time.time() + (21600 if status == "unavailable" else 60))
                 for asset, ts, price, status, reason in rows],
            )

    async def _fetch_point_fallback(
        self, points: set[tuple[str, int]], concurrency: int,
    ) -> tuple[dict[tuple[str, int], float], dict[tuple[str, int], str], int]:
        by_timestamp: dict[int, list[str]] = defaultdict(list)
        for asset, timestamp in points:
            by_timestamp[timestamp].append(asset)
        jobs = [(timestamp, coins) for timestamp, values in by_timestamp.items()
                for coins in _chunks(sorted(set(values)), POINT_COIN_LIMIT)]
        sem = asyncio.Semaphore(max(1, concurrency))

        async def one(job):
            timestamp, coins = job
            async with sem:
                try:
                    payload = await self.llama.historical_prices(timestamp, coins)
                    return timestamp, coins, payload, None
                except Exception as exc:
                    return timestamp, coins, {}, type(exc).__name__

        values, errors = {}, {}
        for timestamp, coins, payload, error in await asyncio.gather(*(one(job) for job in jobs)):
            for coin in coins:
                key = (coin, timestamp)
                if error:
                    errors[key] = error
                else:
                    price = extract_price(payload, coin)
                    if price is not None:
                        values[key] = price
        return values, errors, len(jobs)

    async def _fetch_missing(
        self, points: set[tuple[str, int]], concurrency: int,
    ) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[str, int]]:
        cached = self._load_cache(points)
        now = time.time()
        reusable = {key: row for key, row in cached.items()
                    if row["status"] == "success" or
                    (row["status"] == "unavailable" and float(row.get("expires_at") or 0) > now)}
        missing = points - set(reusable)
        metrics = {
            "unique_points": len(points), "cache_hits": len(reusable), "fetched_points": 0,
            "unavailable_points": sum(row["status"] == "unavailable" for row in reusable.values()),
            "external_calls": 0,
        }
        values: dict[tuple[str, int], float] = {}
        errors: dict[tuple[str, int], str] = {}
        batch_method = getattr(self.llama, "batch_historical", None)
        if missing and callable(batch_method):
            chunks = _batch_chunks(missing)
            sem = asyncio.Semaphore(max(1, concurrency))

            async def one(chunk):
                async with sem:
                    try:
                        return chunk, await batch_method(chunk), None
                    except Exception as exc:
                        return chunk, {}, type(exc).__name__

            results = await asyncio.gather(*(one(chunk) for chunk in chunks))
            metrics["external_calls"] += len(results)
            for chunk, payload, error in results:
                requested = {(asset, ts) for asset, timestamps in chunk.items() for ts in timestamps}
                if error:
                    errors.update({key: error for key in requested})
                else:
                    values.update(_parse_batch(payload))
            # A deployment can expose the SDK method before its API gateway supports the
            # endpoint. In that case retain correctness via deduplicated point lookups.
            if errors:
                fallback_values, fallback_errors, calls = await self._fetch_point_fallback(set(errors), concurrency)
                metrics["external_calls"] += calls
                values.update(fallback_values)
                errors = fallback_errors
        elif missing:
            values, errors, calls = await self._fetch_point_fallback(missing, concurrency)
            metrics["external_calls"] += calls

        store_rows = []
        for key in missing:
            if key in values:
                store_rows.append((key[0], key[1], values[key], "success", None))
            elif key in errors:
                store_rows.append((key[0], key[1], None, "retryable", errors[key]))
            else:
                store_rows.append((key[0], key[1], None, "unavailable", "provider has no historical price"))
        self._store_cache(store_rows)
        metrics["fetched_points"] = len(values)
        metrics["unavailable_points"] += sum(row[3] == "unavailable" for row in store_rows)
        output = dict(reusable)
        output.update({key: {"price": price, "status": "success", "reason": None} for key, price in values.items()})
        output.update({key: {"price": None, "status": "retryable", "reason": reason} for key, reason in errors.items()})
        for asset, timestamp, _, status, reason in store_rows:
            output.setdefault((asset, timestamp), {"price": None, "status": status, "reason": reason})
        return output, metrics

    @staticmethod
    def _price(cache: dict[tuple[str, int], dict[str, Any]], asset: str, timestamp: int) -> float | None:
        row = cache.get((asset, timestamp)) or {}
        try:
            return float(row["price"]) if row.get("status") == "success" and row.get("price") is not None else None
        except (TypeError, ValueError):
            return None

    async def label_episodes(
        self, episodes: list[dict[str, Any]], horizons: tuple[int, ...] = DEFAULT_HORIZONS,
        concurrency: int = 8, progress=None,
    ) -> dict[str, int | float]:
        started = time.perf_counter()
        if not episodes:
            return {"labels": 0, "failed": 0, "unique_points": 0, "cache_hits": 0,
                    "fetched_points": 0, "unavailable_points": 0, "external_calls": 0, "elapsed_seconds": 0.0}
        episode_ids = [ep["episode_id"] for ep in episodes]
        with self.storage.conn() as db:
            db.executemany("INSERT OR IGNORE INTO market_horizons(episode_id,horizon_seconds) VALUES(?,?)",
                           [(eid, horizon) for eid in episode_ids for horizon in horizons])
        states: dict[tuple[str, int], dict[str, Any]] = {}
        labels_existing: set[tuple[str, str, int]] = set()
        contexts_existing: set[str] = set()
        for id_chunk in _chunks(episode_ids, 400):
            placeholders = ",".join("?" for _ in id_chunk)
            for row in self.storage.fetchall(f"SELECT * FROM market_horizons WHERE episode_id IN ({placeholders})", id_chunk):
                states[(row["episode_id"], int(row["horizon_seconds"]))] = row
            for row in self.storage.fetchall(
                f"SELECT episode_id,asset_key,horizon_seconds FROM market_labels WHERE simple_return IS NOT NULL AND episode_id IN ({placeholders})", id_chunk,
            ):
                labels_existing.add((row["episode_id"], row["asset_key"], int(row["horizon_seconds"])))
            contexts_existing.update(row["episode_id"] for row in self.storage.fetchall(
                f"SELECT episode_id FROM episode_market_context WHERE episode_id IN ({placeholders})", id_chunk,
            ))

        now = time.time()
        points: set[tuple[str, int]] = set()
        eligible: list[tuple[dict[str, Any], str | None, int, dict[str, Any]]] = []
        context_episodes: list[dict[str, Any]] = []
        immediate_states: list[tuple[str, str | None, int, float, str, int]] = []
        for episode in episodes:
            eid, start = episode["episode_id"], int(episode["start_ts"])
            asset = episode.get("target_asset_key") or episode.get("primary_asset_key")
            for horizon in horizons:
                state = states[(eid, horizon)]
                if state["state"] in {"success", "permanently unavailable"} or state["next_retry"] > now:
                    continue
                if asset and (eid, asset, horizon) in labels_existing:
                    immediate_states.append(("success", None, state["attempts"], 0, eid, horizon))
                elif not asset:
                    immediate_states.append(("permanently unavailable", "unknown asset", state["attempts"], 0, eid, horizon))
                elif start + horizon > now:
                    immediate_states.append(("pending", "horizon has not matured", state["attempts"], start + horizon, eid, horizon))
                else:
                    eligible.append((episode, asset, horizon, state))
                    for coin in {asset, BTC, ETH}:
                        points.add((coin, start)); points.add((coin, start + horizon))
            if eid not in contexts_existing:
                context_episodes.append(episode)
                for timestamp in [start - PRE_EVENT_STEP * i for i in range(4, -1, -1)]:
                    points.add((BTC, timestamp)); points.add((ETH, timestamp))

        cache, metrics = await self._fetch_missing(points, concurrency)
        label_rows, state_rows, context_rows = [], list(immediate_states), []
        failures = 0
        for episode, asset, horizon, state in eligible:
            eid, start = episode["episode_id"], int(episode["start_ts"])
            required = [cache.get((asset, start), {}), cache.get((asset, start + horizon), {})]
            if any(row.get("status") == "retryable" for row in required):
                attempts = int(state["attempts"]) + 1
                reason = next((row.get("reason") for row in required if row.get("status") == "retryable"), "market fetch failed")
                status = "permanently unavailable" if attempts >= 3 else "retryable failure"
                reason = ("retry budget exhausted: " if attempts >= 3 else "") + str(reason)[:220]
                state_rows.append((status, reason, attempts, 0 if attempts >= 3 else time.time() + min(60, 2**attempts), eid, horizon))
                failures += 1
                continue
            if any(row.get("status") == "unavailable" for row in required):
                attempts = int(state["attempts"]) + 1
                status = "permanently unavailable" if attempts >= 3 else "retryable failure"
                retry_at = max((float(row.get("expires_at") or 0) for row in required), default=time.time() + 21600)
                reason = "provider has no historical price; negative cache will be revalidated"
                if attempts >= 3:
                    reason = "retry budget exhausted: " + reason
                state_rows.append((status, reason, attempts, 0 if attempts >= 3 else retry_at, eid, horizon))
                failures += 1
                continue
            p0, p1 = self._price(cache, asset, start), self._price(cache, asset, start + horizon)
            simple = ret(p0, p1)
            if simple is None:
                state_rows.append(("retryable failure", "market point temporarily unavailable", int(state["attempts"]) + 1,
                                   time.time() + 21600, eid, horizon))
                continue
            bp0, bp1 = self._price(cache, BTC, start), self._price(cache, BTC, start + horizon)
            ep0, ep1 = self._price(cache, ETH, start), self._price(cache, ETH, start + horizon)
            btc_return, eth_return = ret(bp0, bp1), ret(ep0, ep1)
            label_rows.append((eid, asset, horizon, "defillama.historical_price", p0, p1, simple,
                               btc_return, eth_return, simple-btc_return if btc_return is not None else None,
                               simple-eth_return if eth_return is not None else None,
                               canonical_json({"cache": "market_time_series", "timestamps": [start, start+horizon]})))
            state_rows.append(("success", None, state["attempts"], 0, eid, horizon))
            if progress:
                progress(success=1, failed=0)

        for episode in context_episodes:
            eid, start = episode["episode_id"], int(episode["start_ts"])
            timestamps = [start - PRE_EVENT_STEP * i for i in range(4, -1, -1)]
            context_points = [cache.get((coin, ts), {}) for coin in (BTC, ETH) for ts in timestamps]
            if any(row.get("status") == "retryable" for row in context_points):
                continue
            btc_prices = [self._price(cache, BTC, ts) for ts in timestamps]
            eth_prices = [self._price(cache, ETH, ts) for ts in timestamps]
            btc_24h, eth_24h = ret(btc_prices[0], btc_prices[-1]), ret(eth_prices[0], eth_prices[-1])
            context_rows.append((eid, "defillama.historical_price", btc_24h, eth_24h,
                                 _volatility_proxy(btc_prices), _volatility_proxy(eth_prices), _regime(btc_24h),
                                 canonical_json({"timestamps": timestamps, "btc_prices": btc_prices, "eth_prices": eth_prices,
                                                 "cache": "market_time_series"})))
        with self.storage.conn() as db:
            db.executemany(
                """INSERT OR REPLACE INTO market_labels(episode_id,asset_key,horizon_seconds,source,price_start,
                   price_end,simple_return,btc_return,eth_return,excess_vs_btc,excess_vs_eth,payload_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", label_rows,
            )
            db.executemany("UPDATE market_horizons SET state=?,reason=?,attempts=?,next_retry=? WHERE episode_id=? AND horizon_seconds=?", state_rows)
            db.executemany(
                """INSERT OR REPLACE INTO episode_market_context(episode_id,source,btc_return_24h,eth_return_24h,
                   btc_volatility_proxy,eth_volatility_proxy,regime,payload_json) VALUES(?,?,?,?,?,?,?,?)""", context_rows,
            )
            db.executemany(
                """INSERT INTO artifact_provenance(artifact_type,artifact_id,version,input_hash,updated_at)
                   VALUES('market_label',?,'2.0-bulk-exact',?,?) ON CONFLICT(artifact_type,artifact_id) DO UPDATE SET
                   version=excluded.version,input_hash=excluded.input_hash,updated_at=excluded.updated_at""",
                [(f"{row[0]}|{row[1]}|{row[2]}",
                  hashlib.sha256(canonical_json([row[0], row[1], row[2], row[4], row[5]]).encode()).hexdigest(),
                  utc_now_iso()) for row in label_rows],
            )
        metrics.update(labels=len(label_rows), failed=failures,
                       elapsed_seconds=time.perf_counter() - started,
                       horizons_processed=len(state_rows))
        return metrics

    async def save_pre_event_context(self, episode: dict[str, Any]) -> dict[str, Any]:
        await self.label_episodes([episode], horizons=())
        row = self.storage.fetchone("SELECT * FROM episode_market_context WHERE episode_id=?", (episode["episode_id"],))
        return row or {}

    async def label_episode(self, episode: dict[str, Any], horizons: tuple[int, ...] = DEFAULT_HORIZONS) -> int:
        result = await self.label_episodes([episode], horizons)
        return int(result["labels"])


async def snapshot_live_derivatives(okx: OKXMarketProvider, storage: Storage, instruments: tuple[str, ...] = ("BTC-USDT-SWAP", "ETH-USDT-SWAP")) -> None:
    captured = utc_now_iso()
    with storage.conn() as db:
        for inst in instruments:
            funding = await okx.funding_rate(inst)
            oi = await okx.open_interest(inst_type="SWAP", inst_id=inst)
            db.execute("INSERT OR REPLACE INTO market_snapshots(captured_at,source,instrument,kind,payload_json) VALUES(?,?,?,?,?)", (captured, "okx", inst, "funding_rate", canonical_json(funding)))
            db.execute("INSERT OR REPLACE INTO market_snapshots(captured_at,source,instrument,kind,payload_json) VALUES(?,?,?,?,?)", (captured, "okx", inst, "open_interest", canonical_json(oi)))
