"""Synthetic 30k+ end-to-end CPU/cache benchmark; never touches the project database."""
from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import time
from dataclasses import replace
from pathlib import Path

from smartwallet.incremental import IncrementalAnalyzer
from smartwallet.pipeline.classify import classify_episodes
from smartwallet.pipeline.episodes import build_episodes
from smartwallet.pipeline.market import DEFAULT_HORIZONS, MarketLabeler
from smartwallet.settings import Settings
from smartwallet.storage import Storage
from smartwallet.webdb import WebDB


class BatchProvider:
    def __init__(self):
        self.calls = 0

    async def batch_historical(self, points):
        self.calls += 1
        return {"coins": {asset: {"prices": [
            {"timestamp": ts, "price": 100 + (abs(hash(asset)) % 1000) + ts % 113}
            for ts in timestamps
        ]} for asset, timestamps in points.items()}}


def event(i: int, base: int):
    token = f"0x{i % 100:040x}"
    return {
        "event_id": f"event-{i}", "entity_id": "bench", "wallet": f"0x{i:040x}", "chain": "eth",
        "tx_hash": f"0x{i:064x}", "event_index": 0, "ts": base + i, "source": "synthetic",
        "action_type": "SWAP", "cate_id": "swap", "cex_id": None, "project_id": "dex",
        "usd_value": 1000.0, "primary_token_id": token, "primary_asset_key": f"ethereum:{token}",
        "evidence": {"sends": [{"token_id": "usdc", "amount": 1000, "price": 1}],
                     "receives": [{"token_id": token, "amount": 1, "price": 1000}],
                     "tokens": {token: {"symbol": f"T{i % 100}"}, "usdc": {"symbol": "USDC"}}},
        "raw": {},
    }


async def benchmark(count: int):
    with tempfile.TemporaryDirectory(prefix="smartwallet-pipeline-") as root:
        base_settings = Settings.load()
        settings = replace(base_settings, db_path=Path(root) / "bench.sqlite", raw_dir=Path(root) / "raw",
                           report_dir=Path(root) / "reports")
        web = WebDB(settings.db_path)
        run_id = web.create_run({"mode": "full", "entities": ["bench"], "from_date": "2024-01-01",
                                 "to_date": "2026-12-31", "settings": {}}, {"bench": "Benchmark"})
        storage = Storage(settings)
        storage.run_id = run_id
        storage.run_bounds = (1704067200, 1798761599)
        timings = {}

        started = time.perf_counter()
        base = 1704067200
        for offset in range(0, count, 1000):
            storage.save_events(event(i, base) for i in range(offset, min(count, offset + 1000)))
        timings["event_write_seconds"] = time.perf_counter() - started

        started = time.perf_counter()
        episodes = build_episodes(storage, "bench", settings.episode_gap_seconds,
                                  settings.max_episode_events, start_ts=base, end_ts=base + count, run_id=run_id)
        timings["episode_build_seconds"] = time.perf_counter() - started

        started = time.perf_counter()
        classified = await classify_episodes(storage, None, "bench", run_id=run_id)
        timings["classification_seconds"] = time.perf_counter() - started

        provider = BatchProvider()
        cold = await MarketLabeler(provider, storage).label_episodes(
            storage.fetchall("SELECT * FROM episodes WHERE episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)", (run_id,)),
            DEFAULT_HORIZONS, concurrency=8,
        )
        with storage.conn() as db:
            db.execute("DELETE FROM market_labels")
            db.execute("DELETE FROM episode_market_context")
            db.execute("UPDATE market_horizons SET state='pending',reason=NULL,attempts=0,next_retry=0")
        warm_provider = BatchProvider()
        warm = await MarketLabeler(warm_provider, storage).label_episodes(
            storage.fetchall("SELECT * FROM episodes WHERE episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)", (run_id,)),
            DEFAULT_HORIZONS, concurrency=8,
        )

        analyzer = IncrementalAnalyzer(web)
        started = time.perf_counter()
        analysis_calls = 0
        while True:
            result = analyzer.refresh(run_id, batch_size=10_000)
            analysis_calls += 1
            if not result["observations"] and not result["primary_observations"]:
                break
        timings["incremental_analysis_seconds"] = time.perf_counter() - started
        started = time.perf_counter()
        analyzer.refresh_expensive(run_id, dirty_only=False)
        timings["final_statistics_seconds"] = time.perf_counter() - started
        observations = web.row("SELECT COUNT(*) n FROM primary_analysis_observations WHERE run_id=?", (run_id,))["n"]
        result = {
            "events": count, "episodes": len(episodes), "classified": classified, "usable_observations": observations,
            "timings": {key: round(value, 3) for key, value in timings.items()},
            "throughput": {
                "event_write_per_second": round(count / timings["event_write_seconds"], 1),
                "episode_build_per_second": round(len(episodes) / timings["episode_build_seconds"], 1),
                "classification_per_second": round(classified / timings["classification_seconds"], 1),
                "analysis_observations_per_second": round(observations / timings["incremental_analysis_seconds"], 1),
            },
            "market_cold": cold, "market_warm": warm, "analysis_batches": analysis_calls,
            "external_calls_before_estimate": count * (1 + len(DEFAULT_HORIZONS) + 5),
        }
        storage.close()
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--episodes", type=int, default=30_000)
    print(json.dumps(asyncio.run(benchmark(parser.parse_args().episodes)), indent=2))
