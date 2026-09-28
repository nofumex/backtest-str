"""Offline benchmark for run-level market-point planning and persistent cache reuse."""
from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from dataclasses import replace
from pathlib import Path

from smartwallet.pipeline.market import DEFAULT_HORIZONS, MarketLabeler
from smartwallet.settings import Settings
from smartwallet.storage import Storage


class SyntheticBatchProvider:
    def __init__(self):
        self.calls = 0

    async def batch_historical(self, points):
        self.calls += 1
        return {"coins": {asset: {"prices": [
            {"timestamp": timestamp, "price": 10 + (abs(hash(asset)) % 1000) + timestamp % 101}
            for timestamp in timestamps
        ]} for asset, timestamps in points.items()}}


def rows(prefix: str, count: int):
    base = 1_704_067_200
    assets = [f"ethereum:asset-{i}" for i in range(25)]
    return [{"episode_id": f"{prefix}-{i}", "start_ts": base + i * 3600,
             "target_asset_key": assets[i % len(assets)]} for i in range(count)]


async def run(count: int):
    with tempfile.TemporaryDirectory(prefix="smartwallet-market-") as root:
        base = Settings.load()
        settings = replace(base, db_path=Path(root) / "bench.sqlite", raw_dir=Path(root) / "raw",
                           report_dir=Path(root) / "reports")
        storage = Storage(settings)
        cold_provider = SyntheticBatchProvider()
        cold = await MarketLabeler(cold_provider, storage).label_episodes(rows("cold", count), DEFAULT_HORIZONS, concurrency=8)
        warm_provider = SyntheticBatchProvider()
        warm = await MarketLabeler(warm_provider, storage).label_episodes(rows("warm", count), DEFAULT_HORIZONS, concurrency=8)
        # Previous algorithm reused the common start request in-process, but still made
        # one request per future horizon plus five differently-shaped context requests.
        before_calls = count * (1 + len(DEFAULT_HORIZONS) + 5)
        result = {
            "episodes": count,
            "horizons": count * len(DEFAULT_HORIZONS),
            "external_calls_before_estimate": before_calls,
            "cold": {**cold, "labeling_throughput_eps_s": round(count / cold["elapsed_seconds"], 1)},
            "warm": {**warm, "labeling_throughput_eps_s": round(count / warm["elapsed_seconds"], 1)},
        }
        storage.close()
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--episodes", type=int, default=3000)
    print(json.dumps(asyncio.run(run(parser.parse_args().episodes)), indent=2))
