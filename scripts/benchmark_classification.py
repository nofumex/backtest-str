"""CPU-only deterministic intent-classifier throughput benchmark."""

from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from pathlib import Path

from smartwallet.pipeline.classify import ClassificationIndex, classify_episode_deterministic
from smartwallet.settings import Settings
from smartwallet.storage import Storage


USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def synthetic_episode(i: int) -> dict:
    stable = {"token_id": USDC, "amount": 1000, "price": 1}
    asset = {"token_id": "eth", "amount": .5, "price": 2000}
    cases = (
        ("ASSET_EXCHANGE_LIKE", [stable], [asset], None, None),
        ("ASSET_EXCHANGE_LIKE", [asset], [stable], None, None),
        ("TRANSFER_OUT", [{**asset, "to_addr": "0xb"}], [], None, None),
        ("DEFI_INTERACTION_OUT", [asset], [], "stargate", "bridge"),
        ("CONTRACT_INTERACTION", [], [], None, None),
    )
    action, sends, receives, project, cate = cases[i % len(cases)]
    return {
        "episode_id": f"synthetic-{i}", "start_ts": 1_700_000_000 + i,
        "wallets": ["0xa"], "motif": action, "primary_asset_key": "coingecko:ethereum",
        "target_asset_key": "coingecko:ethereum",
        "evidence": {"events": [{
            "wallet": "0xa", "chain": "eth", "action_type": action,
            "project_id": project, "cate_id": cate,
            "evidence": {"sends": sends, "receives": receives, "tokens": {}},
            "tx_enrichments": {},
        }]},
    }


def rate(count: int, seconds: float) -> float:
    return count / seconds if seconds else float("inf")


def benchmark_synthetic(count: int) -> dict:
    episodes = [synthetic_episode(i) for i in range(count)]
    index = ClassificationIndex({}, {}, {"0xa", "0xb"}, {}, {})
    started = time.perf_counter()
    results = [classify_episode_deterministic(episode, index) for episode in episodes]
    elapsed = time.perf_counter() - started
    labels: dict[str, int] = defaultdict(int)
    for result in results:
        labels[result["label"]] += 1
    return {"episodes": count, "seconds": elapsed, "episodes_per_second": rate(count, elapsed), "labels": dict(labels)}


def benchmark_existing(path: Path, limit: int) -> dict:
    settings = Settings(
        hub_base_url="https://example.invalid", api_hub_key="benchmark", llm_api_key="",
        llm_base_url="http://127.0.0.1", llm_model="disabled", db_path=path,
        raw_dir=path.parent / "raw", report_dir=path.parent / "reports", http_timeout=1,
        http_retries=1, concurrency=1, episode_gap_seconds=21600, max_episode_events=50,
        bridge_check_min_usd=100000,
    )
    storage = Storage(settings)
    try:
        rows = storage.fetchall("SELECT * FROM episodes ORDER BY entity_id,start_ts LIMIT ?", (limit,))
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            grouped[row["entity_id"]].append(row)
        preload_started = time.perf_counter()
        indexes = {entity: ClassificationIndex.load(storage, entity) for entity in grouped}
        preload_seconds = time.perf_counter() - preload_started
        started = time.perf_counter()
        results = [classify_episode_deterministic(row, indexes[entity]) for entity, values in grouped.items() for row in values]
        elapsed = time.perf_counter() - started
        return {
            "episodes": len(results), "preload_seconds": preload_seconds, "classification_seconds": elapsed,
            "episodes_per_second": rate(len(results), elapsed),
        }
    finally:
        storage.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--episodes", type=int, default=10_000)
    parser.add_argument("--db", type=Path)
    args = parser.parse_args()
    output = {"synthetic": benchmark_synthetic(max(1, args.episodes))}
    if args.db and args.db.exists():
        output["existing"] = benchmark_existing(args.db, max(1, args.episodes))
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
