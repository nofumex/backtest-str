from __future__ import annotations

from pathlib import Path

from smartwallet.incremental import IncrementalAnalyzer, maturity_for
from smartwallet.settings import Settings
from smartwallet.storage import Storage
from smartwallet.webdb import WebDB


def settings(tmp_path: Path) -> Settings:
    return Settings(
        hub_base_url="https://example.test", api_hub_key="test", llm_api_key="test",
        llm_base_url="https://example.test/v1", llm_model="test", db_path=tmp_path / "terminal.db",
        raw_dir=tmp_path / "raw", report_dir=tmp_path / "reports", http_timeout=1,
        http_retries=1, concurrency=1, episode_gap_seconds=3600, max_episode_events=20,
        bridge_check_min_usd=1000,
    )


def test_maturity_never_promotes_tiny_sample():
    assert maturity_for(n=3, ci_low=-0.2, ci_high=-0.1, qvalue=0.001,
                        holdout_accuracy=1, train_mean=-0.1, test_mean=-0.1) == "EARLY"
    assert maturity_for(n=120, ci_low=-0.04, ci_high=-0.01, qvalue=0.01,
                        holdout_accuracy=.7, train_mean=-0.02, test_mean=-0.03) == "ROBUST"


def test_incremental_refresh_is_idempotent(tmp_path: Path):
    cfg = settings(tmp_path)
    storage = Storage(cfg)
    web = WebDB(cfg.db_path)
    run_id = web.create_run({
        "entities": ["wintermute"], "from_date": "2024-01-01", "to_date": "2026-12-31",
        "mode": "diagnostic", "settings": {},
    }, {"wintermute": "Wintermute"})
    episode = {
        "episode_id": "ep-1", "entity_id": "wintermute", "start_ts": 1735689600,
        "end_ts": 1735689660, "wallets": ["0xabc"], "event_ids": ["event-1"],
        "motif": "LP_REMOVE>CEX_INTERACTION_OUT", "primary_asset_key": "coingecko:ethereum",
        "gross_usd": 1_000_000, "evidence": {"patterns": ["BIGRAM:LP_REMOVE>CEX_INTERACTION_OUT"]},
        "intent_label": "sell", "intent": {"label": "sell"}, "intent_confidence": .8,
        "classified_at": "2025-01-01T00:00:00+00:00",
    }
    storage.save_episode(episode)
    storage.save_market_label({
        "episode_id": "ep-1", "asset_key": "coingecko:ethereum", "horizon_seconds": 21600,
        "source": "test", "price_start": 100, "price_end": 97, "simple_return": -.03,
        "payload": {},
    })
    analyzer = IncrementalAnalyzer(web)
    first = analyzer.refresh(run_id)
    second = analyzer.refresh(run_id)
    aggregate = web.row("SELECT * FROM pattern_aggregates WHERE run_id=?", (run_id,))
    assert first["observations"] == 1
    assert second["observations"] == 0
    assert aggregate and aggregate["n"] == 1 and aggregate["mean_return"] == -.03
    storage.close()


def test_interrupted_run_and_wallet_are_resumable(tmp_path: Path):
    web = WebDB(tmp_path / "terminal.db")
    run_id = web.create_run({
        "entities": ["wintermute"], "from_date": "2024-01-01", "to_date": "2026-12-31",
        "mode": "full", "settings": {},
    }, {"wintermute": "Wintermute"})
    web.update_run(run_id, status="running")
    web.execute(
        "INSERT INTO run_wallets(run_id,entity_id,address,chain,status) VALUES(?,?,?,?,?)",
        (run_id, "wintermute", "0xabc", "eth", "running"),
    )
    web.recover_interrupted()
    assert web.run(run_id)["status"] == "paused"
    assert web.row("SELECT status FROM run_wallets WHERE run_id=?", (run_id,))["status"] == "pending"
