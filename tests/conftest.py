from pathlib import Path

import pytest

from smartwallet.settings import Settings


@pytest.fixture
def settings(tmp_path: Path):
    return Settings(
        hub_base_url="https://hub.arbitron.dev",
        api_hub_key="test-key",
        llm_api_key="test-llm",
        llm_base_url="http://llm.local/v1",
        llm_model="test-model",
        db_path=tmp_path / "db.sqlite",
        raw_dir=tmp_path / "raw",
        report_dir=tmp_path / "reports",
        http_timeout=5,
        http_retries=2,
        concurrency=4,
        episode_gap_seconds=21600,
        max_episode_events=50,
        bridge_check_min_usd=100000,
    )
