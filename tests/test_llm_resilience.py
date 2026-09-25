import json

import httpx
import pytest

from smartwallet.llm import FreeLLMClient
from smartwallet.settings import Settings


@pytest.mark.asyncio
async def test_llm_retries_transport_failure(tmp_path):
    attempts = 0

    async def handler(request):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise httpx.ConnectTimeout("transient")
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps({"ok": True})}}]})

    s = Settings(
        hub_base_url="https://hub.arbitron.dev", api_hub_key="x", llm_api_key="x",
        llm_base_url="http://127.0.0.1:3001/v1", llm_model="test",
        db_path=tmp_path / "db.sqlite", raw_dir=tmp_path / "raw", report_dir=tmp_path / "reports",
        http_timeout=1.0, http_retries=4, concurrency=2, episode_gap_seconds=3600,
        max_episode_events=20, bridge_check_min_usd=1.0,
    )
    c = FreeLLMClient(s, transport=httpx.MockTransport(handler))
    try:
        out = await c.chat_json("s", {"x": 1})
        assert out == {"ok": True}
        assert attempts == 3
    finally:
        await c.aclose()
