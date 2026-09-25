import httpx
import pytest

from smartwallet.contracts import CONTRACTS
from smartwallet.http import HubClient


def test_all_contracts_have_docs_and_run_provider():
    assert len(CONTRACTS) >= 30
    for key, c in CONTRACTS.items():
        assert c.docs_url.startswith("https://hub.arbitron.dev/providers/")
        assert c.provider
        assert c.path.startswith("/")
        assert c.method in {"GET", "POST"}


@pytest.mark.asyncio
async def test_undocumented_query_param_rejected(settings):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"status": 200, "data": {}}))
    client = HubClient(settings, transport=transport)
    try:
        with pytest.raises(ValueError, match="undocumented query"):
            await client.request("debank.history", query={"user_addr": "0xabc", "chain": "eth", "madeUp": 1})
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_documented_request_uses_hub_key_header(settings):
    seen = {}
    def handler(request):
        seen["url"] = str(request.url)
        seen["key"] = request.headers.get("X-Hub-Key")
        return httpx.Response(200, json={"status": 200, "data": {"numAddresses": 1}})
    client = HubClient(settings, transport=httpx.MockTransport(handler))
    try:
        r = await client.request("arkham.entity_summary", path_params={"entity_id": "wintermute"})
        assert r.data["numAddresses"] == 1
        assert seen["key"] == "test-key"
        assert "test-key" not in seen["url"]
    finally:
        await client.aclose()
