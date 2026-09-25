import httpx
import pytest

from smartwallet.http import HubClient
from smartwallet.providers.debank import DeBankProvider
from smartwallet.storage import Storage


@pytest.mark.asyncio
async def test_debank_cursor_uses_oldest_time_at(settings):
    calls = []
    def handler(request):
        q = dict(request.url.params)
        calls.append(q)
        start = int(q.get("start_time", "0"))
        if start == 0:
            data = {"history_list": [{"id":"a","idx":0,"time_at":300},{"id":"b","idx":0,"time_at":200}], "cate_dict":{},"cex_dict":{},"memo_dict":{},"project_dict":{},"token_dict":{}}
        elif start == 200:
            data = {"history_list": [{"id":"c","idx":0,"time_at":150},{"id":"d","idx":0,"time_at":100}], "cate_dict":{},"cex_dict":{},"memo_dict":{},"project_dict":{},"token_dict":{}}
        else:
            data = {"history_list": []}
        return httpx.Response(200, json={"status":200,"data":data})
    hub = HubClient(settings, transport=httpx.MockTransport(handler))
    provider = DeBankProvider(hub, Storage(settings))
    try:
        got = []
        async for row, _ in provider.iter_history("0xabc", "eth", min_timestamp=120):
            got.append(row["id"])
        assert got == ["a","b","c"]
        assert calls[1]["start_time"] == "200"
    finally:
        await hub.aclose()
