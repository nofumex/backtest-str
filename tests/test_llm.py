import httpx
import pytest

from smartwallet.llm import FreeLLMClient


@pytest.mark.asyncio
async def test_llm_structured_intent(settings):
    def handler(request):
        return httpx.Response(200, json={"choices":[{"message":{"content":"{\"economic_action\":\"rebalance\",\"intent_probabilities\":{\"inventory_rebalance\":0.8,\"unknown\":0.2},\"confidence\":0.7,\"evidence\":[\"x\"]}"}}]})
    c = FreeLLMClient(settings, transport=httpx.MockTransport(handler))
    try:
        r = await c.classify_episode({"events":[]})
        assert r["label"] == "inventory_rebalance"
        assert abs(sum(r["intent_probabilities"].values()) - 1.0) < 1e-9
    finally:
        await c.aclose()
