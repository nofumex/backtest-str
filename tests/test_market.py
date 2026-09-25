import pytest
import asyncio

from smartwallet.pipeline.market import _regime, _volatility_proxy, extract_price, ret
from smartwallet.pipeline.market import MarketLabeler


def test_price_and_return():
    p = {"coins":{"coingecko:ethereum":{"price":2000}}}
    assert extract_price(p,"coingecko:ethereum") == 2000
    assert ret(100,110) == pytest.approx(0.1)


def test_pre_event_regime_and_volatility_are_past_only_math():
    assert _regime(0.03) == "risk_on"
    assert _regime(-0.03) == "risk_off"
    assert _regime(0.0) == "neutral"
    assert _volatility_proxy([100, 101, 99, 102, 103]) is not None


@pytest.mark.asyncio
async def test_market_price_inflight_deduplicates(settings):
    class Llama:
        calls = 0
        async def historical_prices(self, timestamp, coins):
            self.calls += 1
            await asyncio.sleep(0.01)
            return {"coins": {coin: {"price": 1} for coin in coins}}
    llama = Llama()
    labeler = MarketLabeler(llama, __import__("smartwallet.storage", fromlist=["Storage"]).Storage(settings))
    await asyncio.gather(*(labeler.prices(1, ["coingecko:ethereum"]) for _ in range(8)))
    assert llama.calls == 1
