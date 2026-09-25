import pytest

from smartwallet.pipeline.market import _regime, _volatility_proxy, extract_price, ret


def test_price_and_return():
    p = {"coins":{"coingecko:ethereum":{"price":2000}}}
    assert extract_price(p,"coingecko:ethereum") == 2000
    assert ret(100,110) == pytest.approx(0.1)


def test_pre_event_regime_and_volatility_are_past_only_math():
    assert _regime(0.03) == "risk_on"
    assert _regime(-0.03) == "risk_off"
    assert _regime(0.0) == "neutral"
    assert _volatility_proxy([100, 101, 99, 102, 103]) is not None
