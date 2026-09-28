import pytest
import asyncio

from smartwallet.pipeline.market import _regime, _volatility_proxy, extract_price, ret
from smartwallet.pipeline.market import MarketLabeler
from smartwallet.storage import Storage


class BatchLlama:
    def __init__(self, unavailable=()):
        self.calls = []
        self.point_calls = 0
        self.unavailable = set(unavailable)

    async def batch_historical(self, points):
        self.calls.append({asset: list(timestamps) for asset, timestamps in points.items()})
        return {"coins": {asset: {"prices": [
            {"timestamp": timestamp, "price": 100 + timestamp % 97}
            for timestamp in timestamps
        ]} for asset, timestamps in points.items() if asset not in self.unavailable}}

    async def historical_prices(self, timestamp, coins):
        self.point_calls += 1
        return {"coins": {coin: {"price": 100 + timestamp % 97} for coin in coins}}


def episode(eid, start=1_700_000_000, asset="ethereum:token"):
    return {"episode_id": eid, "start_ts": start, "target_asset_key": asset}


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


@pytest.mark.asyncio
async def test_bulk_duplicate_points_fetched_once_and_context_is_not_per_episode(settings):
    llama = BatchLlama()
    storage = Storage(settings)
    rows = [episode(f"ep-{i}") for i in range(100)]
    result = await MarketLabeler(llama, storage).label_episodes(rows, (300,))
    requested = [(asset, timestamp) for call in llama.calls for asset, timestamps in call.items() for timestamp in timestamps]
    assert len(requested) == len(set(requested))
    assert len(llama.calls) == 1
    assert llama.point_calls == 0
    assert result["labels"] == 100


@pytest.mark.asyncio
async def test_repeated_run_reuses_persistent_market_cache(settings):
    storage = Storage(settings)
    first = BatchLlama()
    cold = await MarketLabeler(first, storage).label_episodes([episode("cold")], (300,))
    second = BatchLlama()
    warm = await MarketLabeler(second, storage).label_episodes([episode("warm")], (300,))
    assert cold["external_calls"] == 1
    assert warm["external_calls"] == 0
    assert warm["cache_hits"] == warm["unique_points"]
    assert second.calls == []


@pytest.mark.asyncio
async def test_partially_cached_horizon_fetches_only_missing_point(settings):
    storage = Storage(settings)
    await MarketLabeler(BatchLlama(), storage).label_episodes([episode("first")], (300,))
    with storage.conn() as db:
        db.execute("DELETE FROM market_time_series WHERE asset_key=? AND timestamp=?", ("ethereum:token", 1_700_000_300))
    llama = BatchLlama()
    result = await MarketLabeler(llama, storage).label_episodes([episode("second")], (300,))
    requested = {(asset, timestamp) for call in llama.calls for asset, timestamps in call.items() for timestamp in timestamps}
    assert requested == {("ethereum:token", 1_700_000_300)}
    assert result["fetched_points"] == 1


@pytest.mark.asyncio
async def test_unavailable_asset_does_not_block_other_episodes(settings):
    storage = Storage(settings)
    llama = BatchLlama(unavailable={"ethereum:bad"})
    result = await MarketLabeler(llama, storage).label_episodes(
        [episode("bad", asset="ethereum:bad"), episode("good", asset="ethereum:good")], (300,),
    )
    assert storage.fetchone("SELECT state FROM market_horizons WHERE episode_id='bad'")["state"] == "permanently unavailable"
    assert storage.fetchone("SELECT state FROM market_horizons WHERE episode_id='good'")["state"] == "success"
    assert result["labels"] == 1


@pytest.mark.asyncio
async def test_retry_state_is_persisted_when_bulk_and_point_provider_fail(settings):
    class FailingLlama:
        async def batch_historical(self, points):
            raise TimeoutError("batch timeout")
        async def historical_prices(self, timestamp, coins):
            raise TimeoutError("point timeout")

    storage = Storage(settings)
    result = await MarketLabeler(FailingLlama(), storage).label_episodes([episode("retry")], (300,))
    row = storage.fetchone("SELECT state,attempts,next_retry FROM market_horizons WHERE episode_id='retry'")
    assert row["state"] == "retryable failure"
    assert row["attempts"] == 1 and row["next_retry"] > 0
    assert storage.fetchone("SELECT 1 FROM episode_market_context WHERE episode_id='retry'") is None
    assert result["failed"] == 1
