import json

from smartwallet.pipeline.backtest import run_backtest
from smartwallet.storage import Storage


def test_backtest_builds_group(settings):
    s = Storage(settings)
    for i,r in enumerate([-0.01,-0.02,-0.03,-0.01,-0.04,-0.02]):
        ep={"episode_id":f"ep{i}","entity_id":"x","start_ts":1000+i,"end_ts":1000+i,"wallets":["0xa"],"event_ids":[f"e{i}"],"motif":"CEX_INTERACTION_OUT","primary_asset_key":"coingecko:ethereum","gross_usd":1000,"evidence":{},"intent_label":"sell","intent":{"label":"sell"},"intent_confidence":0.8,"classified_at":"now"}
        s.save_episode(ep)
        s.save_market_label({"episode_id":f"ep{i}","asset_key":"coingecko:ethereum","horizon_seconds":3600,"source":"defillama.historical_price","price_start":100,"price_end":100*(1+r),"simple_return":r,"btc_return":0,"eth_return":r,"excess_vs_btc":r,"excess_vs_eth":0,"payload":{}})
    run_id, frame=run_backtest(s,min_n=5)
    assert len(frame)==2
    assert set(frame["scope_type"]) == {"entity", "wallet"}
    assert frame.iloc[0]["negative_rate"]==1.0
    assert frame.iloc[0]["median_return"]<0


def test_episode_target_asset_survives_sqlite_reload(settings):
    s = Storage(settings)
    s.save_episode({"episode_id":"target", "entity_id":"x", "start_ts":1, "end_ts":1, "wallets":[], "event_ids":[], "motif":"x", "primary_asset_key":"stable", "target_asset_key":"coingecko:ethereum", "evidence":{}})
    row = s.fetchone("SELECT target_asset_key FROM episodes WHERE episode_id='target'")
    assert row["target_asset_key"] == "coingecko:ethereum"
