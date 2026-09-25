from smartwallet.pipeline.episodes import build_episodes
from smartwallet.storage import Storage


def ev(i, ts, wallet, action="TRANSFER_OUT", cp=None):
    return {
        "event_id":f"e{i}","entity_id":"x","wallet":wallet,"chain":"eth","tx_hash":f"0x{i}","event_index":0,
        "ts":ts,"source":"debank.history","action_type":action,"cate_id":None,"cex_id":None,"project_id":None,
        "usd_value":100,"primary_token_id":"eth","primary_asset_key":"coingecko:ethereum",
        "evidence":{"sends":[{"to_addr":cp}] if cp else [],"receives":[]},"raw":{}
    }


def test_episode_window_does_not_chain_forever(settings):
    s = Storage(settings)
    for x in [ev(1,0,"0xa"),ev(2,10000,"0xa"),ev(3,20000,"0xa"),ev(4,30000,"0xa")]: s.save_event(x)
    eps = build_episodes(s,"x",21600)
    assert len(eps) == 2


def test_confirmed_wallet_transfer_can_merge(settings):
    s = Storage(settings)
    s.upsert_wallet("x","0xa","evm","test")
    s.upsert_wallet("x","0xb","evm","test")
    s.save_event(ev(1,1000,"0xa",cp="0xb"))
    s.save_event(ev(2,1200,"0xb"))
    eps = build_episodes(s,"x",21600)
    assert len(eps) == 1
    assert sorted(eps[0]["wallets"]) == ["0xa","0xb"]
