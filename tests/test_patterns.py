from smartwallet.pipeline.episodes import behavior_patterns


def test_behavior_patterns_include_specific_and_hierarchical_forms():
    rows = [
        {"action_type": "DEFI_INTERACTION_OUT", "project_id": "uniswap", "cex_id": None, "cate_id": None},
        {"action_type": "CEX_INTERACTION_OUT", "project_id": None, "cex_id": "binance", "cate_id": None},
    ]
    p = behavior_patterns(rows)
    assert "ACTION:DEFI_INTERACTION_OUT@project:uniswap" in p
    assert "ACTION:CEX_INTERACTION_OUT@cex:binance" in p
    assert "BIGRAM:DEFI_INTERACTION_OUT@project:uniswap>CEX_INTERACTION_OUT@cex:binance" in p
    assert "ENDPOINT:DEFI_INTERACTION_OUT@project:uniswap>CEX_INTERACTION_OUT@cex:binance" in p
    assert "FULL:DEFI_INTERACTION_OUT@project:uniswap>CEX_INTERACTION_OUT@cex:binance" in p
