from smartwallet.normalize import normalize_debank_event


def test_cex_out_is_evidence_not_sell_intent():
    row = {
        "id":"0x1","idx":0,"time_at":1000,"cate_id":None,"cex_id":"binance","project_id":None,
        "sends":[{"amount":2,"price":100,"to_addr":"0xcex","token_id":"0xtoken"}],
        "receives":[],"tx":{"from_addr":"0xwallet","to_addr":"0xcex","id":"0x1"},"token_approve":None,
    }
    e = normalize_debank_event("wintermute", "0xwallet", "eth", row, {"cex_dict":{"binance":{"name":"Binance"}}})
    assert e["action_type"] == "CEX_INTERACTION_OUT"
    assert e["usd_value"] == 200
    assert "SELL" not in e["action_type"]
