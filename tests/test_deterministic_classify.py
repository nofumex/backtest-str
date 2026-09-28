import json

import pytest

from smartwallet.pipeline.classify import ClassificationIndex, classify_episode_deterministic, classify_episodes
from smartwallet.storage import Storage


USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def index(*owned):
    return ClassificationIndex(events={}, enrichments={}, owned_wallets=set(owned), wallet_rows={}, wallet_times={})


def episode(*, sends=(), receives=(), action="ASSET_EXCHANGE_LIKE", project=None, cate=None, owned_wallet="0xa"):
    event = {
        "wallet": owned_wallet,
        "chain": "eth",
        "action_type": action,
        "project_id": project,
        "cate_id": cate,
        "evidence": {"sends": list(sends), "receives": list(receives), "tokens": {}},
        "tx_enrichments": {},
    }
    return {
        "episode_id": "ep",
        "start_ts": 100,
        "motif": action,
        "wallets": [owned_wallet],
        "primary_asset_key": "coingecko:ethereum",
        "target_asset_key": "coingecko:ethereum",
        "evidence": {"events": [event], "entity_context_pre_event": {"flow": {"value": 1}}},
    }


def leg(token, *, to=None, from_=None, amount=1, price=100):
    return {"token_id": token, "to_addr": to, "from_addr": from_, "amount": amount, "price": price}


@pytest.mark.parametrize(
    ("payload", "owned", "expected"),
    [
        (episode(sends=[leg(USDC)], receives=[leg("eth")]), ["0xa"], "buy"),
        (episode(sends=[leg("eth")], receives=[leg(USDC)]), ["0xa"], "sell"),
        (episode(sends=[], receives=[leg("eth", from_="0xcex")], action="CEX_INTERACTION_IN"), ["0xa"], "accumulation"),
        (episode(sends=[leg("eth", to="0x1"), leg("eth", to="0x2")], receives=[], action="TRANSFER_OUT"), ["0xa"], "distribution"),
        (episode(sends=[leg("eth", to="0xb")], receives=[], action="TRANSFER_OUT"), ["0xa", "0xb"], "internal_rebalance"),
        (episode(sends=[leg("eth")], receives=[leg("eth")]), ["0xa"], "inventory_rebalance"),
        (episode(sends=[leg("eth")], receives=[leg("0x1111111111111111111111111111111111111111")]), ["0xa"], "asset_rotation"),
        (episode(sends=[leg("eth")], receives=[], project="stargate"), ["0xa"], "bridge_reposition"),
        (episode(sends=[leg("eth")], receives=[], action="LP_ADD", cate="liquidity_pool"), ["0xa"], "liquidity_management"),
        (episode(sends=[], receives=[], action="CONTRACT_INTERACTION"), ["0xa"], "unknown"),
    ],
)
def test_deterministic_intent_rules(payload, owned, expected):
    result = classify_episode_deterministic(payload, index(*owned))
    assert result["label"] == expected
    assert 0 <= result["confidence"] <= 1
    assert result["reason"]
    assert result["features"]["actions"]
    assert result["classifier"] == "deterministic_intent"
    assert result["network_calls"] == 0


@pytest.mark.asyncio
async def test_batch_classifier_never_calls_llm_and_preserves_existing(settings):
    storage = Storage(settings)
    for episode_id, label in (("new", None), ("old", "sell")):
        payload = episode(sends=[leg(USDC)], receives=[leg("eth")])
        payload.update(episode_id=episode_id, entity_id="x", end_ts=100, event_ids=[], gross_usd=100)
        if label:
            payload.update(intent_label=label, intent={"label": label, "source": "legacy_llm"}, intent_confidence=.7)
        storage.save_episode(payload)

    class Bomb:
        async def classify_episode(self, _):
            raise AssertionError("LLM must not be called")

    assert await classify_episodes(storage, Bomb(), "x") == 1
    new = storage.fetchone("SELECT intent_label,intent_json FROM episodes WHERE episode_id='new'")
    old = storage.fetchone("SELECT intent_label,intent_json FROM episodes WHERE episode_id='old'")
    assert new["intent_label"] == "buy"
    assert json.loads(new["intent_json"])["classifier"] == "deterministic_intent"
    assert old["intent_label"] == "sell"
    assert json.loads(old["intent_json"])["source"] == "legacy_llm"
    storage.close()

