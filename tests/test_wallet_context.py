import pytest

from smartwallet.pipeline.wallet_context import snapshot_evm_wallet_context
from smartwallet.storage import Storage


class FakeArkham:
    async def address_balances(self, *a, **k): return {"balances": {}}
    async def address_history(self, *a, **k): return {"ethereum": []}
    async def address_flow(self, *a, **k): return {"ethereum": []}
    async def address_loans(self, *a, **k): return {"balances": {}}
    async def hypercore_account_perp(self, *a, **k): return {}
    async def hypercore_account_spot(self, *a, **k): return {}
    async def hypercore_account_portfolio(self, *a, **k): return {}
    async def hypercore_account_summary(self, *a, **k): return {}


class FakeOKLink:
    async def address_transactions(self, *a, **k): return {"kind": "tx"}
    async def token_transfers(self, *a, **k): return {"kind": "token"}
    async def internal_transactions(self, *a, **k): return {"kind": "internal"}
    async def defi_protocols(self, *a, **k): return {"kind": "defi"}


class FakeGecko:
    async def token_pools(self, *a, **k): return {"data": []}


@pytest.mark.asyncio
async def test_wallet_context_archives_documented_surfaces(settings):
    storage = Storage(settings)
    storage.upsert_wallet("x", "0x1111111111111111111111111111111111111111", "ethereum", "test", {})
    storage.save_event({
        "event_id": "e1", "entity_id": "x", "wallet": "0x1111111111111111111111111111111111111111",
        "chain": "eth", "tx_hash": "0xabc", "event_index": 0, "ts": 1, "source": "debank.history",
        "action_type": "TRANSFER_OUT", "cate_id": None, "cex_id": None, "project_id": None,
        "usd_value": 100.0, "primary_token_id": "0x2222222222222222222222222222222222222222",
        "primary_asset_key": "ethereum:0x2222222222222222222222222222222222222222", "evidence": {}, "raw": {},
    })
    result = await snapshot_evm_wallet_context(FakeArkham(), FakeOKLink(), FakeGecko(), storage, entity_id="x", address="0x1111111111111111111111111111111111111111")
    assert result["snapshots_saved"] == 9
    assert result["top_token_pool_snapshots"] == 1
