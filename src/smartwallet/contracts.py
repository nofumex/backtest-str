from __future__ import annotations

from dataclasses import dataclass
from typing import FrozenSet


@dataclass(frozen=True)
class EndpointContract:
    key: str
    provider: str
    method: str
    path: str
    query_params: FrozenSet[str]
    docs_url: str

    @property
    def run_path(self) -> str:
        return f"/run/{self.provider}{self.path}"


def _c(key: str, provider: str, path: str, params: tuple[str, ...], docs: str, method: str = "GET") -> EndpointContract:
    return EndpointContract(key, provider, method, path, frozenset(params), docs)


# Every provider call used by the project is declared here. Paths and parameter names
# are copied from the API Hub endpoint documentation. Provider code is forbidden from
# issuing arbitrary Hub routes outside this registry.
CONTRACTS: dict[str, EndpointContract] = {
    # Arkham Intelligence
    "arkham.entity": _c("arkham.entity", "arkham", "/intelligence/entity/{entity_id}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fintelligence%2Fentity%2F%7Bentity_id%7D"),
    "arkham.entity_summary": _c("arkham.entity_summary", "arkham", "/intelligence/entity/{entity_id}/summary", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fintelligence%2Fentity%2F%7Bentity_id%7D%2Fsummary"),
    "arkham.search": _c("arkham.search", "arkham", "/intelligence/search", ("query", "arkhamEntities", "arkhamAddresses", "userEntities", "userAddresses", "ens", "types", "services", "twitter", "opensea", "tokens", "pools", "tags"), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fintelligence%2Fsearch"),
    "arkham.address_enriched_all": _c("arkham.address_enriched_all", "arkham", "/intelligence/address_enriched/{address}/all", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fintelligence%2Faddress_enriched%2F%7Baddress%7D%2Fall"),
    "arkham.entity_top_address": _c("arkham.entity_top_address", "arkham", "/balances/entity_top_address/{entity_id}", ("customEntity",), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fbalances%2Fentity_top_address%2F%7Bentity_id%7D"),
    "arkham.entity_balances": _c("arkham.entity_balances", "arkham", "/balances/entity/{entity_id}", ("cheap",), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fbalances%2Fentity%2F%7Bentity_id%7D"),
    "arkham.address_balances": _c("arkham.address_balances", "arkham", "/balances/address/{address}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fbalances%2Faddress%2F%7Baddress%7D"),
    "arkham.address_flow": _c("arkham.address_flow", "arkham", "/flow/address/{address}", ("timeLast",), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fflow%2Faddress%2F%7Baddress%7D"),
    "arkham.address_history": _c("arkham.address_history", "arkham", "/history/address/{address}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhistory%2Faddress%2F%7Baddress%7D"),
    "arkham.address_loans": _c("arkham.address_loans", "arkham", "/loans/address/{address}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Floans%2Faddress%2F%7Baddress%7D"),
    "arkham.solana_entity_subaccounts": _c("arkham.solana_entity_subaccounts", "arkham", "/balances/solana/subaccounts/entity/{entities}", ("pricingId",), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fbalances%2Fsolana%2Fsubaccounts%2Fentity%2F%7Bentities%7D"),
    "arkham.entity_flow": _c("arkham.entity_flow", "arkham", "/flow/entity/{entity_id}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fflow%2Fentity%2F%7Bentity_id%7D"),
    "arkham.entity_history": _c("arkham.entity_history", "arkham", "/history/entity/{entity_id}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhistory%2Fentity%2F%7Bentity_id%7D"),
    "arkham.entity_volume": _c("arkham.entity_volume", "arkham", "/volume/entity/{entity_id}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fvolume%2Fentity%2F%7Bentity_id%7D"),
    "arkham.entity_loans": _c("arkham.entity_loans", "arkham", "/loans/entity/{entity_id}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Floans%2Fentity%2F%7Bentity_id%7D"),
    "arkham.hypercore_account_perp": _c("arkham.hypercore_account_perp", "arkham", "/hypercore/account/{address}/perp-positions", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Faccount%2F%7Baddress%7D%2Fperp-positions"),
    "arkham.hypercore_account_spot": _c("arkham.hypercore_account_spot", "arkham", "/hypercore/account/{address}/spot-balances", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Faccount%2F%7Baddress%7D%2Fspot-balances"),
    "arkham.hypercore_account_portfolio": _c("arkham.hypercore_account_portfolio", "arkham", "/hypercore/account/{address}/portfolio-history", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Faccount%2F%7Baddress%7D%2Fportfolio-history"),
    "arkham.hypercore_account_summary": _c("arkham.hypercore_account_summary", "arkham", "/hypercore/account/{address}/summary", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Faccount%2F%7Baddress%7D%2Fsummary"),
    "arkham.hypercore_perp": _c("arkham.hypercore_perp", "arkham", "/hypercore/entity/{entity_id}/perp-positions", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Fentity%2F%7Bentity_id%7D%2Fperp-positions"),
    "arkham.hypercore_spot": _c("arkham.hypercore_spot", "arkham", "/hypercore/entity/{entity_id}/spot-balances", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Fentity%2F%7Bentity_id%7D%2Fspot-balances"),
    "arkham.hypercore_portfolio": _c("arkham.hypercore_portfolio", "arkham", "/hypercore/entity/{entity_id}/portfolio-history", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Fentity%2F%7Bentity_id%7D%2Fportfolio-history"),
    "arkham.hypercore_summary": _c("arkham.hypercore_summary", "arkham", "/hypercore/entity/{entity_id}/summary", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fhypercore%2Fentity%2F%7Bentity_id%7D%2Fsummary"),
    "arkham.swaps": _c("arkham.swaps", "arkham", "/swaps", ("base", "flow", "timeLast", "limit", "offset"), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Fswaps"),
    "arkham.tx": _c("arkham.tx", "arkham", "/tx/{hash}", (), "https://hub.arbitron.dev/providers/arkham/endpoint?method=GET&path=%2Ftx%2F%7Bhash%7D"),

    # DeBank
    "debank.used_chains": _c("debank.used_chains", "debank", "/user/used_chains", ("id",), "https://hub.arbitron.dev/providers/debank/endpoint?method=GET&path=%2Fuser%2Fused_chains"),
    "debank.history": _c("debank.history", "debank", "/history/list", ("user_addr", "chain", "start_time", "page_count", "token_id"), "https://hub.arbitron.dev/providers/debank/endpoint?method=GET&path=%2Fhistory%2Flist"),
    "debank.positions": _c("debank.positions", "debank", "/portfolio/project_list", ("user_addr",), "https://hub.arbitron.dev/providers/debank/endpoint?method=GET&path=%2Fportfolio%2Fproject_list"),

    # OKLink
    "oklink.address_transactions": _c("oklink.address_transactions", "oklink", "/api/explorer/v2/{chain}/addresses/{address}/transactionsByClassfy/condition", ("limit", "offset", "nonzeroValue"), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv2%2F%7Bchain%7D%2Faddresses%2F%7Baddress%7D%2FtransactionsByClassfy%2Fcondition"),
    "oklink.internal_transactions": _c("oklink.internal_transactions", "oklink", "/api/explorer/v1/{chain}/addresses/{address}/internalTx/condition", ("limit", "offset"), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv1%2F%7Bchain%7D%2Faddresses%2F%7Baddress%7D%2FinternalTx%2Fcondition"),
    "oklink.token_transfers": _c("oklink.token_transfers", "oklink", "/api/explorer/v2/{chain}/addresses/{address}/transfers/condition/token", ("limit", "offset"), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv2%2F%7Bchain%7D%2Faddresses%2F%7Baddress%7D%2Ftransfers%2Fcondition%2Ftoken"),
    "oklink.defi_protocols": _c("oklink.defi_protocols", "oklink", "/api/explorer/v2/{chain}/addresses/{address}/defi/protocol/list", (), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv2%2F%7Bchain%7D%2Faddresses%2F%7Baddress%7D%2Fdefi%2Fprotocol%2Flist"),
    "oklink.tx_logs": _c("oklink.tx_logs", "oklink", "/api/explorer/v1/{chain}/transactions/{txHash}/logs", ("limit", "offset"), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv1%2F%7Bchain%7D%2Ftransactions%2F%7BtxHash%7D%2Flogs"),
    "oklink.tx_detail": _c("oklink.tx_detail", "oklink", "/api/explorer/v1/{chain}/transactions/{hash}", (), "https://hub.arbitron.dev/providers/oklink/endpoint?method=GET&path=%2Fapi%2Fexplorer%2Fv1%2F%7Bchain%7D%2Ftransactions%2F%7Bhash%7D"),

    # GeckoTerminal documented public v2 surface
    "gecko.search_pools": _c("gecko.search_pools", "geckoterminal", "/official/search/pools", ("query", "page"), "https://hub.arbitron.dev/providers/geckoterminal/endpoint?method=GET&path=%2Fofficial%2Fsearch%2Fpools"),
    "gecko.token_pools": _c("gecko.token_pools", "geckoterminal", "/official/networks/{network}/tokens/{token_address}/pools", ("page",), "https://hub.arbitron.dev/providers/geckoterminal/endpoint?method=GET&path=%2Fofficial%2Fnetworks%2F%7Bnetwork%7D%2Ftokens%2F%7Btoken_address%7D%2Fpools"),
    "gecko.ohlcv": _c("gecko.ohlcv", "geckoterminal", "/official/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}", ("aggregate", "limit"), "https://hub.arbitron.dev/providers/geckoterminal/endpoint?method=GET&path=%2Fofficial%2Fnetworks%2F%7Bnetwork%7D%2Fpools%2F%7Bpool_address%7D%2Fohlcv%2F%7Btimeframe%7D"),

    # DefiLlama
    "defillama.chains": _c("defillama.chains", "defillama", "/coins/chains", (), "https://hub.arbitron.dev/providers/defillama/endpoint?method=GET&path=%2Fcoins%2Fchains"),
    "defillama.historical_price": _c("defillama.historical_price", "defillama", "/coins/prices/historical/{timestamp}/{coins}", (), "https://hub.arbitron.dev/providers/defillama/endpoint?method=GET&path=%2Fcoins%2Fprices%2Fhistorical%2F%7Btimestamp%7D%2F%7Bcoins%7D"),

    # Bridge evidence
    "lifi.status": _c("lifi.status", "lifi", "/pipeline/v1/status", ("txHash",), "https://hub.arbitron.dev/providers/lifi/endpoint?method=GET&path=%2Fpipeline%2Fv1%2Fstatus"),
    "rubic.status": _c("rubic.status", "rubic", "/sdk/info/status", ("srcTxHash",), "https://hub.arbitron.dev/providers/rubic/endpoint?method=GET&path=%2Fsdk%2Finfo%2Fstatus"),
    "stargate.transfer_volume": _c("stargate.transfer_volume", "stargate", "/stats/overview/transfers/volume", ("srcAddress", "start", "end"), "https://hub.arbitron.dev/providers/stargate/endpoint?method=GET&path=%2Fstats%2Foverview%2Ftransfers%2Fvolume"),

    # Jupiter Portfolio / Solana
    "jupiter.activities": _c("jupiter.activities", "jupiter_portfolio", "/worker/v2/activities/{address}", ("product", "page"), "https://hub.arbitron.dev/providers/jupiter_portfolio/endpoint?method=GET&path=%2Fworker%2Fv2%2Factivities%2F%7Baddress%7D"),
    "jupiter.swap_transactions": _c("jupiter.swap_transactions", "jupiter_portfolio", "/worker/swap-transactions/{address}", (), "https://hub.arbitron.dev/providers/jupiter_portfolio/endpoint?method=GET&path=%2Fworker%2Fswap-transactions%2F%7Baddress%7D"),
    "jupiter.positions": _c("jupiter.positions", "jupiter_portfolio", "/portfolio/positions/{address}", ("platforms",), "https://hub.arbitron.dev/providers/jupiter_portfolio/endpoint?method=GET&path=%2Fportfolio%2Fpositions%2F%7Baddress%7D"),
    "jupiter.transfers": _c("jupiter.transfers", "jupiter_portfolio", "/data/transfers/{address}", (), "https://hub.arbitron.dev/providers/jupiter_portfolio/endpoint?method=GET&path=%2Fdata%2Ftransfers%2F%7Baddress%7D"),
    "jupiter.user_trades": _c("jupiter.user_trades", "jupiter_portfolio", "/data/user-trades", ("addresses",), "https://hub.arbitron.dev/providers/jupiter_portfolio/endpoint?method=GET&path=%2Fdata%2Fuser-trades"),

    # OKX public market reads
    "okx.history_candles": _c("okx.history_candles", "web3okx", "/api/v5/market/history-candles", ("instId", "bar", "limit", "before", "after"), "https://hub.arbitron.dev/providers/web3okx/endpoint?method=GET&path=%2Fapi%2Fv5%2Fmarket%2Fhistory-candles"),
    "okx.funding_rate": _c("okx.funding_rate", "web3okx", "/api/v5/public/funding-rate", ("instId",), "https://hub.arbitron.dev/providers/web3okx/endpoint?method=GET&path=%2Fapi%2Fv5%2Fpublic%2Ffunding-rate"),
    "okx.open_interest": _c("okx.open_interest", "web3okx", "/api/v5/public/open-interest", ("instType", "instId", "uly"), "https://hub.arbitron.dev/providers/web3okx/endpoint?method=GET&path=%2Fapi%2Fv5%2Fpublic%2Fopen-interest"),
}


def contract(key: str) -> EndpointContract:
    try:
        return CONTRACTS[key]
    except KeyError as exc:
        raise KeyError(f"Undocumented/unregistered endpoint key: {key}") from exc
