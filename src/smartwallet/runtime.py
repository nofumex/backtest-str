from __future__ import annotations

from dataclasses import dataclass

from .http import HubClient
from .llm import FreeLLMClient
from .providers import (
    ArkhamProvider, BridgeProvider, DeBankProvider, DefiLlamaProvider, GeckoTerminalProvider,
    JupiterProvider, OKLinkProvider, OKXMarketProvider, RPCProvider,
)
from .settings import Settings
from .storage import Storage


@dataclass
class Runtime:
    settings: Settings
    storage: Storage
    hub: HubClient
    arkham: ArkhamProvider
    debank: DeBankProvider
    oklink: OKLinkProvider
    gecko: GeckoTerminalProvider
    llama: DefiLlamaProvider
    bridges: BridgeProvider
    jupiter: JupiterProvider
    okx: OKXMarketProvider
    rpc: RPCProvider
    llm: FreeLLMClient

    @classmethod
    def create(cls, settings: Settings) -> "Runtime":
        storage = Storage(settings)
        hub = HubClient(settings)
        return cls(
            settings=settings,
            storage=storage,
            hub=hub,
            arkham=ArkhamProvider(hub, storage),
            debank=DeBankProvider(hub, storage),
            oklink=OKLinkProvider(hub, storage),
            gecko=GeckoTerminalProvider(hub, storage),
            llama=DefiLlamaProvider(hub, storage),
            bridges=BridgeProvider(hub, storage),
            jupiter=JupiterProvider(hub, storage),
            okx=OKXMarketProvider(hub, storage),
            rpc=RPCProvider(hub),
            llm=FreeLLMClient(settings),
        )

    async def aclose(self) -> None:
        await self.llm.aclose()
        await self.hub.aclose()
