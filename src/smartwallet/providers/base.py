from __future__ import annotations

from typing import Any
import asyncio

from ..contracts import contract
from ..http import HubClient, HubResponse
from ..storage import Storage


class BaseProvider:
    def __init__(self, hub: HubClient, storage: Storage):
        self.hub = hub
        self.storage = storage

    async def call(self, endpoint_key: str, *, path_params: dict[str, Any] | None = None, query: dict[str, Any] | None = None, body: Any | None = None, allow_upstream_error: bool = False) -> HubResponse:
        response = await self.hub.request(
            endpoint_key,
            path_params=path_params,
            query=query,
            body=body,
            allow_upstream_error=allow_upstream_error,
        )
        spec = contract(endpoint_key)
        await asyncio.to_thread(
            self.storage.archive_raw, spec.provider, endpoint_key,
            {"path_params": path_params or {}, "query": query or {}, "body": body}, response.raw,
        )
        return response
