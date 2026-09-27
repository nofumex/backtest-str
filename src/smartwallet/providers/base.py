from __future__ import annotations

from typing import Any
import asyncio
import json
from datetime import datetime, timezone
from ..storage import canonical_json

from ..contracts import contract
from ..http import HubClient, HubResponse
from ..storage import Storage


class BaseProvider:
    def __init__(self, hub: HubClient, storage: Storage):
        self.hub = hub
        self.storage = storage

    async def call(self, endpoint_key: str, *, path_params: dict[str, Any] | None = None, query: dict[str, Any] | None = None, body: Any | None = None, allow_upstream_error: bool = False) -> HubResponse:
        spec = contract(endpoint_key)
        request = {"path_params":path_params or {},"query":query or {},"body":body}
        cached = self.storage.fetchone("SELECT response_json,fetched_at FROM raw_responses WHERE endpoint_key=? AND request_json=? ORDER BY fetched_at DESC LIMIT 1", (endpoint_key,canonical_json(request)))
        if cached and spec.method == "GET":
            age = (datetime.now(timezone.utc)-datetime.fromisoformat(cached["fetched_at"])).total_seconds()
            historical = endpoint_key == "debank.history" and int((query or {}).get("start_time") or 0) > 0
            if historical or age < 3600:
                payload = json.loads(cached["response_json"])
                if isinstance(payload,dict) and int(payload.get("status",200)) < 400:
                    self.hub.metrics["cache_hit"] += 1
                    self.hub._record(endpoint_key,spec.provider,0,"cache",None,None)
                    return HubResponse(endpoint_key,int(payload.get("status",200)),payload.get("data"),payload)
        response = await self.hub.request(
            endpoint_key,
            path_params=path_params,
            query=query,
            body=body,
            allow_upstream_error=allow_upstream_error,
        )
        spec = contract(endpoint_key)
        self.storage.archive_raw_queued(
            spec.provider, endpoint_key,
            {"path_params": path_params or {}, "query": query or {}, "body": body}, response.raw,
        )
        return response
