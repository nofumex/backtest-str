from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Mapping
from urllib.parse import quote

import httpx

from .contracts import contract
from .settings import Settings


class HubError(RuntimeError):
    pass


@dataclass
class HubResponse:
    endpoint_key: str
    status: int
    data: Any
    raw: dict[str, Any]


class HubClient:
    """Strict API Hub client.

    The client can only execute routes declared in contracts.py. Query parameters not
    present in the corresponding Hub documentation are rejected before a request is sent.
    """

    def __init__(self, settings: Settings, transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        self._client = httpx.AsyncClient(
            base_url=settings.hub_base_url,
            headers={"X-Hub-Key": settings.api_hub_key, "Accept": "application/json"},
            timeout=httpx.Timeout(settings.http_timeout),
            transport=transport,
            follow_redirects=True,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _render_path(template: str, path_params: Mapping[str, Any]) -> str:
        out = template
        for key, value in path_params.items():
            out = out.replace("{" + key + "}", quote(str(value), safe=":,._-"))
        if "{" in out or "}" in out:
            raise ValueError(f"Missing path parameter for {template}: got {sorted(path_params)}")
        return out

    @staticmethod
    def _retry_after(resp: httpx.Response, attempt: int) -> float:
        raw = resp.headers.get("Retry-After")
        if raw:
            try:
                return max(0.0, float(raw))
            except ValueError:
                try:
                    dt = parsedate_to_datetime(raw)
                    return max(0.0, dt.timestamp() - time.time())
                except Exception:
                    pass
        return min(15.0, (2 ** attempt) + random.random())

    async def request(
        self,
        endpoint_key: str,
        *,
        path_params: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        body: Any | None = None,
        allow_upstream_error: bool = False,
    ) -> HubResponse:
        spec = contract(endpoint_key)
        path_params = dict(path_params or {})
        query = {k: v for k, v in dict(query or {}).items() if v is not None}
        unknown = set(query) - set(spec.query_params)
        if unknown:
            raise ValueError(
                f"{endpoint_key}: undocumented query parameter(s): {sorted(unknown)}; "
                f"allowed={sorted(spec.query_params)}"
            )
        path = self._render_path(spec.run_path, path_params)

        last_exc: Exception | None = None
        for attempt in range(self.settings.http_retries):
            try:
                resp = await self._client.request(spec.method, path, params=query, json=body if spec.method != "GET" else None)
                if resp.status_code == 429:
                    await asyncio.sleep(self._retry_after(resp, attempt))
                    continue
                if 500 <= resp.status_code < 600:
                    await asyncio.sleep(self._retry_after(resp, attempt))
                    continue
                resp.raise_for_status()
                payload = resp.json()
                if not isinstance(payload, dict):
                    raise HubError(f"{endpoint_key}: Hub envelope must be an object, got {type(payload).__name__}")
                hub_status = int(payload.get("status", resp.status_code))
                if hub_status >= 400 and not allow_upstream_error:
                    raise HubError(f"{endpoint_key}: upstream status={hub_status}, error={payload.get('error')!r}")
                return HubResponse(endpoint_key, hub_status, payload.get("data"), payload)
            except (httpx.TimeoutException, httpx.TransportError, json.JSONDecodeError, HubError) as exc:
                last_exc = exc
                # HubError for non-transient 4xx should not be retried.
                if isinstance(exc, HubError) and "upstream status=4" in str(exc):
                    raise
                if attempt + 1 >= self.settings.http_retries:
                    break
                await asyncio.sleep(min(15.0, (2 ** attempt) + random.random()))
        raise HubError(f"{endpoint_key}: request failed after {self.settings.http_retries} attempts: {last_exc}")

    async def rpc(self, chain: str, method: str, params: list[Any] | dict[str, Any], request_id: int = 1) -> Any:
        # /docs/rpc documents POST /rpc/{chain} with standard JSON-RPC 2.0 and one request per HTTP call.
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        resp = await self._client.post(f"/rpc/{quote(chain, safe='_-')}", json=payload)
        if resp.status_code == 429:
            await asyncio.sleep(self._retry_after(resp, 0))
            resp = await self._client.post(f"/rpc/{quote(chain, safe='_-')}", json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise HubError(f"rpc/{chain} {method}: {data['error']}")
        return data.get("result")
