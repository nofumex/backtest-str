from __future__ import annotations

import asyncio
import json
import random
import os
import time
from contextvars import ContextVar

request_context = ContextVar("request_context", default=(None, None))
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
        self.observer = None
        self._limits = {}
        self._cooldowns = {}
        self._rate_locks = {}
        self._next_start = {}
        self._adaptive_rps = {}
        self.metrics = {"requests": 0, "success": 0, "cache_hit": 0, "retry": 0, "failed": 0}

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

        return await self._execute(endpoint_key, spec.provider, spec.method, path, query, body, allow_upstream_error)

    async def _execute(self, endpoint, provider, method, path, query=None, body=None, allow_error=False):
        limit = self._limits.setdefault(provider, asyncio.Semaphore(max(1, self.settings.concurrency)))
        attempts = max(1, min(8, self.settings.http_retries))
        for attempt in range(1, attempts + 1):
            retryable, status, response = True, None, None
            try:
                async with limit:
                    lock = self._rate_locks.setdefault(provider, asyncio.Lock())
                    async with lock:
                        await asyncio.sleep(max(0, max(self._cooldowns.get(provider,0),self._next_start.get(provider,0))-time.time()))
                        env_key = "SMARTWALLET_PROVIDER_RPS_" + provider.upper().replace(".", "_").replace("-", "_")
                        configured = max(.1, float(os.getenv(env_key, os.getenv("SMARTWALLET_PROVIDER_RPS", str(min(8, self.settings.concurrency))))))
                        rps = self._adaptive_rps.setdefault(provider, configured)
                        self._next_start[provider] = time.time()+1/rps
                    self.metrics["requests"] += 1
                    response = await self._client.request(method, path, params=query, json=body if method != "GET" else None)
                status = response.status_code
                if status < 400:
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise ValueError("invalid envelope")
                    status = int(payload.get("status", status))
                if status >= 400:
                    retryable = status in (408, 425, 429) or status >= 500
                    raise HubError(f"HTTP/upstream {status}")
                if endpoint.startswith("rpc.") and payload.get("error"):
                    code = payload["error"].get("code") if isinstance(payload["error"],dict) else None
                    retryable = code in (-32005,-32002)
                    raise HubError("RPC error")
                self.metrics["success"] += 1
                ceiling = max(.1, float(os.getenv("SMARTWALLET_PROVIDER_RPS_MAX", str(max(8, self.settings.concurrency)))))
                self._adaptive_rps[provider] = min(ceiling, self._adaptive_rps.get(provider, 1.0) + 0.05)
                self._record(endpoint, provider, attempt, "success", None, None)
                return HubResponse(endpoint, status, payload.get("result") if endpoint.startswith("rpc.") else payload.get("data"), payload)
            except (httpx.TransportError, ValueError, HubError) as exc:
                # Never persist provider response bodies, URLs or credentials.
                reason = f"status={status}" if status and status >= 400 else type(exc).__name__
                exhausted = not retryable or attempt == attempts
                delay = min(30.0, self._retry_after(response, attempt - 1) if response is not None else 2 ** (attempt - 1)) + random.uniform(.1, 1)
                next_retry = None if exhausted else time.time() + delay
                self._record(endpoint, provider, attempt, "failed" if exhausted else "retry", reason, next_retry)
                if exhausted:
                    self.metrics["failed"] += 1
                    raise HubError(f"{provider}/{endpoint}: {reason}; exhausted after {attempt} attempt(s)") from None
                self.metrics["retry"] += 1
                if status == 429:
                    self._cooldowns[provider] = next_retry
                    self._adaptive_rps[provider] = max(.1, self._adaptive_rps.get(provider, 1.0) * 0.5)
                await asyncio.sleep(delay)

    def _record(self, endpoint, provider, attempt, state, error, next_retry):
        if self.observer:
            self.observer(endpoint, provider, attempt, state, error, next_retry, request_context.get())

    async def rpc(self, chain: str, method: str, params: list[Any] | dict[str, Any], request_id: int = 1) -> Any:
        payload = {"jsonrpc":"2.0","id":request_id,"method":method,"params":params}
        response = await self._execute(f"rpc.{chain}.{method}", f"rpc.{chain}", "POST", f"/rpc/{quote(chain,safe='_-')}", body=payload)
        return response.data
