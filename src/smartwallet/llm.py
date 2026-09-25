from __future__ import annotations

import asyncio
import json
import random
import re
from typing import Any

import httpx

from .settings import Settings


INTENT_LABELS = [
    "sell", "buy", "inventory_rebalance", "collateral", "lending", "otc_settlement",
    "treasury_transfer", "liquidity_management", "bridge_reposition", "distribution", "staking", "unknown"
]
ROLE_LABELS = ["execution_mm", "treasury", "settlement", "lp_liquidity", "lending_collateral", "bridge_routing", "mixed", "unknown"]


def _extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("LLM response did not contain a JSON object")
    value = json.loads(m.group(0))
    if not isinstance(value, dict):
        raise ValueError("LLM JSON must be an object")
    return value


def _normalize_probs(value: Any, labels: list[str]) -> dict[str, float]:
    src = value if isinstance(value, dict) else {}
    out = {k: max(0.0, float(src.get(k, 0.0) or 0.0)) for k in labels}
    total = sum(out.values())
    if total <= 0:
        return {k: (1.0 if k == "unknown" else 0.0) for k in labels}
    return {k: v / total for k, v in out.items()}


class FreeLLMClient:
    """OpenAI-compatible /v1/chat/completions client using the configured FREE_LLM service."""

    def __init__(self, settings: Settings, transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None):
        self.settings = settings
        timeout = httpx.Timeout(
            connect=min(10.0, settings.http_timeout),
            read=settings.http_timeout,
            write=min(30.0, settings.http_timeout),
            pool=min(10.0, settings.http_timeout),
        )
        self.client = httpx.AsyncClient(timeout=timeout, transport=transport, follow_redirects=True)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def chat_json(self, system: str, user_payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.llm_api_key:
            raise RuntimeError("FREE_LLM_API is required for LLM classification")

        url = self.settings.llm_base_url.rstrip("/") + "/chat/completions"
        body = {
            "model": self.settings.llm_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, separators=(",", ":"))},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.settings.llm_api_key}",
            "Content-Type": "application/json",
        }

        last_exc: Exception | None = None
        for attempt in range(self.settings.http_retries):
            try:
                resp = await self.client.post(url, headers=headers, json=body)
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    if attempt + 1 < self.settings.http_retries:
                        await asyncio.sleep(min(15.0, (2 ** attempt) + random.random()))
                        continue
                resp.raise_for_status()
                payload = resp.json()
                content = payload["choices"][0]["message"]["content"]
                return _extract_json(content)
            except (httpx.TimeoutException, httpx.TransportError, json.JSONDecodeError, KeyError, ValueError) as exc:
                last_exc = exc
                if attempt + 1 >= self.settings.http_retries:
                    break
                await asyncio.sleep(min(15.0, (2 ** attempt) + random.random()))
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                # Non-transient 4xx errors should fail immediately; 429 is handled above.
                if 400 <= exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise
                if attempt + 1 >= self.settings.http_retries:
                    break
                await asyncio.sleep(min(15.0, (2 ** attempt) + random.random()))

        raise RuntimeError(
            f"FREE_LLM request failed after {self.settings.http_retries} attempts: {type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    async def classify_episode(self, episode_payload: dict[str, Any]) -> dict[str, Any]:
        system = (
            "You classify economic intent from PRE-EVENT on-chain evidence only. Do not use future price information. "
            "Distinguish factual actions from inferred intent. Return JSON only with keys economic_action, "
            "intent_probabilities, confidence, evidence. intent_probabilities must contain exactly these labels: "
            + ", ".join(INTENT_LABELS)
            + ". Probabilities must sum to 1. Use unknown when evidence is insufficient."
        )
        out = await self.chat_json(system, episode_payload)
        probs = _normalize_probs(out.get("intent_probabilities"), INTENT_LABELS)
        label = max(probs, key=probs.get)
        confidence = min(1.0, max(0.0, float(out.get("confidence", probs[label]) or 0.0)))
        return {
            "economic_action": str(out.get("economic_action") or label),
            "intent_probabilities": probs,
            "confidence": confidence,
            "evidence": out.get("evidence") if isinstance(out.get("evidence"), list) else [],
            "label": label,
        }

    async def classify_wallet_role(self, wallet_payload: dict[str, Any]) -> dict[str, Any]:
        system = (
            "Classify the functional role of a smart wallet from its on-chain activity summary. Return JSON only with "
            "keys role_probabilities, confidence, evidence. role_probabilities must contain exactly: "
            + ", ".join(ROLE_LABELS)
            + ". Probabilities must sum to 1."
        )
        out = await self.chat_json(system, wallet_payload)
        probs = _normalize_probs(out.get("role_probabilities"), ROLE_LABELS)
        role = max(probs, key=probs.get)
        confidence = min(1.0, max(0.0, float(out.get("confidence", probs[role]) or 0.0)))
        return {"role": role, "role_probabilities": probs, "confidence": confidence, "evidence": out.get("evidence") or []}
