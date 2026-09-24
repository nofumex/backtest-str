#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Entity Alpha Engine
===================
Single-file research system for:
- API Hub OpenAPI discovery
- Arkham entity transfer collection
- historical/context enrichment
- dual-LLM semantic classification (grok-4.6 + grok-4.5)
- GeckoTerminal OHLCV through API Hub
- event studies, OOS/walk-forward backtests, FDR correction
- Streamlit dashboard

The code intentionally keeps all business logic in this one file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import duckdb
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from scipy import stats
from statsmodels.stats.multitest import multipletests


APP_NAME = "Entity Alpha Engine"
HERE = Path(__file__).resolve().parent
DEFAULT_CONFIG = HERE / "config.json"
OPENAPI_URL = "https://hub.arbitron.dev/openapi.json"
HUB_BASE = "https://hub.arbitron.dev"
DEFAULT_LLM_BASE = "https://llmass.arbitron.dev/v1"
UTC = timezone.utc

EVENT_LABELS = [
    "CEX_DEPOSIT",
    "CEX_WITHDRAWAL",
    "DEX_SELL_LIKE",
    "DEX_BUY_LIKE",
    "LP_ADD",
    "LP_REMOVE",
    "BRIDGE_OUT",
    "BRIDGE_IN",
    "BORROW",
    "REPAY",
    "COLLATERAL_DEPOSIT",
    "COLLATERAL_WITHDRAWAL",
    "STAKE",
    "UNSTAKE",
    "OTC_SETTLEMENT",
    "INTERNAL_TRANSFER",
    "MARKET_MAKER_REBALANCE",
    "PROTOCOL_MIGRATION",
    "DISTRIBUTION",
    "ACCUMULATION",
    "UNKNOWN",
]

CEX_HINTS = {
    "binance", "coinbase", "okx", "bybit", "kraken", "kucoin", "gate.io", "gate",
    "bitfinex", "bitstamp", "mexc", "upbit", "bithumb", "htx", "huobi", "crypto.com",
}

CHAIN_TO_GECKO = {
    "ethereum": "eth",
    "eth": "eth",
    "arbitrum_one": "arbitrum",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "bsc": "bsc",
    "binance_smart_chain": "bsc",
    "polygon": "polygon_pos",
    "polygon_pos": "polygon_pos",
    "avalanche": "avax",
    "avax": "avax",
    "solana": "solana",
    "tron": "tron",
}

CHAIN_TO_DEBANK = {
    "ethereum": "eth", "eth": "eth", "arbitrum_one": "arb", "arbitrum": "arb",
    "base": "base", "optimism": "op", "bsc": "bsc", "polygon": "matic",
    "polygon_pos": "matic", "avalanche": "avax", "avax": "avax",
}

CHAIN_TO_EVM_ID = {
    "ethereum": 1, "eth": 1, "bsc": 56, "polygon": 137, "polygon_pos": 137,
    "arbitrum_one": 42161, "arbitrum": 42161, "optimism": 10, "base": 8453,
    "avalanche": 43114, "avax": 43114,
}

BENCHMARK_QUERY = {
    "eth": "WETH USDC",
    "arbitrum": "WETH USDC",
    "base": "WETH USDC",
    "optimism": "WETH USDC",
    "bsc": "WBNB USDT",
    "polygon_pos": "WMATIC USDC",
    "avax": "WAVAX USDC",
    "solana": "SOL USDC",
    "tron": "TRX USDT",
}

SIZE_BUCKETS = [0, 250_000, 1_000_000, 5_000_000, 20_000_000, 100_000_000, float("inf")]
SIZE_LABELS = ["<250k", "250k-1m", "1m-5m", "5m-20m", "20m-100m", "100m+"]
PORTFOLIO_BUCKETS = [-float("inf"), 0, 0.5, 1, 3, 10, 25, 100, float("inf")]
PORTFOLIO_LABELS = ["unknown/0", "0-0.5%", "0.5-1%", "1-3%", "3-10%", "10-25%", "25-100%", "100%+"]
IMPACT_BUCKETS = [0, 0.001, 0.005, 0.01, 0.025, 0.05, 0.10, 0.25, 0.50, 1.0, float("inf")]
IMPACT_LABELS = ["<0.1%", "0.1-0.5%", "0.5-1%", "1-2.5%", "2.5-5%", "5-10%", "10-25%", "25-50%", "50-100%", "100%+"]


# ----------------------------- helpers -------------------------------------

def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def iso_to_ts(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 10_000_000_000:  # ms
            x /= 1000.0
        return int(x)
    s = str(v).strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d+(\.\d+)?", s):
            return iso_to_ts(float(s))
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), UTC).isoformat()


def canonical(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("$", "").strip()
        return float(v)
    except Exception:
        return None


def stable_id(*parts: Any) -> str:
    text = "|".join(str(x or "") for x in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def json_dumps(x: Any) -> str:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":"), default=str)


def json_loads(s: Any, default=None):
    if s is None:
        return default
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return default


def deep_get_alias(obj: Any, aliases: Sequence[str]) -> Any:
    """Breadth-first lookup by normalized key, including nested dicts."""
    wanted = {canonical(a) for a in aliases}
    q = [obj]
    seen = 0
    while q and seen < 5000:
        cur = q.pop(0)
        seen += 1
        if isinstance(cur, dict):
            for k, v in cur.items():
                if canonical(k) in wanted and v not in (None, "", [], {}):
                    return v
                if isinstance(v, (dict, list)):
                    q.append(v)
        elif isinstance(cur, list):
            q.extend(cur[:200])
    return None


def get_nested(obj: Dict[str, Any], path: Sequence[str]) -> Any:
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def extract_name(address_obj: Any) -> Optional[str]:
    if isinstance(address_obj, str):
        return None
    if not isinstance(address_obj, dict):
        return None
    candidates = [
        get_nested(address_obj, ["arkhamEntity", "name"]),
        get_nested(address_obj, ["entity", "name"]),
        address_obj.get("label"),
        address_obj.get("name"),
        get_nested(address_obj, ["arkhamLabel", "name"]),
    ]
    return next((str(x) for x in candidates if x), None)


def extract_address(address_obj: Any) -> Optional[str]:
    if isinstance(address_obj, str):
        return address_obj
    if isinstance(address_obj, dict):
        for k in ("address", "id", "hash"):
            if address_obj.get(k):
                return str(address_obj[k])
    return None


def first_list(obj: Any, likely_keys=("transfers", "data", "items", "results", "rows")) -> List[Any]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in likely_keys:
            v = obj.get(k)
            if isinstance(v, list):
                return v
        # common nested envelope
        for v in obj.values():
            if isinstance(v, dict):
                x = first_list(v, likely_keys)
                if x:
                    return x
    return []


def nearest_value(points: pd.DataFrame, target_ts: int, max_gap_s: int) -> Optional[float]:
    if points.empty:
        return None
    arr = points["ts"].to_numpy(dtype=np.int64)
    i = int(np.searchsorted(arr, target_ts))
    cand = []
    if i < len(arr): cand.append(i)
    if i > 0: cand.append(i - 1)
    if not cand:
        return None
    j = min(cand, key=lambda z: abs(int(arr[z]) - target_ts))
    if abs(int(arr[j]) - target_ts) > max_gap_s:
        return None
    return safe_float(points.iloc[j]["close"])

def value_at_or_before(points: pd.DataFrame, target_ts: int, max_gap_s: int) -> Optional[float]:
    if points.empty:
        return None
    p = points.sort_values("ts")
    x = p[p.ts <= target_ts]
    if x.empty:
        return None
    r = x.iloc[-1]
    if target_ts - int(r.ts) > max_gap_s:
        return None
    return safe_float(r.close)


def value_at_or_after(points: pd.DataFrame, target_ts: int, max_gap_s: int) -> Optional[float]:
    if points.empty:
        return None
    p = points.sort_values("ts")
    x = p[p.ts >= target_ts]
    if x.empty:
        return None
    r = x.iloc[0]
    if int(r.ts) - target_ts > max_gap_s:
        return None
    return safe_float(r.close)


def bh_qvalues(pvalues: Sequence[float]) -> List[float]:
    if not pvalues:
        return []
    arr = np.array([1.0 if (x is None or not np.isfinite(x)) else float(x) for x in pvalues])
    _, q, _, _ = multipletests(arr, method="fdr_bh")
    return q.tolist()


def bootstrap_ci(values: np.ndarray, n=1500, alpha=0.05, seed=42) -> Tuple[Optional[float], Optional[float]]:
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if len(values) < 2:
        return None, None
    rng = np.random.default_rng(seed)
    means = np.empty(n)
    for i in range(n):
        means[i] = np.mean(rng.choice(values, size=len(values), replace=True))
    return float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2))


def wilson_ci(successes: int, total: int, z: float = 1.959963984540054) -> Tuple[Optional[float], Optional[float]]:
    """Wilson interval for a binomial hit rate."""
    if total <= 0:
        return None, None
    p = successes / total
    denom = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


def permutation_p(values: np.ndarray, n=2000, seed=42) -> float:
    """Two-sided sign permutation test against zero mean."""
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]
    if len(values) < 3:
        return 1.0
    obs = abs(float(np.mean(values)))
    rng = np.random.default_rng(seed)
    ge = 0
    for _ in range(n):
        signs = rng.choice([-1.0, 1.0], size=len(values))
        if abs(float(np.mean(values * signs))) >= obs:
            ge += 1
    return (ge + 1) / (n + 1)


def profit_factor(returns: Sequence[float]) -> Optional[float]:
    a = np.asarray(list(returns), dtype=float)
    a = a[np.isfinite(a)]
    if len(a) == 0:
        return None
    pos = float(a[a > 0].sum())
    neg = float(-a[a < 0].sum())
    if neg == 0:
        return float("inf") if pos > 0 else None
    return pos / neg


def max_drawdown(equity: Sequence[float]) -> Optional[float]:
    x = np.asarray(list(equity), dtype=float)
    if len(x) == 0:
        return None
    peak = np.maximum.accumulate(x)
    dd = x / np.where(peak == 0, np.nan, peak) - 1
    return float(np.nanmin(dd))


# ----------------------------- configuration -------------------------------

def load_config(path: Path = DEFAULT_CONFIG) -> Dict[str, Any]:
    if not path.exists():
        example = HERE / "config.example.json"
        if example.exists():
            path.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            raise FileNotFoundError(path)
    cfg = json.loads(path.read_text(encoding="utf-8"))
    cfg.setdefault("database", "entity_alpha.duckdb")
    cfg.setdefault("horizons_minutes", [5, 15, 60, 240, 1440, 4320])
    cfg.setdefault("pre_windows_minutes", [60, 1440])
    cfg.setdefault("fee_bps", 5.0)
    cfg.setdefault("slippage_bps", 8.0)
    cfg.setdefault("train_frac", 0.60)
    cfg.setdefault("valid_frac", 0.20)
    cfg.setdefault("random_seed", 42)
    cfg.setdefault("execution_delay_seconds", 120)
    cfg.setdefault("validation_fdr", 0.20)
    cfg.setdefault("backtest_return_basis", "excess")  # excess = token leg hedged by benchmark
    cfg.setdefault("signal_cooldown_minutes", 30)
    cfg.setdefault("llm_max_sequence_records", 120)
    return cfg


# ----------------------------- database ------------------------------------
class DB:
    def __init__(self, path: str):
        p = Path(path)
        if not p.is_absolute():
            p = HERE / p
        self.path = p
        self.con = duckdb.connect(str(p))
        self.init_schema()

    def init_schema(self):
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS route_cache(
            job VARCHAR PRIMARY KEY, path VARCHAR, method VARCHAR, score DOUBLE,
            operation_json VARCHAR, updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS raw_http(
            request_id VARCHAR PRIMARY KEY, source VARCHAR, path VARCHAR,
            params_json VARCHAR, fetched_at TIMESTAMP, status INTEGER, payload_json VARCHAR
        );
        CREATE TABLE IF NOT EXISTS transfers(
            event_id VARCHAR PRIMARY KEY,
            entity VARCHAR, ts BIGINT, chain VARCHAR, token_symbol VARCHAR, token_address VARCHAR,
            amount DOUBLE, usd_value DOUBLE,
            from_address VARCHAR, from_label VARCHAR, to_address VARCHAR, to_label VARCHAR,
            tx_hash VARCHAR, direction VARCHAR, raw_json VARCHAR, collected_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS contexts(
            event_id VARCHAR PRIMARY KEY,
            portfolio_usd DOUBLE, token_portfolio_usd DOUBLE, portfolio_pct DOUBLE,
            history_json VARCHAR, sequence_json VARCHAR, context_json VARCHAR, updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS classifications(
            event_id VARCHAR PRIMARY KEY,
            primary_json VARCHAR, auditor_json VARCHAR, final_json VARCHAR,
            event_class VARCHAR, confidence DOUBLE, rationale VARCHAR,
            tags_json VARCHAR, created_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS pool_cache(
            chain VARCHAR, token_key VARCHAR, pool_address VARCHAR, pool_name VARCHAR,
            reserve_usd DOUBLE, raw_json VARCHAR, updated_at TIMESTAMP,
            PRIMARY KEY(chain, token_key)
        );
        CREATE TABLE IF NOT EXISTS prices(
            chain VARCHAR, token_key VARCHAR, pool_address VARCHAR,
            ts BIGINT, close DOUBLE, volume DOUBLE, timeframe VARCHAR, source VARCHAR,
            PRIMARY KEY(chain, token_key, ts, timeframe)
        );
        CREATE TABLE IF NOT EXISTS event_features(
            event_id VARCHAR PRIMARY KEY,
            size_bucket VARCHAR, portfolio_bucket VARCHAR, liquidity_bucket VARCHAR, volume_bucket VARCHAR,
            liquidity_ratio DOUBLE, volume_ratio DOUBLE,
            pre_1h DOUBLE, pre_24h DOUBLE, vol_24h DOUBLE, volume_z DOUBLE,
            benchmark_pre_24h DOUBLE, market_regime VARCHAR, flow_state VARCHAR,
            feature_json VARCHAR, updated_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS event_returns(
            event_id VARCHAR, horizon_min INTEGER,
            raw_return DOUBLE, benchmark_return DOUBLE, excess_return DOUBLE,
            entry_price DOUBLE, exit_price DOUBLE, benchmark_entry DOUBLE, benchmark_exit DOUBLE,
            PRIMARY KEY(event_id, horizon_min)
        );
        CREATE TABLE IF NOT EXISTS pattern_results(
            run_id VARCHAR, pattern_id VARCHAR,
            entity VARCHAR, event_class VARCHAR, token_symbol VARCHAR, chain VARCHAR, counterparty VARCHAR,
            sequence_pattern VARCHAR, flow_state VARCHAR, size_bucket VARCHAR, portfolio_bucket VARCHAR, liquidity_bucket VARCHAR, volume_bucket VARCHAR, market_regime VARCHAR,
            horizon_min INTEGER, side INTEGER,
            train_n INTEGER, valid_n INTEGER, test_n INTEGER,
            train_mean DOUBLE, valid_mean DOUBLE, test_mean DOUBLE,
            test_median DOUBLE, test_hit DOUBLE, test_std DOUBLE,
            test_hit_ci_low DOUBLE, test_hit_ci_high DOUBLE,
            valid_perm_p DOUBLE, valid_q DOUBLE, selected BOOLEAN,
            ci_low DOUBLE, ci_high DOUBLE, t_p DOUBLE, perm_p DOUBLE, q_value DOUBLE,
            profit_factor DOUBLE, max_dd DOUBLE, total_return DOUBLE,
            wf_positive_folds INTEGER, wf_folds INTEGER, robustness DOUBLE,
            created_at TIMESTAMP
        );
        """)
        # Lightweight migrations for older local databases.
        for ddl in (
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS chain VARCHAR",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS counterparty VARCHAR",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS sequence_pattern VARCHAR",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS flow_state VARCHAR",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS valid_perm_p DOUBLE",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS valid_q DOUBLE",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS selected BOOLEAN",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS test_hit_ci_low DOUBLE",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS test_hit_ci_high DOUBLE",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS liquidity_bucket VARCHAR",
            "ALTER TABLE pattern_results ADD COLUMN IF NOT EXISTS volume_bucket VARCHAR",
            "ALTER TABLE event_features ADD COLUMN IF NOT EXISTS liquidity_bucket VARCHAR",
            "ALTER TABLE event_features ADD COLUMN IF NOT EXISTS volume_bucket VARCHAR",
            "ALTER TABLE event_features ADD COLUMN IF NOT EXISTS liquidity_ratio DOUBLE",
            "ALTER TABLE event_features ADD COLUMN IF NOT EXISTS volume_ratio DOUBLE",
        ):
            try:
                self.con.execute(ddl)
            except Exception:
                pass

    def df(self, sql: str, params: Sequence[Any] = ()) -> pd.DataFrame:
        return self.con.execute(sql, params).df()

    def one(self, sql: str, params: Sequence[Any] = ()):
        return self.con.execute(sql, params).fetchone()

    def execute(self, sql: str, params: Sequence[Any] = ()):
        return self.con.execute(sql, params)

    def upsert_raw(self, source, path, params, status, payload):
        rid = stable_id(source, path, json_dumps(params))
        self.con.execute("""
            INSERT OR REPLACE INTO raw_http VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [rid, source, path, json_dumps(params), now_iso(), status, json_dumps(payload)])



# ----------------------------- Stable Hub route map ------------------------
# Core research routes are intentionally pinned to documented API Hub paths.
# We do NOT use keyword similarity for these jobs: OpenAPI is used only to
# verify that the expected route exists and to load its parameter schema.
PINNED_ROUTES: Dict[str, List[Tuple[str, str]]] = {
    "arkham_transfers": [("GET", "/run/arkham/transfers")],
    "arkham_transfers_unenriched": [("GET", "/run/arkham/transfers/unenriched")],
    "arkham_history_entity": [("GET", "/run/arkham/history/entity/{entity_id}")],
    "arkham_portfolio_entity": [("GET", "/run/arkham/portfolio/timeSeries/entity/{entity_id}")],
    "arkham_tx_detail": [("GET", "/run/arkham/tx/{hash}"), ("GET", "/run/arkham/transfers/tx/{tx_hash}")],
    "gecko_token_pools": [("GET", "/run/geckoterminal/official/networks/{network}/tokens/{token_address}/pools")],
    "gecko_search_pools": [("GET", "/run/geckoterminal/official/search/pools")],
    "gecko_ohlcv": [("GET", "/run/geckoterminal/official/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}")],
    "debank_all_history": [("GET", "/run/debank/history/list")],
}

# ----------------------------- Hub OpenAPI client --------------------------
@dataclass
class Operation:
    path: str
    method: str
    summary: str
    description: str
    tags: List[str]
    params: List[Dict[str, Any]]
    raw: Dict[str, Any]

    @property
    def text(self) -> str:
        return " ".join([self.path, self.summary, self.description, " ".join(self.tags)]).lower()


class HubClient:
    def __init__(self, db: DB, key: Optional[str]):
        self.db = db
        self.key = key
        self.s = requests.Session()
        self.spec_cache: Optional[Dict[str, Any]] = None

    @property
    def headers(self):
        h = {"Accept": "application/json", "User-Agent": "entity-alpha-engine/1.0"}
        if self.key:
            h["X-Hub-Key"] = self.key
        return h

    def get_openapi(self) -> Dict[str, Any]:
        if self.spec_cache is not None:
            return self.spec_cache
        cache = HERE / ".openapi_cache.json"
        try:
            r = self.s.get(OPENAPI_URL, timeout=30)
            r.raise_for_status()
            self.spec_cache = r.json()
            cache.write_text(json.dumps(self.spec_cache), encoding="utf-8")
        except Exception:
            if cache.exists():
                self.spec_cache = json.loads(cache.read_text(encoding="utf-8"))
            else:
                raise
        return self.spec_cache

    def operations(self) -> List[Operation]:
        spec = self.get_openapi()
        out: List[Operation] = []
        for path, path_obj in spec.get("paths", {}).items():
            common = path_obj.get("parameters", []) if isinstance(path_obj, dict) else []
            for method in ("get", "post"):
                op = path_obj.get(method) if isinstance(path_obj, dict) else None
                if not isinstance(op, dict):
                    continue
                # only GET and read-only POST as hub exposes them
                params = list(common) + list(op.get("parameters", []))
                out.append(Operation(
                    path=path,
                    method=method.upper(),
                    summary=str(op.get("summary", "")),
                    description=str(op.get("description", "")),
                    tags=[str(x) for x in op.get("tags", [])],
                    params=params,
                    raw=op,
                ))
        return out

    def operation_exact(self, method: str, path: str) -> Operation:
        """Return one exact OpenAPI operation; never guess by keywords."""
        method = method.upper()
        for op in self.operations():
            if op.method == method and op.path == path:
                return op
        raise RuntimeError(f"Pinned API Hub route missing from OpenAPI: {method} {path}")

    def pinned(self, job: str, optional: bool = False) -> Optional[Operation]:
        """Resolve a stable job from PINNED_ROUTES, verifying it against live OpenAPI."""
        candidates = PINNED_ROUTES.get(job, [])
        errors = []
        for method, path in candidates:
            try:
                op = self.operation_exact(method, path)
                payload = {
                    "summary": op.summary, "description": op.description, "tags": op.tags,
                    "params": op.params, "raw": op.raw,
                }
                self.db.execute("INSERT OR REPLACE INTO route_cache VALUES (?, ?, ?, ?, ?, ?)",
                                [job, op.path, op.method, 9999.0, json_dumps(payload), now_iso()])
                return op
            except Exception as ex:
                errors.append(str(ex))
        if optional:
            return None
        if not candidates:
            raise RuntimeError(f"No pinned route configured for {job}")
        raise RuntimeError(f"Pinned route unavailable for {job}: {'; '.join(errors)}")

    def strict_rpc(self, optional: bool = True) -> Optional[Operation]:
        """Resolve unified RPC only from paths that are explicitly RPC routes.

        This deliberately rejects SDK/provider/status endpoints. It does not use
        semantic similarity. The exact RPC path can vary across Hub revisions, so
        we accept only an OpenAPI path whose path itself contains an RPC segment
        and whose schema can carry a JSON-RPC method/body.
        """
        candidates = []
        for op in self.operations():
            path_low = op.path.lower()
            segs = [x for x in path_low.split('/') if x]
            if 'rpc' not in segs and not any(seg.startswith('rpc') for seg in segs):
                continue
            if any(bad in path_low for bad in ('sdk-info', '/provider', '/status')):
                continue
            names = {canonical(x) for x in self.parameter_names(op)}
            body = op.raw.get('requestBody', {}) if isinstance(op.raw, dict) else {}
            has_body = bool(body)
            if not ({'method', 'params'} <= names or has_body):
                continue
            # Prefer /rpc... and /run/rpc... over provider-specific paths.
            score = 0
            if path_low.startswith('/rpc'): score += 20
            if path_low.startswith('/run/rpc'): score += 15
            if '{chain' in path_low or '{network' in path_low: score += 5
            candidates.append((score, len(op.path), op))
        if not candidates:
            if optional:
                return None
            raise RuntimeError('No explicit unified RPC route found in OpenAPI')
        candidates.sort(key=lambda x: (-x[0], x[1], x[2].path))
        op = candidates[0][2]
        payload = {"summary": op.summary, "description": op.description, "tags": op.tags,
                   "params": op.params, "raw": op.raw}
        self.db.execute("INSERT OR REPLACE INTO route_cache VALUES (?, ?, ?, ?, ?, ?)",
                        ["hub_rpc", op.path, op.method, 9999.0, json_dumps(payload), now_iso()])
        return op

    def discover(self, job: str, provider_terms: Sequence[str], route_terms: Sequence[str],
                 negative_terms: Sequence[str] = (), available_values: Optional[Dict[str, Any]] = None) -> Operation:
        """Discover the best documented Hub route for a job.

        When available_values is supplied, routes with path placeholders that cannot be
        satisfied from those values are rejected. This is important for families such as
        Arkham where both a transfer-list route and /transfers/tx/{tx_hash} exist: keyword
        scoring alone must never choose a detail route for a list/history job.
        """
        def compatible(op: Operation) -> bool:
            if not available_values:
                return True
            for ph in re.findall(r"\{([^}]+)\}", op.path):
                val = available_values.get(ph)
                if val is None:
                    val = semantic_lookup(ph, available_values)
                if val is None:
                    return False
            return True

        cached = self.db.one("SELECT path, method, operation_json FROM route_cache WHERE job=?", [job])
        if cached:
            raw = json_loads(cached[2], {})
            op = Operation(cached[0], cached[1], raw.get("summary", ""), raw.get("description", ""),
                           raw.get("tags", []), raw.get("params", []), raw.get("raw", {}))
            if compatible(op):
                return op
            # Old discovery cache may contain a semantically wrong route. Drop it and rediscover.
            self.db.execute("DELETE FROM route_cache WHERE job=?", [job])

        ops = self.operations()
        best = None
        best_score = -1e9
        for op in ops:
            if not compatible(op):
                continue
            txt = op.text
            score = 0.0
            for t in provider_terms:
                if t.lower() in txt:
                    score += 6
            for t in route_terms:
                if t.lower() in txt:
                    score += 3
            for t in negative_terms:
                if t.lower() in txt:
                    score -= 8
            # paths are strong evidence
            for t in provider_terms:
                if t.lower() in op.path.lower():
                    score += 8
            for t in route_terms:
                if t.lower() in op.path.lower():
                    score += 4
            # Prefer routes whose documented parameters match the values this job actually has.
            if available_values:
                pnames = {canonical(x) for x in self.parameter_names(op)}
                for k in available_values:
                    ck = canonical(k)
                    if ck in pnames:
                        score += 1.5
                # List/history jobs should prefer pagination/filter surfaces over object-detail routes.
                if any(x in pnames for x in ("limit", "offset", "page", "timegte", "timelte")):
                    score += 3
            if score > best_score:
                best_score, best = score, op
        if best is None or best_score <= 0:
            raise RuntimeError(f"Could not discover a compatible API Hub route for {job}")
        payload = {
            "summary": best.summary, "description": best.description, "tags": best.tags,
            "params": best.params, "raw": best.raw,
        }
        self.db.execute("INSERT OR REPLACE INTO route_cache VALUES (?, ?, ?, ?, ?, ?)",
                        [job, best.path, best.method, best_score, json_dumps(payload), now_iso()])
        return best

    def _resolve_ref(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(schema, dict):
            return {}
        ref = schema.get("$ref")
        if not ref:
            return schema
        cur = self.get_openapi()
        for part in ref.lstrip("#/").split("/"):
            cur = cur.get(part, {})
        return cur if isinstance(cur, dict) else {}

    def parameter_names(self, op: Operation) -> List[str]:
        out = []
        for p in op.params:
            if "$ref" in p:
                p = self._resolve_ref(p)
            if p.get("name"):
                out.append(str(p["name"]))
        # body properties for read-only POST
        body = op.raw.get("requestBody", {})
        content = body.get("content", {}) if isinstance(body, dict) else {}
        for c in content.values():
            schema = self._resolve_ref(c.get("schema", {})) if isinstance(c, dict) else {}
            for k in schema.get("properties", {}).keys():
                if k not in out: out.append(k)
        return out

    def call(self, op: Operation, values: Dict[str, Any], timeout=45) -> Any:
        if not self.key:
            raise RuntimeError("API_HUB_KEY is missing in .env")
        path = op.path
        query: Dict[str, Any] = {}
        body: Dict[str, Any] = dict(values.get("__body") or {})
        params_meta = {}
        for p in op.params:
            if "$ref" in p:
                p = self._resolve_ref(p)
            if p.get("name"):
                params_meta[p["name"]] = p

        # replace path placeholders using exact key first, then aliases passed in values
        for ph in re.findall(r"\{([^}]+)\}", path):
            val = values.get(ph)
            if val is None:
                val = semantic_lookup(ph, values)
            if val is None:
                raise ValueError(f"Missing path parameter {ph} for {op.path}; values={list(values)}")
            path = path.replace("{" + ph + "}", quote(str(val), safe=""))

        body_props = set()
        rb = op.raw.get("requestBody", {})
        if isinstance(rb, dict):
            for c in rb.get("content", {}).values():
                if isinstance(c, dict):
                    schema = self._resolve_ref(c.get("schema", {}))
                    body_props |= set(schema.get("properties", {}).keys())

        for name in self.parameter_names(op):
            val = values.get(name)
            if val is None:
                val = semantic_lookup(name, values)
            if val is None:
                continue
            if isinstance(val, list):
                # APIs in hub commonly accept comma-separated lists for upstream query arrays
                val = ",".join(map(str, val))
            if name in body_props and name not in params_meta:
                body[name] = val
            else:
                query[name] = val

        url = HUB_BASE + path
        for attempt in range(6):
            if op.method == "POST":
                r = self.s.post(url, headers=self.headers, params=query, json=body or None, timeout=timeout)
            else:
                r = self.s.get(url, headers=self.headers, params=query, timeout=timeout)
            try:
                payload = r.json()
            except Exception:
                payload = {"text": r.text[:5000]}
            self.db.upsert_raw("hub", path, {"query": query, "body": body}, r.status_code, payload)
            if r.status_code == 429:
                wait = safe_float(r.headers.get("Retry-After")) or min(2 ** attempt, 20)
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                upstream_status = payload.get("status") if isinstance(payload, dict) else None
                upstream_error = str(payload.get("error", "")).lower() if isinstance(payload, dict) else ""
                if upstream_status == 401 or "upstream 401" in upstream_error:
                    raise RuntimeError(
                        f"Hub HTTP {r.status_code} {path}: upstream provider returned 401; "
                        f"envelope={str(payload)[:1200]}"
                    )
                raise RuntimeError(
                    f"Hub {r.status_code} {path}: {str(payload)[:1200]} "
                    f"(HTTP {r.status_code}; check Hub key/permissions for 401/403, "
                    "request schema for 400/422, and upstream/provider availability for 502/503)"
                )
            return payload
        raise RuntimeError(f"Hub rate limit persisted for {path}")


def semantic_lookup(param_name: str, values: Dict[str, Any]) -> Any:
    p = canonical(param_name)
    aliases = {
        "entity": ["entity", "base", "id", "entityid"],
        "base": ["base", "entity"],
        "chains": ["chains", "chain", "network", "networks"],
        "chain": ["chain", "network"],
        "network": ["network", "chain"],
        "q": ["q", "query", "search"],
        "query": ["query", "q", "search"],
        "address": ["address", "tokenaddress", "token", "contractaddress"],
        "tokenaddress": ["tokenaddress", "address", "token"],
        "pooladdress": ["pooladdress", "pool_address", "pool"],
        "pool": ["pool", "pooladdress", "pool_address"],
        "timeframe": ["timeframe", "interval"],
        "aggregate": ["aggregate"],
        "beforetimestamp": ["beforetimestamp", "before_timestamp", "before"],
        "limit": ["limit", "pagesize", "count"],
        "offset": ["offset"],
        "page": ["page"],
        "flow": ["flow", "direction"],
        "timegte": ["timegte", "start", "fromtimestamp", "starttime"],
        "timelte": ["timelte", "end", "totimestamp", "endtime"],
        "usdgte": ["usdgte", "minusd", "min_transfer_usd"],
        "sortkey": ["sortkey"],
        "sortdir": ["sortdir"],
        "currency": ["currency"],
        "token": ["token", "tokenaddress", "address"],
    }
    # exact semantic alias group
    for key, names in aliases.items():
        if p == canonical(key) or p in {canonical(x) for x in names}:
            for n in names:
                if n in values:
                    return values[n]
                for vk, vv in values.items():
                    if canonical(vk) == canonical(n):
                        return vv
    # fuzzy exact canonical against supplied keys
    for k, v in values.items():
        if canonical(k) == p:
            return v
    return None


# ----------------------------- LLM client ----------------------------------
class LLM:
    def __init__(self, key: Optional[str], base: str, primary: str, auditor: str):
        self.key = key
        self.base = base.rstrip("/")
        self.primary = primary
        self.auditor = auditor
        self.s = requests.Session()

    def _chat_json(self, model: str, system: str, user: str, max_tokens=1800) -> Dict[str, Any]:
        if not self.key:
            raise RuntimeError("LLMASS_API_KEY is missing in .env")
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
            "temperature": 0,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
        }
        headers = {"Authorization": f"Bearer {self.key}", "Content-Type": "application/json"}
        url = self.base + "/chat/completions"
        for attempt in range(4):
            r = self.s.post(url, headers=headers, json=payload, timeout=120)
            if r.status_code == 400 and "response_format" in r.text:
                payload.pop("response_format", None)
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            if isinstance(content, dict):
                return content
            m = re.search(r"\{.*\}", str(content), flags=re.S)
            if not m:
                raise ValueError(f"Model {model} did not return JSON: {content[:500]}")
            return json.loads(m.group(0))
        raise RuntimeError(f"LLM request failed for {model}")

    def classify(self, event: Dict[str, Any], sequence: List[Dict[str, Any]], history_context: Any) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        schema_text = {
            "event_class": "one of " + ", ".join(EVENT_LABELS),
            "confidence": "0..1",
            "economic_intent": "short phrase",
            "rationale": "concise evidence-based explanation",
            "is_probably_sale": "boolean",
            "is_probably_purchase": "boolean",
            "is_operational": "boolean",
            "sequence_pattern": "e.g. LP_REMOVE->BRIDGE->CEX_DEPOSIT or UNKNOWN",
            "tags": ["short_machine_tags"],
            "evidence": ["specific facts from supplied data"],
            "ambiguities": ["what cannot be known"],
        }
        system = (
            "You are a forensic on-chain event classifier. Classify economic intent from transaction sequences, "
            "not from token sentiment. Never infer facts absent from the input. Operational/MM/internal flows must "
            "not be mislabeled as sales. A transfer to a CEX is CEX_DEPOSIT unless surrounding evidence supports a "
            "more specific higher-level intent. Output strict JSON only. "
            f"Required schema: {json_dumps(schema_text)}"
        )
        pack = {
            "target_event": event,
            "neighboring_sequence": sequence,
            "historical_portfolio_or_history_context": history_context,
        }
        primary = self._chat_json(self.primary, system, json_dumps(pack))

        audit_system = (
            "You independently audit an on-chain classifier. Inspect raw event/context and the candidate classification. "
            "Return strict JSON with fields agree:boolean, confidence:0..1, corrected_event_class, corrected_intent, "
            "rationale, missed_evidence:list, ambiguity:list. Be skeptical of CEX deposit=sale assumptions."
        )
        auditor = self._chat_json(self.auditor, audit_system, json_dumps({"input": pack, "candidate": primary}))

        pclass = str(primary.get("event_class", "UNKNOWN")).upper()
        aclass = str(auditor.get("corrected_event_class", pclass)).upper()
        pconf = safe_float(primary.get("confidence")) or 0.0
        agree = bool(auditor.get("agree")) and aclass == pclass
        if agree and pconf >= 0.70:
            final = dict(primary)
            final["adjudication"] = "primary+auditor_agree"
        else:
            adjud_system = (
                "Act as final adjudicator for an on-chain classification disagreement. Use only supplied evidence. "
                f"Choose one event_class from {EVENT_LABELS}. Return strict JSON with event_class, confidence, "
                "economic_intent, rationale, sequence_pattern, tags, evidence, ambiguities."
            )
            final = self._chat_json(self.primary, adjud_system, json_dumps({"input": pack, "primary": primary, "audit": auditor}))
            final["adjudication"] = "grok-4.6_after_audit"
        if str(final.get("event_class", "UNKNOWN")).upper() not in EVENT_LABELS:
            final["event_class"] = "UNKNOWN"
        final["confidence"] = clamp(safe_float(final.get("confidence")) or 0.0, 0.0, 1.0)
        return primary, auditor, final


# ----------------------------- Arkham collection ---------------------------
class ArkhamAdapter:
    def __init__(self, hub: HubClient, db: DB, cfg: Dict[str, Any]):
        self.hub, self.db, self.cfg = hub, db, cfg
        transfer_values = {
            "base": "entity-slug", "entity": "entity-slug", "chains": ["ethereum"],
            "flow": "all", "timeGte": 1, "timeLte": 2, "usdGte": 0,
            "sortKey": "time", "sortDir": "desc", "limit": 100, "offset": 0, "page": 1,
        }
        self.transfers_op = hub.pinned("arkham_transfers")
        self.transfers_unenriched_op = hub.pinned("arkham_transfers_unenriched", optional=True)
        self.history_op = hub.pinned("arkham_history_entity", optional=True)
        self.portfolio_op = hub.pinned("arkham_portfolio_entity", optional=True)
        self.tx_op = hub.pinned("arkham_tx_detail", optional=True)

    def collect_entity(self, entity: str) -> int:
        # Arkham /transfers documents timeGte/timeLte as Unix MILLISECONDS.
        # Internal event timestamps stay in seconds; only the outbound API query uses ms.
        now_s = int(time.time())
        start_s = now_s - int(self.cfg.get("history_days", 365)) * 86400
        page_size = int(self.cfg.get("arkham_page_size", 5000))
        max_pages = int(self.cfg.get("max_pages_per_entity", 200))
        min_usd = float(self.cfg.get("min_transfer_usd", 250000))
        names = {canonical(x) for x in self.hub.parameter_names(self.transfers_op)}

        # Fail early with a minimal request before starting a potentially long collection.
        try:
            self.hub.call(self.transfers_op, {"base": entity, "flow": "all", "limit": 1, "offset": 0})
        except RuntimeError as ex:
            msg = str(ex)
            if "Hub 401" in msg and "/run/arkham/transfers" in msg and "upstream provider returned 401" not in msg:
                raise RuntimeError("API Hub rejected the request with HTTP 401. Verify that API_HUB_KEY is current and authorized.") from ex
            if "upstream provider returned 401" in msg and "/run/arkham/transfers" in msg:
                raise RuntimeError(
                    "API Hub accepted the request, but Arkham's upstream provider returned 401. "
                    "This indicates upstream authorization/entitlement or provider policy; the Hub key itself was accepted."
                ) from ex
            if "Hub 502" in msg and "/run/arkham/transfers" in msg:
                fallback_ok = False
                if self.transfers_unenriched_op:
                    try:
                        self.hub.call(self.transfers_unenriched_op, {"base": entity, "flow": "all", "timeLast": "30d", "limit": 1, "offset": 0})
                        fallback_ok = True
                    except Exception:
                        pass
                if fallback_ok:
                    raise RuntimeError(
                        "Arkham enriched /transfers is currently returning HTTP 502, while "
                        "/transfers/unenriched is reachable. The unenriched route only exposes a "
                        "relative window up to 30d, so using it would silently destroy the requested "
                        "multi-year backtest. Full collection is stopped intentionally; retry when "
                        "the enriched upstream/provider is available."
                    ) from ex
                raise RuntimeError(
                    "Arkham /transfers is returning HTTP 502 even for a minimal request. "
                    "This indicates an upstream/provider or transport failure; inspect the "
                    "Hub response body and provider status. "
                    "The engine refuses to substitute incomplete history for a full backtest."
                ) from ex
            raise

        offset = 0
        total = 0
        seen_ids = set()
        oldest_seen_s = now_s
        upper_s = now_s
        for page in range(max_pages):
            values = {
                "base": entity,
                "entity": entity,
                "chains": self.cfg.get("chains", []),
                "flow": "all",
                "timeGte": start_s * 1000,
                "timeLte": upper_s * 1000,
                "usdGte": min_usd,
                "sortKey": "time",
                "sortDir": "desc",
                "limit": page_size,
                "offset": offset,
                "page": page + 1,
            }
            payload = self.hub.call(self.transfers_op, values)
            rows = first_list(payload)
            if not rows:
                break
            inserted_this_page = 0
            page_ts = []
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                ev = normalize_transfer(entity, raw)
                if not ev or ev["event_id"] in seen_ids:
                    continue
                seen_ids.add(ev["event_id"])
                page_ts.append(ev["ts"])
                self.db.execute("""
                    INSERT OR REPLACE INTO transfers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    ev["event_id"], entity, ev["ts"], ev["chain"], ev["token_symbol"], ev["token_address"],
                    ev["amount"], ev["usd_value"], ev["from_address"], ev["from_label"], ev["to_address"],
                    ev["to_label"], ev["tx_hash"], ev["direction"], json_dumps(raw), now_iso()
                ])
                inserted_this_page += 1
            total += inserted_this_page
            if page_ts:
                oldest_seen_s = min(oldest_seen_s, min(page_ts))
            if len(rows) < page_size:
                break
            if "offset" in names:
                offset += len(rows)
            elif "page" in names:
                # semantic lookup will pick page only if op exposes it; inject via offset value not enough
                offset += len(rows)
            elif oldest_seen_s <= start_s:
                break
            else:
                # without explicit pagination support, moving timeLte is the safest generic fallback
                upper_s = oldest_seen_s - 1
            if inserted_this_page == 0:
                break
        return total

    def historical_context(self, entity: str, ts: int) -> Any:
        """Fetch point-in-time portfolio when available; never leak today's portfolio into a historical backtest."""
        if self.portfolio_op:
            vals = {
                "entity": entity, "time": str(ts * 1000), "timestamp": str(ts * 1000), "at": str(ts * 1000),
                "chains": self.cfg.get("chains", []),
            }
            try:
                return {"kind": "portfolio_snapshot", "at": ts, "data": self.hub.call(self.portfolio_op, vals)}
            except Exception:
                pass
        if self.history_op:
            try:
                return {"kind": "balance_history", "at": ts, "data": self.hub.call(self.history_op, {"entity": entity, "chains": self.cfg.get("chains", [])})}
            except Exception:
                pass
        return None

    def fetch_sequence(self, entity: str, ts: int, hours: int = 12) -> List[Dict[str, Any]]:
        """Dense causal context: prior 12h plus same-block/same-transaction legs only. No future-action leakage."""
        values = {
            "base": entity, "entity": entity, "chains": self.cfg.get("chains", []), "flow": "all",
            "timeGte": (ts - hours * 3600) * 1000, "timeLte": (ts + 60) * 1000,
            "usdGte": 0, "sortKey": "time", "sortDir": "asc", "limit": 5000, "offset": 0, "page": 1,
        }
        try:
            payload = self.hub.call(self.transfers_op, values)
        except Exception:
            return []
        out = []
        for raw in first_list(payload):
            if not isinstance(raw, dict):
                continue
            ev = normalize_transfer(entity, raw)
            if ev:
                out.append(ev)
        return sorted(out, key=lambda x: x.get("ts") or 0)

    def tx_detail(self, tx_hash: Optional[str], chain: Optional[str] = None) -> Any:
        if not tx_hash or not self.tx_op:
            return None
        try:
            return self.hub.call(self.tx_op, {"hash": tx_hash, "tx_hash": tx_hash, "transaction_hash": tx_hash, "chain": chain})
        except Exception:
            return None


def normalize_transfer(entity: str, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ts = iso_to_ts(raw.get("blockTimestamp") or raw.get("timestamp") or raw.get("time") or deep_get_alias(raw, ["block_timestamp", "created_at"]))
    if not ts:
        return None
    fr = raw.get("fromAddress") or raw.get("from") or raw.get("sender") or {}
    to = raw.get("toAddress") or raw.get("to") or raw.get("recipient") or {}
    chain = raw.get("chain") or (fr.get("chain") if isinstance(fr, dict) else None) or (to.get("chain") if isinstance(to, dict) else None)
    token_symbol = raw.get("tokenSymbol") or raw.get("symbol") or deep_get_alias(raw, ["token_symbol", "asset", "ticker"])
    token_addr = raw.get("tokenAddress") or raw.get("contractAddress") or deep_get_alias(raw, ["token_address", "contract_address"])
    amount = safe_float(raw.get("unitValue"))
    if amount is None:
        amount = safe_float(raw.get("fromValue")) or safe_float(raw.get("toValue")) or safe_float(deep_get_alias(raw, ["amount", "value"]))
    usd = safe_float(raw.get("historicalUSD"))
    if usd is None:
        usd = safe_float(deep_get_alias(raw, ["historical_usd", "usd_value", "value_usd", "usd"]))
    tx = raw.get("transactionHash") or raw.get("txHash") or raw.get("txid") or deep_get_alias(raw, ["hash", "transaction_hash"])
    from_addr, to_addr = extract_address(fr), extract_address(to)
    from_label, to_label = extract_name(fr), extract_name(to)
    direction = "unknown"
    e = canonical(entity)
    fl = canonical(from_label)
    tl = canonical(to_label)
    if e and (e in fl or fl in e): direction = "out"
    if e and (e in tl or tl in e): direction = "in" if direction == "unknown" else "internal"
    eid = stable_id(entity, tx, raw.get("id"), ts, from_addr, to_addr, token_addr, amount)
    return {
        "event_id": eid, "ts": ts, "chain": str(chain or "unknown"), "token_symbol": str(token_symbol or "UNKNOWN"),
        "token_address": str(token_addr) if token_addr else None, "amount": amount, "usd_value": usd,
        "from_address": from_addr, "from_label": from_label, "to_address": to_addr, "to_label": to_label,
        "tx_hash": str(tx) if tx else None, "direction": direction,
    }


# ----------------------------- extra semantic context ----------------------
class DeBankAdapter:
    """Optional historical decoded wallet actions. Used only for context available at the event timestamp."""
    def __init__(self, hub: HubClient):
        self.hub = hub
        self.history_op = hub.pinned("debank_all_history", optional=True)

    def history_around(self, address: Optional[str], chain: str, ts: int, hours=12, max_pages=25) -> List[Dict[str, Any]]:
        if not self.history_op or not address or not str(address).startswith("0x"):
            return []
        # Causal: ask for history earlier than one minute after event, then page backwards.
        low, cursor = ts - hours * 3600, ts + 60
        cid = CHAIN_TO_DEBANK.get(str(chain).lower())
        out: List[Dict[str, Any]] = []
        seen = set()
        for _ in range(max_pages):
            vals = {
                "id": address, "address": address, "user_addr": address,
                "start_time": cursor, "page_count": 20,
                "chain_id": cid, "chain_ids": [cid] if cid else None, "chain": cid,
            }
            try:
                payload = self.hub.call(self.history_op, vals)
            except Exception:
                break
            rows = first_list(payload, ("history_list", "data", "items", "results"))
            if not rows:
                break
            oldest = cursor
            added = 0
            for x in rows:
                if not isinstance(x, dict):
                    continue
                xt = iso_to_ts(x.get("time_at") or x.get("timestamp") or x.get("time"))
                if xt is None:
                    continue
                oldest = min(oldest, xt)
                xid = str(x.get("id") or x.get("tx", {}).get("id") or stable_id(address, xt, json_dumps(x)))
                if xid in seen:
                    continue
                seen.add(xid)
                if low <= xt <= ts + 60:
                    out.append(x)
                    added += 1
            if oldest <= low or len(rows) < 20:
                break
            if oldest >= cursor:
                break
            cursor = oldest - 1
        return sorted(out, key=lambda x: iso_to_ts(x.get("time_at") or x.get("timestamp") or x.get("time")) or 0)


class RPCAdapter:
    """Optional read-only transaction verification through Hub's unified RPC plane."""
    def __init__(self, hub: HubClient):
        self.hub = hub
        self.rpc_op = hub.strict_rpc(optional=True)

    def tx_context(self, chain: str, tx_hash: Optional[str]) -> Any:
        if not self.rpc_op or not tx_hash or str(chain).lower() not in CHAIN_TO_EVM_ID:
            return None
        chain_id = CHAIN_TO_EVM_ID[str(chain).lower()]
        result = {}
        for method in ("eth_getTransactionByHash", "eth_getTransactionReceipt"):
            body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": [tx_hash]}
            vals = {
                "chain": chain, "network": chain, "chain_id": chain_id, "chainId": chain_id,
                "method": method, "params": [tx_hash], "jsonrpc": "2.0", "id": 1, "__body": body,
            }
            try:
                result[method] = self.hub.call(self.rpc_op, vals)
            except Exception:
                pass
        return result or None


# ----------------------------- contexts + classification -------------------
class ContextBuilder:
    def __init__(self, db: DB, arkham: ArkhamAdapter, debank: Optional[DeBankAdapter] = None, rpc: Optional[RPCAdapter] = None):
        self.db, self.arkham, self.debank, self.rpc = db, arkham, debank, rpc

    def build_one(self, event_id: str) -> Dict[str, Any]:
        row = self.db.df("SELECT * FROM transfers WHERE event_id=?", [event_id])
        if row.empty:
            raise KeyError(event_id)
        e = row.iloc[0].to_dict()
        ts = int(e["ts"])
        entity = str(e["entity"])
        # Dense causal sequence: prior 12h + same-block tail; never use later future actions to label a tradable event.
        seq_records = self.arkham.fetch_sequence(entity, ts, hours=12)
        if not seq_records:
            seq = self.db.df("""
                SELECT event_id, ts, chain, token_symbol, amount, usd_value,
                       from_address, from_label, to_address, to_label, tx_hash, direction
                FROM transfers WHERE entity=? AND ts BETWEEN ? AND ? ORDER BY ts
            """, [entity, ts - 12 * 3600, ts + 60])
            seq_records = seq.to_dict("records")
        # Most recent causal actions are the most useful; cap prompt size deterministically.
        max_seq = int(self.arkham.cfg.get("llm_max_sequence_records", 120))
        seq_records = seq_records[-max_seq:]
        hist = self.arkham.historical_context(entity, ts)
        portfolio_usd, token_usd, pct = infer_historical_portfolio(hist, str(e.get("token_symbol") or ""))
        # Choose the entity-side wallet for address-level DeBank history.
        entity_addr = e.get("from_address") if str(e.get("direction")) in ("out", "internal") else e.get("to_address")
        debank_hist = self.debank.history_around(entity_addr, str(e.get("chain")), ts) if self.debank else []
        arkham_tx = self.arkham.tx_detail(e.get("tx_hash"), str(e.get("chain")))
        rpc_tx = self.rpc.tx_context(str(e.get("chain")), e.get("tx_hash")) if self.rpc else None
        context = {
            "target": serialize_record(e),
            "neighbor_count": len(seq_records),
            "historical_portfolio_available": hist is not None,
            "debank_history": debank_hist,
            "arkham_tx_detail": arkham_tx,
            "rpc_tx": rpc_tx,
        }
        self.db.execute("""
            INSERT OR REPLACE INTO contexts VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [event_id, portfolio_usd, token_usd, pct, json_dumps(hist), json_dumps(seq_records), json_dumps(context), now_iso()])
        return {
            "event": serialize_record(e), "sequence": seq_records,
            "history": compact_history_for_llm(hist, str(e.get("token_symbol") or ""), ts),
            "debank_history": prune_json(debank_hist[-60:], max_list=60),
            "arkham_tx_detail": prune_json(arkham_tx), "rpc_tx": prune_json(rpc_tx),
        }


def prune_json(obj: Any, max_list: int = 60, depth: int = 0) -> Any:
    """Keep LLM context bounded while preserving deterministic evidence."""
    if depth > 7:
        return "<depth-truncated>"
    if isinstance(obj, list):
        xs = obj[-max_list:] if len(obj) > max_list else obj
        return [prune_json(x, max_list, depth + 1) for x in xs]
    if isinstance(obj, dict):
        return {str(k): prune_json(v, max_list, depth + 1) for k, v in list(obj.items())[:120]}
    if isinstance(obj, str) and len(obj) > 2000:
        return obj[:2000] + "<truncated>"
    return obj


def compact_history_for_llm(hist: Any, token_symbol: str, ts: int) -> Any:
    if hist is None:
        return None
    if isinstance(hist, dict) and hist.get("kind") == "portfolio_snapshot":
        src = hist.get("data")
        holdings = []
        q = [src]
        while q and len(holdings) < 5000:
            x = q.pop(0)
            if isinstance(x, dict):
                sym = x.get("symbol") or x.get("tokenSymbol")
                usd = safe_float(x.get("usd") or x.get("usdValue") or x.get("balanceUsd"))
                if sym and usd is not None:
                    holdings.append({"symbol": str(sym), "usd": usd, "balance": safe_float(x.get("balance")), "price": safe_float(x.get("price"))})
                for v in x.values():
                    if isinstance(v, (dict, list)): q.append(v)
            elif isinstance(x, list): q.extend(x[:1000])
        holdings.sort(key=lambda z: z.get("usd") or 0, reverse=True)
        target = [x for x in holdings if x["symbol"].upper() == token_symbol.upper()]
        top = holdings[:30]
        for x in target:
            if x not in top: top.append(x)
        return {"kind": "portfolio_snapshot", "at": ts, "top_holdings": top, "total_usd": sum(max(0, x.get("usd") or 0) for x in holdings)}
    # Balance history is usually compact time/usd points; keep tail near event.
    return prune_json(hist, max_list=80)


def infer_historical_portfolio(hist: Any, token_symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if hist is None:
        return None, None, None
    # Only portfolio_snapshot has token-level point-in-time composition. Balance history is useful context but not portfolio %.
    if isinstance(hist, dict) and hist.get("kind") == "balance_history":
        return None, None, None
    source = hist.get("data") if isinstance(hist, dict) and "data" in hist else hist
    candidates = []
    q = [source]
    while q and len(candidates) < 5000:
        x = q.pop(0)
        if isinstance(x, dict):
            sym = x.get("symbol") or x.get("tokenSymbol")
            usd = safe_float(x.get("usd") or x.get("usdValue") or x.get("balanceUsd"))
            if sym and usd is not None:
                candidates.append((str(sym).upper(), usd))
            for v in x.values():
                if isinstance(v, (dict, list)): q.append(v)
        elif isinstance(x, list):
            q.extend(x[:1000])
    if not candidates:
        return None, None, None
    total = sum(max(0, x[1]) for x in candidates)
    tok = sum(max(0, usd) for sym, usd in candidates if sym == token_symbol.upper())
    pct = (tok / total * 100.0) if total > 0 else None
    return total or None, tok or None, pct


def serialize_record(rec: Any) -> Dict[str, Any]:
    if hasattr(rec, "to_dict"):
        rec = rec.to_dict()
    out = {}
    for k, v in dict(rec).items():
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            out[k] = None
        elif isinstance(v, (np.integer,)): out[k] = int(v)
        elif isinstance(v, (np.floating,)): out[k] = float(v)
        else: out[k] = v
    return out


class Classifier:
    def __init__(self, db: DB, builder: ContextBuilder, llm: LLM):
        self.db, self.builder, self.llm = db, builder, llm

    def classify_pending(self, limit: Optional[int] = None):
        sql = """
            SELECT t.event_id FROM transfers t
            LEFT JOIN classifications c ON c.event_id=t.event_id
            WHERE c.event_id IS NULL ORDER BY t.ts
        """
        if limit: sql += f" LIMIT {int(limit)}"
        ids = [r[0] for r in self.db.execute(sql).fetchall()]
        for i, event_id in enumerate(ids, 1):
            try:
                pack = self.builder.build_one(event_id)
                semantic_context = {
                    "arkham_history_or_portfolio": pack.get("history"),
                    "debank_history": pack.get("debank_history"),
                    "arkham_tx_detail": pack.get("arkham_tx_detail"),
                    "rpc_tx": pack.get("rpc_tx"),
                }
                p, a, f = self.llm.classify(pack["event"], pack["sequence"], semantic_context)
                self.db.execute("""
                    INSERT OR REPLACE INTO classifications VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [event_id, json_dumps(p), json_dumps(a), json_dumps(f),
                      str(f.get("event_class", "UNKNOWN")).upper(), safe_float(f.get("confidence")) or 0.0,
                      str(f.get("rationale", "")), json_dumps(f.get("tags", [])), now_iso()])
                print(f"[{i}/{len(ids)}] {event_id[:8]} -> {f.get('event_class')} ({f.get('confidence')})")
            except Exception as ex:
                print(f"classification failed {event_id}: {ex}", file=sys.stderr)


# ----------------------------- GeckoTerminal market data ------------------
class GeckoAdapter:
    def __init__(self, hub: HubClient, db: DB):
        self.hub, self.db = hub, db
        self.token_pools_op = hub.pinned("gecko_token_pools", optional=True)
        self.search_op = hub.pinned("gecko_search_pools", optional=True)
        self.ohlcv_op = hub.pinned("gecko_ohlcv")

    def resolve_pool(self, chain: str, token_address: Optional[str], symbol: str, benchmark=False) -> Optional[Dict[str, Any]]:
        network = CHAIN_TO_GECKO.get(str(chain).lower())
        if not network:
            return None
        token_key = "BENCHMARK" if benchmark else (token_address or symbol.upper())
        cached = self.db.df("SELECT * FROM pool_cache WHERE chain=? AND token_key=?", [network, token_key])
        if not cached.empty:
            row = cached.iloc[0].to_dict()
            meta = json_loads(row.get("raw_json"), {}) or {}
            row["token_side"] = meta.get("token_side")
            return row

        payload = None
        if token_address and self.token_pools_op and not benchmark:
            try:
                payload = self.hub.call(self.token_pools_op, {
                    "network": network, "chain": network, "address": token_address,
                    "token_address": token_address, "page": 1
                })
            except Exception:
                payload = None
        if payload is None and self.search_op:
            q = BENCHMARK_QUERY.get(network, "WETH USDC") if benchmark else (token_address or symbol)
            try:
                payload = self.hub.call(self.search_op, {"query": q, "q": q, "network": network, "page": 1})
            except Exception:
                payload = None
        candidates = parse_gecko_pools(payload)
        if not candidates:
            return None
        best = max(candidates, key=lambda x: (x.get("reserve_usd") or 0, x.get("volume_24h") or 0))
        target = str(token_address or "").lower()
        side = None
        if target:
            if str(best.get("base_token") or "").lower().endswith(target): side = "base"
            elif str(best.get("quote_token") or "").lower().endswith(target): side = "quote"
        if side is None:
            wanted_symbol = (BENCHMARK_QUERY.get(network, "").split()[:1] or [symbol])[0] if benchmark else symbol
            name = str(best.get("name") or "").upper().replace("-", " / ")
            parts = [x.strip() for x in name.split("/") if x.strip()]
            if parts:
                if str(wanted_symbol).upper() in parts[0]: side = "base"
                elif len(parts) > 1 and str(wanted_symbol).upper() in parts[1]: side = "quote"
        side = side or "base"
        cache_meta = {**best, "token_side": side}
        self.db.execute("""
            INSERT OR REPLACE INTO pool_cache VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [network, token_key, best["address"], best.get("name"), best.get("reserve_usd"), json_dumps(cache_meta), now_iso()])
        return {"chain": network, "token_key": token_key, "pool_address": best["address"], "pool_name": best.get("name"), "reserve_usd": best.get("reserve_usd"), "token_side": side}

    def ensure_window(self, chain: str, token_address: Optional[str], symbol: str, center_ts: int, benchmark=False) -> pd.DataFrame:
        network = CHAIN_TO_GECKO.get(str(chain).lower())
        if not network:
            return pd.DataFrame()
        token_key = "BENCHMARK" if benchmark else (token_address or symbol.upper())
        pool = self.resolve_pool(chain, token_address, symbol, benchmark=benchmark)
        if not pool:
            return pd.DataFrame()
        pool_address = pool["pool_address"]

        # Need fine bars around event and hourly bars for 3d horizon/pre24h.
        # Gecko's cursor is an upper bound. Request far enough into the future
        # to cover every configured horizon, while retaining the causal pre-window.
        max_horizon_s = max((int(x) for x in self.cfg.get("horizons_minutes", [4320])), default=4320) * 60
        wants = [
            ("minute", 1, center_ts + max(6 * 3600, max_horizon_s), 900),
            ("hour", 1, center_ts + max(4 * 86400, max_horizon_s), 240),
        ]
        for timeframe, aggregate, before_ts, limit in wants:
            required_lo = center_ts - (6 * 3600 if timeframe == "minute" else 2 * 86400)
            required_hi = center_ts + (max(5 * 3600, min(max_horizon_s, 4 * 3600)) if timeframe == "minute" else max(4 * 86400, max_horizon_s))
            existing = self.db.one("""
                SELECT COUNT(*) FROM prices
                WHERE chain=? AND token_key=? AND timeframe=? AND ts BETWEEN ? AND ?
            """, [network, token_key, timeframe, required_lo, required_hi])
            min_expected = 60 if timeframe == "minute" else max(20, int(max_horizon_s // 3600))
            need = not (existing and existing[0] >= min_expected)
            if not need:
                continue
            vals = {
                "network": network, "chain": network, "pool_address": pool_address, "pool": pool_address,
                "timeframe": timeframe, "aggregate": aggregate, "before_timestamp": before_ts,
                "limit": limit, "currency": "usd", "token": pool.get("token_side") or "base",
            }
            cursor = before_ts
            for _ in range(12):
                try:
                    vals["before_timestamp"] = cursor
                    payload = self.hub.call(self.ohlcv_op, vals)
                    candles = parse_ohlcv(payload)
                    if not candles:
                        break
                    oldest = min(c["ts"] for c in candles)
                    for c in candles:
                        self.db.execute("""
                            INSERT OR REPLACE INTO prices VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, [network, token_key, pool_address, c["ts"], c["close"], c.get("volume"), timeframe, "geckoterminal"])
                    if oldest <= required_lo or oldest >= cursor:
                        break
                    cursor = oldest - 1
                except Exception as ex:
                    print(f"OHLCV failed {network}/{token_key}/{timeframe}: {ex}", file=sys.stderr)
                    break
        return self.db.df("""
            SELECT ts, close, volume, timeframe FROM prices
            WHERE chain=? AND token_key=? ORDER BY ts
        """, [network, token_key])


def parse_gecko_pools(payload: Any) -> List[Dict[str, Any]]:
    rows = first_list(payload)
    out = []
    for x in rows:
        if not isinstance(x, dict): continue
        a = x.get("attributes", x)
        address = a.get("address") or x.get("id")
        if isinstance(address, str) and "_" in address:
            # GeckoTerminal JSON:API ids are commonly <network>_<pool-address>.
            address = address.split("_", 1)[1]
        if not address: continue
        rel = x.get("relationships", {}) if isinstance(x.get("relationships"), dict) else {}
        def rid(name):
            try:
                return rel[name]["data"]["id"]
            except Exception:
                return None
        out.append({
            "address": str(address),
            "name": a.get("name") or a.get("pool_name"),
            "reserve_usd": safe_float(a.get("reserve_in_usd") or a.get("reserve_usd") or a.get("liquidity_usd")),
            "volume_24h": safe_float((a.get("volume_usd") or {}).get("h24") if isinstance(a.get("volume_usd"), dict) else a.get("volume_usd")),
            "base_token": rid("base_token"), "quote_token": rid("quote_token"),
            "raw": x,
        })
    return out


def parse_ohlcv(payload: Any) -> List[Dict[str, Any]]:
    arr = deep_get_alias(payload, ["ohlcv_list", "ohlcv", "candles", "data"])
    # Gecko shape: data.attributes.ohlcv_list
    if isinstance(payload, dict):
        d = payload.get("data")
        if isinstance(d, dict):
            attrs = d.get("attributes")
            if isinstance(attrs, dict) and isinstance(attrs.get("ohlcv_list"), list):
                arr = attrs["ohlcv_list"]
    out = []
    if not isinstance(arr, list):
        return out
    for x in arr:
        if isinstance(x, list) and len(x) >= 6:
            ts = iso_to_ts(x[0])
            close = safe_float(x[4])
            if ts is not None and close is not None and close > 0:
                out.append({"ts": ts, "close": close, "volume": safe_float(x[5])})
        elif isinstance(x, dict):
            ts = iso_to_ts(x.get("timestamp") or x.get("time") or x.get("ts"))
            close = safe_float(x.get("close") or x.get("c"))
            if ts and close is not None:
                out.append({"ts": ts, "close": close, "volume": safe_float(x.get("volume") or x.get("v"))})
    return out


# ----------------------------- feature/return enrichment ------------------
class Enricher:
    def __init__(self, db: DB, gecko: GeckoAdapter, cfg: Dict[str, Any]):
        self.db, self.gecko, self.cfg = db, gecko, cfg

    def enrich_all(self, limit: Optional[int] = None):
        sql = """
            SELECT t.*, c.portfolio_pct, cl.event_class, cl.confidence, cl.final_json
            FROM transfers t
            JOIN classifications cl ON cl.event_id=t.event_id
            LEFT JOIN contexts c ON c.event_id=t.event_id
            ORDER BY t.ts
        """
        if limit: sql += f" LIMIT {int(limit)}"
        df = self.db.df(sql)
        for i, e in df.iterrows():
            try:
                self.enrich_one(e.to_dict())
                print(f"[{i+1}/{len(df)}] enriched {e['event_id'][:8]} {e['token_symbol']}")
            except Exception as ex:
                print(f"enrich failed {e['event_id']}: {ex}", file=sys.stderr)

    def enrich_one(self, e: Dict[str, Any]):
        ts = int(e["ts"])
        chain = str(e["chain"])
        symbol = str(e["token_symbol"])
        token_address = e.get("token_address")
        px = self.gecko.ensure_window(chain, token_address, symbol, ts, benchmark=False)
        bm = self.gecko.ensure_window(chain, None, "BENCHMARK", ts, benchmark=True)
        pool_info = self.gecko.resolve_pool(chain, token_address, symbol, benchmark=False)
        if px.empty:
            return

        minute = px[px.timeframe == "minute"].sort_values("ts")
        hourly = px[px.timeframe == "hour"].sort_values("ts")
        bmin = bm[bm.timeframe == "minute"].sort_values("ts") if not bm.empty else pd.DataFrame()
        bhour = bm[bm.timeframe == "hour"].sort_values("ts") if not bm.empty else pd.DataFrame()
        delay = int(self.cfg.get("execution_delay_seconds", 120))
        entry_ts = ts + delay
        # Entry is explicitly AFTER the signal + execution delay. This avoids same-candle lookahead.
        entry = value_at_or_after(minute if not minute.empty else hourly, entry_ts, 3600)
        if not entry or entry <= 0:
            return
        bentry = value_at_or_after(bmin if not bmin.empty else bhour, entry_ts, 3600) if not bm.empty else None

        for h in self.cfg["horizons_minutes"]:
            target = entry_ts + int(h) * 60
            source = minute if h <= 240 and not minute.empty else hourly
            exitp = value_at_or_after(source, target, 360 if h <= 240 else 5400)
            bsource = bmin if h <= 240 and not bmin.empty else bhour
            bexit = value_at_or_after(bsource, target, 360 if h <= 240 else 5400) if not bsource.empty else None
            raw_ret = (exitp / entry - 1) if exitp and exitp > 0 else None
            bret = (bexit / bentry - 1) if (bentry and bexit and bentry > 0) else None
            # Missing benchmark data must remain missing in excess-return mode.
            excess = (raw_ret - bret) if (raw_ret is not None and bret is not None) else None
            self.db.execute("""
                INSERT OR REPLACE INTO event_returns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [e["event_id"], int(h), raw_ret, bret, excess, entry, exitp, bentry, bexit])

        pre1 = calc_return(hourly if not hourly.empty else minute, ts - 3600, ts)
        pre24 = calc_return(hourly if not hourly.empty else minute, ts - 86400, ts)
        bpre24 = calc_return(bhour if not bhour.empty else bmin, ts - 86400, ts) if not bm.empty else None
        vol24 = calc_vol(hourly, ts - 86400, ts)
        volume_z = calc_volume_z(hourly, ts)
        regime = market_regime(pre24, bpre24, vol24)
        usd_value = safe_float(e.get("usd_value"))
        size_bucket = bucketize(usd_value, SIZE_BUCKETS, SIZE_LABELS)
        pp = safe_float(e.get("portfolio_pct"))
        portfolio_bucket = bucketize(pp if pp is not None else -1e-9, PORTFOLIO_BUCKETS, PORTFOLIO_LABELS)
        reserve_usd = safe_float(pool_info.get("reserve_usd")) if pool_info else None
        trailing_volume = None
        if not hourly.empty and "volume" in hourly:
            vv = hourly[(hourly.ts <= ts) & (hourly.ts >= ts - 86400)].volume.dropna()
            if len(vv): trailing_volume = float(vv.sum())
        liquidity_ratio = (usd_value / reserve_usd) if (usd_value is not None and reserve_usd and reserve_usd > 0) else None
        volume_ratio = (usd_value / trailing_volume) if (usd_value is not None and trailing_volume and trailing_volume > 0) else None
        liquidity_bucket = bucketize(liquidity_ratio, IMPACT_BUCKETS, IMPACT_LABELS)
        volume_bucket = bucketize(volume_ratio, IMPACT_BUCKETS, IMPACT_LABELS)
        flow_state = infer_flow_state(self.db, str(e["entity"]), symbol, ts)
        features = {
            "usd_value": usd_value, "portfolio_pct": pp, "reserve_usd": reserve_usd, "trailing_24h_volume": trailing_volume,
            "liquidity_ratio": liquidity_ratio, "volume_ratio": volume_ratio,
            "pre_1h": pre1, "pre_24h": pre24, "benchmark_pre_24h": bpre24,
            "vol_24h": vol24, "volume_z": volume_z, "market_regime": regime, "flow_state": flow_state,
            "llm_confidence": safe_float(e.get("confidence")),
        }
        self.db.execute("""
            INSERT OR REPLACE INTO event_features
            (event_id,size_bucket,portfolio_bucket,liquidity_bucket,volume_bucket,liquidity_ratio,volume_ratio,
             pre_1h,pre_24h,vol_24h,volume_z,benchmark_pre_24h,market_regime,flow_state,feature_json,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [e["event_id"], size_bucket, portfolio_bucket, liquidity_bucket, volume_bucket, liquidity_ratio, volume_ratio,
              pre1, pre24, vol24, volume_z, bpre24, regime, flow_state, json_dumps(features), now_iso()])


def calc_return(df: pd.DataFrame, a: int, b: int) -> Optional[float]:
    if df.empty: return None
    p0 = value_at_or_before(df, a, 7200)
    p1 = value_at_or_before(df, b, 7200)
    return (p1 / p0 - 1) if p0 and p1 and p0 > 0 else None


def calc_vol(df: pd.DataFrame, a: int, b: int) -> Optional[float]:
    if df.empty: return None
    x = df[(df.ts >= a) & (df.ts <= b)].copy()
    if len(x) < 5: return None
    r = np.diff(np.log(x.close.astype(float).to_numpy()))
    return float(np.std(r, ddof=1) * math.sqrt(24)) if len(r) > 1 else None


def calc_volume_z(df: pd.DataFrame, ts: int) -> Optional[float]:
    if df.empty or "volume" not in df: return None
    x = df[(df.ts >= ts - 7 * 86400) & (df.ts <= ts)].dropna(subset=["volume"]).copy()
    if len(x) < 24: return None
    v = float(x.iloc[-1].volume)
    mu, sd = float(x.volume.mean()), float(x.volume.std(ddof=1))
    return (v - mu) / sd if sd > 0 else 0.0


def market_regime(asset_pre24, benchmark_pre24, vol24) -> str:
    b = benchmark_pre24 if benchmark_pre24 is not None else asset_pre24
    if b is None: direction = "UNKNOWN"
    elif b > 0.02: direction = "RISK_ON"
    elif b < -0.02: direction = "RISK_OFF"
    else: direction = "FLAT"
    if vol24 is None: vol = "VOL_UNKNOWN"
    elif vol24 >= 0.08: vol = "HIGH_VOL"
    elif vol24 <= 0.025: vol = "LOW_VOL"
    else: vol = "MID_VOL"
    return f"{direction}_{vol}"


def bucketize(v: Optional[float], edges: List[float], labels: List[str]) -> str:
    if v is None: return "unknown"
    for i in range(len(edges) - 1):
        if edges[i] <= v < edges[i + 1]:
            return labels[i]
    return labels[-1]


def infer_flow_state(db: DB, entity: str, token: str, ts: int) -> str:
    x = db.df("""
        SELECT t.direction, t.usd_value, cl.event_class
        FROM transfers t LEFT JOIN classifications cl ON cl.event_id=t.event_id
        WHERE t.entity=? AND t.token_symbol=? AND t.ts BETWEEN ? AND ?
    """, [entity, token, ts - 7 * 86400, ts - 1])
    if x.empty: return "NO_HISTORY"
    out = x[x.event_class.isin(["CEX_DEPOSIT", "DISTRIBUTION", "DEX_SELL_LIKE"])].usd_value.fillna(0).sum()
    inn = x[x.event_class.isin(["CEX_WITHDRAWAL", "ACCUMULATION", "DEX_BUY_LIKE"])].usd_value.fillna(0).sum()
    total = out + inn
    if total <= 0: return "NEUTRAL"
    ratio = (inn - out) / total
    if ratio > .25: return "ACCUMULATING"
    if ratio < -.25: return "DISTRIBUTING"
    return "NEUTRAL"


# ----------------------------- backtesting ---------------------------------
class Backtester:
    def __init__(self, db: DB, cfg: Dict[str, Any]):
        self.db, self.cfg = db, cfg
        self.seed = int(cfg.get("random_seed", 42))

    def dataset(self) -> pd.DataFrame:
        return self.db.df("""
            SELECT t.event_id, t.entity, t.ts, t.chain, t.token_symbol, t.usd_value,
                   cl.event_class, cl.confidence,
                   COALESCE(json_extract_string(cl.final_json, '$.sequence_pattern'), 'UNKNOWN') AS sequence_pattern,
                   CASE WHEN t.direction='out' THEN COALESCE(t.to_label,t.to_address,'UNKNOWN')
                        WHEN t.direction='in' THEN COALESCE(t.from_label,t.from_address,'UNKNOWN')
                        ELSE COALESCE(t.to_label,t.from_label,'UNKNOWN') END AS counterparty,
                   ef.size_bucket, ef.portfolio_bucket, ef.liquidity_bucket, ef.volume_bucket, ef.market_regime, ef.flow_state,
                   ef.liquidity_ratio, ef.volume_ratio, ef.pre_1h, ef.pre_24h, ef.vol_24h, ef.volume_z,
                   er.horizon_min, er.raw_return, er.benchmark_return, er.excess_return
            FROM transfers t
            JOIN classifications cl ON cl.event_id=t.event_id
            JOIN event_features ef ON ef.event_id=t.event_id
            JOIN event_returns er ON er.event_id=t.event_id
            WHERE er.raw_return IS NOT NULL AND cl.confidence >= 0.50
            ORDER BY t.ts
        """)

    def run(self) -> pd.DataFrame:
        df = self.dataset()
        if df.empty:
            raise RuntimeError("No enriched event returns. Run collect -> classify -> enrich first.")
        basis_col = "excess_return" if str(self.cfg.get("backtest_return_basis", "excess")).lower() == "excess" else "raw_return"
        df = df[df[basis_col].notna()].copy()
        if df.empty:
            if basis_col == "excess_return":
                raise RuntimeError("No benchmark-adjusted returns are available. Check benchmark pool and price coverage; choose backtest_return_basis=raw only for an explicitly unhedged study.")
            raise RuntimeError("No raw forward returns are available. Check token price coverage and event timestamps.")
        df = dedupe_signal_events(df, int(self.cfg.get("signal_cooldown_minutes", 30)))
        run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        min_train = int(self.cfg.get("min_pattern_events_train", 20))
        min_test = int(self.cfg.get("min_pattern_events_test", 8))
        candidates = []

        # Patterns are intentionally interpretable. Each level adds conditioning only when sample size survives.
        group_specs = [
            ["entity", "event_class", "token_symbol", "horizon_min"],
            ["entity", "event_class", "token_symbol", "chain", "horizon_min"],
            ["entity", "event_class", "token_symbol", "counterparty", "horizon_min"],
            ["entity", "event_class", "token_symbol", "sequence_pattern", "horizon_min"],
            ["entity", "event_class", "token_symbol", "size_bucket", "horizon_min"],
            ["entity", "event_class", "token_symbol", "liquidity_bucket", "horizon_min"],
            ["entity", "event_class", "token_symbol", "volume_bucket", "horizon_min"],
            ["entity", "event_class", "token_symbol", "market_regime", "horizon_min"],
            ["entity", "event_class", "token_symbol", "flow_state", "horizon_min"],
            ["entity", "event_class", "token_symbol", "sequence_pattern", "size_bucket", "liquidity_bucket", "volume_bucket", "market_regime", "horizon_min"],
            ["entity", "event_class", "token_symbol", "size_bucket", "portfolio_bucket", "market_regime", "flow_state", "horizon_min"],
        ]

        seen = set()
        for spec in group_specs:
            for keys, g in df.groupby(spec, dropna=False):
                if not isinstance(keys, tuple): keys = (keys,)
                keymap = dict(zip(spec, keys))
                pid = stable_id(*[f"{k}={keymap.get(k, '*')}" for k in spec])
                if pid in seen: continue
                seen.add(pid)
                g = g.sort_values("ts").copy()
                n = len(g)
                ntr = max(1, int(n * float(self.cfg.get("train_frac", .6))))
                nva = max(1, int(n * float(self.cfg.get("valid_frac", .2))))
                if ntr + nva >= n:
                    continue
                train, valid, test = g.iloc[:ntr], g.iloc[ntr:ntr+nva], g.iloc[ntr+nva:]
                if len(train) < min_train or len(test) < min_test:
                    continue

                # Direction is selected on TRAIN ONLY. Valid/test are untouched.
                train_mean = float(train[basis_col].mean())
                side = 1 if train_mean >= 0 else -1
                train_net = strategy_returns(train, side, self.cfg)
                valid_net = strategy_returns(valid, side, self.cfg)
                test_net = strategy_returns(test, side, self.cfg)
                if len(test_net) < min_test: continue
                ci = bootstrap_ci(test_net, seed=self.seed)
                t_p = float(stats.ttest_1samp(test_net, 0, nan_policy="omit").pvalue) if len(test_net) >= 3 else 1.0
                perm = permutation_p(test_net, seed=self.seed)
                eq = nonoverlap_equity(test, side, self.cfg)
                wf_pos, wf_folds = walk_forward_score(g, self.cfg)
                robustness = robustness_score(train_net, valid_net, test_net, wf_pos, wf_folds)
                candidates.append({
                    "run_id": run_id, "pattern_id": pid,
                    "entity": keymap.get("entity", "*"), "event_class": keymap.get("event_class", "*"),
                    "token_symbol": keymap.get("token_symbol", "*"), "chain": keymap.get("chain", "*"),
                    "counterparty": keymap.get("counterparty", "*"), "sequence_pattern": keymap.get("sequence_pattern", "*"),
                    "flow_state": keymap.get("flow_state", "*"), "size_bucket": keymap.get("size_bucket", "*"),
                    "portfolio_bucket": keymap.get("portfolio_bucket", "*"), "liquidity_bucket": keymap.get("liquidity_bucket", "*"),
                    "volume_bucket": keymap.get("volume_bucket", "*"), "market_regime": keymap.get("market_regime", "*"),
                    "horizon_min": int(keymap.get("horizon_min")), "side": side,
                    "train_n": len(train), "valid_n": len(valid), "test_n": len(test),
                    "train_mean": float(np.mean(train_net)), "valid_mean": float(np.mean(valid_net)), "test_mean": float(np.mean(test_net)),
                    "test_median": float(np.median(test_net)), "test_hit": float(np.mean(test_net > 0)), "test_std": float(np.std(test_net, ddof=1)) if len(test_net)>1 else 0,
                    "test_hit_ci_low": wilson_ci(int(np.sum(test_net > 0)), len(test_net))[0],
                    "test_hit_ci_high": wilson_ci(int(np.sum(test_net > 0)), len(test_net))[1],
                    "valid_perm_p": permutation_p(valid_net, seed=self.seed), "valid_q": 1.0, "selected": False,
                    "ci_low": ci[0], "ci_high": ci[1], "t_p": t_p, "perm_p": perm, "q_value": 1.0,
                    "profit_factor": profit_factor(test_net), "max_dd": max_drawdown(eq["equity"]) if eq else None,
                    "total_return": (eq["equity"][-1]-1) if eq else None,
                    "wf_positive_folds": wf_pos, "wf_folds": wf_folds, "robustness": robustness,
                    "created_at": now_iso(),
                })

        if not candidates:
            print("No patterns passed minimum sample sizes.")
            return pd.DataFrame()
        # Pattern direction is fixed on TRAIN. Selection is performed only on TRAIN + VALIDATION.
        # The TEST slice is therefore a true final holdout and never chooses the rule.
        valid_q = bh_qvalues([x["valid_perm_p"] for x in candidates])
        fdr = float(self.cfg.get("validation_fdr", 0.20))
        for x, qq in zip(candidates, valid_q):
            x["valid_q"] = qq
            # The selected side is learned on TRAIN. Validation must confirm
            # that same directional trade, including SHORT patterns.
            x["selected"] = bool(x["side"] * x["train_mean"] > 0 and x["side"] * x["valid_mean"] > 0 and qq <= fdr)
        selected_idx = [i for i, x in enumerate(candidates) if x["selected"]]
        selected_test_q = bh_qvalues([candidates[i]["perm_p"] for i in selected_idx])
        for i, qq in zip(selected_idx, selected_test_q):
            candidates[i]["q_value"] = qq
        self.db.execute("DELETE FROM pattern_results WHERE run_id=?", [run_id])
        cols = [
            "run_id","pattern_id","entity","event_class","token_symbol","chain","counterparty","sequence_pattern","flow_state",
            "size_bucket","portfolio_bucket","liquidity_bucket","volume_bucket","market_regime", "horizon_min","side","train_n","valid_n","test_n","train_mean","valid_mean","test_mean","test_median","test_hit","test_std","test_hit_ci_low","test_hit_ci_high",
            "valid_perm_p","valid_q","selected","ci_low","ci_high","t_p","perm_p","q_value","profit_factor","max_dd","total_return","wf_positive_folds","wf_folds","robustness","created_at"
        ]
        for x in candidates:
            self.db.execute(
                f"INSERT INTO pattern_results ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
                [x[c] for c in cols]
            )
        out = pd.DataFrame(candidates).sort_values(["selected", "q_value", "robustness", "test_mean"], ascending=[False, True, False, False])
        return out


def dedupe_signal_events(df: pd.DataFrame, cooldown_min: int) -> pd.DataFrame:
    """Collapse bursts of the same entity/action/token so one wallet operation is not counted as many independent samples."""
    if cooldown_min <= 0 or df.empty:
        return df
    event_cols = [c for c in df.columns if c != "horizon_min"]
    ev = df.sort_values("ts").drop_duplicates("event_id")[event_cols].copy()
    keep = set()
    gap = cooldown_min * 60
    for _, g in ev.groupby(["entity", "event_class", "token_symbol"], dropna=False):
        g = g.sort_values("ts")
        cluster = []
        cluster_end = None
        def flush(xs):
            if not xs: return
            best = max(xs, key=lambda r: abs(safe_float(r.get("usd_value")) or 0))
            keep.add(best["event_id"])
        for rec in g.to_dict("records"):
            t = int(rec["ts"])
            horizon_end = t + int(rec.get("horizon_min") or 0) * 60
            if cluster_end is None or t <= cluster_end + gap:
                cluster.append(rec); cluster_end = max(cluster_end or horizon_end, horizon_end)
            else:
                flush(cluster); cluster = [rec]; cluster_end = t
        flush(cluster)
    return df[df.event_id.isin(keep)].copy()


def trading_cost(cfg: Dict[str, Any]) -> float:
    """fee_bps/slippage_bps are per fill. Directional = entry+exit; excess = token+benchmark, entry+exit."""
    per_fill = (float(cfg.get("fee_bps", 0)) + float(cfg.get("slippage_bps", 0))) / 10_000.0
    fills = 4 if str(cfg.get("backtest_return_basis", "excess")).lower() == "excess" else 2
    return per_fill * fills


def strategy_returns(df: pd.DataFrame, side: int, cfg: Dict[str, Any]) -> np.ndarray:
    col = "excess_return" if str(cfg.get("backtest_return_basis", "excess")).lower() == "excess" else "raw_return"
    cost = trading_cost(cfg)
    r = side * df[col].to_numpy(dtype=float) - cost
    return r[np.isfinite(r)]


def nonoverlap_equity(df: pd.DataFrame, side: int, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if df.empty: return None
    g = df.sort_values("ts")
    cost = trading_cost(cfg)
    col = "excess_return" if str(cfg.get("backtest_return_basis", "excess")).lower() == "excess" else "raw_return"
    equity = [1.0]
    trades = []
    last_exit = -1
    for _, r in g.iterrows():
        entry = int(r.ts)
        exit_ts = entry + int(r.horizon_min) * 60
        if entry < last_exit:
            continue
        ret = side * float(r[col]) - cost
        equity.append(equity[-1] * (1 + ret))
        trades.append((entry, exit_ts, ret))
        last_exit = exit_ts
    return {"equity": equity, "trades": trades}


def walk_forward_score(g: pd.DataFrame, cfg: Dict[str, Any], folds=5) -> Tuple[int, int]:
    g = g.sort_values("ts").copy()
    n = len(g)
    if n < 20: return 0, 0
    step = max(3, n // (folds + 2))
    positives = total = 0
    for end_train in range(step * 2, n - step + 1, step):
        train = g.iloc[:end_train]
        test = g.iloc[end_train:min(end_train + step, n)]
        if len(test) < 3: continue
        col = "excess_return" if str(cfg.get("backtest_return_basis", "excess")).lower() == "excess" else "raw_return"
        side = 1 if train[col].mean() >= 0 else -1
        rr = strategy_returns(test, side, cfg)
        if len(rr) == 0: continue
        positives += int(float(np.mean(rr)) > 0)
        total += 1
    return positives, total


def robustness_score(train: np.ndarray, valid: np.ndarray, test: np.ndarray, wf_pos: int, wf_folds: int) -> float:
    def sign_consistency(a):
        return 1.0 if len(a) and np.mean(a) > 0 else 0.0
    score = 0.20 * sign_consistency(train) + 0.25 * sign_consistency(valid) + 0.35 * sign_consistency(test)
    score += 0.20 * (wf_pos / wf_folds if wf_folds else 0)
    return float(score)


# ----------------------------- dashboard -----------------------------------
def render_ui(db: DB, cfg: Dict[str, Any]):
    import streamlit as st
    import plotly.express as px
    import plotly.graph_objects as go

    st.set_page_config(page_title=APP_NAME, layout="wide")
    st.title(APP_NAME)
    st.caption("Entity behavior → semantic intent → market response → out-of-sample evidence")

    t = db.one("SELECT COUNT(*), COUNT(DISTINCT entity), MIN(ts), MAX(ts) FROM transfers") or (0,0,None,None)
    c = db.one("SELECT COUNT(*) FROM classifications") or (0,)
    e = db.one("SELECT COUNT(DISTINCT event_id) FROM event_returns") or (0,)
    p = db.one("SELECT COUNT(*) FROM pattern_results") or (0,)
    cols = st.columns(5)
    cols[0].metric("Transfers", f"{t[0]:,}")
    cols[1].metric("Entities", f"{t[1]:,}")
    cols[2].metric("Classified", f"{c[0]:,}")
    cols[3].metric("Priced events", f"{e[0]:,}")
    cols[4].metric("Patterns tested", f"{p[0]:,}")
    if t[2]: st.caption(f"Coverage: {ts_to_iso(t[2])[:10]} → {ts_to_iso(t[3])[:10]}")

    tabs = st.tabs(["Overview", "Events", "Entity", "Backtests", "Pattern Scanner", "LLM Review", "Data Quality"])

    with tabs[0]:
        x = db.df("""
            SELECT cl.event_class, COUNT(*) n, AVG(t.usd_value) avg_usd, AVG(cl.confidence) confidence
            FROM transfers t JOIN classifications cl ON cl.event_id=t.event_id
            GROUP BY 1 ORDER BY n DESC
        """)
        if not x.empty:
            st.plotly_chart(px.bar(x, x="event_class", y="n", hover_data=["avg_usd","confidence"]), use_container_width=True)
        recent = db.df("""
            SELECT to_timestamp(t.ts) time, t.entity, cl.event_class, t.token_symbol, t.usd_value,
                   t.from_label, t.to_label, cl.confidence
            FROM transfers t JOIN classifications cl ON cl.event_id=t.event_id
            ORDER BY t.ts DESC LIMIT 200
        """)
        st.dataframe(recent, use_container_width=True, hide_index=True)

    with tabs[1]:
        entities = [r[0] for r in db.execute("SELECT DISTINCT entity FROM transfers ORDER BY entity").fetchall()]
        classes = [r[0] for r in db.execute("SELECT DISTINCT event_class FROM classifications ORDER BY event_class").fetchall()]
        a,b,c1 = st.columns(3)
        ent = a.selectbox("Entity", ["ALL"] + entities, key="ev_ent")
        cls = b.selectbox("Event class", ["ALL"] + classes, key="ev_cls")
        minusd = c1.number_input("Min USD", value=0.0, step=100000.0)
        where, params = ["COALESCE(t.usd_value,0)>=?"], [minusd]
        if ent != "ALL": where.append("t.entity=?"); params.append(ent)
        if cls != "ALL": where.append("cl.event_class=?"); params.append(cls)
        q = f"""
            SELECT t.event_id, to_timestamp(t.ts) time, t.entity, cl.event_class, t.token_symbol,
                   t.usd_value, ctx.portfolio_pct, ef.market_regime, ef.flow_state, cl.confidence,
                   t.from_label, t.to_label, t.tx_hash
            FROM transfers t JOIN classifications cl ON cl.event_id=t.event_id
            LEFT JOIN contexts ctx ON ctx.event_id=t.event_id
            LEFT JOIN event_features ef ON ef.event_id=t.event_id
            WHERE {' AND '.join(where)} ORDER BY t.ts DESC LIMIT 5000
        """
        st.dataframe(db.df(q, params), use_container_width=True, hide_index=True)

    with tabs[2]:
        entities = [r[0] for r in db.execute("SELECT DISTINCT entity FROM transfers ORDER BY entity").fetchall()]
        if entities:
            ent = st.selectbox("Entity", entities, key="entity_detail")
            x = db.df("""
                SELECT to_timestamp(t.ts) time, t.ts, cl.event_class, t.token_symbol, t.usd_value,
                       ef.size_bucket, ef.market_regime, ef.flow_state, er.horizon_min,
                       er.raw_return, er.benchmark_return, er.excess_return
                FROM transfers t JOIN classifications cl USING(event_id)
                JOIN event_features ef USING(event_id) JOIN event_returns er USING(event_id)
                WHERE t.entity=? ORDER BY t.ts
            """, [ent])
            if not x.empty:
                cls = st.selectbox("Class", ["ALL"] + sorted(x.event_class.dropna().unique().tolist()))
                tok = st.selectbox("Token", ["ALL"] + sorted(x.token_symbol.dropna().unique().tolist()))
                y = x.copy()
                if cls != "ALL": y = y[y.event_class == cls]
                if tok != "ALL": y = y[y.token_symbol == tok]
                stat = y.groupby("horizon_min").excess_return.agg(["count","mean","median","std"]).reset_index()
                st.dataframe(stat, use_container_width=True, hide_index=True)
                fig = px.scatter(y, x="time", y="excess_return", color="event_class", hover_data=["token_symbol","usd_value","horizon_min"])
                fig.add_hline(y=0)
                st.plotly_chart(fig, use_container_width=True)

    with tabs[3]:
        latest = db.one("SELECT run_id FROM pattern_results ORDER BY created_at DESC LIMIT 1")
        if not latest:
            st.info("Run `python entity_alpha.py backtest` first.")
        else:
            run = latest[0]
            pats = db.df("SELECT * FROM pattern_results WHERE run_id=? ORDER BY q_value, robustness DESC", [run])
            if not pats.empty:
                pid = st.selectbox("Pattern", pats.pattern_id.tolist(), format_func=lambda pid: format_pattern(pats[pats.pattern_id==pid].iloc[0]))
                r = pats[pats.pattern_id == pid].iloc[0]
                st.json({k: (None if pd.isna(v) else v) for k,v in r.to_dict().items() if k not in ("run_id","pattern_id","created_at")})
                filt = pattern_filter_sql(r)
                x = db.df(f"""
                    SELECT t.ts, to_timestamp(t.ts) time, t.event_id, er.excess_return
                    FROM transfers t JOIN classifications cl USING(event_id)
                    JOIN event_features ef USING(event_id) JOIN event_returns er USING(event_id)
                    WHERE {filt[0]} ORDER BY t.ts
                """, filt[1])
                if not x.empty:
                    side = int(r.side)
                    cost = trading_cost(cfg)
                    basis = "excess_return" if str(cfg.get("backtest_return_basis", "excess")).lower() == "excess" else "raw_return"
                    x["strategy_return"] = side*x[basis]-cost
                    ntr=int(len(x)*cfg.get("train_frac",.6)); nva=int(len(x)*cfg.get("valid_frac",.2))
                    x["split"]="test"; x.loc[:ntr-1,"split"]="train"; x.loc[ntr:ntr+nva-1,"split"]="valid"
                    x["equity"]=(1+x.strategy_return).cumprod()
                    st.plotly_chart(px.line(x, x="time", y="equity", color="split", markers=True), use_container_width=True)
                    st.dataframe(x, use_container_width=True, hide_index=True)

    with tabs[4]:
        latest = db.one("SELECT run_id FROM pattern_results ORDER BY created_at DESC LIMIT 1")
        if latest:
            pats = db.df("""
                SELECT entity,event_class,token_symbol,chain,counterparty,sequence_pattern,flow_state,
                       size_bucket,portfolio_bucket,liquidity_bucket,volume_bucket,market_regime,horizon_min,
                       CASE WHEN side=1 THEN 'LONG' ELSE 'SHORT' END side,
                        train_n,valid_n,test_n,valid_mean,test_mean,test_hit,test_hit_ci_low,test_hit_ci_high,valid_perm_p,valid_q,selected,
                       ci_low,ci_high,perm_p,q_value,profit_factor,max_dd,total_return,wf_positive_folds,wf_folds,robustness
                FROM pattern_results WHERE run_id=? ORDER BY q_value, robustness DESC, test_mean DESC
            """, [latest[0]])
            only_selected = st.checkbox("Only patterns selected on TRAIN+VALIDATION", value=True)
            qmax = st.slider("Max TEST FDR q-value", 0.0, 1.0, 0.20, 0.01)
            robust = st.slider("Min robustness", 0.0, 1.0, 0.60, 0.05)
            show = pats[(pats.q_value <= qmax) & (pats.robustness >= robust)]
            if only_selected:
                show = show[show.selected == True]
            st.dataframe(show, use_container_width=True, hide_index=True)
        else: st.info("No backtest run yet.")

    with tabs[5]:
        x = db.df("""
            SELECT t.event_id, to_timestamp(t.ts) time, t.entity, t.token_symbol, t.usd_value,
                   cl.event_class, cl.confidence, cl.rationale, cl.primary_json, cl.auditor_json, cl.final_json
            FROM transfers t JOIN classifications cl USING(event_id)
            ORDER BY cl.confidence ASC, t.ts DESC LIMIT 500
        """)
        st.dataframe(x[[c for c in x.columns if c not in ("primary_json","auditor_json","final_json")]], use_container_width=True, hide_index=True)
        if not x.empty:
            eid = st.selectbox("Inspect raw LLM decision", x.event_id.tolist())
            rr = x[x.event_id==eid].iloc[0]
            st.subheader("grok-4.6 primary"); st.json(json_loads(rr.primary_json, {}))
            st.subheader("grok-4.5 audit"); st.json(json_loads(rr.auditor_json, {}))
            st.subheader("final"); st.json(json_loads(rr.final_json, {}))

    with tabs[6]:
        quality = {
            "transfers_without_classification": db.one("SELECT COUNT(*) FROM transfers t LEFT JOIN classifications c USING(event_id) WHERE c.event_id IS NULL")[0],
            "classified_without_market_features": db.one("SELECT COUNT(*) FROM classifications c LEFT JOIN event_features e USING(event_id) WHERE e.event_id IS NULL")[0],
            "events_without_any_forward_return": db.one("SELECT COUNT(*) FROM transfers t LEFT JOIN event_returns r USING(event_id) WHERE r.event_id IS NULL")[0],
            "events_with_historical_portfolio": db.one("SELECT COUNT(*) FROM contexts WHERE portfolio_usd IS NOT NULL")[0],
            "low_confidence_llm": db.one("SELECT COUNT(*) FROM classifications WHERE confidence < 0.70")[0],
            "events_without_benchmark_adjusted_return": db.one("SELECT COUNT(*) FROM transfers t LEFT JOIN event_returns r USING(event_id) WHERE r.event_id IS NULL OR r.excess_return IS NULL")[0],
            "classified_with_empty_sequence": db.one("SELECT COUNT(*) FROM contexts WHERE sequence_json IS NULL OR sequence_json='[]'")[0],
            "unknown_classifications": db.one("SELECT COUNT(*) FROM classifications WHERE event_class='UNKNOWN'")[0],
        }
        st.json(quality)
        routes = db.df("SELECT job,path,method,score,updated_at FROM route_cache ORDER BY job")
        st.subheader("Auto-discovered API Hub routes")
        st.dataframe(routes, use_container_width=True, hide_index=True)


def format_pattern(r: pd.Series) -> str:
    bits = [str(r.entity), str(r.event_class), str(r.token_symbol)]
    for name in ("chain", "counterparty", "sequence_pattern", "flow_state", "size_bucket", "liquidity_bucket", "volume_bucket", "market_regime"):
        v = getattr(r, name, "*")
        if v not in ("*", None) and not (isinstance(v, float) and pd.isna(v)):
            bits.append(str(v))
    bits.append(f"{int(r.horizon_min)}m")
    bits.append("LONG" if int(r.side)==1 else "SHORT")
    return " | ".join(bits)


def pattern_filter_sql(r: pd.Series) -> Tuple[str, List[Any]]:
    clauses = ["t.entity=?", "cl.event_class=?", "t.token_symbol=?", "er.horizon_min=?"]
    params = [r.entity, r.event_class, r.token_symbol, int(r.horizon_min)]
    if getattr(r, "chain", "*") != "*": clauses.append("t.chain=?"); params.append(r.chain)
    if getattr(r, "counterparty", "*") != "*":
        clauses.append("(CASE WHEN t.direction='out' THEN COALESCE(t.to_label,t.to_address,'UNKNOWN') WHEN t.direction='in' THEN COALESCE(t.from_label,t.from_address,'UNKNOWN') ELSE COALESCE(t.to_label,t.from_label,'UNKNOWN') END)=?")
        params.append(r.counterparty)
    if getattr(r, "sequence_pattern", "*") != "*":
        clauses.append("COALESCE(json_extract_string(cl.final_json, '$.sequence_pattern'), 'UNKNOWN')=?"); params.append(r.sequence_pattern)
    if getattr(r, "flow_state", "*") != "*": clauses.append("ef.flow_state=?"); params.append(r.flow_state)
    if r.size_bucket != "*": clauses.append("ef.size_bucket=?"); params.append(r.size_bucket)
    if r.portfolio_bucket != "*": clauses.append("ef.portfolio_bucket=?"); params.append(r.portfolio_bucket)
    if getattr(r, "liquidity_bucket", "*") != "*": clauses.append("ef.liquidity_bucket=?"); params.append(r.liquidity_bucket)
    if getattr(r, "volume_bucket", "*") != "*": clauses.append("ef.volume_bucket=?"); params.append(r.volume_bucket)
    if r.market_regime != "*": clauses.append("ef.market_regime=?"); params.append(r.market_regime)
    return " AND ".join(clauses), params


# ----------------------------- orchestration -------------------------------
def make_components(cfg: Dict[str, Any]):
    load_dotenv(HERE / ".env")
    db = DB(cfg["database"])
    hub = HubClient(db, os.getenv("API_HUB_KEY"))
    llm = LLM(os.getenv("LLMASS_API_KEY"), os.getenv("LLMASS_BASE_URL", DEFAULT_LLM_BASE), cfg.get("llm_primary","grok-4.6"), cfg.get("llm_auditor","grok-4.5"))
    return db, hub, llm


def doctor(cfg, db, hub, llm):
    print(APP_NAME)
    print(f"DB: {db.path}")
    print("API_HUB_KEY:", "OK" if hub.key else "MISSING")
    print("LLMASS_API_KEY:", "OK" if llm.key else "MISSING")
    print("LLM endpoint:", llm.base)
    try:
        spec = hub.get_openapi()
        print("Hub OpenAPI:", "OK", "paths=", len(spec.get("paths", {})))
    except Exception as ex:
        print("Hub OpenAPI: FAIL", ex)
    print("Entities:", ", ".join(cfg.get("entities", [])))


def discover_all(hub: HubClient):
    """Verify pinned core routes. No fuzzy route selection for production jobs."""
    jobs = [
        "arkham_transfers",
        "arkham_transfers_unenriched",
        "arkham_history_entity",
        "arkham_portfolio_entity",
        "gecko_token_pools",
        "gecko_search_pools",
        "gecko_ohlcv",
        "debank_all_history",
        "arkham_tx_detail",
    ]
    for job in jobs:
        try:
            op = hub.pinned(job, optional=False)
            print(f"{job:28} {op.method:4} {op.path}")
        except Exception as ex:
            print(f"{job:28} unavailable: {ex}")

    rpc = hub.strict_rpc(optional=True)
    if rpc:
        print(f"{'hub_rpc':28} {rpc.method:4} {rpc.path}")
    else:
        print(f"{'hub_rpc':28} optional: no explicit unified RPC route found; RPC verification disabled")


def validate_collection_access(hub: HubClient, cfg: Dict[str, Any]) -> None:
    """Perform a tiny upstream request and report Hub-auth separately from provider-auth."""
    entity = next(iter(cfg.get("entities", [])), None)
    if not entity:
        raise RuntimeError("No entities configured")
    op = hub.pinned("arkham_transfers")
    try:
        payload = hub.call(op, {"base": entity, "flow": "all", "limit": 1, "offset": 0})
    except RuntimeError as ex:
        msg = str(ex)
        if "upstream provider returned 401" in msg:
            raise RuntimeError(
                "Hub key was accepted, but Arkham upstream returned 401. This account/key "
                "does not currently have working access to the enriched Arkham transfers source."
            ) from ex
        if "Hub 401" in msg:
            raise RuntimeError("Hub rejected API_HUB_KEY (HTTP 401); verify the key and its Hub permissions.") from ex
        raise
    print(f"Arkham transfers access: OK; sample rows={len(first_list(payload))}")

def run_collect(cfg, db, hub):
    ark = ArkhamAdapter(hub, db, cfg)
    validate_collection_access(hub, cfg)
    total = 0
    for ent in cfg.get("entities", []):
        print(f"Collecting {ent}...")
        n = ark.collect_entity(ent)
        total += n
        print(f"  {n} normalized transfers")
    print("Total:", total)


def run_classify(cfg, db, hub, llm, limit=None):
    ark = ArkhamAdapter(hub, db, cfg)
    debank = DeBankAdapter(hub)
    rpc = RPCAdapter(hub)
    builder = ContextBuilder(db, ark, debank=debank, rpc=rpc)
    Classifier(db, builder, llm).classify_pending(limit=limit)


def run_enrich(cfg, db, hub, limit=None):
    gecko = GeckoAdapter(hub, db)
    Enricher(db, gecko, cfg).enrich_all(limit=limit)


def run_backtest(cfg, db):
    out = Backtester(db, cfg).run()
    if not out.empty:
        cols = ["selected","entity","event_class","token_symbol","sequence_pattern","size_bucket","market_regime","horizon_min","side","valid_q","test_n","test_mean","test_hit","perm_p","q_value","profit_factor","max_dd","robustness"]
        print(out[cols].head(30).to_string(index=False))


def parse_cli():
    p = argparse.ArgumentParser(description=APP_NAME)
    p.add_argument("command", nargs="?", default="doctor", choices=["doctor","discover","collect","classify","enrich","backtest","run-all"])
    p.add_argument("--config", default=str(DEFAULT_CONFIG))
    p.add_argument("--limit", type=int, default=None, help="Limit classify/enrich for smoke tests")
    p.add_argument("--ui", action="store_true")
    return p.parse_args()


def main():
    # Streamlit passes our --ui after `--`; detect before argparse noise.
    if "--ui" in sys.argv:
        cfg_path = DEFAULT_CONFIG
        if "--config" in sys.argv:
            try: cfg_path = Path(sys.argv[sys.argv.index("--config")+1])
            except Exception: pass
        cfg = load_config(Path(cfg_path))
        db, hub, llm = make_components(cfg)
        render_ui(db, cfg)
        return

    args = parse_cli()
    cfg = load_config(Path(args.config))
    db, hub, llm = make_components(cfg)
    if args.command == "doctor": doctor(cfg, db, hub, llm)
    elif args.command == "discover": discover_all(hub)
    elif args.command == "collect": run_collect(cfg, db, hub)
    elif args.command == "classify": run_classify(cfg, db, hub, llm, args.limit)
    elif args.command == "enrich": run_enrich(cfg, db, hub, args.limit)
    elif args.command == "backtest": run_backtest(cfg, db)
    elif args.command == "run-all":
        discover_all(hub)
        validate_collection_access(hub, cfg)
        run_collect(cfg, db, hub)
        run_classify(cfg, db, hub, llm, args.limit)
        run_enrich(cfg, db, hub, args.limit)
        run_backtest(cfg, db)


if __name__ == "__main__":
    main()
