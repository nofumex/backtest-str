from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .settings import Settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    if is_dataclass(value):
        value = asdict(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS raw_responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    endpoint_key TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    sha256 TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS entities (
    entity_id TEXT PRIMARY KEY,
    name TEXT,
    entity_type TEXT,
    num_addresses INTEGER,
    volume_usd REAL,
    balance_usd REAL,
    first_tx TEXT,
    last_tx TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wallets (
    entity_id TEXT NOT NULL,
    address TEXT NOT NULL,
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    ownership_confidence REAL NOT NULL DEFAULT 1.0,
    role TEXT NOT NULL DEFAULT 'unknown',
    role_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    first_seen TEXT,
    last_seen TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(entity_id, address, chain, source)
);
CREATE INDEX IF NOT EXISTS idx_wallet_entity ON wallets(entity_id);
CREATE INDEX IF NOT EXISTS idx_wallet_addr ON wallets(address);

CREATE TABLE IF NOT EXISTS entity_snapshots (
    entity_id TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(entity_id, source, kind, captured_at)
);

CREATE TABLE IF NOT EXISTS wallet_positions (
    entity_id TEXT NOT NULL,
    address TEXT NOT NULL,
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(entity_id, address, chain, source, captured_at)
);

CREATE TABLE IF NOT EXISTS wallet_events (
    event_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    chain TEXT NOT NULL,
    tx_hash TEXT,
    event_index INTEGER,
    ts INTEGER NOT NULL,
    source TEXT NOT NULL,
    action_type TEXT NOT NULL,
    cate_id TEXT,
    cex_id TEXT,
    project_id TEXT,
    usd_value REAL,
    primary_token_id TEXT,
    primary_asset_key TEXT,
    target_asset_key TEXT,
    evidence_json TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_entity_time ON wallet_events(entity_id, ts);
CREATE INDEX IF NOT EXISTS idx_event_wallet_time ON wallet_events(wallet, ts);
CREATE INDEX IF NOT EXISTS idx_event_tx ON wallet_events(tx_hash);

CREATE TABLE IF NOT EXISTS tx_enrichment (
    tx_hash TEXT NOT NULL,
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    PRIMARY KEY(tx_hash, chain, source)
);

CREATE TABLE IF NOT EXISTS wallet_roles (
    entity_id TEXT NOT NULL,
    wallet TEXT NOT NULL,
    role TEXT NOT NULL,
    confidence REAL NOT NULL,
    probabilities_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    classified_at TEXT NOT NULL,
    PRIMARY KEY(entity_id, wallet)
);

CREATE TABLE IF NOT EXISTS episodes (
    episode_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    end_ts INTEGER NOT NULL,
    wallets_json TEXT NOT NULL,
    event_ids_json TEXT NOT NULL,
    motif TEXT NOT NULL,
    primary_asset_key TEXT,
    gross_usd REAL,
    evidence_json TEXT NOT NULL,
    intent_label TEXT,
    intent_json TEXT,
    intent_confidence REAL,
    classified_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_episode_entity_time ON episodes(entity_id, start_ts);

CREATE TABLE IF NOT EXISTS episode_market_context (
    episode_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    btc_return_24h REAL,
    eth_return_24h REAL,
    btc_volatility_proxy REAL,
    eth_volatility_proxy REAL,
    regime TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_labels (
    episode_id TEXT NOT NULL,
    asset_key TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    source TEXT NOT NULL,
    price_start REAL,
    price_end REAL,
    simple_return REAL,
    btc_return REAL,
    eth_return REAL,
    excess_vs_btc REAL,
    excess_vs_eth REAL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(episode_id, asset_key, horizon_seconds, source)
);

CREATE TABLE IF NOT EXISTS market_price_cache (
    cache_key TEXT PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    coins_json TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_time_series (
    asset_key TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    bucket INTEGER NOT NULL,
    price REAL,
    source TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    resolution INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'success',
    reason TEXT,
    expires_at REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(asset_key, timestamp, resolution, source)
);
CREATE INDEX IF NOT EXISTS idx_market_series_lookup ON market_time_series(asset_key,timestamp,status);

CREATE TABLE IF NOT EXISTS wallet_history_coverage (
    entity_id TEXT NOT NULL,
    address TEXT NOT NULL,
    chain TEXT NOT NULL,
    covered_from INTEGER NOT NULL DEFAULT 0,
    covered_to INTEGER NOT NULL DEFAULT 0,
    cursor INTEGER NOT NULL DEFAULT 0,
    pages INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(entity_id,address,chain)
);

CREATE TABLE IF NOT EXISTS deferred_jobs (
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    item_key TEXT NOT NULL,
    entity_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry REAL NOT NULL DEFAULT 0,
    error TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id,kind,item_key)
);
CREATE INDEX IF NOT EXISTS idx_deferred_jobs_queue ON deferred_jobs(run_id,kind,state,next_retry,priority);

CREATE TABLE IF NOT EXISTS artifact_provenance (
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(artifact_type,artifact_id)
);

CREATE TABLE IF NOT EXISTS analysis_invalidations (
    run_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    PRIMARY KEY(run_id,layer,pattern_key)
);

CREATE TABLE IF NOT EXISTS enrichment_invalidations (
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id,event_id)
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    captured_at TEXT NOT NULL,
    source TEXT NOT NULL,
    instrument TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(captured_at, source, instrument, kind)
);

CREATE TABLE IF NOT EXISTS backtest_results (
    run_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    motif TEXT NOT NULL,
    intent_label TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    asset_key TEXT NOT NULL,
    n INTEGER NOT NULL,
    mean_return REAL,
    median_return REAL,
    std_return REAL,
    negative_rate REAL,
    positive_rate REAL,
    ci_low REAL,
    ci_high REAL,
    sign_pvalue REAL,
    qvalue REAL,
    shrunk_mean REAL,
    train_n INTEGER,
    test_n INTEGER,
    test_mean_return REAL,
    test_direction_accuracy REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, entity_id, scope_type, scope_id, motif, intent_label, horizon_seconds, asset_key)
);
"""


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.run_id = None
        self.run_bounds = (0, 2**62)
        settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        settings.raw_dir.mkdir(parents=True, exist_ok=True)
        settings.report_dir.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._archive_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="raw-writer")
        self._archive_slots = threading.BoundedSemaphore(256)
        with self.conn() as db:
            db.executescript(SCHEMA)
            from .migrations import migrate
            migrate(db)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(episodes)")}
            if "target_asset_key" not in columns:
                db.execute("ALTER TABLE episodes ADD COLUMN target_asset_key TEXT")
                db.execute("UPDATE episodes SET target_asset_key=primary_asset_key WHERE target_asset_key IS NULL")

    def close(self) -> None:
        self._archive_executor.shutdown(wait=True)
        db = getattr(self._local, "db", None)
        if db is not None:
            db.close()
            self._local.db = None

    def _connect(self) -> sqlite3.Connection:
        db = getattr(self._local, "db", None)
        if db is None:
            db = sqlite3.connect(self.settings.db_path, timeout=30)
            db.row_factory = sqlite3.Row
            self._local.db = db
        return db

    @contextmanager
    def conn(self):
        db = self._connect()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            pass

    def archive_raw(self, provider: str, endpoint_key: str, request: dict[str, Any], response: Any) -> str:
        record = {
            "provider": provider,
            "endpoint_key": endpoint_key,
            "request": request,
            "response": response,
            "fetched_at": utc_now_iso(),
        }
        blob = canonical_json(record).encode("utf-8")
        digest = hashlib.sha256(blob).hexdigest()
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        folder = self.settings.raw_dir / provider / day
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{digest}.json.gz"
        if not path.exists():
            with gzip.open(path, "wb") as f:
                f.write(blob)
        db = sqlite3.connect(self.settings.db_path, timeout=30)
        try:
            db.execute(
                "INSERT OR IGNORE INTO raw_responses(provider,endpoint_key,request_json,response_json,fetched_at,sha256) VALUES(?,?,?,?,?,?)",
                (provider, endpoint_key, canonical_json(request), canonical_json(response), record["fetched_at"], digest),
            )
            db.commit()
        finally:
            db.close()
        return digest

    def archive_raw_queued(self, provider: str, endpoint_key: str, request: dict[str, Any], response: Any) -> Future:
        self._archive_slots.acquire()
        future = self._archive_executor.submit(self.archive_raw, provider, endpoint_key, request, response)
        future.add_done_callback(lambda _: self._archive_slots.release())
        return future

    def upsert_entity(self, entity_id: str, name: str | None, summary: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        with self.conn() as db:
            db.execute(
                """INSERT INTO entities(entity_id,name,entity_type,num_addresses,volume_usd,balance_usd,first_tx,last_tx,metadata_json,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(entity_id) DO UPDATE SET
                    name=excluded.name, entity_type=excluded.entity_type, num_addresses=excluded.num_addresses,
                    volume_usd=excluded.volume_usd, balance_usd=excluded.balance_usd,
                    first_tx=excluded.first_tx, last_tx=excluded.last_tx,
                    metadata_json=excluded.metadata_json, updated_at=excluded.updated_at""",
                (
                    entity_id,
                    name,
                    (metadata or {}).get("type"),
                    summary.get("numAddresses"),
                    summary.get("volumeUsd"),
                    summary.get("balanceUsd"),
                    summary.get("firstTx"),
                    summary.get("lastTx"),
                    canonical_json(metadata or {}),
                    utc_now_iso(),
                ),
            )

    def upsert_wallet(self, entity_id: str, address: str, chain: str, source: str, metadata: dict[str, Any] | None = None, confidence: float = 1.0) -> None:
        if not address:
            return
        norm = address.lower() if address.startswith("0x") else address
        with self.conn() as db:
            db.execute(
                """INSERT INTO wallets(entity_id,address,chain,source,ownership_confidence,metadata_json,updated_at)
                   VALUES(?,?,?,?,?,?,?)
                   ON CONFLICT(entity_id,address,chain,source) DO UPDATE SET
                    ownership_confidence=MAX(wallets.ownership_confidence,excluded.ownership_confidence),
                    metadata_json=excluded.metadata_json, updated_at=excluded.updated_at""",
                (entity_id, norm, chain, source, confidence, canonical_json(metadata or {}), utc_now_iso()),
            )

    def save_entity_snapshot(self, entity_id: str, source: str, kind: str, payload: Any) -> None:
        with self.conn() as db:
            db.execute(
                "INSERT INTO entity_snapshots(entity_id,source,kind,captured_at,payload_json) VALUES(?,?,?,?,?)",
                (entity_id, source, kind, utc_now_iso(), canonical_json(payload)),
            )

    def save_position(self, entity_id: str, address: str, chain: str, source: str, payload: Any, request_key: str | None = None) -> None:
        with self.conn() as db:
            db.execute(
                "INSERT OR REPLACE INTO wallet_positions(entity_id,address,chain,source,captured_at,payload_json) VALUES(?,?,?,?,?,?)",
                (entity_id, address, chain, f"{source}|{request_key}" if request_key else source, utc_now_iso(), canonical_json(payload)),
            )

    def position_exists(self, entity_id: str, address: str, chain: str, source: str, request_key: str | None = None) -> bool:
        source = f"{source}|{request_key}" if request_key else source
        return self.fetchone("SELECT 1 FROM wallet_positions WHERE entity_id=? AND address=? AND chain=? AND source=? LIMIT 1", (entity_id, address, chain, source)) is not None

    def enrichment_exists(self, tx_hash: str, chain: str, source: str) -> bool:
        return self.fetchone("SELECT 1 FROM tx_enrichment WHERE tx_hash=? AND chain=? AND source=? LIMIT 1", (tx_hash, chain, source)) is not None

    def save_event(self, event: dict[str, Any]) -> None:
        self.save_events([event])

    def save_events(self, events: Iterable[dict[str, Any]]) -> None:
        events = list(events)
        now = utc_now_iso()
        values = [(
            e["event_id"], e["entity_id"], e["wallet"], e["chain"], e.get("tx_hash"), e.get("event_index"),
            e["ts"], e["source"], e["action_type"], e.get("cate_id"), e.get("cex_id"), e.get("project_id"),
            e.get("usd_value"), e.get("primary_token_id"), e.get("primary_asset_key"),
            canonical_json(e.get("evidence", {})), canonical_json(e.get("raw", {})), now,
        ) for e in events]
        if not values:
            return
        from .normalize import asset_key
        assets: dict[str, tuple[str, str | None]] = {}
        for event in events:
            for token_id, token in (event.get("evidence", {}).get("tokens") or {}).items():
                if isinstance(token, dict):
                    key = asset_key(event["chain"], token_id)
                    if key and token.get("symbol"):
                        assets[key] = (str(token["symbol"]), token.get("name"))
        with self.conn() as db:
            db.executemany(
                """INSERT OR IGNORE INTO wallet_events(event_id,entity_id,wallet,chain,tx_hash,event_index,ts,source,
                action_type,cate_id,cex_id,project_id,usd_value,primary_token_id,primary_asset_key,evidence_json,raw_json,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", values,
            )
            db.executemany(
                """INSERT INTO assets(asset_key,symbol,name) VALUES(?,?,?)
                   ON CONFLICT(asset_key) DO UPDATE SET symbol=excluded.symbol,name=excluded.name""",
                [(key, symbol, name) for key, (symbol, name) in assets.items()],
            )
            if self.run_id:
                eligible = [e for e in events if self.run_bounds[0] <= e["ts"] <= self.run_bounds[1]]
                db.executemany("INSERT OR IGNORE INTO run_events(run_id,event_id) VALUES(?,?)",
                               [(self.run_id, e["event_id"]) for e in eligible])

    def market_price_get(self, timestamp: int, coins: tuple[str, ...]) -> dict[str, Any] | None:
        key = hashlib.sha256(canonical_json([int(timestamp), coins]).encode()).hexdigest()
        row = self.fetchone("SELECT payload_json FROM market_price_cache WHERE cache_key=?", (key,))
        return json.loads(row["payload_json"]) if row else None

    def market_price_put(self, timestamp: int, coins: tuple[str, ...], payload: dict[str, Any]) -> None:
        key = hashlib.sha256(canonical_json([int(timestamp), coins]).encode()).hexdigest()
        with self.conn() as db:
            db.execute("INSERT OR IGNORE INTO market_price_cache(cache_key,timestamp,coins_json,payload_json) VALUES(?,?,?,?)",
                       (key, int(timestamp), canonical_json(coins), canonical_json(payload)))

    def save_enrichment(self, tx_hash: str, chain: str, source: str, payload: Any) -> None:
        with self.conn() as db:
            db.execute(
                "INSERT OR REPLACE INTO tx_enrichment(tx_hash,chain,source,payload_json,captured_at) VALUES(?,?,?,?,?)",
                (tx_hash, chain, source, canonical_json(payload), utc_now_iso()),
            )

    def save_wallet_role(self, entity_id: str, wallet: str, role: str, confidence: float, probabilities: dict[str, float], evidence: Any) -> None:
        with self.conn() as db:
            db.execute(
                "INSERT OR REPLACE INTO wallet_roles(entity_id,wallet,role,confidence,probabilities_json,evidence_json,classified_at) VALUES(?,?,?,?,?,?,?)",
                (entity_id, wallet, role, confidence, canonical_json(probabilities), canonical_json(evidence), utc_now_iso()),
            )
            db.execute("UPDATE wallets SET role=?, role_json=? WHERE entity_id=? AND address=?", (role, canonical_json(probabilities), entity_id, wallet))

    def save_episode(self, episode: dict[str, Any]) -> None:
        with self.conn() as db:
            db.execute(
                """INSERT INTO episodes(episode_id,entity_id,start_ts,end_ts,wallets_json,event_ids_json,motif,primary_asset_key,target_asset_key,gross_usd,evidence_json,
                   intent_label,intent_json,intent_confidence,classified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(episode_id) DO UPDATE SET
                     entity_id=excluded.entity_id,start_ts=excluded.start_ts,end_ts=excluded.end_ts,
                     wallets_json=excluded.wallets_json,event_ids_json=excluded.event_ids_json,motif=excluded.motif,
                     primary_asset_key=excluded.primary_asset_key,target_asset_key=excluded.target_asset_key,
                     gross_usd=excluded.gross_usd,evidence_json=excluded.evidence_json""",
                (
                    episode["episode_id"], episode["entity_id"], episode["start_ts"], episode["end_ts"],
                    canonical_json(episode["wallets"]), canonical_json(episode["event_ids"]), episode["motif"],
                    episode.get("primary_asset_key"), episode.get("target_asset_key") or episode.get("primary_asset_key"), episode.get("gross_usd"), canonical_json(episode.get("evidence", {})),
                    episode.get("intent_label"), canonical_json(episode.get("intent", {})) if episode.get("intent") is not None else None,
                    episode.get("intent_confidence"), episode.get("classified_at"),
                ),
            )

        with self.conn() as db:
            db.executemany("INSERT OR IGNORE INTO market_horizons(episode_id,horizon_seconds) VALUES(?,?)", [(episode["episode_id"],h) for h in (300,3600,21600,86400,259200)])

    def save_episodes(self, episodes: Iterable[dict[str, Any]], *, run_id: str | None = None,
                      replace_episode_ids: Iterable[str] = ()) -> None:
        episodes = list(episodes)
        if not episodes:
            return
        values = [(
            ep["episode_id"], ep["entity_id"], ep["start_ts"], ep["end_ts"], canonical_json(ep["wallets"]),
            canonical_json(ep["event_ids"]), ep["motif"], ep.get("primary_asset_key"),
            ep.get("target_asset_key") or ep.get("primary_asset_key"), ep.get("gross_usd"),
            canonical_json(ep.get("evidence", {})), ep.get("intent_label"),
            canonical_json(ep.get("intent", {})) if ep.get("intent") is not None else None,
            ep.get("intent_confidence"), ep.get("classified_at"),
        ) for ep in episodes]
        with self.conn() as db:
            if run_id:
                db.executemany("DELETE FROM run_episodes WHERE run_id=? AND episode_id=?",
                               [(run_id, episode_id) for episode_id in replace_episode_ids])
            db.executemany(
                """INSERT INTO episodes(episode_id,entity_id,start_ts,end_ts,wallets_json,event_ids_json,motif,
                   primary_asset_key,target_asset_key,gross_usd,evidence_json,intent_label,intent_json,
                   intent_confidence,classified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(episode_id) DO UPDATE SET entity_id=excluded.entity_id,start_ts=excluded.start_ts,
                   end_ts=excluded.end_ts,wallets_json=excluded.wallets_json,event_ids_json=excluded.event_ids_json,
                   motif=excluded.motif,primary_asset_key=excluded.primary_asset_key,
                   target_asset_key=excluded.target_asset_key,gross_usd=excluded.gross_usd,
                   intent_label=CASE WHEN episodes.evidence_json=excluded.evidence_json THEN episodes.intent_label END,
                   intent_json=CASE WHEN episodes.evidence_json=excluded.evidence_json THEN episodes.intent_json END,
                   intent_confidence=CASE WHEN episodes.evidence_json=excluded.evidence_json THEN episodes.intent_confidence END,
                   classified_at=CASE WHEN episodes.evidence_json=excluded.evidence_json THEN episodes.classified_at END,
                   evidence_json=excluded.evidence_json""", values,
            )
            if run_id:
                db.executemany("INSERT OR IGNORE INTO run_episodes(run_id,episode_id) VALUES(?,?)",
                               [(run_id, ep["episode_id"]) for ep in episodes])
            db.executemany("INSERT OR IGNORE INTO market_horizons(episode_id,horizon_seconds) VALUES(?,?)",
                           [(ep["episode_id"], horizon) for ep in episodes
                            for horizon in (300, 3600, 21600, 86400, 259200)])

    def update_episode_intent(self, episode_id: str, label: str, intent: dict[str, Any], confidence: float) -> None:
        with self.conn() as db:
            db.execute(
                "UPDATE episodes SET intent_label=?, intent_json=?, intent_confidence=?, classified_at=? WHERE episode_id=?",
                (label, canonical_json(intent), confidence, utc_now_iso(), episode_id),
            )

    def save_episode_market_context(self, row: dict[str, Any]) -> None:
        with self.conn() as db:
            db.execute(
                """INSERT OR REPLACE INTO episode_market_context(episode_id,source,btc_return_24h,eth_return_24h,btc_volatility_proxy,eth_volatility_proxy,regime,payload_json)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (row["episode_id"], row["source"], row.get("btc_return_24h"), row.get("eth_return_24h"),
                 row.get("btc_volatility_proxy"), row.get("eth_volatility_proxy"), row["regime"], canonical_json(row.get("payload", {}))),
            )

    def save_market_label(self, row: dict[str, Any]) -> None:
        with self.conn() as db:
            db.execute(
                """INSERT OR REPLACE INTO market_labels(episode_id,asset_key,horizon_seconds,source,price_start,price_end,simple_return,btc_return,eth_return,
                   excess_vs_btc,excess_vs_eth,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["episode_id"], row["asset_key"], row["horizon_seconds"], row["source"], row.get("price_start"), row.get("price_end"),
                    row.get("simple_return"), row.get("btc_return"), row.get("eth_return"), row.get("excess_vs_btc"), row.get("excess_vs_eth"),
                    canonical_json(row.get("payload", {})),
                ),
            )

    def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.conn() as db:
            return [dict(r) for r in db.execute(sql, tuple(params)).fetchall()]

    def fetchone(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        with self.conn() as db:
            row = db.execute(sql, tuple(params)).fetchone()
            return dict(row) if row else None
