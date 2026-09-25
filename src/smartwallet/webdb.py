from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


WEB_SCHEMA = r"""
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;

CREATE TABLE IF NOT EXISTS analysis_runs (
    run_id TEXT PRIMARY KEY,
    sequence INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL,
    desired_status TEXT NOT NULL,
    mode TEXT NOT NULL,
    from_date TEXT NOT NULL,
    to_date TEXT NOT NULL,
    entities_json TEXT NOT NULL,
    settings_json TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'queued',
    current_work TEXT,
    progress REAL NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    analysis_updated_at TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS run_entities (
    run_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    name TEXT NOT NULL,
    wallets_total INTEGER NOT NULL DEFAULT 0,
    wallets_processed INTEGER NOT NULL DEFAULT 0,
    wallets_failed INTEGER NOT NULL DEFAULT 0,
    events INTEGER NOT NULL DEFAULT 0,
    episodes INTEGER NOT NULL DEFAULT 0,
    classified INTEGER NOT NULL DEFAULT 0,
    market_labels INTEGER NOT NULL DEFAULT 0,
    usable_observations INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id, entity_id),
    FOREIGN KEY(run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_wallets (
    run_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    address TEXT NOT NULL,
    chain TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    events INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    started_at TEXT,
    finished_at TEXT,
    PRIMARY KEY(run_id, entity_id, address)
);

CREATE TABLE IF NOT EXISTS run_watermarks (
    run_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    entity_id TEXT NOT NULL DEFAULT '',
    value_integer INTEGER NOT NULL DEFAULT 0,
    value_text TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id, stream, entity_id)
);

CREATE TABLE IF NOT EXISTS run_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    entity_id TEXT,
    item TEXT,
    message TEXT NOT NULL,
    retryable INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_errors_run ON run_errors(run_id, id DESC);

CREATE TABLE IF NOT EXISTS run_metrics (
    run_id TEXT PRIMARY KEY,
    api_requests INTEGER NOT NULL DEFAULT 0,
    api_success INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    retries INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    llm_completed INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_observations (
    run_id TEXT NOT NULL,
    observation_key TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    pattern TEXT NOT NULL,
    intent_label TEXT NOT NULL,
    asset_key TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    episode_ts INTEGER NOT NULL,
    return_value REAL NOT NULL,
    gross_usd REAL,
    PRIMARY KEY(run_id, observation_key)
);
CREATE INDEX IF NOT EXISTS idx_obs_group ON analysis_observations(run_id, entity_id, pattern, intent_label, asset_key, horizon_seconds, episode_ts);

CREATE TABLE IF NOT EXISTS pattern_aggregates (
    run_id TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    pattern TEXT NOT NULL,
    intent_label TEXT NOT NULL,
    asset_key TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    n INTEGER NOT NULL,
    sum_return REAL NOT NULL,
    sum_sq_return REAL NOT NULL,
    negative_count INTEGER NOT NULL,
    positive_count INTEGER NOT NULL,
    mean_return REAL NOT NULL,
    median_return REAL,
    ci_low REAL,
    ci_high REAL,
    sign_pvalue REAL,
    qvalue REAL,
    shrunk_mean REAL,
    train_n INTEGER,
    test_n INTEGER,
    test_mean_return REAL,
    holdout_accuracy REAL,
    maturity TEXT NOT NULL DEFAULT 'EARLY',
    robust_n INTEGER NOT NULL DEFAULT 0,
    last_expensive_n INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(run_id, pattern_key)
);
CREATE INDEX IF NOT EXISTS idx_patterns_rank ON pattern_aggregates(run_id, maturity, n DESC);

CREATE TABLE IF NOT EXISTS pattern_checkpoints (
    run_id TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    n INTEGER NOT NULL,
    negative_rate REAL NOT NULL,
    mean_return REAL NOT NULL,
    ci_low REAL,
    ci_high REAL,
    maturity TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, pattern_key, n)
);

CREATE TABLE IF NOT EXISTS discovery_feed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    pattern_key TEXT,
    entity_id TEXT,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feed_run ON discovery_feed(run_id, id DESC);
"""


class WebDB:
    def __init__(self, path: Path | str | None = None):
        self.path = Path(path or os.getenv("SMARTWALLET_DB", "data/smartwallet.db"))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(WEB_SCHEMA)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(sql, tuple(params)).fetchall()]

    def row(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        with self.connect() as db:
            value = db.execute(sql, tuple(params)).fetchone()
            return dict(value) if value else None

    def execute(self, sql: str, params: Iterable[Any] = ()) -> None:
        with self.connect() as db:
            db.execute(sql, tuple(params))

    def create_run(self, payload: dict[str, Any], entity_names: dict[str, str]) -> str:
        run_id = str(uuid.uuid4())
        timestamp = now_iso()
        with self.connect() as db:
            sequence = int(db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM analysis_runs").fetchone()[0])
            db.execute(
                """INSERT INTO analysis_runs(run_id,sequence,status,desired_status,mode,from_date,to_date,entities_json,
                   settings_json,created_at,updated_at) VALUES(?,?,'queued','running',?,?,?,?,?,?,?)""",
                (run_id, sequence, payload["mode"], payload["from_date"], payload["to_date"],
                 json.dumps(payload["entities"]), json.dumps(payload.get("settings") or {}), timestamp, timestamp),
            )
            for entity_id in payload["entities"]:
                db.execute(
                    "INSERT INTO run_entities(run_id,entity_id,name,updated_at) VALUES(?,?,?,?)",
                    (run_id, entity_id, entity_names.get(entity_id, entity_id), timestamp),
                )
        return run_id

    def update_run(self, run_id: str, **fields: Any) -> None:
        if not fields:
            return
        allowed = {"status", "desired_status", "stage", "current_work", "progress", "started_at", "finished_at", "analysis_updated_at", "error"}
        values = {key: value for key, value in fields.items() if key in allowed}
        values["updated_at"] = now_iso()
        sql = "UPDATE analysis_runs SET " + ",".join(f"{key}=?" for key in values) + " WHERE run_id=?"
        self.execute(sql, [*values.values(), run_id])

    def add_error(self, run_id: str, stage: str, message: str, *, entity_id: str | None = None, item: str | None = None, retryable: bool = True) -> None:
        self.execute(
            "INSERT INTO run_errors(run_id,stage,entity_id,item,message,retryable,created_at) VALUES(?,?,?,?,?,?,?)",
            (run_id, stage, entity_id, item, message[:2000], int(retryable), now_iso()),
        )

    def recover_interrupted(self) -> None:
        with self.connect() as db:
            db.execute("UPDATE run_wallets SET status='pending',started_at=NULL WHERE status='running'")
            db.execute(
                """UPDATE analysis_runs SET status='paused',desired_status='paused',stage='interrupted',
                   current_work='Process restarted — ready to resume',updated_at=? WHERE status IN ('running','stopping')""",
                (now_iso(),),
            )

    def run(self, run_id: str) -> dict[str, Any] | None:
        row = self.row("SELECT * FROM analysis_runs WHERE run_id=?", (run_id,))
        if row:
            row["entities"] = json.loads(row.pop("entities_json"))
            row["settings"] = json.loads(row.pop("settings_json"))
        return row
