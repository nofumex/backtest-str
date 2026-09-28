"""Additive, idempotent migrations. Legacy data is never implicitly assigned to runs."""
SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS run_events(run_id TEXT NOT NULL,event_id TEXT NOT NULL,built INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,event_id));
CREATE TABLE IF NOT EXISTS run_episodes(run_id TEXT NOT NULL,episode_id TEXT NOT NULL,PRIMARY KEY(run_id,episode_id));
CREATE TABLE IF NOT EXISTS market_horizons(episode_id TEXT NOT NULL,horizon_seconds INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',reason TEXT,attempts INTEGER NOT NULL DEFAULT 0,next_retry REAL NOT NULL DEFAULT 0,PRIMARY KEY(episode_id,horizon_seconds));
CREATE TABLE IF NOT EXISTS request_attempts(id INTEGER PRIMARY KEY,run_id TEXT,entity_id TEXT,wallet TEXT,provider TEXT,endpoint TEXT,attempt INTEGER,state TEXT,error TEXT,next_retry REAL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE INDEX IF NOT EXISTS idx_request_run ON request_attempts(run_id,entity_id);
CREATE TABLE IF NOT EXISTS wallet_collection_cache(entity_id TEXT,address TEXT,from_ts INTEGER,to_ts INTEGER,max_pages INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(entity_id,address,from_ts,to_ts,max_pages));
CREATE TABLE IF NOT EXISTS classification_jobs(run_id TEXT,episode_id TEXT,attempts INTEGER DEFAULT 0,error TEXT,PRIMARY KEY(run_id,episode_id));
CREATE TABLE IF NOT EXISTS assets(asset_key TEXT PRIMARY KEY,symbol TEXT,name TEXT);
CREATE TABLE IF NOT EXISTS market_time_series(asset_key TEXT NOT NULL,timestamp INTEGER NOT NULL,bucket INTEGER NOT NULL,price REAL,source TEXT NOT NULL,fetched_at TEXT NOT NULL,resolution INTEGER NOT NULL DEFAULT 1,status TEXT NOT NULL DEFAULT 'success',reason TEXT,PRIMARY KEY(asset_key,timestamp,resolution,source));
CREATE INDEX IF NOT EXISTS idx_market_series_lookup ON market_time_series(asset_key,timestamp,status);
INSERT OR IGNORE INTO schema_migrations(version) VALUES(1);
"""

def migrate(db):
    db.executescript(SCHEMA)
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='raw_responses'").fetchone():
        db.execute("CREATE INDEX IF NOT EXISTS idx_raw_request ON raw_responses(endpoint_key,request_json,fetched_at)")

    if not db.execute("SELECT 1 FROM schema_migrations WHERE version=2").fetchone() and db.execute("SELECT 1 FROM sqlite_master WHERE name='episodes'").fetchone():
        for horizon in (300,3600,21600,86400,259200):
            db.execute("""INSERT OR IGNORE INTO market_horizons(episode_id,horizon_seconds,state)
                SELECT e.episode_id,?,CASE WHEN EXISTS(SELECT 1 FROM market_labels m WHERE m.episode_id=e.episode_id
                AND m.horizon_seconds=? AND m.simple_return IS NOT NULL) THEN 'success' ELSE 'pending' END FROM episodes e""", (horizon,horizon))
        db.execute("INSERT INTO schema_migrations(version) VALUES(2)")

    if db.execute("SELECT 1 FROM sqlite_master WHERE name='run_entities'").fetchone():
        columns = {r[1] for r in db.execute("PRAGMA table_info(run_entities)")}
        if "wallets_partial" not in columns:
            db.execute("ALTER TABLE run_entities ADD COLUMN wallets_partial INTEGER NOT NULL DEFAULT 0")
        db.execute("INSERT OR IGNORE INTO schema_migrations(version) VALUES(3)")

    if db.execute("SELECT 1 FROM sqlite_master WHERE name='run_metrics'").fetchone():
        columns = {r[1] for r in db.execute("PRAGMA table_info(run_metrics)")}
        if "deterministic_completed" not in columns:
            db.execute("ALTER TABLE run_metrics ADD COLUMN deterministic_completed INTEGER NOT NULL DEFAULT 0")
        if "classification_seconds" not in columns:
            db.execute("ALTER TABLE run_metrics ADD COLUMN classification_seconds REAL NOT NULL DEFAULT 0")
        for name, definition in (
            ("market_cache_hits", "INTEGER NOT NULL DEFAULT 0"),
            ("market_fetched_points", "INTEGER NOT NULL DEFAULT 0"),
            ("market_unavailable_points", "INTEGER NOT NULL DEFAULT 0"),
            ("market_external_calls", "INTEGER NOT NULL DEFAULT 0"),
            ("market_horizons_processed", "INTEGER NOT NULL DEFAULT 0"),
            ("market_seconds", "REAL NOT NULL DEFAULT 0"),
        ):
            if name not in columns:
                db.execute(f"ALTER TABLE run_metrics ADD COLUMN {name} {definition}")
        db.execute("INSERT OR IGNORE INTO schema_migrations(version) VALUES(4)")
