from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import numpy as np
from scipy.stats import binomtest

from .pipeline.backtest import bh_qvalues, bootstrap_ci
from .webdb import WebDB, now_iso


CHECKPOINTS = (10, 25, 50, 100, 200, 500, 1000)


def pattern_key(entity: str, pattern: str, intent: str, asset: str, horizon: int) -> str:
    raw = json.dumps([entity, pattern, intent, asset, int(horizon)], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def primary_pattern_key(entity: str, pattern: str, asset: str, horizon: int) -> str:
    raw = json.dumps([entity, pattern, asset, int(horizon)], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def record_analysis_invalidations(conn, run_id: str, episode_ids: list[str]) -> None:
    """Persist aggregate keys before affected observations are removed."""
    if not run_id or not episode_ids:
        return
    placeholders = ",".join("?" for _ in episode_ids)
    secondary = conn.execute(
        f"""SELECT DISTINCT entity_id,pattern,intent_label,asset_key,horizon_seconds
             FROM analysis_observations WHERE run_id=? AND episode_id IN ({placeholders})""",
        [run_id, *episode_ids],
    ).fetchall()
    primary = conn.execute(
        f"""SELECT DISTINCT entity_id,pattern,asset_key,horizon_seconds
             FROM primary_analysis_observations WHERE run_id=? AND episode_id IN ({placeholders})""",
        [run_id, *episode_ids],
    ).fetchall()
    conn.executemany(
        "INSERT OR IGNORE INTO analysis_invalidations(run_id,layer,pattern_key) VALUES(?,'secondary',?)",
        [(run_id, pattern_key(*row)) for row in secondary],
    )
    conn.executemany(
        "INSERT OR IGNORE INTO analysis_invalidations(run_id,layer,pattern_key) VALUES(?,'primary',?)",
        [(run_id, primary_pattern_key(*row)) for row in primary],
    )


def maturity_for(*, n: int, ci_low: float | None, ci_high: float | None, qvalue: float | None,
                 holdout_accuracy: float | None, train_mean: float | None, test_mean: float | None) -> str:
    """Conservative, sample-aware display state; never a trading recommendation."""
    if n < 20:
        return "EARLY"
    significant_ci = ci_low is not None and ci_high is not None and (ci_low > 0 or ci_high < 0)
    same_direction = train_mean is not None and test_mean is not None and train_mean * test_mean > 0
    if n >= 100 and significant_ci and qvalue is not None and qvalue <= 0.05 and same_direction and (holdout_accuracy or 0) >= 0.60:
        return "ROBUST"
    if n >= 50 and significant_ci and same_direction and (holdout_accuracy or 0) >= 0.55:
        return "ESTABLISHING"
    return "PROMISING"


def _patterns(evidence_json: str, motif: str) -> list[str]:
    try:
        payload = json.loads(evidence_json or "{}")
        values = payload.get("patterns") or []
    except (ValueError, TypeError):
        values = []
    canonical = set()
    for value in values or ["FULL:" + motif]:
        prefix, _, sequence = str(value).partition(":")
        size = len(sequence.split(">"))
        if prefix == "FULL" and size <= 3:
            prefix = {1:"ACTION",2:"BIGRAM",3:"TRIGRAM"}[size]
        if prefix == "ENDPOINT" and len(motif.split(">")) == 2 and sequence == motif:
            prefix = "BIGRAM"
        canonical.add(prefix + ":" + sequence)
    return sorted(canonical)


class IncrementalAnalyzer:
    def __init__(self, db: WebDB):
        self.db = db

    def refresh(self, run_id: str, *, batch_size: int = 1000, expensive: bool = False) -> dict[str, int]:
        run = self.db.run(run_id)
        if not run:
            return {"observations": 0, "patterns": 0}
        entities = run["entities"]
        if not entities:
            return {"observations": 0, "patterns": 0}
        placeholders = ",".join("?" for _ in entities)
        from_ts = int(datetime.fromisoformat(run["from_date"]).replace(tzinfo=timezone.utc).timestamp())
        to_dt = datetime.fromisoformat(run["to_date"])
        to_ts = int(to_dt.replace(tzinfo=timezone.utc).timestamp()) + 86399

        self._consume_invalidations(run_id)
        primary_inserted, primary_dirty = self._refresh_primary(
            run_id, entities, from_ts, to_ts, batch_size, clean_stale=expensive,
        )
        # Episode membership/evidence mutations delete their affected observations at the
        # write boundary. A full anti-join on every micro-batch is quadratic; keep one
        # defensive stale sweep for the final/explicit expensive refresh.
        stale_dirty = self._remove_stale(run_id) if expensive else set()
        dirty = set(stale_dirty)
        deltas: dict[str, list[float | int]] = {}
        rows = self.db.rows(
            f"""SELECT e.episode_id,e.entity_id,e.start_ts,e.motif,e.evidence_json,e.intent_label,e.gross_usd,
                       m.asset_key,m.horizon_seconds,m.simple_return
                FROM episodes e JOIN market_labels m ON m.episode_id=e.episode_id
                WHERE e.entity_id IN ({placeholders}) AND e.start_ts BETWEEN ? AND ?
                  AND e.episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)
                  AND e.intent_label IS NOT NULL AND m.simple_return IS NOT NULL
                  AND EXISTS (
                    SELECT 1 WHERE NOT EXISTS (
                      SELECT 1 FROM analysis_observations o
                      WHERE o.run_id=? AND o.episode_id=e.episode_id AND o.asset_key=m.asset_key
                        AND o.horizon_seconds=m.horizon_seconds
                    )
                  )
                ORDER BY e.start_ts LIMIT ?""",
            [*entities, from_ts, to_ts, run_id, run_id, batch_size],
        )
        inserted = 0
        observation_rows = []
        aggregate_rows: dict[str, tuple[Any, ...]] = {}
        with self.db.connect() as conn:
            for row in rows:
                for pattern in _patterns(row["evidence_json"], row["motif"]):
                    key = pattern_key(row["entity_id"], pattern, row["intent_label"], row["asset_key"], row["horizon_seconds"])
                    observation_key = hashlib.sha256(
                        f'{row["episode_id"]}|{row["asset_key"]}|{row["horizon_seconds"]}|{pattern}'.encode()
                    ).hexdigest()
                    observation_rows.append(
                        (run_id, observation_key, row["episode_id"], row["entity_id"], pattern, row["intent_label"],
                         row["asset_key"], row["horizon_seconds"], row["start_ts"], row["simple_return"], row["gross_usd"])
                    )
                    dirty.add(key)
                    value = float(row["simple_return"])
                    delta = deltas.setdefault(key, [0.0, 0.0, 0, 0, 0])
                    delta[0] += value
                    delta[1] += value * value
                    delta[2] += int(value < 0)
                    delta[3] += int(value > 0)
                    delta[4] += 1
                    aggregate_rows.setdefault(
                        key, (run_id, key, row["entity_id"], pattern, row["intent_label"], row["asset_key"],
                              row["horizon_seconds"], now_iso()),
                    )
            before = conn.total_changes
            conn.executemany(
                """INSERT OR IGNORE INTO analysis_observations(run_id,observation_key,episode_id,entity_id,pattern,
                   intent_label,asset_key,horizon_seconds,episode_ts,return_value,gross_usd)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""", observation_rows,
            )
            inserted = conn.total_changes - before
            conn.executemany(
                """INSERT OR IGNORE INTO pattern_aggregates(run_id,pattern_key,entity_id,pattern,intent_label,
                   asset_key,horizon_seconds,n,sum_return,sum_sq_return,negative_count,positive_count,
                   mean_return,maturity,updated_at) VALUES(?,?,?,?,?,?,?,0,0,0,0,0,0,'EARLY',?)""",
                aggregate_rows.values(),
            )
            if inserted != len(observation_rows):
                stale_dirty.update(aggregate_rows)
            existing_n = {r[0]: int(r[1]) for r in conn.execute(
                "SELECT pattern_key,n FROM pattern_aggregates WHERE run_id=?", (run_id,)
            )}
            crossing = {key for key, delta in deltas.items()
                        if any(existing_n.get(key, 0) < mark <= existing_n.get(key, 0) + int(delta[4])
                               for mark in CHECKPOINTS)}
            incremental = []
            for key, delta in deltas.items():
                if key in stale_dirty:
                    continue
                total, total_sq, neg, pos, n = delta
                incremental.append((n, total, total_sq, neg, pos, total, n, n, now_iso(), run_id, key))
            conn.executemany(
                """UPDATE pattern_aggregates SET n=n+?,sum_return=sum_return+?,sum_sq_return=sum_sq_return+?,
                   negative_count=negative_count+?,positive_count=positive_count+?,
                   mean_return=(sum_return+?)/(n+?),
                   maturity=CASE WHEN n+?>=20 AND maturity='EARLY' THEN 'PROMISING' ELSE maturity END,
                   updated_at=? WHERE run_id=? AND pattern_key=?""",
                incremental,
            )
            for key in stale_dirty:
                self._rebuild_cheap(conn, run_id, key)
            for key in crossing - stale_dirty:
                self._write_secondary_checkpoints(conn, run_id, key, existing_n.get(key, 0))

        # BH is deferred to the expensive refresh; doing a full family scan for every
        # micro-batch is quadratic and existing p-values did not change here.
        if expensive:
            self.refresh_expensive(run_id, dirty_only=False)
        self.db.update_run(run_id, analysis_updated_at=now_iso())
        with self.db.connect() as conn:
            counts = conn.execute(
                "SELECT (SELECT COUNT(*) FROM primary_analysis_observations WHERE run_id=?),"
                "(SELECT COUNT(*) FROM analysis_observations WHERE run_id=?)", (run_id, run_id),
            ).fetchone()
            input_hash = hashlib.sha256(f"{counts[0]}|{counts[1]}".encode()).hexdigest()
            conn.execute(
                """INSERT INTO artifact_provenance(artifact_type,artifact_id,version,input_hash,updated_at)
                   VALUES('statistics',?,'2.0-incremental-bulk',?,?) ON CONFLICT(artifact_type,artifact_id)
                   DO UPDATE SET version=excluded.version,input_hash=excluded.input_hash,updated_at=excluded.updated_at""",
                (run_id, input_hash, now_iso()),
            )
        return {
            "observations": inserted,
            "patterns": len(dirty),
            "primary_observations": primary_inserted,
            "primary_patterns": primary_dirty,
        }

    def _consume_invalidations(self, run_id: str) -> None:
        with self.db.connect() as conn:
            rows = conn.execute(
                "SELECT layer,pattern_key FROM analysis_invalidations WHERE run_id=?", (run_id,)
            ).fetchall()
            for layer, key in rows:
                if layer == "primary":
                    self._rebuild_primary_cheap(conn, run_id, key)
                else:
                    self._rebuild_cheap(conn, run_id, key)
            if rows:
                conn.execute("DELETE FROM analysis_invalidations WHERE run_id=?", (run_id,))

    def _refresh_primary(
        self, run_id: str, entities: list[str], from_ts: int, to_ts: int, batch_size: int,
        *, clean_stale: bool = False,
    ) -> tuple[int, int]:
        """Increment the intent-independent entity+pattern+asset+horizon layer."""
        stale = self.db.rows(
            """SELECT o.observation_key,o.entity_id,o.pattern,o.asset_key,o.horizon_seconds
               FROM primary_analysis_observations o LEFT JOIN episodes e ON e.episode_id=o.episode_id
               WHERE o.run_id=? AND (e.episode_id IS NULL OR NOT EXISTS(
                 SELECT 1 FROM run_episodes r WHERE r.run_id=o.run_id AND r.episode_id=o.episode_id
               )) LIMIT 5000""", (run_id,),
        ) if clean_stale else []
        dirty = {
            primary_pattern_key(r["entity_id"], r["pattern"], r["asset_key"], r["horizon_seconds"])
            for r in stale
        }
        stale_dirty = set(dirty)
        deltas: dict[str, list[float | int]] = {}
        if stale:
            with self.db.connect() as conn:
                conn.executemany(
                    "DELETE FROM primary_analysis_observations WHERE run_id=? AND observation_key=?",
                    [(run_id, row["observation_key"]) for row in stale],
                )
        placeholders = ",".join("?" for _ in entities)
        rows = self.db.rows(
            f"""SELECT e.episode_id,e.entity_id,e.start_ts,e.motif,e.evidence_json,e.gross_usd,
                       m.asset_key,m.horizon_seconds,m.simple_return
                FROM episodes e JOIN market_labels m ON m.episode_id=e.episode_id
                WHERE e.entity_id IN ({placeholders}) AND e.start_ts BETWEEN ? AND ?
                  AND e.episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)
                  AND m.simple_return IS NOT NULL
                  AND NOT EXISTS(SELECT 1 FROM primary_analysis_observations o
                    WHERE o.run_id=? AND o.episode_id=e.episode_id AND o.asset_key=m.asset_key
                      AND o.horizon_seconds=m.horizon_seconds)
                ORDER BY e.start_ts LIMIT ?""",
            [*entities, from_ts, to_ts, run_id, run_id, batch_size],
        )
        inserted = 0
        observation_rows = []
        aggregate_rows: dict[str, tuple[Any, ...]] = {}
        with self.db.connect() as conn:
            for row in rows:
                for pattern in _patterns(row["evidence_json"], row["motif"]):
                    key = primary_pattern_key(row["entity_id"], pattern, row["asset_key"], row["horizon_seconds"])
                    observation_key = hashlib.sha256(
                        f'primary|{row["episode_id"]}|{row["asset_key"]}|{row["horizon_seconds"]}|{pattern}'.encode()
                    ).hexdigest()
                    observation_rows.append(
                        (run_id, observation_key, row["episode_id"], row["entity_id"], pattern,
                         row["asset_key"], row["horizon_seconds"], row["start_ts"], row["simple_return"], row["gross_usd"])
                    )
                    dirty.add(key)
                    value = float(row["simple_return"])
                    delta = deltas.setdefault(key, [0.0, 0.0, 0, 0, 0])
                    delta[0] += value
                    delta[1] += value * value
                    delta[2] += int(value < 0)
                    delta[3] += int(value > 0)
                    delta[4] += 1
                    aggregate_rows.setdefault(
                        key, (run_id, key, row["entity_id"], pattern, row["asset_key"],
                              row["horizon_seconds"], now_iso()),
                    )
            before = conn.total_changes
            conn.executemany(
                """INSERT OR IGNORE INTO primary_analysis_observations(run_id,observation_key,episode_id,
                   entity_id,pattern,asset_key,horizon_seconds,episode_ts,return_value,gross_usd)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""", observation_rows,
            )
            inserted = conn.total_changes - before
            conn.executemany(
                """INSERT OR IGNORE INTO primary_pattern_aggregates(run_id,pattern_key,entity_id,pattern,
                   asset_key,horizon_seconds,n,sum_return,sum_sq_return,negative_count,positive_count,
                   mean_return,maturity,updated_at) VALUES(?,?,?,?,?,?,0,0,0,0,0,0,'EARLY',?)""",
                aggregate_rows.values(),
            )
            if inserted != len(observation_rows):
                stale_dirty.update(aggregate_rows)
            existing_n = {r[0]: int(r[1]) for r in conn.execute(
                "SELECT pattern_key,n FROM primary_pattern_aggregates WHERE run_id=?", (run_id,)
            )}
            crossing = {key for key, delta in deltas.items()
                        if any(existing_n.get(key, 0) < mark <= existing_n.get(key, 0) + int(delta[4])
                               for mark in CHECKPOINTS)}
            incremental = []
            for key, delta in deltas.items():
                if key in stale_dirty:
                    continue
                total, total_sq, neg, pos, n = delta
                incremental.append((n, total, total_sq, neg, pos, total, n, n, now_iso(), run_id, key))
            conn.executemany(
                """UPDATE primary_pattern_aggregates SET n=n+?,sum_return=sum_return+?,sum_sq_return=sum_sq_return+?,
                   negative_count=negative_count+?,positive_count=positive_count+?,
                   mean_return=(sum_return+?)/(n+?),
                   maturity=CASE WHEN n+?>=20 AND maturity='EARLY' THEN 'PROMISING' ELSE maturity END,
                   updated_at=? WHERE run_id=? AND pattern_key=?""",
                incremental,
            )
            for key in stale_dirty:
                self._rebuild_primary_cheap(conn, run_id, key)
            for key in crossing - stale_dirty:
                self._write_primary_checkpoints(conn, run_id, key, existing_n.get(key, 0))
        return inserted, len(dirty)

    def _write_primary_checkpoints(self, conn, run_id: str, key: str, old_n: int) -> None:
        group = conn.execute(
            """SELECT entity_id,pattern,asset_key,horizon_seconds,n FROM primary_pattern_aggregates
               WHERE run_id=? AND pattern_key=?""", (run_id, key),
        ).fetchone()
        if not group:
            return
        entity, pattern, asset, horizon, n = group
        for mark in CHECKPOINTS:
            if old_n < mark <= n:
                values = [float(row[0]) for row in conn.execute(
                    """SELECT return_value FROM primary_analysis_observations WHERE run_id=? AND entity_id=?
                       AND pattern=? AND asset_key=? AND horizon_seconds=? ORDER BY episode_ts LIMIT ?""",
                    (run_id, entity, pattern, asset, horizon, mark),
                )]
                low, high = bootstrap_ci(np.asarray(values), samples=500)
                conn.execute(
                    """INSERT OR IGNORE INTO primary_pattern_checkpoints(run_id,pattern_key,n,negative_rate,
                       mean_return,ci_low,ci_high,maturity,created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (run_id, key, mark, sum(v < 0 for v in values) / mark, float(np.mean(values)), low, high,
                     "EARLY" if mark < 20 else "PROMISING", now_iso()),
                )

    def _rebuild_primary_cheap(self, conn, run_id: str, key: str) -> None:
        group = conn.execute(
            "SELECT entity_id,pattern,asset_key,horizon_seconds FROM primary_pattern_aggregates WHERE run_id=? AND pattern_key=?",
            (run_id, key),
        ).fetchone()
        if group is None:
            return
        entity, pattern, asset, horizon = group
        sample = conn.execute(
            """SELECT COUNT(*),SUM(return_value),SUM(return_value*return_value),
                      SUM(return_value<0),SUM(return_value>0)
               FROM primary_analysis_observations WHERE run_id=? AND entity_id=? AND pattern=?
                 AND asset_key=? AND horizon_seconds=?""",
            (run_id, entity, pattern, asset, horizon),
        ).fetchone()
        n, total, total_sq, neg, pos = sample
        if not n:
            conn.execute("DELETE FROM primary_pattern_aggregates WHERE run_id=? AND pattern_key=?", (run_id, key))
            return
        previous = conn.execute(
            "SELECT n,maturity,negative_count FROM primary_pattern_aggregates WHERE run_id=? AND pattern_key=?",
            (run_id, key),
        ).fetchone()
        old_n = int(previous[0]) if previous else 0
        maturity = "EARLY" if n < 20 else (previous[1] if previous and previous[1] != "EARLY" else "PROMISING")
        conn.execute(
            """UPDATE primary_pattern_aggregates SET n=?,sum_return=?,sum_sq_return=?,negative_count=?,
               positive_count=?,mean_return=?,maturity=?,updated_at=? WHERE run_id=? AND pattern_key=?""",
            (n, total, total_sq, neg, pos, total / n, maturity, now_iso(), run_id, key),
        )
        for mark in CHECKPOINTS:
            if old_n < mark <= n:
                values = [float(row[0]) for row in conn.execute(
                    """SELECT return_value FROM primary_analysis_observations WHERE run_id=? AND entity_id=?
                       AND pattern=? AND asset_key=? AND horizon_seconds=? ORDER BY episode_ts LIMIT ?""",
                    (run_id, entity, pattern, asset, horizon, mark),
                )]
                low, high = bootstrap_ci(np.asarray(values), samples=500)
                conn.execute(
                    """INSERT OR IGNORE INTO primary_pattern_checkpoints(run_id,pattern_key,n,negative_rate,
                       mean_return,ci_low,ci_high,maturity,created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (run_id, key, mark, sum(v < 0 for v in values) / mark, float(np.mean(values)), low, high,
                     "EARLY" if mark < 20 else "PROMISING", now_iso()),
                )

    def _remove_stale(self, run_id: str) -> set[str]:
        stale = self.db.rows(
            """SELECT o.observation_key,o.entity_id,o.pattern,o.intent_label,o.asset_key,o.horizon_seconds
               FROM analysis_observations o LEFT JOIN episodes e ON e.episode_id=o.episode_id
               WHERE o.run_id=? AND (e.episode_id IS NULL OR NOT EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=o.run_id AND r.episode_id=o.episode_id)) LIMIT 5000""",
            (run_id,),
        )
        dirty = {pattern_key(r["entity_id"], r["pattern"], r["intent_label"], r["asset_key"], r["horizon_seconds"]) for r in stale}
        if stale:
            with self.db.connect() as conn:
                conn.executemany(
                    "DELETE FROM analysis_observations WHERE run_id=? AND observation_key=?",
                    [(run_id, row["observation_key"]) for row in stale],
                )
        return dirty

    def _write_secondary_checkpoints(self, conn, run_id: str, key: str, old_n: int) -> None:
        group = conn.execute(
            """SELECT entity_id,pattern,intent_label,asset_key,horizon_seconds,n,negative_count
               FROM pattern_aggregates WHERE run_id=? AND pattern_key=?""", (run_id, key),
        ).fetchone()
        if not group:
            return
        entity, pattern, intent, asset, horizon, n, negative = group
        if old_n > 0 and n - old_n >= 5:
            conn.execute(
                "INSERT INTO discovery_feed(run_id,pattern_key,entity_id,kind,title,detail,created_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, key, entity, "sample", f"Sample increased {old_n} → {n}",
                 f"Down rate updated to {negative / n:.0%}", now_iso()),
            )
        for mark in CHECKPOINTS:
            if old_n < mark <= n:
                values = [float(row[0]) for row in conn.execute(
                    """SELECT return_value FROM analysis_observations WHERE run_id=? AND entity_id=? AND pattern=?
                       AND intent_label=? AND asset_key=? AND horizon_seconds=? ORDER BY episode_ts LIMIT ?""",
                    (run_id, entity, pattern, intent, asset, horizon, mark),
                )]
                low, high = bootstrap_ci(np.asarray(values), samples=500)
                conn.execute(
                    """INSERT OR IGNORE INTO pattern_checkpoints(run_id,pattern_key,n,negative_rate,mean_return,
                       ci_low,ci_high,maturity,created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (run_id, key, mark, sum(v < 0 for v in values) / mark, float(np.mean(values)), low, high,
                     "EARLY" if mark < 20 else "PROMISING", now_iso()),
                )
                conn.execute(
                    "INSERT INTO discovery_feed(run_id,pattern_key,entity_id,kind,title,detail,created_at) VALUES(?,?,?,?,?,?,?)",
                    (run_id, key, entity, "milestone", f"Reached n={mark}",
                     f"{pattern} now has {n} observations", now_iso()),
                )

    def _rebuild_cheap(self, conn, run_id: str, key: str) -> None:
        group = conn.execute(
            """SELECT entity_id,pattern,intent_label,asset_key,horizon_seconds
               FROM pattern_aggregates WHERE run_id=? AND pattern_key=?""", (run_id, key),
        ).fetchone()
        if group is None:
            return
        entity, pattern, intent, asset, horizon = group
        totals = conn.execute(
            """SELECT COUNT(*),SUM(return_value),SUM(return_value*return_value),
                      SUM(return_value<0),SUM(return_value>0)
               FROM analysis_observations WHERE run_id=? AND entity_id=? AND pattern=?
                 AND intent_label=? AND asset_key=? AND horizon_seconds=?""",
            (run_id, entity, pattern, intent, asset, horizon),
        ).fetchone()
        n, total, total_sq, neg, pos = totals
        if not n:
            conn.execute("DELETE FROM pattern_aggregates WHERE run_id=? AND pattern_key=?", (run_id, key))
            return
        previous = conn.execute("SELECT n,maturity,negative_count FROM pattern_aggregates WHERE run_id=? AND pattern_key=?", (run_id, key)).fetchone()
        maturity = "EARLY" if n < 20 else (previous[1] if previous and previous[1] != "EARLY" else "PROMISING")
        conn.execute(
            """INSERT INTO pattern_aggregates(run_id,pattern_key,entity_id,pattern,intent_label,asset_key,horizon_seconds,n,
               sum_return,sum_sq_return,negative_count,positive_count,mean_return,maturity,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(run_id,pattern_key) DO UPDATE SET n=excluded.n,sum_return=excluded.sum_return,
                 sum_sq_return=excluded.sum_sq_return,negative_count=excluded.negative_count,positive_count=excluded.positive_count,
                 mean_return=excluded.mean_return,maturity=excluded.maturity,updated_at=excluded.updated_at""",
            (run_id, key, entity, pattern, intent, asset, horizon, n, total, total_sq, neg, pos, total / n, maturity, now_iso()),
        )
        old_n = int(previous[0]) if previous else 0
        if previous and old_n > 0 and n - old_n >= 5:
            old_rate = int(previous[2]) / old_n
            conn.execute(
                "INSERT INTO discovery_feed(run_id,pattern_key,entity_id,kind,title,detail,created_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, key, entity, "sample", f"Sample increased {old_n} → {n}",
                 f"Down rate {old_rate:.0%} → {neg / n:.0%}", now_iso()),
            )
        for mark in CHECKPOINTS:
            if old_n < mark <= n:
                checkpoint_values = [float(r[0]) for r in conn.execute(
                    """SELECT return_value FROM analysis_observations WHERE run_id=? AND entity_id=? AND pattern=?
                       AND intent_label=? AND asset_key=? AND horizon_seconds=? ORDER BY episode_ts LIMIT ?""",
                    (run_id, entity, pattern, intent, asset, horizon, mark),
                )]
                checkpoint_mean = float(np.mean(checkpoint_values))
                checkpoint_negative = sum(value < 0 for value in checkpoint_values) / mark
                checkpoint_low, checkpoint_high = bootstrap_ci(np.asarray(checkpoint_values), samples=500)
                conn.execute(
                    """INSERT OR IGNORE INTO pattern_checkpoints(run_id,pattern_key,n,negative_rate,mean_return,ci_low,ci_high,maturity,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (run_id, key, mark, checkpoint_negative, checkpoint_mean, checkpoint_low, checkpoint_high,
                     "EARLY" if mark < 20 else "PROMISING", now_iso()),
                )
                conn.execute(
                    "INSERT INTO discovery_feed(run_id,pattern_key,entity_id,kind,title,detail,created_at) VALUES(?,?,?,?,?,?,?)",
                    (run_id, key, entity, "milestone", f"Reached n={mark}", f"{pattern} now has {n} observations", now_iso()),
                )

    def refresh_expensive(self, run_id: str, *, dirty_only: bool = True) -> int:
        self._refresh_primary_expensive(run_id, dirty_only=dirty_only)
        aggregates = self.db.rows(
            """SELECT * FROM pattern_aggregates WHERE run_id=? AND n>0
               AND (?=0 OR (n>=10 AND (last_expensive_n=0 OR n>=MIN(last_expensive_n+10, CAST(CEIL(last_expensive_n*1.2) AS INTEGER))))) ORDER BY n DESC""",
            (run_id, int(dirty_only)),
        )
        selected = {row["pattern_key"] for row in aggregates}
        grouped_values: dict[str, list[float]] = {key: [] for key in selected}
        if selected:
            value_rows = self.db.rows(
                """SELECT g.pattern_key,o.return_value FROM pattern_aggregates g
                   JOIN analysis_observations o ON o.run_id=g.run_id AND o.entity_id=g.entity_id
                    AND o.pattern=g.pattern AND o.intent_label=g.intent_label AND o.asset_key=g.asset_key
                    AND o.horizon_seconds=g.horizon_seconds
                   WHERE g.run_id=? AND g.n>0 AND (?=0 OR (g.n>=10 AND (g.last_expensive_n=0 OR
                     g.n>=MIN(g.last_expensive_n+10,CAST(CEIL(g.last_expensive_n*1.2) AS INTEGER)))))
                   ORDER BY g.pattern_key,o.episode_ts""", (run_id, int(dirty_only)),
            )
            for row in value_rows:
                grouped_values[row["pattern_key"]].append(float(row["return_value"]))
        prior_rows = self.db.rows(
            """SELECT entity_id,intent_label,asset_key,horizon_seconds,AVG(return_value) prior FROM (
                 SELECT DISTINCT entity_id,intent_label,asset_key,horizon_seconds,episode_id,return_value
                 FROM analysis_observations WHERE run_id=?)
               GROUP BY entity_id,intent_label,asset_key,horizon_seconds""", (run_id,),
        )
        priors = {(r["entity_id"], r["intent_label"], r["asset_key"], r["horizon_seconds"]): float(r["prior"] or 0)
                  for r in prior_rows}
        updates: list[dict[str, Any]] = []
        for aggregate in aggregates:
            arr = np.asarray(grouped_values.get(aggregate["pattern_key"], []), dtype=float)
            if not len(arr):
                continue
            low, high = bootstrap_ci(arr, samples=1000)
            effective = int(np.sum(arr != 0))
            neg = int(np.sum(arr < 0))
            pvalue = float(binomtest(min(neg, effective - neg), n=effective, p=0.5).pvalue) if effective else 1.0
            split = max(1, int(len(arr) * 0.7))
            train, test = arr[:split], arr[split:]
            train_mean = float(np.mean(train))
            test_mean = float(np.mean(test)) if len(test) else None
            direction = -1 if train_mean < 0 else 1
            accuracy = float(np.mean(np.sign(test) == direction)) if len(test) else None
            updates.append({**aggregate, "median": float(np.median(arr)), "low": low, "high": high, "p": pvalue,
                            "train_n": len(train), "test_n": len(test), "train_mean": train_mean,
                            "test_mean": test_mean, "accuracy": accuracy})
        family = {r["pattern_key"]: r["sign_pvalue"] if r["sign_pvalue"] is not None else 1.0 for r in self.db.rows("SELECT pattern_key,sign_pvalue FROM pattern_aggregates WHERE run_id=?", (run_id,))}
        family.update({item["pattern_key"]:item["p"] for item in updates})
        qmap = dict(zip(family, bh_qvalues(list(family.values()))))
        qvalues = [qmap[item["pattern_key"]] for item in updates]
        with self.db.connect() as conn:
            for item, qvalue in zip(updates, qvalues):
                old_maturity = item["maturity"]
                maturity = maturity_for(n=item["n"], ci_low=item["low"], ci_high=item["high"], qvalue=qvalue,
                                        holdout_accuracy=item["accuracy"], train_mean=item["train_mean"], test_mean=item["test_mean"])
                prior = priors.get((item["entity_id"], item["intent_label"], item["asset_key"], item["horizon_seconds"]), 0.0)
                shrunk = (item["n"] * item["mean_return"] + 20 * prior) / (item["n"] + 20)
                conn.execute(
                    """UPDATE pattern_aggregates SET median_return=?,ci_low=?,ci_high=?,sign_pvalue=?,qvalue=?,shrunk_mean=?,
                       train_n=?,test_n=?,test_mean_return=?,holdout_accuracy=?,maturity=?,last_expensive_n=?,updated_at=?
                       WHERE run_id=? AND pattern_key=?""",
                    (item["median"], item["low"], item["high"], item["p"], qvalue, shrunk, item["train_n"], item["test_n"],
                     item["test_mean"], item["accuracy"], maturity, item["n"], now_iso(), run_id, item["pattern_key"]),
                )
                if maturity != old_maturity:
                    conn.execute(
                        "INSERT INTO discovery_feed(run_id,pattern_key,entity_id,kind,title,detail,created_at) VALUES(?,?,?,?,?,?,?)",
                        (run_id, item["pattern_key"], item["entity_id"], "maturity", "Maturity changed",
                         f"{old_maturity} → {maturity}", now_iso()),
                    )
        with self.db.connect() as conn:
            conn.executemany("UPDATE pattern_aggregates SET qvalue=? WHERE run_id=? AND pattern_key=?", [(q,run_id,key) for key,q in qmap.items()])
        maturity_rows = []
        for row in self.db.rows("SELECT * FROM pattern_aggregates WHERE run_id=?", (run_id,)):
            train_mean = ((row["mean_return"]*row["n"]-(row["test_mean_return"] or 0)*(row["test_n"] or 0))/row["train_n"]) if row["train_n"] else None
            maturity_rows.append((maturity_for(n=row["n"],ci_low=row["ci_low"],ci_high=row["ci_high"],qvalue=row["qvalue"],holdout_accuracy=row["holdout_accuracy"],train_mean=train_mean,test_mean=row["test_mean_return"]),run_id,row["pattern_key"]))
        with self.db.connect() as conn:
            conn.executemany("UPDATE pattern_aggregates SET maturity=? WHERE run_id=? AND pattern_key=?", maturity_rows)
        return len(updates)

    def _refresh_primary_expensive(self, run_id: str, *, dirty_only: bool = True) -> int:
        aggregates = self.db.rows(
            """SELECT * FROM primary_pattern_aggregates WHERE run_id=? AND n>0
               AND (?=0 OR (n>=10 AND (last_expensive_n=0 OR n>=MIN(last_expensive_n+10,
               CAST(CEIL(last_expensive_n*1.2) AS INTEGER))))) ORDER BY n DESC""",
            (run_id, int(dirty_only)),
        )
        selected = {row["pattern_key"] for row in aggregates}
        grouped_values: dict[str, list[float]] = {key: [] for key in selected}
        if selected:
            value_rows = self.db.rows(
                """SELECT g.pattern_key,o.return_value FROM primary_pattern_aggregates g
                   JOIN primary_analysis_observations o ON o.run_id=g.run_id AND o.entity_id=g.entity_id
                    AND o.pattern=g.pattern AND o.asset_key=g.asset_key AND o.horizon_seconds=g.horizon_seconds
                   WHERE g.run_id=? AND g.n>0 AND (?=0 OR (g.n>=10 AND (g.last_expensive_n=0 OR
                     g.n>=MIN(g.last_expensive_n+10,CAST(CEIL(g.last_expensive_n*1.2) AS INTEGER)))))
                   ORDER BY g.pattern_key,o.episode_ts""", (run_id, int(dirty_only)),
            )
            for row in value_rows:
                grouped_values[row["pattern_key"]].append(float(row["return_value"]))
        prior_rows = self.db.rows(
            """SELECT entity_id,asset_key,horizon_seconds,AVG(return_value) prior FROM (
                 SELECT DISTINCT entity_id,asset_key,horizon_seconds,episode_id,return_value
                 FROM primary_analysis_observations WHERE run_id=?)
               GROUP BY entity_id,asset_key,horizon_seconds""", (run_id,),
        )
        priors = {(r["entity_id"], r["asset_key"], r["horizon_seconds"]): float(r["prior"] or 0)
                  for r in prior_rows}
        updates: list[dict[str, Any]] = []
        for aggregate in aggregates:
            arr = np.asarray(grouped_values.get(aggregate["pattern_key"], []), dtype=float)
            if not len(arr):
                continue
            low, high = bootstrap_ci(arr, samples=1000)
            effective = int(np.sum(arr != 0))
            neg = int(np.sum(arr < 0))
            pvalue = float(binomtest(min(neg, effective - neg), n=effective, p=0.5).pvalue) if effective else 1.0
            split = max(1, int(len(arr) * 0.7))
            train, test = arr[:split], arr[split:]
            train_mean = float(np.mean(train))
            test_mean = float(np.mean(test)) if len(test) else None
            direction = -1 if train_mean < 0 else 1
            accuracy = float(np.mean(np.sign(test) == direction)) if len(test) else None
            updates.append({
                **aggregate, "median": float(np.median(arr)), "low": low, "high": high, "p": pvalue,
                "train_n": len(train), "test_n": len(test), "train_mean": train_mean,
                "test_mean": test_mean, "accuracy": accuracy,
            })
        family = {
            row["pattern_key"]: row["sign_pvalue"] if row["sign_pvalue"] is not None else 1.0
            for row in self.db.rows("SELECT pattern_key,sign_pvalue FROM primary_pattern_aggregates WHERE run_id=?", (run_id,))
        }
        family.update({item["pattern_key"]: item["p"] for item in updates})
        qmap = dict(zip(family, bh_qvalues(list(family.values()))))
        with self.db.connect() as conn:
            for item in updates:
                qvalue = qmap[item["pattern_key"]]
                maturity = maturity_for(
                    n=item["n"], ci_low=item["low"], ci_high=item["high"], qvalue=qvalue,
                    holdout_accuracy=item["accuracy"], train_mean=item["train_mean"], test_mean=item["test_mean"],
                )
                prior = priors.get((item["entity_id"], item["asset_key"], item["horizon_seconds"]), 0.0)
                shrunk = (item["n"] * item["mean_return"] + 20 * prior) / (item["n"] + 20)
                conn.execute(
                    """UPDATE primary_pattern_aggregates SET median_return=?,ci_low=?,ci_high=?,sign_pvalue=?,
                       qvalue=?,shrunk_mean=?,train_n=?,test_n=?,test_mean_return=?,holdout_accuracy=?,maturity=?,
                       last_expensive_n=?,updated_at=? WHERE run_id=? AND pattern_key=?""",
                    (item["median"], item["low"], item["high"], item["p"], qvalue, shrunk,
                     item["train_n"], item["test_n"], item["test_mean"], item["accuracy"], maturity,
                     item["n"], now_iso(), run_id, item["pattern_key"]),
                )
            conn.executemany(
                "UPDATE primary_pattern_aggregates SET qvalue=? WHERE run_id=? AND pattern_key=?",
                [(q, run_id, key) for key, q in qmap.items()],
            )
        return len(updates)
