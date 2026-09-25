from __future__ import annotations

import hashlib
import json
import math
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
    return sorted({str(value) for value in values if value}) or ["FULL:" + motif]


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

        dirty = self._remove_stale(run_id)
        rows = self.db.rows(
            f"""SELECT e.episode_id,e.entity_id,e.start_ts,e.motif,e.evidence_json,e.intent_label,e.gross_usd,
                       m.asset_key,m.horizon_seconds,m.simple_return
                FROM episodes e JOIN market_labels m ON m.episode_id=e.episode_id
                WHERE e.entity_id IN ({placeholders}) AND e.start_ts BETWEEN ? AND ?
                  AND e.intent_label IS NOT NULL AND m.simple_return IS NOT NULL
                  AND EXISTS (
                    SELECT 1 WHERE NOT EXISTS (
                      SELECT 1 FROM analysis_observations o
                      WHERE o.run_id=? AND o.episode_id=e.episode_id AND o.asset_key=m.asset_key
                        AND o.horizon_seconds=m.horizon_seconds
                    )
                  )
                ORDER BY e.start_ts LIMIT ?""",
            [*entities, from_ts, to_ts, run_id, batch_size],
        )
        inserted = 0
        with self.db.connect() as conn:
            for row in rows:
                for pattern in _patterns(row["evidence_json"], row["motif"]):
                    key = pattern_key(row["entity_id"], pattern, row["intent_label"], row["asset_key"], row["horizon_seconds"])
                    observation_key = hashlib.sha256(
                        f'{row["episode_id"]}|{row["asset_key"]}|{row["horizon_seconds"]}|{pattern}'.encode()
                    ).hexdigest()
                    before = conn.total_changes
                    conn.execute(
                        """INSERT OR IGNORE INTO analysis_observations(run_id,observation_key,episode_id,entity_id,pattern,
                           intent_label,asset_key,horizon_seconds,episode_ts,return_value,gross_usd)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (run_id, observation_key, row["episode_id"], row["entity_id"], pattern, row["intent_label"],
                         row["asset_key"], row["horizon_seconds"], row["start_ts"], row["simple_return"], row["gross_usd"]),
                    )
                    if conn.total_changes > before:
                        inserted += 1
                        dirty.add(key)
                        conn.execute(
                            """INSERT OR IGNORE INTO pattern_aggregates(run_id,pattern_key,entity_id,pattern,intent_label,
                               asset_key,horizon_seconds,n,sum_return,sum_sq_return,negative_count,positive_count,
                               mean_return,maturity,updated_at) VALUES(?,?,?,?,?,?,?,0,0,0,0,0,0,'EARLY',?)""",
                            (run_id, key, row["entity_id"], pattern, row["intent_label"], row["asset_key"],
                             row["horizon_seconds"], now_iso()),
                        )
            for key in dirty:
                self._rebuild_cheap(conn, run_id, key)

        if expensive:
            self.refresh_expensive(run_id, dirty_only=False)
        self.db.update_run(run_id, analysis_updated_at=now_iso())
        return {"observations": inserted, "patterns": len(dirty)}

    def _remove_stale(self, run_id: str) -> set[str]:
        stale = self.db.rows(
            """SELECT o.observation_key,o.entity_id,o.pattern,o.intent_label,o.asset_key,o.horizon_seconds
               FROM analysis_observations o LEFT JOIN episodes e ON e.episode_id=o.episode_id
               WHERE o.run_id=? AND e.episode_id IS NULL LIMIT 5000""",
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

    def _rebuild_cheap(self, conn, run_id: str, key: str) -> None:
        sample = conn.execute(
            """SELECT entity_id,pattern,intent_label,asset_key,horizon_seconds,COUNT(*) n,SUM(return_value) total,
                      SUM(return_value*return_value) total_sq,
                      SUM(return_value<0) neg,SUM(return_value>0) pos
               FROM analysis_observations WHERE run_id=? AND
                 entity_id||'|'||pattern||'|'||intent_label||'|'||asset_key||'|'||horizon_seconds IN (
                   SELECT entity_id||'|'||pattern||'|'||intent_label||'|'||asset_key||'|'||horizon_seconds
                   FROM pattern_aggregates WHERE run_id=? AND pattern_key=?
                 ) GROUP BY entity_id,pattern,intent_label,asset_key,horizon_seconds""",
            (run_id, run_id, key),
        ).fetchone()
        if sample is None:
            # New keys are resolved by matching the deterministic hash in Python.
            candidates = conn.execute(
                """SELECT entity_id,pattern,intent_label,asset_key,horizon_seconds,COUNT(*) n,SUM(return_value) total,
                          SUM(return_value*return_value) total_sq,SUM(return_value<0) neg,SUM(return_value>0) pos
                   FROM analysis_observations WHERE run_id=? GROUP BY entity_id,pattern,intent_label,asset_key,horizon_seconds""",
                (run_id,),
            )
            sample = next((r for r in candidates if pattern_key(r[0], r[1], r[2], r[3], r[4]) == key), None)
        if sample is None:
            conn.execute("DELETE FROM pattern_aggregates WHERE run_id=? AND pattern_key=?", (run_id, key))
            return
        entity, pattern, intent, asset, horizon, n, total, total_sq, neg, pos = sample
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
        aggregates = self.db.rows(
            """SELECT * FROM pattern_aggregates WHERE run_id=? AND n>=10
               AND (?=0 OR n>=MAX(last_expensive_n+10, CAST(last_expensive_n*1.2 AS INTEGER))) ORDER BY n DESC""",
            (run_id, int(dirty_only)),
        )
        updates: list[dict[str, Any]] = []
        for aggregate in aggregates:
            values = self.db.rows(
                """SELECT return_value FROM analysis_observations WHERE run_id=? AND entity_id=? AND pattern=?
                   AND intent_label=? AND asset_key=? AND horizon_seconds=? ORDER BY episode_ts""",
                (run_id, aggregate["entity_id"], aggregate["pattern"], aggregate["intent_label"],
                 aggregate["asset_key"], aggregate["horizon_seconds"]),
            )
            arr = np.asarray([row["return_value"] for row in values], dtype=float)
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
        if not updates:
            return 0
        qvalues = bh_qvalues([item["p"] for item in updates])
        with self.db.connect() as conn:
            for item, qvalue in zip(updates, qvalues):
                old_maturity = item["maturity"]
                maturity = maturity_for(n=item["n"], ci_low=item["low"], ci_high=item["high"], qvalue=qvalue,
                                        holdout_accuracy=item["accuracy"], train_mean=item["train_mean"], test_mean=item["test_mean"])
                prior = conn.execute(
                    "SELECT AVG(return_value) FROM analysis_observations WHERE run_id=? AND entity_id=? AND intent_label=? AND asset_key=? AND horizon_seconds=?",
                    (run_id, item["entity_id"], item["intent_label"], item["asset_key"], item["horizon_seconds"]),
                ).fetchone()[0] or 0.0
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
        return len(updates)
