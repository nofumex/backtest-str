from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from .config import load_entities
from .incremental import IncrementalAnalyzer
from .pipeline.backfill import backfill_evm_wallet, collect_solana_wallet, parse_date
from .pipeline.classify import classify_episodes
from .pipeline.discovery import discover_entity, snapshot_entity_context
from .pipeline.enrich import enrich_evm_event
from .pipeline.episodes import build_episodes
from .pipeline.market import DEFAULT_HORIZONS, MarketLabeler
from .pipeline.wallet_context import snapshot_evm_wallet_context
from .runtime import Runtime
from .settings import Settings
from .webdb import WebDB, now_iso


class SafeStop(Exception):
    pass


class RunOrchestrator:
    """Owns background work; persisted desired_status is the control plane."""

    def __init__(self, db: WebDB):
        self.db = db
        self.tasks: dict[str, asyncio.Task] = {}
        self.runtime_metrics: dict[str, dict[str, Any]] = {}
        self._wakeups: dict[str, asyncio.Event] = {}

    def active(self, run_id: str) -> bool:
        task = self.tasks.get(run_id)
        return bool(task and not task.done())

    def launch(self, run_id: str) -> None:
        if self.active(run_id):
            return
        self._wakeups[run_id] = asyncio.Event()
        self.tasks[run_id] = asyncio.create_task(self._run(run_id), name=f"analysis-{run_id[:8]}")

    async def control(self, run_id: str, action: str) -> dict[str, Any]:
        run = self.db.run(run_id)
        if not run:
            raise KeyError(run_id)
        if action == "pause":
            self.db.update_run(run_id, desired_status="paused", status="pausing")
        elif action == "resume":
            other = self.db.row(
                "SELECT run_id FROM analysis_runs WHERE run_id!=? AND status IN ('running','queued','pausing','paused','stopping') LIMIT 1",
                (run_id,),
            )
            if other:
                raise ValueError("Another analysis owns the worker; stop it before resuming this run")
            self.db.update_run(run_id, desired_status="running", status="queued", error=None)
            self.launch(run_id)
        elif action == "stop":
            self.db.update_run(run_id, desired_status="stopped", status="stopping")
        else:
            raise ValueError(action)
        if run_id in self._wakeups:
            self._wakeups[run_id].set()
        return self.db.run(run_id) or {}

    async def _checkpoint(self, run_id: str) -> None:
        while True:
            state = self.db.row("SELECT desired_status FROM analysis_runs WHERE run_id=?", (run_id,))
            desired = state["desired_status"] if state else "stopped"
            if desired == "stopped":
                raise SafeStop()
            if desired != "paused":
                return
            self.db.update_run(run_id, status="paused", current_work="Paused at a safe batch boundary")
            wakeup = self._wakeups[run_id]
            wakeup.clear()
            try:
                await asyncio.wait_for(wakeup.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    async def _run(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if not run:
            return
        rt: Runtime | None = None
        processor: asyncio.Task | None = None
        started = time.monotonic()
        try:
            settings = Settings.load()
            requested = run["settings"]
            settings = replace(settings, concurrency=max(1, min(32, int(requested.get("concurrency", settings.concurrency)))))
            rt = Runtime.create(settings)
            self.runtime_metrics[run_id] = {"started_monotonic": started, "hub": rt.hub.metrics, "llm_completed": 0}
            self.db.update_run(run_id, status="running", stage="discovery", started_at=run.get("started_at") or now_iso(), error=None)
            await self._discover(rt, run)
            processor = asyncio.create_task(self._processor_loop(rt, run_id), name=f"processor-{run_id[:8]}")
            await self._collect(rt, run)
            self.db.update_run(run_id, stage="draining", current_work="Finishing classification, labels and statistics", progress=0.94)
            for _ in range(100):
                await self._checkpoint(run_id)
                pending = await self._process_once(rt, run_id, final=True)
                if pending == 0:
                    break
            IncrementalAnalyzer(self.db).refresh(run_id, expensive=True)
            self._refresh_counts(run_id)
            self._persist_metrics(run_id, rt)
            self.db.update_run(run_id, status="completed", desired_status="completed", stage="completed",
                               current_work="Analysis complete", progress=1.0, finished_at=now_iso())
        except SafeStop:
            self.db.update_run(run_id, status="stopped", desired_status="stopped", stage="stopped",
                               current_work="Stopped safely", finished_at=now_iso())
        except Exception as exc:
            self.db.add_error(run_id, "orchestrator", f"{type(exc).__name__}: {exc}", retryable=True)
            self.db.update_run(run_id, status="failed", desired_status="paused", stage="failed", error=str(exc),
                               current_work="Run failed — inspect errors, then resume")
        finally:
            if processor:
                processor.cancel()
                await asyncio.gather(processor, return_exceptions=True)
            if rt:
                await rt.aclose()
            self.runtime_metrics.pop(run_id, None)

    async def _discover(self, rt: Runtime, run: dict[str, Any]) -> None:
        configured = {row["id"]: row["name"] for row in load_entities()}
        for index, entity_id in enumerate(run["entities"]):
            await self._checkpoint(run["run_id"])
            self.db.update_run(run["run_id"], current_work=f"Discovering {configured.get(entity_id, entity_id)}",
                               progress=0.02 + 0.05 * index / max(1, len(run["entities"])))
            try:
                await discover_entity(rt.arkham, rt.storage, entity_id, configured.get(entity_id, entity_id))
                await snapshot_entity_context(rt.arkham, rt.storage, entity_id)
            except Exception as exc:
                self.db.add_error(run["run_id"], "discovery", str(exc), entity_id=entity_id)
                # Existing discovered wallets still make the run resumable/useful.
            rows = rt.storage.fetchall(
                "SELECT address,MIN(chain) chain FROM wallets WHERE entity_id=? GROUP BY address ORDER BY address", (entity_id,)
            )
            maximum = run["settings"].get("max_wallets")
            if maximum:
                rows = rows[: int(maximum)]
            with self.db.connect() as db:
                for row in rows:
                    db.execute(
                        """INSERT OR IGNORE INTO run_wallets(run_id,entity_id,address,chain,status)
                           VALUES(?,?,?,?, 'pending')""", (run["run_id"], entity_id, row["address"], row["chain"]),
                    )
                db.execute(
                    "UPDATE run_entities SET wallets_total=(SELECT COUNT(*) FROM run_wallets w WHERE w.run_id=? AND w.entity_id=?),updated_at=? WHERE run_id=? AND entity_id=?",
                    (run["run_id"], entity_id, now_iso(), run["run_id"], entity_id),
                )
        total = self.db.row("SELECT COUNT(*) n FROM run_wallets WHERE run_id=?", (run["run_id"],))["n"]
        if not total:
            raise RuntimeError("Discovery produced no wallets. Check API_HUB_KEY, provider availability and run errors.")

    async def _collect(self, rt: Runtime, run: dict[str, Any]) -> None:
        run_id = run["run_id"]
        maximum_pages = run["settings"].get("max_pages")
        batch_size = max(1, min(rt.settings.concurrency, 8))
        while True:
            await self._checkpoint(run_id)
            rows = self.db.rows(
                "SELECT * FROM run_wallets WHERE run_id=? AND status IN ('pending','failed') AND attempts<3 ORDER BY attempts,address LIMIT ?",
                (run_id, batch_size),
            )
            if not rows:
                break
            entity_progress = self.db.row(
                """SELECT COUNT(*) total,SUM(status='completed') done FROM run_wallets
                   WHERE run_id=? AND entity_id=?""", (run_id, rows[0]["entity_id"]),
            ) or {"total": 0, "done": 0}
            self.db.update_run(run_id, stage="historical_backfill",
                               current_work=(f'{rows[0]["entity_id"]} · wallet {(entity_progress.get("done") or 0) + 1}'
                                             f' / {entity_progress.get("total") or 0} · batch of {len(rows)}'))
            results = await asyncio.gather(*(self._collect_wallet(rt, run, row, maximum_pages) for row in rows), return_exceptions=True)
            for row, result in zip(rows, results):
                if isinstance(result, Exception):
                    self.db.execute(
                        "UPDATE run_wallets SET status='failed',attempts=attempts+1,error=?,finished_at=? WHERE run_id=? AND entity_id=? AND address=?",
                        (str(result)[:2000], now_iso(), run_id, row["entity_id"], row["address"]),
                    )
                    self.db.add_error(run_id, "backfill", str(result), entity_id=row["entity_id"], item=row["address"])
                else:
                    count = int(result.get("events", result.get("swap_activities", 0)))
                    self.db.execute(
                        "UPDATE run_wallets SET status='completed',attempts=attempts+1,events=?,error=NULL,finished_at=? WHERE run_id=? AND entity_id=? AND address=?",
                        (count, now_iso(), run_id, row["entity_id"], row["address"]),
                    )
            self._refresh_counts(run_id)
            if run_id in self._wakeups:
                self._wakeups[run_id].set()

    async def _collect_wallet(self, rt: Runtime, run: dict[str, Any], row: dict[str, Any], max_pages: int | None) -> dict[str, Any]:
        self.db.execute(
            "UPDATE run_wallets SET status='running',started_at=? WHERE run_id=? AND entity_id=? AND address=?",
            (now_iso(), run["run_id"], row["entity_id"], row["address"]),
        )
        address = row["address"]
        if address.startswith("0x"):
            result = await backfill_evm_wallet(rt.debank, rt.storage, entity_id=row["entity_id"], address=address,
                                               min_timestamp=parse_date(run["from_date"]), max_pages=max_pages)
            try:
                await snapshot_evm_wallet_context(rt.arkham, rt.oklink, rt.gecko, rt.storage,
                                                  entity_id=row["entity_id"], address=address, max_tokens=3)
            except Exception as exc:
                self.db.add_error(run["run_id"], "wallet_context", str(exc), entity_id=row["entity_id"], item=address)
            return result
        if row.get("chain") == "solana":
            return await collect_solana_wallet(rt.jupiter, rt.storage, entity_id=row["entity_id"], address=address, max_pages=max_pages)
        return {"events": 0}

    async def _processor_loop(self, rt: Runtime, run_id: str) -> None:
        expensive_counter = 0
        while True:
            try:
                await self._checkpoint(run_id)
                expensive_counter += 1
                await self._process_once(rt, run_id, final=expensive_counter % 5 == 0)
                wakeup = self._wakeups[run_id]
                wakeup.clear()
                try:
                    await asyncio.wait_for(wakeup.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
            except SafeStop:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.db.add_error(run_id, "incremental_processor", str(exc))
                await asyncio.sleep(2)

    async def _process_once(self, rt: Runtime, run_id: str, *, final: bool = False) -> int:
        run = self.db.run(run_id)
        if not run:
            return 0
        from_ts = parse_date(run["from_date"])
        to_ts = parse_date(run["to_date"]) + 86399
        # Only entities with newly inserted event rowids are rebuilt, and only around their dirty time range.
        for entity_id in run["entities"]:
            watermark = self.db.row(
                "SELECT value_integer FROM run_watermarks WHERE run_id=? AND stream='events' AND entity_id=?", (run_id, entity_id)
            )
            last = int(watermark["value_integer"]) if watermark else 0
            dirty = rt.storage.fetchone(
                "SELECT MIN(ts) low,MAX(ts) high,MAX(rowid) max_row FROM wallet_events WHERE entity_id=? AND rowid>? AND ts BETWEEN ? AND ?",
                (entity_id, last, from_ts, to_ts),
            )
            if dirty and dirty.get("max_row"):
                threshold = float(run["settings"].get("enrichment_threshold", 100000) or 0)
                candidates = rt.storage.fetchall(
                    """SELECT e.* FROM wallet_events e WHERE e.entity_id=? AND e.rowid>? AND e.rowid<=?
                       AND e.tx_hash IS NOT NULL AND COALESCE(e.usd_value,0)>=?
                       AND NOT EXISTS (SELECT 1 FROM tx_enrichment x WHERE x.tx_hash=e.tx_hash AND x.chain=e.chain)
                       ORDER BY e.usd_value DESC""",
                    (entity_id, last, dirty["max_row"], threshold),
                )
                if candidates:
                    semaphore = asyncio.Semaphore(rt.settings.concurrency)
                    async def enrich_one(event):
                        async with semaphore:
                            return await enrich_evm_event(
                                rt.storage, rt.oklink, rt.arkham, rt.rpc, rt.bridges, event,
                                bridge_min_usd=rt.settings.bridge_check_min_usd,
                            )
                    results = await asyncio.gather(*(
                        enrich_one(event) for event in candidates
                    ), return_exceptions=True)
                    for event, result in zip(candidates, results):
                        if isinstance(result, Exception):
                            self.db.add_error(run_id, "enrichment", str(result), entity_id=entity_id, item=event.get("tx_hash"))
                gap = rt.settings.episode_gap_seconds
                await asyncio.to_thread(build_episodes, rt.storage, entity_id, gap, rt.settings.max_episode_events,
                                        start_ts=max(from_ts, int(dirty["low"]) - gap * 2), end_ts=min(to_ts, int(dirty["high"]) + gap * 2))
                self.db.execute(
                    """INSERT INTO run_watermarks(run_id,stream,entity_id,value_integer,updated_at) VALUES(?,'events',?,?,?)
                       ON CONFLICT(run_id,stream,entity_id) DO UPDATE SET value_integer=excluded.value_integer,updated_at=excluded.updated_at""",
                    (run_id, entity_id, dirty["max_row"], now_iso()),
                )

        llm_limit = 100 if final else 20
        if not rt.settings.llm_api_key:
            raise RuntimeError("FREE_LLM_API is required before episode classification can continue")
        for entity_id in run["entities"]:
            llm_concurrency = max(1, min(16, int(run["settings"].get("llm_concurrency", 4))))
            completed = await classify_episodes(rt.storage, rt.llm, entity_id, concurrency=min(llm_concurrency, rt.settings.concurrency),
                                                start_ts=from_ts, end_ts=to_ts, limit=llm_limit)
            if run_id in self.runtime_metrics:
                self.runtime_metrics[run_id]["llm_completed"] += completed

        episodes = rt.storage.fetchall(
            """SELECT e.* FROM episodes e WHERE e.entity_id IN ({}) AND e.start_ts BETWEEN ? AND ?
               AND e.intent_label IS NOT NULL AND COALESCE(e.target_asset_key,e.primary_asset_key) IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM episode_market_context c WHERE c.episode_id=e.episode_id)
               ORDER BY e.start_ts LIMIT ?""".format(",".join("?" for _ in run["entities"])),
            [*run["entities"], from_ts, to_ts, 100 if final else 20],
        )
        if episodes:
            await MarketLabeler(rt.llama, rt.storage).label_episodes(episodes, DEFAULT_HORIZONS, concurrency=min(8, rt.settings.concurrency))
        IncrementalAnalyzer(self.db).refresh(run_id, expensive=final)
        self._refresh_counts(run_id)
        self._persist_metrics(run_id, rt)
        queues = self.queue_counts(run_id)
        return sum(queues.values())

    def queue_counts(self, run_id: str) -> dict[str, int]:
        run = self.db.run(run_id)
        if not run:
            return {"episode_builder": 0, "llm_classification": 0, "market_labeling": 0, "analysis": 0}
        entities = run["entities"]
        marks = ",".join("?" for _ in entities)
        from_ts, to_ts = parse_date(run["from_date"]), parse_date(run["to_date"]) + 86399
        llm = self.db.row(f"SELECT COUNT(*) n FROM episodes WHERE entity_id IN ({marks}) AND start_ts BETWEEN ? AND ? AND intent_label IS NULL", [*entities, from_ts, to_ts])["n"]
        market = self.db.row(f"""SELECT COUNT(*) n FROM episodes e WHERE entity_id IN ({marks}) AND start_ts BETWEEN ? AND ? AND intent_label IS NOT NULL
          AND COALESCE(target_asset_key,primary_asset_key) IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM episode_market_context c WHERE c.episode_id=e.episode_id)""", [*entities, from_ts, to_ts])["n"]
        analysis = self.db.row(f"""SELECT COUNT(*) n FROM episodes e JOIN market_labels m ON m.episode_id=e.episode_id
          WHERE e.entity_id IN ({marks}) AND e.start_ts BETWEEN ? AND ? AND m.simple_return IS NOT NULL AND NOT EXISTS
          (SELECT 1 FROM analysis_observations o WHERE o.run_id=? AND o.episode_id=e.episode_id AND o.asset_key=m.asset_key AND o.horizon_seconds=m.horizon_seconds)""",
          [*entities, from_ts, to_ts, run_id])["n"]
        return {"episode_builder": 0, "llm_classification": int(llm), "market_labeling": int(market), "analysis": int(analysis)}

    def _refresh_counts(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if not run:
            return
        from_ts, to_ts = parse_date(run["from_date"]), parse_date(run["to_date"]) + 86399
        with self.db.connect() as db:
            for entity_id in run["entities"]:
                wallet = db.execute("SELECT COUNT(*),SUM(status='completed'),SUM(status='failed') FROM run_wallets WHERE run_id=? AND entity_id=?", (run_id, entity_id)).fetchone()
                events = db.execute("SELECT COUNT(*) FROM wallet_events WHERE entity_id=? AND ts BETWEEN ? AND ?", (entity_id, from_ts, to_ts)).fetchone()[0]
                episodes = db.execute("SELECT COUNT(*),SUM(intent_label IS NOT NULL) FROM episodes WHERE entity_id=? AND start_ts BETWEEN ? AND ?", (entity_id, from_ts, to_ts)).fetchone()
                labels = db.execute("SELECT COUNT(*) FROM market_labels m JOIN episodes e ON e.episode_id=m.episode_id WHERE e.entity_id=? AND e.start_ts BETWEEN ? AND ?", (entity_id, from_ts, to_ts)).fetchone()[0]
                usable = db.execute("""SELECT COUNT(*) FROM (
                  SELECT 1 FROM analysis_observations WHERE run_id=? AND entity_id=?
                  GROUP BY episode_id,asset_key,horizon_seconds
                )""", (run_id, entity_id)).fetchone()[0]
                db.execute("""UPDATE run_entities SET wallets_total=?,wallets_processed=?,wallets_failed=?,events=?,episodes=?,classified=?,market_labels=?,usable_observations=?,updated_at=? WHERE run_id=? AND entity_id=?""",
                           (wallet[0] or 0, wallet[1] or 0, wallet[2] or 0, events, episodes[0] or 0, episodes[1] or 0, labels, usable, now_iso(), run_id, entity_id))
            totals = db.execute("SELECT COALESCE(SUM(wallets_total),0),COALESCE(SUM(wallets_processed+wallets_failed),0) FROM run_entities WHERE run_id=?", (run_id,)).fetchone()
            progress = 0.08 + 0.82 * (totals[1] / totals[0] if totals[0] else 0)
            db.execute("UPDATE analysis_runs SET progress=MAX(progress,?),updated_at=? WHERE run_id=?", (min(progress, .9), now_iso(), run_id))

    def live_metrics(self, run_id: str) -> dict[str, Any]:
        metrics = self.runtime_metrics.get(run_id, {})
        run = self.db.run(run_id) or {}
        if metrics:
            elapsed = max(1.0, time.monotonic() - metrics.get("started_monotonic", time.monotonic()))
            hub = dict(metrics.get("hub") or {})
            llm_completed = metrics.get("llm_completed", 0)
        else:
            started = datetime.fromisoformat(run["started_at"]) if run.get("started_at") else datetime.now(timezone.utc)
            ended = datetime.fromisoformat(run["finished_at"]) if run.get("finished_at") else datetime.now(timezone.utc)
            elapsed = max(1.0, (ended - started).total_seconds())
            saved = self.db.row("SELECT * FROM run_metrics WHERE run_id=?", (run_id,)) or {}
            hub = {"requests": saved.get("api_requests", 0), "success": saved.get("api_success", 0),
                   "cache_hit": saved.get("cache_hits", 0), "retry": saved.get("retries", 0), "failed": saved.get("failed", 0)}
            llm_completed = saved.get("llm_completed", 0)
        processed = self.db.row("SELECT COALESCE(SUM(events),0) events,COALESCE(SUM(status='completed'),0) wallets FROM run_wallets WHERE run_id=?", (run_id,)) or {}
        return {"api": hub, "events_per_minute": round(60 * processed.get("events", 0) / elapsed, 1),
                "wallets_per_hour": round(3600 * processed.get("wallets", 0) / elapsed, 1),
                "api_requests_per_second": round(hub.get("requests", 0) / elapsed, 1),
                "llm_jobs_per_second": round(llm_completed / elapsed, 2)}

    def _persist_metrics(self, run_id: str, rt: Runtime) -> None:
        hub = rt.hub.metrics
        llm_completed = self.runtime_metrics.get(run_id, {}).get("llm_completed", 0)
        self.db.execute(
            """INSERT INTO run_metrics(run_id,api_requests,api_success,cache_hits,retries,failed,llm_completed,updated_at)
               VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET api_requests=excluded.api_requests,
               api_success=excluded.api_success,cache_hits=excluded.cache_hits,retries=excluded.retries,
               failed=excluded.failed,llm_completed=excluded.llm_completed,updated_at=excluded.updated_at""",
            (run_id, hub.get("requests", 0), hub.get("success", 0), hub.get("cache_hit", 0), hub.get("retry", 0),
             hub.get("failed", 0), llm_completed, now_iso()),
        )
