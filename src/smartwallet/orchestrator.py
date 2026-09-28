from __future__ import annotations

import asyncio
import json
import math
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
from .pipeline.episodes import build_episodes, dirty_windows
from .pipeline.market import DEFAULT_HORIZONS, MarketLabeler
from .pipeline.wallet_context import snapshot_evm_wallet_context
from .runtime import Runtime
from .settings import Settings
from .http import request_context
from .webdb import WebDB, now_iso


class SafeStop(Exception):
    pass


class RunOrchestrator:
    """Owns background work; persisted desired_status is the control plane."""

    def __init__(self, db: WebDB):
        self.db = db
        self.tasks: dict[str, asyncio.Task] = {}
        self.runtime_metrics: dict[str, dict[str, Any]] = {}
        self._samples = {}
        self._draining = set()
        self.full_refresh_requests = set()
        self.analysis_locks = {}
        self._wakeups: dict[str, asyncio.Event] = {}

    def active(self, run_id: str) -> bool:
        task = self.tasks.get(run_id)
        return bool(task and not task.done())

    def launch(self, run_id: str) -> None:
        if self.active(run_id):
            return
        self._draining.discard(run_id)
        self._wakeups[run_id] = asyncio.Event()
        self.tasks[run_id] = asyncio.create_task(self._run(run_id), name=f"analysis-{run_id[:8]}")

    async def control(self, run_id: str, action: str) -> dict[str, Any]:
        run = self.db.run(run_id)
        if not run:
            raise KeyError(run_id)
        if action == "pause":
            self.db.update_run(run_id, desired_status="paused", status="pausing" if self.active(run_id) else "paused")
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
            self.db.update_run(run_id, desired_status="stopped", status="stopping" if self.active(run_id) else "stopped",
                               finished_at=None if self.active(run_id) else now_iso())
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
        stage_tasks: list[asyncio.Task] = []
        stages_stop = asyncio.Event()
        started = time.monotonic()
        try:
            settings = Settings.load()
            requested = run["settings"]
            settings = replace(settings, concurrency=max(1, min(32, int(requested.get("concurrency", settings.concurrency)))))
            rt = Runtime.create(settings)
            rt.storage.run_id = run_id
            rt.storage.run_bounds = (parse_date(run["from_date"]), parse_date(run["to_date"])+86399)
            def observe(endpoint, provider, attempt, state, error, next_retry, context):
                self.db.execute("INSERT INTO request_attempts(run_id,entity_id,wallet,provider,endpoint,attempt,state,error,next_retry) VALUES(?,?,?,?,?,?,?,?,?)", (run_id,*context,provider,endpoint,attempt,state,error,next_retry))
            rt.hub.observer = observe
            saved = self.db.row("SELECT * FROM run_metrics WHERE run_id=?", (run_id,)) or {}
            for key,column in (("requests","api_requests"),("success","api_success"),("cache_hit","cache_hits"),("retry","retries"),("failed","failed")):
                if column in saved:
                    rt.hub.metrics[key] = saved[column]
            self.runtime_metrics[run_id] = {
                "started_monotonic": started,
                "hub": rt.hub.metrics,
                "classification_completed": saved.get("deterministic_completed", saved.get("llm_completed", 0)),
                "classification_seconds": saved.get("classification_seconds", 0.0),
                "market_cache_hits": saved.get("market_cache_hits", 0),
                "market_fetched_points": saved.get("market_fetched_points", 0),
                "market_unavailable_points": saved.get("market_unavailable_points", 0),
                "market_external_calls": saved.get("market_external_calls", 0),
                "market_horizons_processed": saved.get("market_horizons_processed", 0),
                "market_seconds": saved.get("market_seconds", 0.0),
            }
            self.db.update_run(run_id, status="running", stage="discovery", started_at=run.get("started_at") or now_iso(), error=None)
            await self._discover(rt, run)
            stage_tasks = [asyncio.create_task(self._stage_loop(rt, run_id, stage, stages_stop),
                                               name=f"{stage}-{run_id[:8]}")
                           for stage in ("episodes", "classification", "market", "analysis", "enrichment")]
            await self._collect(rt, run)
            self._draining.add(run_id)
            self._wakeups[run_id].set()
            self.db.update_run(run_id, stage="draining", current_work="Draining concurrent core queues")
            stable = 0
            while stable < 2:
                await self._checkpoint(run_id)
                pending = self._core_pending(run_id)
                stable = stable + 1 if pending == 0 else 0
                await asyncio.sleep(0.25)
            stages_stop.set()
            await asyncio.gather(*stage_tasks, return_exceptions=False)
            async with self.analysis_locks.setdefault(run_id,asyncio.Lock()):
                await asyncio.to_thread(IncrementalAnalyzer(self.db).refresh, run_id, expensive=True)
            self._refresh_counts(run_id)
            self._persist_metrics(run_id, rt)
            failures = self.db.row("SELECT COUNT(*) n FROM run_wallets WHERE run_id=? AND status IN ('partial','failed')", (run_id,))["n"]
            self.db.update_run(run_id, status="completed", desired_status="completed", stage="completed",
                               current_work="Analysis complete" + (f"; {failures} failed/partial items; inspect Data Quality" if failures else ""), progress=1.0, finished_at=now_iso())
        except SafeStop:
            self.db.update_run(run_id, status="stopped", desired_status="stopped", stage="stopped",
                               current_work="Stopped safely", finished_at=now_iso())
        except asyncio.CancelledError:
            self.db.update_run(run_id, status="paused", desired_status="paused", stage="interrupted",
                               current_work="Worker interrupted safely; resume continues durable queues")
            raise
        except Exception as exc:
            self.db.add_error(run_id, "orchestrator", f"{type(exc).__name__}: {exc}", retryable=True)
            self.db.update_run(run_id, status="failed", desired_status="paused", stage="failed", error=str(exc),
                               current_work="Run failed — inspect errors, then resume")
        finally:
            stages_stop.set()
            for task in stage_tasks:
                task.cancel()
            if stage_tasks:
                await asyncio.gather(*stage_tasks, return_exceptions=True)
            if rt:
                await rt.aclose()
            self.runtime_metrics.pop(run_id, None)

    async def _discover(self, rt: Runtime, run: dict[str, Any]) -> None:
        configured = {row["id"]: row["name"] for row in load_entities()}
        for index, entity_id in enumerate(run["entities"]):
            await self._checkpoint(run["run_id"])
            self.db.update_run(run["run_id"], current_work=f"Discovering {configured.get(entity_id, entity_id)}",
                               progress=0.02 + 0.05 * index / max(1, len(run["entities"])))
            request_context.set((entity_id, None))
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
        async def worker():
            while True:
                await self._checkpoint(run_id)
                with self.db.connect() as db:
                    candidate = db.execute(
                        """SELECT entity_id,address FROM run_wallets WHERE run_id=? AND status='pending'
                           AND attempts<3 ORDER BY attempts,address LIMIT 1""", (run_id,),
                    ).fetchone()
                    if not candidate:
                        row = None
                    else:
                        claimed = db.execute(
                            """UPDATE run_wallets SET status='running',attempts=attempts+1,started_at=?
                               WHERE run_id=? AND entity_id=? AND address=? AND status='pending'""",
                            (now_iso(), run_id, candidate[0], candidate[1]),
                        ).rowcount
                        row = dict(db.execute(
                            "SELECT * FROM run_wallets WHERE run_id=? AND entity_id=? AND address=?",
                            (run_id, candidate[0], candidate[1]),
                        ).fetchone()) if claimed else None
                if row is None:
                    return
                eid,address = row["entity_id"],row["address"]
                self.db.update_run(run_id,stage="historical_backfill",current_work=f"{eid}: collecting wallet history")
                status, error = "completed", None
                try:
                    result = await asyncio.wait_for(self._collect_wallet(rt,run,row,run["settings"].get("max_pages")), timeout=float(os.getenv("SMARTWALLET_WALLET_TIMEOUT","600")))
                    if result.get("context_error"):
                        status,error = "partial",result["context_error"]
                except Exception as exc:
                    error = str(exc)[:2000] or "Wallet deadline exceeded"
                    status = "failed"
                    self.db.add_error(run_id,"backfill",error,entity_id=eid,item=address,retryable=False)
                count = self.db.row("SELECT COUNT(*) n FROM run_events r JOIN wallet_events e USING(event_id) WHERE r.run_id=? AND e.entity_id=? AND e.wallet=?", (run_id,eid,address))["n"]
                if count and status == "failed":
                    status = "partial"
                self.db.execute("UPDATE run_wallets SET status=?,events=?,error=?,finished_at=? WHERE run_id=? AND entity_id=? AND address=?", (status,count,error,now_iso(),run_id,eid,address))
                self._persist_metrics(run_id,rt)
                self._refresh_counts(run_id)
                if run_id in self._wakeups:
                    self._wakeups[run_id].set()
        tasks = [asyncio.create_task(worker()) for _ in range(max(1,min(rt.settings.concurrency,8)))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    async def _collect_wallet(self, rt: Runtime, run: dict[str, Any], row: dict[str, Any], max_pages: int | None) -> dict[str, Any]:
        self.db.execute(
            "UPDATE run_wallets SET status='running',started_at=? WHERE run_id=? AND entity_id=? AND address=?",
            (now_iso(), run["run_id"], row["entity_id"], row["address"]),
        )
        address = row["address"]
        request_context.set((row["entity_id"], address))
        cache_params = (row["entity_id"], address, parse_date(run["from_date"]), parse_date(run["to_date"])+86399, max_pages or 0)
        if self.db.row("SELECT 1 FROM wallet_collection_cache WHERE entity_id=? AND address=? AND from_ts=? AND to_ts=? AND max_pages=?", cache_params):
            self.db.execute("INSERT OR IGNORE INTO run_events(run_id,event_id) SELECT ?,event_id FROM wallet_events WHERE entity_id=? AND wallet=? AND ts BETWEEN ? AND ?", (run["run_id"],*cache_params[:4]))
            rt.hub.metrics["cache_hit"] += 1
            count = self.db.row("SELECT COUNT(*) n FROM wallet_events WHERE entity_id=? AND wallet=? AND ts BETWEEN ? AND ?", cache_params[:4])["n"]
            return {"events":count}
        if address.startswith("0x"):
            result = await backfill_evm_wallet(rt.debank, rt.storage, entity_id=row["entity_id"], address=address,
                                               min_timestamp=parse_date(run["from_date"]),
                                               max_timestamp=parse_date(run["to_date"])+86399, max_pages=max_pages)
            self.db.execute(
                """INSERT OR IGNORE INTO deferred_jobs(run_id,kind,item_key,entity_id,payload_json,priority,updated_at)
                   VALUES(?,'wallet_context',?,?,?,0,?)""",
                (run["run_id"], address.lower(), row["entity_id"], json.dumps({"address": address}), now_iso()),
            )
            self.db.execute("INSERT OR IGNORE INTO wallet_collection_cache VALUES(?,?,?,?,?)", cache_params)
            self.db.execute(
                """INSERT OR IGNORE INTO run_events(run_id,event_id)
                   SELECT ?,event_id FROM wallet_events WHERE entity_id=? AND wallet=? AND ts BETWEEN ? AND ?""",
                (run["run_id"], row["entity_id"], address.lower(), cache_params[2], cache_params[3]),
            )
            return result
        if row.get("chain") == "solana":
            return await collect_solana_wallet(rt.jupiter, rt.storage, entity_id=row["entity_id"], address=address, max_pages=max_pages)
        raise RuntimeError("Unsupported chain: " + str(row.get("chain")))

    async def _stage_loop(self, rt: Runtime, run_id: str, stage: str, stop: asyncio.Event) -> None:
        handlers = {
            "episodes": self._build_once,
            "classification": self._classify_once,
            "market": self._market_once,
            "analysis": self._analyze_once,
            "enrichment": self._enrich_once,
        }
        while not stop.is_set():
            try:
                await self._checkpoint(run_id)
                worked = await handlers[stage](rt, run_id)
                if not worked:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=0.35 if stage != "enrichment" else 1.0)
                    except asyncio.TimeoutError:
                        pass
            except SafeStop:
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.db.add_error(run_id, stage, f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(0.5)

    async def _build_once(self, rt: Runtime, run_id: str) -> int:
        run = self.db.run(run_id)
        if not run:
            return 0
        threshold = float(run["settings"].get("enrichment_threshold", 100000) or 0)
        with self.db.connect() as db:
            db.execute(
                """INSERT OR IGNORE INTO deferred_jobs(run_id,kind,item_key,entity_id,payload_json,priority,updated_at)
                   SELECT ?, 'tx_enrichment', e.chain||':'||e.tx_hash,e.entity_id,
                          json_object('event_id',e.event_id),CAST(COALESCE(e.usd_value,0) AS INTEGER),?
                   FROM run_events r JOIN wallet_events e USING(event_id)
                   WHERE r.run_id=? AND r.built=0 AND e.tx_hash IS NOT NULL AND COALESCE(e.usd_value,0)>=?""",
                (run_id, now_iso(), run_id, threshold),
            )
        built = 0
        for entity_id in run["entities"]:
            windows = dirty_windows(rt.storage, run_id, entity_id, rt.settings.episode_gap_seconds, limit=5000)
            for start_ts, end_ts in windows[:8]:
                episodes = await asyncio.to_thread(
                    build_episodes, rt.storage, entity_id, rt.settings.episode_gap_seconds,
                    rt.settings.max_episode_events, start_ts=start_ts, end_ts=end_ts, run_id=run_id,
                )
                built += len(episodes)
        return built

    async def _classify_once(self, rt: Runtime, run_id: str) -> int:
        run = self.db.run(run_id)
        if not run:
            return 0
        completed = 0
        started = time.perf_counter()
        for entity_id in run["entities"]:
            completed += await classify_episodes(rt.storage, None, entity_id, run_id=run_id, limit=1000)
        if completed and run_id in self.runtime_metrics:
            self.runtime_metrics[run_id]["classification_completed"] += completed
            self.runtime_metrics[run_id]["classification_seconds"] += time.perf_counter() - started
        return completed

    async def _market_once(self, rt: Runtime, run_id: str) -> int:
        episodes = rt.storage.fetchall(
            """SELECT e.* FROM episodes e JOIN run_episodes r USING(episode_id) WHERE r.run_id=?
               AND EXISTS(SELECT 1 FROM market_horizons h WHERE h.episode_id=e.episode_id
               AND h.state IN ('pending','retryable failure') AND h.next_retry<=?)
               ORDER BY e.start_ts LIMIT 2000""", (run_id, time.time()),
        )
        if not episodes:
            return 0
        result = await MarketLabeler(rt.llama, rt.storage).label_episodes(
            episodes, DEFAULT_HORIZONS, concurrency=min(8, rt.settings.concurrency),
        )
        runtime = self.runtime_metrics.get(run_id)
        if runtime is not None:
            for key in ("cache_hits", "fetched_points", "unavailable_points", "external_calls", "horizons_processed"):
                runtime[f"market_{key}"] += result.get(key, 0)
            runtime["market_seconds"] += result.get("elapsed_seconds", 0.0)
        return int(result.get("horizons_processed", 0))

    async def _analyze_once(self, rt: Runtime, run_id: str) -> int:
        async with self.analysis_locks.setdefault(run_id, asyncio.Lock()):
            result = await asyncio.to_thread(IncrementalAnalyzer(self.db).refresh, run_id, batch_size=2000)
        return int(result.get("observations", 0)) + int(result.get("primary_observations", 0))

    async def _enrich_once(self, rt: Runtime, run_id: str) -> int:
        with self.db.connect() as db:
            row = db.execute(
                """SELECT * FROM deferred_jobs WHERE run_id=? AND state IN ('pending','retryable')
                   AND next_retry<=? ORDER BY priority DESC,updated_at LIMIT 1""", (run_id, time.time()),
            ).fetchone()
            if not row:
                return 0
            claimed = db.execute(
                """UPDATE deferred_jobs SET state='running',attempts=attempts+1,updated_at=?
                   WHERE run_id=? AND kind=? AND item_key=? AND state IN ('pending','retryable')""",
                (now_iso(), run_id, row["kind"], row["item_key"]),
            ).rowcount
        if not claimed:
            return 0
        job = dict(row)
        try:
            payload = json.loads(job["payload_json"] or "{}")
            if job["kind"] == "wallet_context":
                await snapshot_evm_wallet_context(rt.arkham, rt.oklink, rt.gecko, rt.storage,
                                                  entity_id=job["entity_id"], address=payload["address"], max_tokens=3)
            elif job["kind"] == "tx_enrichment":
                event = rt.storage.fetchone("SELECT * FROM wallet_events WHERE event_id=?", (payload["event_id"],))
                if event:
                    await enrich_evm_event(rt.storage, rt.oklink, rt.arkham, rt.rpc, rt.bridges, event,
                                           bridge_min_usd=rt.settings.bridge_check_min_usd)
                    with self.db.connect() as db:
                        db.execute(
                            """UPDATE run_events SET built=0 WHERE run_id=? AND event_id IN
                               (SELECT event_id FROM wallet_events WHERE tx_hash=? AND chain=?)""",
                            (run_id, event["tx_hash"], event["chain"]),
                        )
            self.db.execute("UPDATE deferred_jobs SET state='complete',error=NULL,updated_at=? WHERE run_id=? AND kind=? AND item_key=?",
                            (now_iso(), run_id, job["kind"], job["item_key"]))
        except Exception as exc:
            attempts = int(job["attempts"]) + 1
            state = "failed" if attempts >= 3 else "retryable"
            self.db.execute(
                """UPDATE deferred_jobs SET state=?,error=?,next_retry=?,updated_at=?
                   WHERE run_id=? AND kind=? AND item_key=?""",
                (state, str(exc)[:500], 0 if state == "failed" else time.time() + 2**attempts,
                 now_iso(), run_id, job["kind"], job["item_key"]),
            )
        return 1

    def _core_pending(self, run_id: str) -> int:
        now = time.time()
        queries = (
            ("SELECT COUNT(*) n FROM run_events WHERE run_id=? AND built=0", (run_id,)),
            ("""SELECT COUNT(*) n FROM run_episodes r JOIN episodes e USING(episode_id)
                WHERE r.run_id=? AND e.intent_label IS NULL""", (run_id,)),
            ("""SELECT COUNT(*) n FROM run_episodes r JOIN market_horizons h USING(episode_id)
                WHERE r.run_id=? AND h.state IN ('pending','retryable failure') AND h.next_retry<=?""", (run_id, now)),
            ("""SELECT COUNT(*) n FROM run_episodes r JOIN market_labels m USING(episode_id)
                WHERE r.run_id=? AND m.simple_return IS NOT NULL AND NOT EXISTS(
                  SELECT 1 FROM primary_analysis_observations o WHERE o.run_id=r.run_id
                  AND o.episode_id=r.episode_id AND o.asset_key=m.asset_key AND o.horizon_seconds=m.horizon_seconds)""", (run_id,)),
        )
        return sum(int((self.db.row(sql, params) or {"n": 0})["n"]) for sql, params in queries)

    async def _processor_loop(self, rt: Runtime, run_id: str) -> None:
        while run_id not in self._draining:
            try:
                await self._checkpoint(run_id)
                await self._process_once(rt, run_id, final=False)
                if run_id in self._draining:
                    return
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
        for entity_id in run["entities"]:
            dirty = self.db.row("SELECT COUNT(*) n FROM run_events r JOIN wallet_events e USING(event_id) WHERE r.run_id=? AND e.entity_id=? AND r.built=0", (run_id,entity_id))
            if dirty["n"]:
                request_context.set((entity_id,None))
                threshold = float(run["settings"].get("enrichment_threshold",100000) or 0)
                candidates = rt.storage.fetchall("""SELECT e.* FROM wallet_events e JOIN run_events r USING(event_id)
                    WHERE r.run_id=? AND r.built=0 AND e.entity_id=? AND e.tx_hash IS NOT NULL AND COALESCE(e.usd_value,0)>=?
                    AND NOT EXISTS(SELECT 1 FROM tx_enrichment x WHERE x.tx_hash=e.tx_hash AND x.chain=e.chain)""", (run_id,entity_id,threshold))
                semaphore = asyncio.Semaphore(min(8,rt.settings.concurrency))
                async def enrich(event):
                    async with semaphore:
                        try:
                            await asyncio.wait_for(enrich_evm_event(rt.storage,rt.oklink,rt.arkham,rt.rpc,rt.bridges,event,bridge_min_usd=rt.settings.bridge_check_min_usd),timeout=120)
                        except Exception as exc:
                            self.db.add_error(run_id,"enrichment",str(exc) or "Enrichment deadline exceeded",entity_id=entity_id,item=event["tx_hash"],retryable=False)
                await asyncio.gather(*(enrich(event) for event in candidates))
                await asyncio.to_thread(build_episodes, rt.storage, entity_id, rt.settings.episode_gap_seconds,
                    rt.settings.max_episode_events, start_ts=from_ts, end_ts=to_ts, run_id=run_id)

        for entity_id in run["entities"]:
            request_context.set((entity_id,None))
            classification_started = time.perf_counter()
            completed = await classify_episodes(
                rt.storage, None, entity_id, start_ts=from_ts, end_ts=to_ts, run_id=run_id,
            )
            if run_id in self.runtime_metrics:
                self.runtime_metrics[run_id]["classification_completed"] += completed
                self.runtime_metrics[run_id]["classification_seconds"] += time.perf_counter() - classification_started

        episodes = rt.storage.fetchall(
            """SELECT e.* FROM episodes e JOIN run_episodes r USING(episode_id) WHERE r.run_id=?
               AND EXISTS(SELECT 1 FROM market_horizons h WHERE h.episode_id=e.episode_id
               AND h.state IN ('pending','retryable failure') AND h.next_retry<=?) ORDER BY e.start_ts""",
            (run_id,time.time()))
        if episodes:
            market_result = await MarketLabeler(rt.llama, rt.storage).label_episodes(
                episodes, DEFAULT_HORIZONS, concurrency=min(8, rt.settings.concurrency),
            )
            runtime = self.runtime_metrics.get(run_id)
            if runtime is not None:
                for key in ("cache_hits", "fetched_points", "unavailable_points", "external_calls", "horizons_processed"):
                    runtime[f"market_{key}"] += market_result.get(key, 0)
                runtime["market_seconds"] += market_result.get("elapsed_seconds", 0.0)
        async with self.analysis_locks.setdefault(run_id,asyncio.Lock()):
            analyzer = IncrementalAnalyzer(self.db)
            await asyncio.to_thread(analyzer.refresh, run_id)
            full = run_id in self.full_refresh_requests
            self.full_refresh_requests.discard(run_id)
            await asyncio.to_thread(analyzer.refresh_expensive, run_id, dirty_only=not full)
        self._refresh_counts(run_id)
        self._persist_metrics(run_id, rt)
        queues = self.queue_counts(run_id)
        return sum(queues.values())

    async def _analysis_loop(self, run_id: str, stop: asyncio.Event) -> None:
        """Primary analysis is independent of intent and slow price requests."""
        while not stop.is_set():
            run = self.db.run(run_id)
            if run and run["desired_status"] == "running":
                try:
                    async with self.analysis_locks.setdefault(run_id,asyncio.Lock()):
                        await asyncio.to_thread(IncrementalAnalyzer(self.db).refresh,run_id)
                except Exception as exc:
                    self.db.add_error(run_id,"analysis",type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(),timeout=5)
            except asyncio.TimeoutError:
                pass

    def queue_counts(self, run_id: str) -> dict[str, int]:
        run = self.db.run(run_id)
        if not run:
            return {"collection": 0, "episode_builder": 0, "intent_classification": 0, "market_labeling": 0, "analysis": 0}
        def count(sql):
            return int(self.db.row(sql, (run_id,))["n"])
        missing_primary = count("SELECT COUNT(*) n FROM run_episodes r JOIN episodes e USING(episode_id) JOIN market_labels m USING(episode_id) WHERE r.run_id=? AND m.simple_return IS NOT NULL AND NOT EXISTS(SELECT 1 FROM primary_analysis_observations o WHERE o.run_id=r.run_id AND o.episode_id=e.episode_id AND o.asset_key=m.asset_key AND o.horizon_seconds=m.horizon_seconds)")
        # A completed pre-upgrade run remains quiescent/readable until explicitly refreshed.
        if run.get("status") == "completed" and not count("SELECT COUNT(*) n FROM primary_analysis_observations WHERE run_id=?") and count("SELECT COUNT(*) n FROM analysis_observations WHERE run_id=?"):
            missing_primary = 0
        return {
            "collection": count("SELECT COUNT(*) n FROM run_wallets WHERE run_id=? AND status IN ('pending','running')"),
            "episode_builder": count("SELECT COUNT(*) n FROM run_events WHERE run_id=? AND built=0"),
            "intent_classification": count("SELECT COUNT(*) n FROM run_episodes r JOIN episodes e USING(episode_id) WHERE r.run_id=? AND e.intent_label IS NULL"),
            "market_labeling": count("SELECT COUNT(*) n FROM run_episodes r JOIN market_horizons h USING(episode_id) WHERE r.run_id=? AND h.state IN ('pending','retryable failure')"),
            "analysis": count("SELECT COUNT(*) n FROM pattern_aggregates WHERE run_id=? AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=pattern_aggregates.run_id) AND n>=10 AND (last_expensive_n=0 OR n>=MIN(last_expensive_n+10,CAST(CEIL(last_expensive_n*1.2) AS INTEGER)))") + missing_primary}

    def _refresh_counts(self, run_id: str) -> None:
        run = self.db.run(run_id)
        if not run:
            return
        from_ts, to_ts = parse_date(run["from_date"]), parse_date(run["to_date"]) + 86399
        with self.db.connect() as db:
            for entity_id in run["entities"]:
                wallet = db.execute("SELECT COUNT(*),SUM(status='completed'),SUM(status='failed'),SUM(status='partial') FROM run_wallets WHERE run_id=? AND entity_id=?", (run_id, entity_id)).fetchone()
                events = db.execute("SELECT COUNT(*) FROM wallet_events WHERE entity_id=? AND event_id IN (SELECT event_id FROM run_events WHERE run_id=?)", (entity_id, run_id)).fetchone()[0]
                episodes = db.execute("SELECT COUNT(*),SUM(intent_label IS NOT NULL) FROM episodes WHERE entity_id=? AND episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?)", (entity_id, run_id)).fetchone()
                labels = db.execute("SELECT COUNT(*) FROM (SELECT m.episode_id,m.asset_key,m.horizon_seconds FROM market_labels m JOIN episodes e ON e.episode_id=m.episode_id WHERE e.entity_id=? AND m.simple_return IS NOT NULL AND e.episode_id IN (SELECT episode_id FROM run_episodes WHERE run_id=?) GROUP BY m.episode_id,m.asset_key,m.horizon_seconds)", (entity_id, run_id)).fetchone()[0]
                usable = db.execute("""SELECT COUNT(*) FROM (
                  SELECT 1 FROM primary_analysis_observations o WHERE run_id=? AND entity_id=?
                  AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=o.run_id AND r.episode_id=o.episode_id)
                  GROUP BY episode_id,asset_key,horizon_seconds
                )""", (run_id, entity_id)).fetchone()[0]
                if not usable:
                    usable = db.execute("""SELECT COUNT(*) FROM (
                      SELECT 1 FROM analysis_observations o WHERE run_id=? AND entity_id=?
                      AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=o.run_id AND r.episode_id=o.episode_id)
                      GROUP BY episode_id,asset_key,horizon_seconds
                    )""", (run_id, entity_id)).fetchone()[0]
                db.execute("""UPDATE run_entities SET wallets_total=?,wallets_processed=?,wallets_failed=?,wallets_partial=?,events=?,episodes=?,classified=?,market_labels=?,usable_observations=?,updated_at=? WHERE run_id=? AND entity_id=?""",
                           (wallet[0] or 0, wallet[1] or 0, wallet[2] or 0, wallet[3] or 0, events, episodes[0] or 0, episodes[1] or 0, labels, usable, now_iso(), run_id, entity_id))
            totals = db.execute("SELECT COALESCE(SUM(wallets_total),0),COALESCE(SUM(wallets_processed+wallets_failed+wallets_partial),0) FROM run_entities WHERE run_id=?", (run_id,)).fetchone()
            counts = db.execute("SELECT COALESCE(SUM(events),0),COALESCE(SUM(episodes),0),COALESCE(SUM(classified),0),COALESCE(SUM(usable_observations),0) FROM run_entities WHERE run_id=?",(run_id,)).fetchone()
            built = db.execute("SELECT COUNT(*) FROM run_events WHERE run_id=? AND built=1",(run_id,)).fetchone()[0]
            market = db.execute("SELECT COUNT(*),COALESCE(SUM(state IN ('success','permanently unavailable')),0) FROM market_horizons h JOIN run_episodes r USING(episode_id) WHERE r.run_id=?",(run_id,)).fetchone()
            labeled = db.execute("SELECT COUNT(*) FROM market_labels m JOIN run_episodes r USING(episode_id) WHERE r.run_id=? AND m.simple_return IS NOT NULL",(run_id,)).fetchone()[0]
            collection = min(1,totals[1]/totals[0]) if totals[0] else 0
            fractions = (built/max(1,counts[0]),counts[2]/max(1,counts[1]),market[1]/max(1,market[0]),counts[3]/max(1,labeled))
            progress = .05 + .45*collection + .1*sum(min(1,f) for f in fractions)
            db.execute("UPDATE analysis_runs SET progress=CASE WHEN status='completed' THEN 1 ELSE ? END,updated_at=? WHERE run_id=?",(min(progress,.95),now_iso(),run_id))

    def live_metrics(self, run_id: str) -> dict[str, Any]:
        metrics = self.runtime_metrics.get(run_id, {})
        run = self.db.run(run_id) or {}
        if metrics:
            elapsed = max(1.0, time.monotonic() - metrics.get("started_monotonic", time.monotonic()))
            hub = dict(metrics.get("hub") or {})
            classification_completed = metrics.get("classification_completed", 0)
            classification_seconds = metrics.get("classification_seconds", 0.0)
            market = {key: metrics.get(f"market_{key}", 0) for key in
                      ("cache_hits", "fetched_points", "unavailable_points", "external_calls", "horizons_processed", "seconds")}
        else:
            started = datetime.fromisoformat(run["started_at"]) if run.get("started_at") else datetime.now(timezone.utc)
            ended = datetime.fromisoformat(run["finished_at"]) if run.get("finished_at") else datetime.now(timezone.utc)
            elapsed = max(1.0, (ended - started).total_seconds())
            saved = self.db.row("SELECT * FROM run_metrics WHERE run_id=?", (run_id,)) or {}
            hub = {"requests": saved.get("api_requests", 0), "success": saved.get("api_success", 0),
                   "cache_hit": saved.get("cache_hits", 0), "retry": saved.get("retries", 0), "failed": saved.get("failed", 0)}
            classification_completed = saved.get("deterministic_completed", saved.get("llm_completed", 0))
            classification_seconds = saved.get("classification_seconds", 0.0)
            market = {key: saved.get(f"market_{key}", 0) for key in
                      ("cache_hits", "fetched_points", "unavailable_points", "external_calls", "horizons_processed", "seconds")}
        totals = self.db.row("SELECT COALESCE(SUM(events),0) events,COALESCE(SUM(episodes),0) episodes,COALESCE(SUM(classified),0) classified,COALESCE(SUM(market_labels),0) labels,COALESCE(SUM(usable_observations),0) analyzed FROM run_entities WHERE run_id=?", (run_id,))
        totals["built_events"] = self.db.row("SELECT COUNT(*) n FROM run_events WHERE run_id=? AND built=1",(run_id,))["n"]
        totals["market_done"] = self.db.row("SELECT COUNT(*) n FROM market_horizons h JOIN run_episodes r USING(episode_id) WHERE r.run_id=? AND state IN ('success','permanently unavailable')",(run_id,))["n"]
        market_total = self.db.row("SELECT COUNT(*) n FROM market_horizons h JOIN run_episodes r USING(episode_id) WHERE r.run_id=?", (run_id,))["n"]
        wallets = self.db.row("SELECT COUNT(*) total,SUM(status IN ('completed','partial','failed')) done FROM run_wallets WHERE run_id=?", (run_id,))
        totals.update(wallets=wallets["done"] or 0, requests=hub.get("requests",0))
        now = time.monotonic()
        samples = self._samples.setdefault(run_id, [])
        if not samples or now-samples[-1][0]>=1:
            samples.append((now,dict(totals)))
        while len(samples)>2 and samples[1][0]<now-300:
            samples.pop(0)
        baseline = next((x for x in reversed(samples) if x[0]<=now-60),samples[0])
        span = now-baseline[0]
        rates = {k:max(0,(v-baseline[1].get(k,0))/span) if span>=1 else 0 for k,v in totals.items()}
        queues = self.queue_counts(run_id)
        def eta(n,rate):
            return 0 if n==0 else int(math.ceil(n/rate)) if rate>0 else None
        remaining = max(0,wallets["total"]-totals["wallets"])
        classification_queue = queues.get("intent_classification", queues.get("llm_classification", 0))
        classification_rate = (classification_completed / classification_seconds) if classification_seconds > 0 else rates["classified"]
        market_rate = (market["horizons_processed"] / market["seconds"]) if market["seconds"] > 0 else rates["market_done"]
        classification_eta = eta(classification_queue, classification_rate)
        etas = {"collection":eta(remaining,rates["wallets"]),
            "episode":eta(queues["episode_builder"],rates["built_events"]),
            "classification":classification_eta,
            "market":eta(queues["market_labeling"],market_rate),
            "analysis":eta(queues["analysis"],rates["analyzed"])}
        # Compatibility for callers supplying the legacy queue contract.
        if "llm_classification" in queues and "intent_classification" not in queues:
            etas["llm"] = etas.pop("classification")
        # Conservative sequential drain estimate. Unknown throughput is never shown as a short ETA.
        etas["total"] = sum(etas.values()) if all(v is not None for v in etas.values()) else None
        stage_progress = {
            "collection": totals["wallets"] / max(1, wallets["total"]),
            "episode_builder": totals["built_events"] / max(1, totals["events"]),
            "intent_classification": totals["classified"] / max(1, totals["episodes"]),
            "market_labeling": totals["market_done"] / max(1, market_total),
            "analysis": totals["analyzed"] / max(1, totals["labels"]),
        }
        return {"api":hub,"events_per_minute":round(60*rates["events"],1),
            "wallets_per_hour":round(3600*rates["wallets"],1),"api_requests_per_second":round(rates["requests"],1),
            "intent_episodes_per_second":round(classification_rate,1),
            "market_horizons_per_second":round(market_rate,1),"market":market,
            "eta":etas,"stage_progress":stage_progress,"window_seconds":round(span)}

    def _persist_metrics(self, run_id: str, rt: Runtime) -> None:
        hub = rt.hub.metrics
        classification_completed = self.runtime_metrics.get(run_id, {}).get("classification_completed", 0)
        classification_seconds = self.runtime_metrics.get(run_id, {}).get("classification_seconds", 0.0)
        runtime = self.runtime_metrics.get(run_id, {})
        self.db.execute(
            """INSERT INTO run_metrics(run_id,api_requests,api_success,cache_hits,retries,failed,llm_completed,
               deterministic_completed,classification_seconds,market_cache_hits,market_fetched_points,
               market_unavailable_points,market_external_calls,market_horizons_processed,market_seconds,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET api_requests=excluded.api_requests,
               api_success=excluded.api_success,cache_hits=excluded.cache_hits,retries=excluded.retries,
               failed=excluded.failed,deterministic_completed=excluded.deterministic_completed,
               classification_seconds=excluded.classification_seconds,market_cache_hits=excluded.market_cache_hits,
               market_fetched_points=excluded.market_fetched_points,market_unavailable_points=excluded.market_unavailable_points,
               market_external_calls=excluded.market_external_calls,market_horizons_processed=excluded.market_horizons_processed,
               market_seconds=excluded.market_seconds,updated_at=excluded.updated_at""",
            (run_id, hub.get("requests", 0), hub.get("success", 0), hub.get("cache_hit", 0), hub.get("retry", 0),
             hub.get("failed", 0), 0, classification_completed, classification_seconds,
             runtime.get("market_cache_hits", 0), runtime.get("market_fetched_points", 0),
             runtime.get("market_unavailable_points", 0), runtime.get("market_external_calls", 0),
             runtime.get("market_horizons_processed", 0), runtime.get("market_seconds", 0.0), now_iso()),
        )
