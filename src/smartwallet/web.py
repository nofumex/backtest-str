from __future__ import annotations

import asyncio
import json
import math
import os
import logging
import secrets
import base64
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, model_validator
from dotenv import load_dotenv

from .config import load_entities
from .orchestrator import RunOrchestrator
from .pipeline.backfill import parse_date
from .webdb import WebDB


logger = logging.getLogger("uvicorn.error")


class RunSettings(BaseModel):
    max_wallets: int | None = Field(None, ge=1, le=10000)
    max_pages: int | None = Field(None, ge=1, le=10000)
    concurrency: int = Field(8, ge=1, le=32)
    enrichment_threshold: float = Field(100000, ge=0)


class RunCreate(BaseModel):
    entities: list[str] = Field(min_length=1)
    from_date: str
    to_date: str
    mode: Literal["diagnostic", "full"] = "full"
    settings: RunSettings = RunSettings()

    @model_validator(mode="after")
    def validate_dates(self):
        if date.fromisoformat(self.from_date) > date.fromisoformat(self.to_date):
            raise ValueError("from_date must not be after to_date")
        return self


load_dotenv()
db = WebDB()
orchestrator = RunOrchestrator(db)


@asynccontextmanager
async def lifespan(_: FastAPI):
    info = db.startup_info()
    logger.info("Smartwallet database path=%s size_bytes=%s analysis_runs=%s",
                info["path"], info["size"], info["analysis_runs"])
    if not info["analysis_runs"]:
        logger.warning("Smartwallet database is empty: path=%s size_bytes=%s. Verify SMARTWALLET_DB if runs were expected.",
                       info["path"], info["size"])
    db.recover_interrupted()
    yield
    for task in orchestrator.tasks.values():
        task.cancel()
    await asyncio.gather(*orchestrator.tasks.values(), return_exceptions=True)


app = FastAPI(title="Smartwallet Research Terminal", version="2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def authenticate(request: Request, call_next):
    secret = os.getenv("SMARTWALLET_AUTH_TOKEN", "")
    if not secret:
        host = request.client.host if request.client else ""
        if os.getenv("SMARTWALLET_ALLOW_INSECURE_LOCAL") != "1" or os.getenv("NODE_ENV") == "production" or host not in ("127.0.0.1", "::1", "testclient"):
            return JSONResponse({"detail":"Configure SMARTWALLET_AUTH_TOKEN before exposing the terminal"}, status_code=503)
    else:
        value = request.headers.get("authorization", "")
        supplied = value[7:] if value.startswith("Bearer ") else ""
        if value.startswith("Basic "):
            try:
                supplied = base64.b64decode(value[6:], validate=True).decode().partition(":")[2]
            except (ValueError, UnicodeError):
                supplied = ""
        if not secrets.compare_digest(supplied.encode(), secret.encode()):
            return JSONResponse({"detail":"Authentication required"}, status_code=401, headers={"WWW-Authenticate":'Basic realm="Smartwallet", charset="UTF-8"'})
    if request.method not in ("GET", "HEAD", "OPTIONS"):
        origin = request.headers.get("origin")
        if origin and origin not in (str(request.base_url).rstrip("/"), os.getenv("SMARTWALLET_PUBLIC_ORIGIN", ""), "http://localhost:5173", "http://127.0.0.1:5173"):
            return JSONResponse({"detail":"Invalid origin"}, status_code=403)
    return await call_next(request)


def _elapsed_seconds(run: dict[str, Any]) -> int:
    if not run.get("started_at"):
        return 0
    start = datetime.fromisoformat(run["started_at"])
    end = datetime.fromisoformat(run["finished_at"]) if run.get("finished_at") else datetime.now(timezone.utc)
    return max(0, int((end - start).total_seconds()))


def _eta(run: dict[str, Any], elapsed: int) -> int | None:
    progress = float(run.get("progress") or 0)
    if run["status"] not in {"running", "pausing", "queued"} or progress <= 0.02:
        return None
    return max(0, int(elapsed * (1 - progress) / progress))


def _pattern_payload(row: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)
    row["negative_rate"] = row["negative_count"] / row["n"] if row["n"] else 0
    row["positive_rate"] = row["positive_count"] / row["n"] if row["n"] else 0
    row["direction"] = "down" if row["mean_return"] < 0 else "up"
    row["asset_label"] = (row["asset_key"].split(":")[-1].upper()
                          .replace("ETHEREUM", "ETH").replace("BITCOIN", "BTC")
                          )
    metadata = db.row("SELECT symbol,name FROM assets WHERE asset_key=?", (row["asset_key"],))
    if metadata and metadata.get("symbol"):
        row["asset_label"] = metadata["symbol"]
        row["asset_name"] = metadata["name"]
    row["chain"] = row["asset_key"].partition(":")[0]
    row["pattern_label"] = row["pattern"].split(":", 1)[-1].replace(">", " → ")
    return row


def snapshot(run_id: str) -> dict[str, Any]:
    run = db.run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    orchestrator._refresh_counts(run_id)
    run = db.run(run_id)
    elapsed = _elapsed_seconds(run)
    entities = db.rows("SELECT * FROM run_entities WHERE run_id=? ORDER BY name", (run_id,))
    totals = {key: sum(int(row.get(key) or 0) for row in entities) for key in (
        "wallets_total", "wallets_processed", "wallets_failed", "wallets_partial", "events", "episodes", "classified", "market_labels", "usable_observations"
    )}
    pattern_rows = db.rows(
        """SELECT *, 'all intents' intent_label, 'primary' hypothesis_layer
           FROM primary_pattern_aggregates WHERE run_id=? AND n>=3
           AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=primary_pattern_aggregates.run_id) ORDER BY
           CASE maturity WHEN 'ROBUST' THEN 4 WHEN 'ESTABLISHING' THEN 3 WHEN 'PROMISING' THEN 2 ELSE 1 END DESC,
           n DESC,ABS(mean_return) DESC LIMIT 24""", (run_id,)
    )
    has_primary = db.row("SELECT 1 present FROM primary_pattern_aggregates WHERE run_id=? LIMIT 1", (run_id,))
    if not pattern_rows and not has_primary:  # Archived runs remain readable before an explicit refresh.
        pattern_rows = db.rows(
            """SELECT *, 'secondary' hypothesis_layer FROM pattern_aggregates WHERE run_id=? AND n>=3
               AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id=pattern_aggregates.run_id)
               ORDER BY n DESC,ABS(mean_return) DESC LIMIT 24""", (run_id,)
        )
    patterns = [_pattern_payload(row) for row in pattern_rows]
    feed = db.rows("SELECT * FROM discovery_feed WHERE run_id=? ORDER BY id DESC LIMIT 30", (run_id,))
    errors = db.rows("SELECT * FROM run_errors WHERE run_id=? ORDER BY id DESC LIMIT 20", (run_id,))
    queues = orchestrator.queue_counts(run_id)
    speed = orchestrator.live_metrics(run_id)
    try:
        database_size = db.path.stat().st_size
        wal = Path(str(db.path) + "-wal")
        if wal.exists():
            database_size += wal.stat().st_size
    except OSError:
        database_size = 0
    return {"run": run, "elapsed_seconds": elapsed, "eta_seconds": speed["eta"]["total"], "totals": totals,
            "entities": entities, "patterns": patterns, "feed": feed, "errors": errors, "queues": queues,
            "speed": speed, "database_size": database_size}


@app.get("/api/config")
def config():
    return {"entities": load_entities(), "today": date.today().isoformat(), "credentials": {
        "api_hub": bool(os.getenv("API_HUB_KEY", "").strip())
    }}


@app.post("/api/runs", status_code=201)
async def create_run(payload: RunCreate):
    known = {item["id"]: item["name"] for item in load_entities()}
    unknown = set(payload.entities) - set(known)
    if unknown:
        raise HTTPException(422, f"Unknown entities: {', '.join(sorted(unknown))}")
    active = db.row("SELECT run_id FROM analysis_runs WHERE status IN ('running','queued','pausing','paused','stopping') LIMIT 1")
    if active:
        raise HTTPException(409, "Another analysis is active; pause or stop it first")
    values = payload.model_dump()
    if payload.mode == "diagnostic":
        values["settings"]["max_wallets"] = values["settings"]["max_wallets"] or 5
        values["settings"]["max_pages"] = values["settings"]["max_pages"] or 2
    run_id = db.create_run(values, known)
    orchestrator.launch(run_id)
    return {"run_id": run_id}


@app.get("/api/runs")
def runs():
    rows = db.rows("""SELECT r.*,
      (SELECT COUNT(*) FROM run_events e WHERE e.run_id=r.run_id) events
      FROM analysis_runs r ORDER BY sequence DESC""")
    for row in rows:
        row["entities"] = json.loads(row.pop("entities_json"))
        row["settings"] = json.loads(row.pop("settings_json"))
        row["elapsed_seconds"] = _elapsed_seconds(row)
    return rows


@app.get("/api/runs/{run_id}")
def run_snapshot(run_id: str):
    return snapshot(run_id)


@app.post("/api/runs/{run_id}/{action}")
async def run_control(run_id: str, action: Literal["pause", "resume", "stop"]):
    try:
        return await orchestrator.control(run_id, action)
    except KeyError:
        raise HTTPException(404, "Run not found")
    except ValueError as exc:
        raise HTTPException(409, str(exc))


@app.get("/api/runs/{run_id}/stream")
async def stream(run_id: str, request: Request):
    if not db.run(run_id):
        raise HTTPException(404, "Run not found")

    async def events():
        while not await request.is_disconnected():
            try:
                payload = snapshot(run_id)
                yield "event: snapshot\ndata: " + json.dumps(payload, default=str, separators=(",", ":")) + "\n\n"
            except Exception as exc:
                yield "event: error\ndata: " + json.dumps({"message": str(exc)}) + "\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/runs/{run_id}/patterns/{pattern_id}")
def pattern_detail(run_id: str, pattern_id: str):
    pattern = db.row(
        "SELECT *, 'all intents' intent_label, 'primary' hypothesis_layer FROM primary_pattern_aggregates WHERE run_id=? AND pattern_key=?",
        (run_id, pattern_id),
    )
    primary = bool(pattern)
    if not pattern:
        pattern = db.row(
            "SELECT *, 'secondary' hypothesis_layer FROM pattern_aggregates WHERE run_id=? AND pattern_key=?",
            (run_id, pattern_id),
        )
    if not pattern:
        raise HTTPException(404, "Pattern not found")
    if primary:
        observations = db.rows(
            """SELECT o.*,e.motif FROM primary_analysis_observations o JOIN episodes e ON e.episode_id=o.episode_id
               WHERE o.run_id=? AND o.entity_id=? AND o.pattern=? AND o.asset_key=? AND o.horizon_seconds=?
               ORDER BY o.episode_ts""",
            (run_id, pattern["entity_id"], pattern["pattern"], pattern["asset_key"], pattern["horizon_seconds"]),
        )
        checkpoints = db.rows("SELECT * FROM primary_pattern_checkpoints WHERE run_id=? AND pattern_key=? ORDER BY n", (run_id, pattern_id))
        horizons = db.rows(
            """SELECT horizon_seconds,COUNT(*) n,AVG(return_value) mean_return
               FROM primary_analysis_observations WHERE run_id=? AND entity_id=? AND pattern=? AND asset_key=?
               GROUP BY horizon_seconds ORDER BY horizon_seconds""",
            (run_id, pattern["entity_id"], pattern["pattern"], pattern["asset_key"]),
        )
    else:
        observations = db.rows(
            """SELECT o.*,e.motif FROM analysis_observations o JOIN episodes e ON e.episode_id=o.episode_id
               WHERE o.run_id=? AND o.entity_id=? AND o.pattern=? AND o.intent_label=? AND o.asset_key=? AND o.horizon_seconds=?
               ORDER BY o.episode_ts""",
            (run_id, pattern["entity_id"], pattern["pattern"], pattern["intent_label"], pattern["asset_key"], pattern["horizon_seconds"]),
        )
        checkpoints = db.rows("SELECT * FROM pattern_checkpoints WHERE run_id=? AND pattern_key=? ORDER BY n", (run_id, pattern_id))
        horizons = db.rows(
            """SELECT horizon_seconds,COUNT(*) n,AVG(return_value) mean_return
               FROM analysis_observations WHERE run_id=? AND entity_id=? AND pattern=? AND intent_label=? AND asset_key=?
               GROUP BY horizon_seconds ORDER BY horizon_seconds""",
            (run_id, pattern["entity_id"], pattern["pattern"], pattern["intent_label"], pattern["asset_key"]),
        )
    values = [float(row["return_value"]) for row in observations]
    histogram = []
    if values:
        low, high = min(values), max(values)
        width = (high - low) / 12 if high > low else 1
        for index in range(12):
            start = low + index * width
            end = high if index == 11 else start + width
            histogram.append({"start": start, "end": end, "count": sum(start <= value <= end if index == 11 else start <= value < end for value in values)})
    return {"pattern": _pattern_payload(pattern), "checkpoints": checkpoints, "horizons": horizons,
            "histogram": histogram, "observations": observations[-200:]}


@app.get("/api/runs/{run_id}/episodes/{episode_id}")
def episode_evidence(run_id: str, episode_id: str):
    run = db.run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    episode = db.row("SELECT e.* FROM episodes e JOIN run_episodes r USING(episode_id) WHERE e.episode_id=? AND r.run_id=?", (episode_id,run_id))
    from_ts, to_ts = parse_date(run["from_date"]), parse_date(run["to_date"]) + 86399
    if (not episode or episode["entity_id"] not in run["entities"]
            or not from_ts <= int(episode["start_ts"]) <= to_ts):
        raise HTTPException(404, "Episode not found in this run")
    episode["evidence"] = json.loads(episode.pop("evidence_json") or "{}")
    episode["wallets"] = json.loads(episode.pop("wallets_json") or "[]")
    episode["event_ids"] = json.loads(episode.pop("event_ids_json") or "[]")
    episode["intent"] = json.loads(episode.pop("intent_json") or "{}")
    return episode


@app.get("/api/runs/{run_id}/quality")
def quality(run_id: str):
    run = db.run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    orchestrator._refresh_counts(run_id)
    entities = db.rows("SELECT * FROM run_entities WHERE run_id=? ORDER BY name", (run_id,))
    metrics = db.row("SELECT * FROM run_metrics WHERE run_id=?", (run_id,)) or {}
    for entity in entities:
        eid = entity["entity_id"]
        base = " FROM episodes e JOIN run_episodes r USING(episode_id) WHERE r.run_id=? AND e.entity_id=?"
        args = (run_id,eid)
        entity["unknown_assets"] = db.row("SELECT COUNT(*) n"+base+" AND COALESCE(target_asset_key,primary_asset_key) IS NULL",args)["n"]
        entity["unclassified"] = db.row("SELECT COUNT(*) n"+base+" AND intent_label IS NULL",args)["n"]
        entity["unknown_intents"] = db.row("SELECT COUNT(*) n"+base+" AND intent_label='unknown'",args)["n"]
        confidence = db.row("SELECT AVG(intent_confidence) value"+base+" AND intent_label IS NOT NULL",args)
        entity["heuristic_confidence"] = confidence["value"]
        entity["deterministic_classified"] = db.row(
            "SELECT COUNT(*) n"+base+" AND json_extract(COALESCE(intent_json,'{}'),'$.classifier')='deterministic_intent'", args,
        )["n"]
        horizons = db.row("SELECT SUM(h.state IN ('pending','retryable failure')) pending,SUM(h.state='permanently unavailable') unavailable FROM market_horizons h JOIN run_episodes r USING(episode_id) JOIN episodes e USING(episode_id) WHERE r.run_id=? AND e.entity_id=?",args)
        entity["missing_market"] = horizons["pending"] or 0
        entity["unavailable_market"] = horizons["unavailable"] or 0
        wallets = db.row("SELECT SUM(status IN ('running','pending')) processing,SUM(status='partial') partial,SUM(address NOT LIKE '0x%' AND COALESCE(chain,'')!='solana') unsupported FROM run_wallets WHERE run_id=? AND entity_id=?",args)
        entity.update(wallets)
        entity["unsupported_chains"] = wallets["unsupported"] or 0
        entity["stale_snapshots"] = None
        coverage = db.row("SELECT MIN(ts) first_ts,MAX(ts) last_ts FROM wallet_events e JOIN run_events r USING(event_id) WHERE r.run_id=? AND e.entity_id=?",args)
        entity.update(coverage_from=coverage["first_ts"],coverage_to=coverage["last_ts"])
        counts = db.row("SELECT COUNT(*) requests,SUM(state IN ('success','cache')) success,SUM(state='retry') retries,SUM(state='failed') failures FROM request_attempts WHERE run_id=? AND entity_id=?",args)
        entity.update(counts)
        entity["api_success_rate"] = counts["success"]/counts["requests"] if counts["requests"] else None
    return {"entities":entities,"api":metrics,
        "requests":db.rows("SELECT * FROM request_attempts WHERE run_id=? AND state!='success' ORDER BY id DESC LIMIT 200",(run_id,)),
        "market_failures":db.rows("SELECT h.* FROM market_horizons h JOIN run_episodes r USING(episode_id) WHERE r.run_id=? AND h.reason IS NOT NULL LIMIT 200",(run_id,)),
        "errors":db.rows("SELECT * FROM run_errors WHERE run_id=? ORDER BY id DESC LIMIT 200",(run_id,))}


@app.get("/api/runs/{run_id}/patterns")
def patterns(run_id: str, min_n: int = 3, max_n: int = 0, entity: str = "", intent: str = "", kind: str = "", horizon: int = 0, search: str = "", sort: str = "best", offset: int = 0, limit: int = 24):
    # The explorer defaults to the intent-independent primary layer. Supplying an intent
    # explicitly switches to the secondary dimension without duplicating cards by default.
    table = "pattern_aggregates" if intent else "primary_pattern_aggregates"
    where, args = f"run_id=? AND n>=? AND EXISTS(SELECT 1 FROM run_episodes r WHERE r.run_id={table}.run_id)", [run_id,max(1,min_n)]
    dimensions = [("entity_id", entity), ("horizon_seconds", horizon)]
    if intent:
        dimensions.append(("intent_label", intent))
    for column,value in dimensions:
        if value:
            where += f" AND {column}=?"; args.append(value)
    if max_n:
        where += " AND n<=?"; args.append(max_n)
    if kind:
        where += " AND pattern LIKE ?"; args.append(kind+":%")
    if search:
        if intent:
            where += " AND (pattern LIKE ? OR asset_key LIKE ? OR intent_label LIKE ? OR asset_key IN (SELECT asset_key FROM assets WHERE symbol LIKE ? OR name LIKE ?))"; args.extend(["%"+search+"%"]*5)
        else:
            where += " AND (pattern LIKE ? OR asset_key LIKE ? OR asset_key IN (SELECT asset_key FROM assets WHERE symbol LIKE ? OR name LIKE ?))"; args.extend(["%"+search+"%"]*4)
    orders = {"sample":"n DESC", "effect":"ABS(mean_return) DESC,n DESC", "hit":"MAX(negative_count,positive_count)*1.0/n DESC,n DESC", "newest":"updated_at DESC"}
    # Bounded effect, evidence strength and shrinkage by sample size.
    best = "(n*1.0/(n+20))*(1+MIN(ABS(COALESCE(shrunk_mean,mean_return)),1))*(1+COALESCE(holdout_accuracy,0))*(1+CASE WHEN qvalue<=0.05 THEN 1 ELSE 0 END) DESC,n DESC"
    order = orders.get(sort,best)
    total = db.row(f"SELECT COUNT(*) n FROM {table} WHERE "+where,args)["n"]
    select = "SELECT *, 'secondary' hypothesis_layer" if intent else "SELECT *, 'all intents' intent_label, 'primary' hypothesis_layer"
    rows = db.rows(f"{select} FROM {table} WHERE "+where+" ORDER BY "+order+" LIMIT ? OFFSET ?",[*args,min(100,max(1,limit)),max(0,offset)])
    has_primary = db.row("SELECT 1 present FROM primary_pattern_aggregates WHERE run_id=? LIMIT 1", (run_id,))
    if not rows and not intent and not has_primary:
        # Completed legacy runs are still browsable until the user requests a refresh.
        legacy_where = where.replace("primary_pattern_aggregates", "pattern_aggregates")
        total = db.row("SELECT COUNT(*) n FROM pattern_aggregates WHERE "+legacy_where,args)["n"]
        rows = db.rows("SELECT *, 'secondary' hypothesis_layer FROM pattern_aggregates WHERE "+legacy_where+" ORDER BY "+order+" LIMIT ? OFFSET ?",[*args,min(100,max(1,limit)),max(0,offset)])
    return {"total":total,"items":[_pattern_payload(r) for r in rows]}


@app.post("/api/runs/{run_id}/analysis/refresh")
async def refresh_analysis(run_id: str):
    from .incremental import IncrementalAnalyzer
    if not db.run(run_id):
        raise HTTPException(404,"Run not found")
    if orchestrator.active(run_id):
        orchestrator.full_refresh_requests.add(run_id)
        if run_id in orchestrator._wakeups:
            orchestrator._wakeups[run_id].set()
        return {"status":"queued"}
    return await asyncio.to_thread(IncrementalAnalyzer(db).refresh,run_id,expensive=True)


frontend = Path(__file__).resolve().parents[2] / "frontend" / "dist"
if frontend.exists():
    assets = frontend / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        target = frontend / path
        if path and target.resolve().is_relative_to(frontend.resolve()) and target.is_file():
            return FileResponse(target)
        return FileResponse(frontend / "index.html")
else:
    @app.get("/")
    def no_frontend():
        return JSONResponse({"status": "backend ready", "message": "Run npm run build to create the dashboard UI."})


def main() -> None:
    import uvicorn
    uvicorn.run("smartwallet.web:app", host=os.getenv("HOST", "127.0.0.1"), port=int(os.getenv("PORT", "8000")), reload=False)


if __name__ == "__main__":
    main()
