from __future__ import annotations

import asyncio
import json
import math
import os
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


class RunSettings(BaseModel):
    max_wallets: int | None = Field(None, ge=1, le=10000)
    max_pages: int | None = Field(None, ge=1, le=10000)
    concurrency: int = Field(8, ge=1, le=32)
    enrichment_threshold: float = Field(100000, ge=0)
    llm_concurrency: int = Field(4, ge=1, le=16)


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
                          .replace("WRAPPED-ETHER", "ETH").replace("WETH", "ETH").replace("WBTC", "BTC"))
    row["pattern_label"] = row["pattern"].split(":", 1)[-1].replace(">", " → ")
    return row


def snapshot(run_id: str) -> dict[str, Any]:
    run = db.run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    elapsed = _elapsed_seconds(run)
    entities = db.rows("SELECT * FROM run_entities WHERE run_id=? ORDER BY name", (run_id,))
    totals = {key: sum(int(row.get(key) or 0) for row in entities) for key in (
        "wallets_total", "wallets_processed", "wallets_failed", "events", "episodes", "classified", "market_labels", "usable_observations"
    )}
    patterns = [_pattern_payload(row) for row in db.rows(
        """SELECT * FROM pattern_aggregates WHERE run_id=? ORDER BY
           CASE maturity WHEN 'ROBUST' THEN 4 WHEN 'ESTABLISHING' THEN 3 WHEN 'PROMISING' THEN 2 ELSE 1 END DESC,
           n DESC,ABS(mean_return) DESC LIMIT 24""", (run_id,)
    )]
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
    return {"run": run, "elapsed_seconds": elapsed, "eta_seconds": _eta(run, elapsed), "totals": totals,
            "entities": entities, "patterns": patterns, "feed": feed, "errors": errors, "queues": queues,
            "speed": speed, "database_size": database_size}


@app.get("/api/config")
def config():
    return {"entities": load_entities(), "today": date.today().isoformat(), "credentials": {
        "api_hub": bool(os.getenv("API_HUB_KEY", "").strip()), "llm": bool(os.getenv("FREE_LLM_API", "").strip())
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
      (SELECT COALESCE(SUM(events),0) FROM run_entities e WHERE e.run_id=r.run_id) events
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
    pattern = db.row("SELECT * FROM pattern_aggregates WHERE run_id=? AND pattern_key=?", (run_id, pattern_id))
    if not pattern:
        raise HTTPException(404, "Pattern not found")
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
    episode = db.row("SELECT * FROM episodes WHERE episode_id=?", (episode_id,))
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
    entities = db.rows("SELECT * FROM run_entities WHERE run_id=? ORDER BY name", (run_id,))
    metrics = db.row("SELECT * FROM run_metrics WHERE run_id=?", (run_id,)) or {}
    api_requests = int(metrics.get("api_requests") or 0)
    api_success_rate = (int(metrics.get("api_success") or 0) / api_requests) if api_requests else None
    from_ts, to_ts = parse_date(run["from_date"]), parse_date(run["to_date"]) + 86399
    for entity in entities:
        entity_id = entity["entity_id"]
        entity["unknown_assets"] = db.row("SELECT COUNT(*) n FROM episodes WHERE entity_id=? AND start_ts BETWEEN ? AND ? AND COALESCE(target_asset_key,primary_asset_key) IS NULL", (entity_id, from_ts, to_ts))["n"]
        entity["unclassified"] = db.row("SELECT COUNT(*) n FROM episodes WHERE entity_id=? AND start_ts BETWEEN ? AND ? AND intent_label IS NULL", (entity_id, from_ts, to_ts))["n"]
        entity["missing_market"] = db.row("""SELECT COUNT(*) n FROM episodes e WHERE entity_id=? AND start_ts BETWEEN ? AND ? AND COALESCE(target_asset_key,primary_asset_key) IS NOT NULL AND (SELECT COUNT(*) FROM market_labels m WHERE m.episode_id=e.episode_id)<5""", (entity_id, from_ts, to_ts))["n"]
        entity["errors"] = db.row("SELECT COUNT(*) n FROM run_errors WHERE run_id=? AND entity_id=?", (run_id, entity_id))["n"]
        coverage = db.row("SELECT MIN(ts) first_ts,MAX(ts) last_ts FROM wallet_events WHERE entity_id=? AND ts BETWEEN ? AND ?", (entity_id, from_ts, to_ts)) or {}
        entity["coverage_from"] = coverage.get("first_ts")
        entity["coverage_to"] = coverage.get("last_ts")
        entity["unsupported_chains"] = db.row("""SELECT COUNT(*) n FROM run_wallets WHERE run_id=? AND entity_id=?
          AND address NOT LIKE '0x%' AND COALESCE(chain,'')!='solana'""", (run_id, entity_id))["n"]
        entity["stale_snapshots"] = db.row("""SELECT COUNT(*) n FROM entity_snapshots WHERE entity_id=?
          AND julianday(captured_at) < julianday('now','-24 hours')""", (entity_id,))["n"]
        entity["api_success_rate"] = api_success_rate
    return {"entities": entities, "api": metrics, "errors": db.rows("SELECT * FROM run_errors WHERE run_id=? ORDER BY id DESC LIMIT 200", (run_id,))}


frontend = Path(__file__).resolve().parents[2] / "frontend" / "dist"
if frontend.exists():
    assets = frontend / "assets"
    if assets.exists():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}")
    def spa(path: str):
        target = frontend / path
        if path and target.is_file():
            return FileResponse(target)
        return FileResponse(frontend / "index.html")
else:
    @app.get("/")
    def no_frontend():
        return JSONResponse({"status": "backend ready", "message": "Run npm run build to create the dashboard UI."})


def main() -> None:
    import uvicorn
    uvicorn.run("smartwallet.web:app", host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")), reload=False)


if __name__ == "__main__":
    main()
