"""Fresh, isolated live diagnostic. Never touches the default database."""
import asyncio
import json
import os
import tempfile
from pathlib import Path
from smartwallet.webdb import WebDB
from smartwallet.orchestrator import RunOrchestrator

async def main():
    folder = Path(tempfile.mkdtemp(prefix="smartwallet-smoke-"))
    os.environ["SMARTWALLET_DB"] = str(folder / "smoke.db")
    os.environ["SMARTWALLET_RAW_DIR"] = str(folder / "raw")
    os.environ["SMARTWALLET_REPORT_DIR"] = str(folder / "reports")
    os.environ["SMARTWALLET_HTTP_TIMEOUT"] = "15"
    os.environ["SMARTWALLET_HTTP_RETRIES"] = "2"
    db = WebDB(folder / "smoke.db")
    run_id = db.create_run(dict(mode="diagnostic", entities=["wintermute"],from_date="2026-09-01",to_date="2026-09-20",settings=dict(max_wallets=1,max_pages=1,concurrency=2,llm_concurrency=2)),{})
    worker = RunOrchestrator(db)
    worker.launch(run_id)
    print("smoke database:",folder,flush=True)
    try:
        await asyncio.wait_for(asyncio.shield(worker.tasks[run_id]),timeout=240)
    except asyncio.TimeoutError:
        await worker.control(run_id,"stop")
        await asyncio.wait_for(worker.tasks[run_id],timeout=60)
    report = {"run":db.run(run_id),"wallets":db.rows("SELECT status,events,attempts,error FROM run_wallets WHERE run_id=?",(run_id,)),"metrics":db.rows("SELECT * FROM run_metrics WHERE run_id=?",(run_id,)),"providers":db.rows("SELECT provider,endpoint,state,error,COUNT(*) n FROM request_attempts WHERE run_id=? GROUP BY provider,endpoint,state,error",(run_id,))}
    Path("data/reports/production-smoke.json").write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2),flush=True)

asyncio.run(main())
