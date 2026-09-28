import asyncio
from dataclasses import replace
from types import SimpleNamespace
import httpx
import pytest
from smartwallet.http import HubClient, HubError
from smartwallet.storage import Storage
from smartwallet.webdb import WebDB
from smartwallet.orchestrator import RunOrchestrator
from smartwallet.incremental import IncrementalAnalyzer, _patterns
from smartwallet.pipeline.market import MarketLabeler, DEFAULT_HORIZONS
from smartwallet.pipeline.backfill import backfill_evm_wallet


def create(db):
    return db.create_run(dict(entities=["x"],from_date="2024-01-01",to_date="2025-12-31",mode="diagnostic",settings={}),{})


def episode(storage, eid="e"):
    storage.save_episode(dict(episode_id=eid,entity_id="x",start_ts=1735689600,end_ts=1735689600,wallets=["0xa"],event_ids=["event"],motif="BUY",primary_asset_key="ethereum:0xtoken",evidence={"patterns":["FULL:BUY","ACTION:BUY"]},intent_label="buy"))


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", [{"arb"}, {"arb", "op"}])
async def test_evm_chain_failure_cannot_deadlock_wallet(settings, broken):
    storage = Storage(settings)
    class Debank:
        async def used_chains(self, address):
            return ["eth", "arb", "op"]
        async def history_page(self, address, chain, **kwargs):
            if chain in broken:
                raise RuntimeError(f"{chain} unavailable")
            return {"history_list": []}
    result = await asyncio.wait_for(
        backfill_evm_wallet(Debank(), storage, entity_id="x", address="0xa",
                            min_timestamp=1, max_timestamp=100),
        timeout=1,
    )
    assert set(result["chain_errors"]) == broken
    assert result["context_error"] and "chain backfills failed" in result["context_error"]
    assert set(result["per_chain"]) == {"eth", "arb", "op"} - broken
    assert storage.fetchone(
        "SELECT COUNT(*) n FROM wallet_history_coverage WHERE state='failed'"
    )["n"] == len(broken)
    storage.close()


@pytest.mark.asyncio
async def test_enrichment_is_bounded_concurrent_and_invalidations_are_coalesced(settings, monkeypatch):
    cfg = replace(settings, concurrency=2)
    storage = Storage(cfg); db = WebDB(cfg.db_path); rid = create(db)
    event = dict(event_id="enrich-event", entity_id="x", wallet="0xa", chain="eth",
                 tx_hash="0xtx", event_index=0, ts=1735689600, source="test",
                 action_type="TRANSFER_OUT", usd_value=200000, primary_token_id="eth",
                 primary_asset_key="coingecko:ethereum", evidence={}, raw={})
    storage.save_event(event)
    db.execute("INSERT INTO run_events(run_id,event_id,built) VALUES(?,?,1)", (rid, event["event_id"]))
    for index in range(10):
        db.execute(
            """INSERT INTO deferred_jobs(run_id,kind,item_key,entity_id,payload_json,updated_at)
               VALUES(?,'tx_enrichment',?,'x',?,datetime('now'))""",
            (rid, f"job-{index}", '{"event_id":"enrich-event"}'),
        )
    active = maximum = 0
    async def enrich(*args, **kwargs):
        nonlocal active, maximum
        active += 1; maximum = max(maximum, active)
        await asyncio.sleep(.01)
        active -= 1
    monkeypatch.setattr("smartwallet.orchestrator.enrich_evm_event", enrich)
    rt = SimpleNamespace(settings=cfg, storage=storage, oklink=None, arkham=None, rpc=None, bridges=None)
    worker = RunOrchestrator(db)
    assert await worker._enrich_once(rt, rid) == 8
    assert maximum == 2
    assert db.row("SELECT built FROM run_events WHERE run_id=?", (rid,))["built"] == 1
    assert db.row("SELECT COUNT(*) n FROM enrichment_invalidations WHERE run_id=?", (rid,))["n"] == 1
    assert await worker._enrich_once(rt, rid) == 2
    assert db.row("SELECT built FROM run_events WHERE run_id=?", (rid,))["built"] == 0
    assert db.row("SELECT COUNT(*) n FROM enrichment_invalidations WHERE run_id=?", (rid,))["n"] == 0
    assert db.row("SELECT COUNT(*) n FROM deferred_jobs WHERE state='complete'", ())["n"] == 10
    storage.close()


def test_runtime_paths_are_project_relative_and_production_db_is_fail_fast(tmp_path, monkeypatch):
    from smartwallet.settings import PROJECT_ROOT, project_path
    monkeypatch.chdir(tmp_path)
    assert project_path(None, "data/smartwallet.db") == (PROJECT_ROOT / "data/smartwallet.db").resolve()
    monkeypatch.setenv("NODE_ENV", "production")
    monkeypatch.delenv("SMARTWALLET_ALLOW_NEW_DB", raising=False)
    with pytest.raises(RuntimeError, match="Refusing to create a new production database"):
        WebDB(tmp_path / "missing" / "smartwallet.db")


def test_additive_upgrade_preserves_run_archive_membership(settings):
    db = WebDB(settings.db_path); rid = create(db)
    with db.connect() as conn:
        conn.execute("INSERT INTO wallet_events(event_id,entity_id,wallet,chain,ts,source,action_type,evidence_json,raw_json,created_at) VALUES('old-event','x','0xa','eth',1,'old','TRANSFER','{}','{}',datetime('now'))")
        conn.execute("INSERT INTO run_events(run_id,event_id,built) VALUES(?,?,1)", (rid, "old-event"))
    WebDB(settings.db_path)
    assert db.row("SELECT COUNT(*) n FROM analysis_runs")["n"] == 1
    assert db.row("SELECT COUNT(*) n FROM run_entities WHERE run_id=?", (rid,))["n"] == 1
    assert db.row("SELECT COUNT(*) n FROM run_events WHERE run_id=?", (rid,))["n"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure",[429,500,400,404,"timeout","network","upstream429"])
async def test_bounded_retry_metrics(settings,monkeypatch,failure):
    original_sleep=asyncio.sleep
    async def fast_sleep(delay): await original_sleep(0)
    monkeypatch.setattr("smartwallet.http.asyncio.sleep",fast_sleep)
    count=0
    async def handler(request):
        nonlocal count
        count+=1
        if failure=="timeout": raise httpx.ReadTimeout("private URL",request=request)
        if failure=="network": raise httpx.ConnectError("private URL",request=request)
        if failure=="upstream429": return httpx.Response(200,json={"status":429,"error":"secret body"})
        return httpx.Response(failure,json={"error":"secret body"})
    hub=HubClient(replace(settings,http_retries=3),transport=httpx.MockTransport(handler))
    audit=[];hub.observer=lambda *args:audit.append(args)
    with pytest.raises(HubError) as exc:
        await hub.request("debank.used_chains",query={"id":"wallet"},allow_upstream_error=True)
    expected=1 if failure in (400,404) else 3
    assert count==expected
    assert hub.metrics["failed"]==1 and hub.metrics["retry"]==expected-1
    assert hub.metrics["success"]==0
    assert audit[-1][3]=="failed" and audit[-1][5] is None
    assert "secret" not in str(exc.value) and "private URL" not in str(exc.value)
    await hub.aclose()


@pytest.mark.asyncio
async def test_recovery_and_other_provider_progress(settings,monkeypatch):
    original_sleep=asyncio.sleep
    async def fast_sleep(delay): await original_sleep(0)
    monkeypatch.setattr("smartwallet.http.asyncio.sleep",fast_sleep)
    calls=[]
    async def handler(request):
        calls.append(request.url.path)
        if "/debank/" in request.url.path and sum("/debank/" in p for p in calls)<3:
            return httpx.Response(500)
        return httpx.Response(200,json={"status":200,"data":{}})
    hub=HubClient(replace(settings,http_retries=3),transport=httpx.MockTransport(handler))
    await asyncio.gather(hub.request("debank.used_chains",query={"id":"wallet"}),hub.request("arkham.entity",path_params={"entity_id":"x"}))
    assert hub.metrics==dict(requests=4,success=2,cache_hit=0,retry=2,failed=0)
    assert "/arkham/" in calls[1]
    await hub.aclose()


def test_run_isolation_and_cache_enrollment(settings):
    storage=Storage(settings);db=WebDB(settings.db_path)
    a,b=create(db),create(db)
    episode(storage)
    storage.save_market_label(dict(episode_id="e",asset_key="ethereum:0xtoken",horizon_seconds=300,source="test",simple_return=.1))
    analyzer=IncrementalAnalyzer(db)
    assert analyzer.refresh(a)["observations"]==0
    db.execute("INSERT INTO run_episodes VALUES(?,?)",(a,"e"))
    assert analyzer.refresh(a)["observations"]==1
    assert analyzer.refresh(a)["observations"]==0
    assert analyzer.refresh(b)["observations"]==0
    worker=RunOrchestrator(db);worker._refresh_counts(b)
    assert db.row("SELECT episodes,events,market_labels FROM run_entities WHERE run_id=?",(b,))==dict(episodes=0,events=0,market_labels=0)
    assert sum(worker.queue_counts(b).values())==0
    assert _patterns('{"patterns":["FULL:A>B","BIGRAM:A>B"]}',"")==["BIGRAM:A>B"]
    storage.close()


@pytest.mark.asyncio
async def test_market_partial_success_survives_retry(settings):
    storage=Storage(settings);episode(storage)
    ep=storage.fetchone("SELECT * FROM episodes WHERE episode_id='e'")
    class Prices:
        hub=SimpleNamespace(metrics={"cache_hit":0})
        broken=True
        async def historical_prices(self,ts,coins):
            if ts==ep["start_ts"]+3600 and self.broken: raise HubError("temporary")
            return {"coins":{c:{"price":100+ts%100} for c in coins}}
    provider=Prices();labeler=MarketLabeler(provider,storage)
    assert await labeler.label_episode(ep)==4
    assert storage.fetchone("SELECT state FROM market_horizons WHERE horizon_seconds=3600")["state"]=="retryable failure"
    with storage.conn() as conn: conn.execute("UPDATE market_horizons SET next_retry=0")
    provider.broken=False
    assert await labeler.label_episode(ep)==1
    assert storage.fetchone("SELECT COUNT(*) n FROM market_horizons WHERE state='success'")["n"]==5
    assert await labeler.label_episode(ep)==0
    storage.close()


@pytest.mark.asyncio
async def test_fresh_collection_smoke_exhaustion_and_progress(settings,monkeypatch):
    from smartwallet.providers.debank import DeBankProvider
    original_sleep=asyncio.sleep
    async def fast_sleep(delay): await original_sleep(0)
    monkeypatch.setattr("smartwallet.http.asyncio.sleep",fast_sleep)
    storage=Storage(settings);db=WebDB(settings.db_path);rid=create(db)
    storage.run_id=rid
    for addr in ("0xbroken","0xhealthy"):
        db.execute("INSERT INTO run_wallets(run_id,entity_id,address,chain,status) VALUES(?,?,?,'eth','pending')",(rid,"x",addr))
    async def handler(request):
        if request.url.params.get("id")=="0xbroken":return httpx.Response(500)
        return httpx.Response(200,json={"status":200,"data":{"chains":[]}})
    hub=HubClient(settings,transport=httpx.MockTransport(handler))
    rt=SimpleNamespace(settings=settings,hub=hub,storage=storage,debank=DeBankProvider(hub,storage))
    worker=RunOrchestrator(db)
    async def collect(rt,run,row,max_pages):
        await rt.debank.used_chains(row["address"])
        return {"events":0}
    monkeypatch.setattr(worker,"_collect_wallet",collect)
    await worker._collect(rt,db.run(rid))
    assert [r["status"] for r in db.rows("SELECT status FROM run_wallets ORDER BY address")]==["failed","completed"]
    assert hub.metrics["failed"]==1 and hub.metrics["success"]==1 and hub.metrics["retry"]==1
    assert db.row("SELECT wallets_processed,wallets_failed,events FROM run_entities WHERE run_id=?",(rid,))==dict(wallets_processed=1,wallets_failed=1,events=0)
    await hub.aclose();storage.close()


def test_dirty_statistics_bh_uses_unchanged_family(settings):
    from smartwallet.pipeline.backtest import bh_qvalues
    storage=Storage(settings);db=WebDB(settings.db_path);rid=create(db)
    def add(start,end,intent):
        for i in range(start,end):
            eid=f"{intent}-{i}"
            episode(storage,eid)
            with storage.conn() as conn: conn.execute("UPDATE episodes SET intent_label=? WHERE episode_id=?",(intent,eid))
            db.execute("INSERT INTO run_episodes VALUES(?,?)",(rid,eid))
            storage.save_market_label(dict(episode_id=eid,asset_key="ethereum:0xtoken",horizon_seconds=300,source="test",simple_return=.1 if i%5 else -.1))
    add(0,10,"buy");add(0,10,"sell")
    analyzer=IncrementalAnalyzer(db);analyzer.refresh(rid);assert analyzer.refresh_expensive(rid)==2
    add(10,11,"buy");analyzer.refresh(rid);assert analyzer.refresh_expensive(rid)==0
    add(11,12,"buy");analyzer.refresh(rid);assert analyzer.refresh_expensive(rid)==1
    rows=db.rows("SELECT * FROM pattern_aggregates WHERE run_id=? ORDER BY intent_label",(rid,))
    assert [r["last_expensive_n"] for r in rows]==[12,10]
    assert [r["qvalue"] for r in rows]==pytest.approx(bh_qvalues([r["sign_pvalue"] for r in rows]))
    storage.close()


def test_builder_preserves_shared_cache_and_excludes_unenrolled(settings):
    from smartwallet.pipeline.episodes import build_episodes
    storage=Storage(settings);db=WebDB(settings.db_path);rid=create(db)
    def event(eid): return dict(event_id=eid,entity_id="x",wallet="0xa",chain="eth",ts=1735689600,source="test",action_type="BUY",primary_asset_key="ethereum:token",evidence={},raw={})
    storage.save_events([event("included"),event("excluded")])
    db.execute("INSERT INTO run_events(run_id,event_id) VALUES(?,?)",(rid,"included"))
    first=build_episodes(storage,"x",3600,run_id=rid)
    assert first[0]["event_ids"]==["included"]
    eid=first[0]["episode_id"]
    storage.update_episode_intent(eid,"buy",{},.9)
    storage.save_market_label(dict(episode_id=eid,asset_key="ethereum:token",horizon_seconds=300,source="test",simple_return=.1))
    build_episodes(storage,"x",3600,run_id=rid)
    assert storage.fetchone("SELECT intent_label FROM episodes WHERE episode_id=?",(eid,))["intent_label"]=="buy"
    assert storage.fetchone("SELECT COUNT(*) n FROM market_labels")["n"]==1
    assert storage.fetchone("SELECT COUNT(*) n FROM wallet_events")["n"]==2
    storage.close()


def test_migration_idempotent_preserves_raw(settings):
    storage=Storage(settings);storage.archive_raw("test","test",{}, {"value":1});storage.close()
    for _ in range(2): WebDB(settings.db_path)
    storage=Storage(settings)
    assert storage.fetchone("SELECT COUNT(*) n FROM raw_responses")["n"]==1
    assert storage.fetchone("SELECT COUNT(*) n FROM run_events")["n"]==0
    storage.close()


def test_auth_and_path_guard(settings,monkeypatch):
    import base64
    from fastapi.testclient import TestClient
    monkeypatch.setenv("SMARTWALLET_DB",str(settings.db_path))
    from smartwallet import web
    monkeypatch.setenv("SMARTWALLET_AUTH_TOKEN","a-private-test-token")
    client=TestClient(web.app)
    assert client.get("/api/config").status_code==401
    assert client.post("/api/runs/x/stop").status_code==401
    assert client.get("/api/config",headers={"Authorization":"Bearer a-private-test-token"}).status_code==200
    auth="Basic "+base64.b64encode(b"user:a-private-test-token").decode()
    assert client.get("/api/config",headers={"Authorization":auth}).status_code==200
    assert client.post("/api/runs/x/stop",headers={"Authorization":auth,"Origin":"https://evil.invalid"}).status_code==403
    monkeypatch.delenv("SMARTWALLET_AUTH_TOKEN")
    monkeypatch.setenv("NODE_ENV","production")
    assert client.get("/api/config").status_code==503


@pytest.mark.asyncio
async def test_provider_raw_cache_reused_without_external_request(settings):
    from smartwallet.providers.debank import DeBankProvider
    storage=Storage(settings)
    storage.archive_raw("debank","debank.used_chains",{"path_params":{},"query":{"id":"0xa"},"body":None},{"status":200,"data":{"chains":["eth"]}})
    async def handler(request): raise AssertionError("cache should avoid HTTP")
    hub=HubClient(settings,transport=httpx.MockTransport(handler))
    assert await DeBankProvider(hub,storage).used_chains("0xa")==["eth"]
    assert hub.metrics["cache_hit"]==1 and hub.metrics["requests"]==0
    await hub.aclose();storage.close()


@pytest.mark.asyncio
async def test_full_pipeline_smoke_and_cached_second_run(settings,monkeypatch):
    from smartwallet import orchestrator as mod
    from smartwallet.providers.debank import DeBankProvider
    original_sleep=asyncio.sleep
    async def fast_sleep(delay): await original_sleep(0)
    monkeypatch.setattr("smartwallet.http.asyncio.sleep",fast_sleep)
    storage=Storage(settings);db=WebDB(settings.db_path)
    async def handler(request):
        if request.url.params.get("id")=="0xbroken": return httpx.Response(500)
        path=request.url.path
        if "used_chain" in path: data={"chains":["eth"]}
        elif "history" in path: data={"history_list":[{"id":"tx1","idx":0,"time_at":1735689600,"sends":[{"token_id":"eth","amount":1,"price":100}],"receives":[]}],"token_dict":{"eth":{"symbol":"ETH"}}}
        else:data=[]
        return httpx.Response(200,json={"status":200,"data":data})
    hub=HubClient(settings,transport=httpx.MockTransport(handler))
    class Llama:
        def __init__(self):self.hub=hub
        async def historical_prices(self,ts,coins):return {"coins":{c:{"price":100+ts%100} for c in coins}}
    class LLM:
        async def classify_episode(self,payload):return {"label":"buy","confidence":.9}
    async def close():pass
    rt=SimpleNamespace(settings=settings,storage=storage,hub=hub,debank=DeBankProvider(hub,storage),llama=Llama(),llm=LLM(),arkham=None,oklink=None,gecko=None,aclose=close)
    async def discover(arkham,store,eid,name):
        store.upsert_wallet(eid,"0xhealthy","eth","test")
        store.upsert_wallet(eid,"0xbroken","eth","test")
    async def noop(*args,**kwargs):pass
    monkeypatch.setattr(mod,"discover_entity",discover)
    monkeypatch.setattr(mod,"snapshot_entity_context",noop)
    monkeypatch.setattr(mod,"snapshot_evm_wallet_context",noop)
    monkeypatch.setattr(mod.Settings,"load",lambda:settings)
    monkeypatch.setattr(mod.Runtime,"create",lambda settings:rt)
    rid=create(db);db.execute("UPDATE analysis_runs SET settings_json=? WHERE run_id=?",('{"max_pages":1}',rid))
    worker=RunOrchestrator(db);worker.launch(rid)
    await asyncio.wait_for(worker.tasks[rid],10)
    assert db.run(rid)["status"]=="completed",db.run(rid)
    assert db.rows("SELECT status FROM run_wallets WHERE run_id=? ORDER BY address",(rid,))==[{"status":"failed"},{"status":"completed"}]
    assert db.row("SELECT events,episodes,classified,market_labels,usable_observations FROM run_entities WHERE run_id=?",(rid,))==dict(events=1,episodes=1,classified=1,market_labels=5,usable_observations=5)
    assert sum(worker.queue_counts(rid).values())==0
    before=hub.metrics["requests"]
    rid2=create(db);db.execute("UPDATE analysis_runs SET settings_json=? WHERE run_id=?",('{"max_pages":1}',rid2))
    worker.launch(rid2);await asyncio.wait_for(worker.tasks[rid2],10)
    assert db.run(rid2)["status"]=="completed"
    # Only the broken wallet calls HTTP again; the successful wallet and labels are reused.
    assert hub.metrics["requests"]-before==settings.http_retries
    assert db.row("SELECT events,market_labels FROM run_entities WHERE run_id=?",(rid2,))==dict(events=1,market_labels=5)
    await hub.aclose();storage.close()


def test_action_grouping_is_distinct_from_action_intent(settings):
    from smartwallet.pipeline.backtest import run_backtest
    storage=Storage(settings)
    for i in range(4):
        episode(storage,str(i))
        with storage.conn() as conn:
            conn.execute("UPDATE episodes SET intent_label=? WHERE episode_id=?",("buy" if i<2 else "sell",str(i)))
        storage.save_market_label(dict(episode_id=str(i),asset_key="ethereum:token",horizon_seconds=300,source="test",simple_return=.1))
    _,frame=run_backtest(storage,min_n=1)
    broad=frame[frame.hierarchy_level=="entity_action"]
    specific=frame[frame.hierarchy_level=="entity_action_intent"]
    assert broad.n.tolist()==[4]
    assert sorted(specific.n.tolist())==[2,2]
    assert all("FULL:" not in v for v in frame.motif)
    storage.close()


@pytest.mark.asyncio
async def test_market_exhaustion_is_explicit(settings):
    storage=Storage(settings);episode(storage)
    ep=storage.fetchone("SELECT * FROM episodes WHERE episode_id='e'")
    class Broken:
        async def historical_prices(self,*args):raise HubError("provider unavailable")
    labeler=MarketLabeler(Broken(),storage)
    for _ in range(3):
        with storage.conn() as conn:conn.execute("UPDATE market_horizons SET next_retry=0")
        await labeler.label_episode(ep,(300,))
    state=storage.fetchone("SELECT * FROM market_horizons WHERE horizon_seconds=300")
    assert state["state"]=="permanently unavailable" and state["attempts"]==3
    assert "retry budget exhausted" in state["reason"]
    storage.close()


def test_eta_includes_downstream_backlog(settings):
    import time
    db=WebDB(settings.db_path);rid=create(db)
    db.execute("UPDATE run_entities SET classified=6,events=6,episodes=6 WHERE run_id=?",(rid,))
    db.execute("INSERT INTO run_wallets(run_id,entity_id,address,status) VALUES(?,'x','done','completed')",(rid,))
    db.execute("INSERT INTO run_wallets(run_id,entity_id,address,status) VALUES(?,'x','next','pending')",(rid,))
    worker=RunOrchestrator(db)
    worker.queue_counts=lambda run_id:dict(episode_builder=0,llm_classification=240,market_labeling=0,analysis=0)
    worker._samples[rid]=[(time.monotonic()-60,dict(events=0,episodes=0,classified=0,labels=0,analyzed=0,wallets=0,requests=0))]
    metrics=worker.live_metrics(rid)
    assert metrics["eta"]["llm"]>=2400
    assert metrics["eta"]["total"]>=metrics["eta"]["llm"]
    worker._samples.clear()
    assert worker.live_metrics(rid)["eta"]["total"] is None


def test_legacy_market_context_does_not_complete_horizons(settings):
    storage=Storage(settings);episode(storage)
    storage.save_episode_market_context(dict(episode_id="e",source="old",regime="unknown"))
    storage.save_market_label(dict(episode_id="e",asset_key="ethereum:0xtoken",horizon_seconds=300,source="test",simple_return=.1))
    with storage.conn() as conn:
        conn.execute("DELETE FROM market_horizons")
        conn.execute("DELETE FROM schema_migrations WHERE version=2")
    WebDB(settings.db_path)
    states=storage.fetchall("SELECT state,COUNT(*) n FROM market_horizons GROUP BY state")
    assert {r["state"]:r["n"] for r in states}=={"pending":4,"success":1}
    storage.close()
