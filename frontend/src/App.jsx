import React, { useEffect, useMemo, useState } from "react";

const API = "/api";
const horizonName = value => ({300:"5m",3600:"1h",21600:"6h",86400:"1d",259200:"3d"}[value] || `${value}s`);
const pct = value => value == null ? "Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ" : `${(value * 100).toFixed(Math.abs(value) < .01 ? 2 : 1)}%`;
const number = value => new Intl.NumberFormat("en-US", {notation: value > 999999 ? "compact" : "standard", maximumFractionDigits: 1}).format(value || 0);
const duration = value => {
  if (value == null) return "CalculatingР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦";
  const h = Math.floor(value / 3600), m = Math.floor(value % 3600 / 60), s = Math.floor(value % 60);
  return h ? `${h}h ${m}m` : m ? `${m}m ${s}s` : `${s}s`;
};
const bytes = value => value > 1e9 ? `${(value/1e9).toFixed(1)} GB` : value > 1e6 ? `${(value/1e6).toFixed(1)} MB` : `${Math.round(value/1e3)} KB`;
const dateTime = value => value ? new Date(value).toLocaleString([], {month:"short",day:"2-digit",hour:"2-digit",minute:"2-digit"}) : "Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ";

async function request(path, options) {
  const response = await fetch(API + path, {headers:{"Content-Type":"application/json"}, ...options});
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || "Request failed");
  return body;
}

function go(path) { history.pushState({}, "", path); dispatchEvent(new PopStateEvent("popstate")); }

function Logo() {
  return <button className="logo" onClick={() => go("/")}><span className="logoMark">S</span><span>SMARTWALLET<small>RESEARCH TERMINAL</small></span></button>;
}

function Shell({children}) {
  const [path,setPath] = useState(location.pathname);
  useEffect(() => { const fn=()=>setPath(location.pathname); addEventListener("popstate",fn); return()=>removeEventListener("popstate",fn); },[]);
  return <><header><Logo/><nav>
    <button className={path==="/"?"active":""} onClick={()=>go("/")}>Terminal</button>
    <button className={path.startsWith("/runs")?"active":""} onClick={()=>go("/runs")}>Runs</button>
    <span className="liveDot"/> LIVE DATA
  </nav></header><main>{children}</main></>;
}

function StartForm({config,onStarted}) {
  const [entities,setEntities]=useState(config.entities.map(e=>e.id));
  const [mode,setMode]=useState("full");
  const [advanced,setAdvanced]=useState(false);
  const [form,setForm]=useState({from_date:"2024-01-01",to_date:config.today,concurrency:8,max_wallets:"",max_pages:"",enrichment_threshold:100000,llm_concurrency:4});
  const [busy,setBusy]=useState(false), [error,setError]=useState("");
  const toggle=id=>setEntities(values=>values.includes(id)?values.filter(x=>x!==id):[...values,id]);
  const start=async()=>{ setBusy(true);setError("");try{
    const settings={...form,max_wallets:form.max_wallets?+form.max_wallets:null,max_pages:form.max_pages?+form.max_pages:null};
    delete settings.from_date;delete settings.to_date;
    const result=await request("/runs",{method:"POST",body:JSON.stringify({entities,from_date:form.from_date,to_date:form.to_date,mode,settings})});onStarted(result.run_id);
  }catch(e){setError(e.message)}finally{setBusy(false)}};
  return <section className="startPanel">
    <div className="eyebrow">NEW RESEARCH RUN</div><h1>Historical analysis</h1><p className="muted">Collect, classify and test institutional wallet behavior as history arrives.</p>
    {(!config.credentials.api_hub||!config.credentials.llm)&&<div className="warning">Credentials incomplete: {!config.credentials.api_hub&&"API_HUB_KEY "}{!config.credentials.llm&&"FREE_LLM_API"}. The terminal works, but a run will pause with an actionable error.</div>}
    <label className="fieldLabel">Entities</label><div className="entityChecks">{config.entities.map(entity=><label key={entity.id} className={entities.includes(entity.id)?"checked":""}>
      <input type="checkbox" checked={entities.includes(entity.id)} onChange={()=>toggle(entity.id)}/><span className="check">Р В Р вЂ Р РЋРЎв„ўР Р†Р вЂљРЎС™</span>{entity.name}
    </label>)}</div>
    <div className="formGrid"><label><span>From</span><input type="date" value={form.from_date} onChange={e=>setForm({...form,from_date:e.target.value})}/></label><label><span>To</span><input type="date" value={form.to_date} max={config.today} onChange={e=>setForm({...form,to_date:e.target.value})}/></label></div>
    <label className="fieldLabel">Mode</label><div className="modeSelect">
      <button className={mode==="diagnostic"?"selected":""} onClick={()=>setMode("diagnostic")}><b>Diagnostic</b><small>5 wallets Р В РІР‚в„ўР вЂ™Р’В· 2 pages per entity</small></button>
      <button className={mode==="full"?"selected":""} onClick={()=>setMode("full")}><b>Full historical analysis</b><small>Complete configured universe</small></button>
    </div>
    <button className="advancedToggle" onClick={()=>setAdvanced(!advanced)}>Advanced settings <span>{advanced?"Р В Р вЂ Р Р†РІР‚С™Р’В¬Р Р†Р вЂљРІвЂћСћ":"+"}</span></button>
    {advanced&&<div className="advanced formGrid">
      {[['max_wallets','Max wallets','Optional cap'],['max_pages','Max pages','Optional cap'],['concurrency','Collection concurrency','1Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎС™32'],['llm_concurrency','LLM concurrency','1Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎС™16'],['enrichment_threshold','Enrich above USD','Transaction value']].map(([key,label,hint])=><label key={key}><span>{label}<small>{hint}</small></span><input type="number" value={form[key]} onChange={e=>setForm({...form,[key]:e.target.value})}/></label>)}
    </div>}
    {error&&<div className="errorBox">{error}</div>}<button className="primary startButton" disabled={busy||!entities.length} onClick={start}>{busy?"STARTINGР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦":"START ANALYSIS"}<span>Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ</span></button>
  </section>;
}

function Progress({run,elapsed,eta}) {
  const status=run.status.toUpperCase().replace("_"," ");
  return <section className="runHero"><div className="runHeroTop"><div><div className={`status ${run.status}`}><i/>{status}</div><h1>Analysis run <span>#{run.sequence}</span></h1><p>{run.from_date} <b>Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ</b> {run.to_date} Р В РІР‚в„ўР вЂ™Р’В· {run.entities.length} entities</p></div><div className="timeStats"><div><span>ELAPSED</span><b>{duration(elapsed)}</b></div><div><span>ESTIMATED REMAINING</span><b>{duration(eta)}</b></div></div></div>
    <div className="progressMeta"><span>{run.stage.replaceAll("_"," ")}</span><b>{Math.round(run.progress*100)}%</b></div><div className="progress"><i style={{width:`${run.progress*100}%`}}/></div><div className="current"><span>CURRENT WORK</span>{run.current_work||"Preparing runР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦"}</div>
    {run.error&&<div className="errorBox">{run.error}</div>}
  </section>;
}

const statsInfo={events:"Normalized blockchain actions collected from wallet history.",episodes:"Related actions grouped into one behavioral sequence.",market_labels:"Future price observations attached to historical episodes.",usable_observations:"Pattern-level observations with enough market data to participate in analysis."};
function MetricCards({totals,databaseSize}) {
 const items=[['wallets_processed','Wallets processed',`${number(totals.wallets_processed)} / ${number(totals.wallets_total)}`],['events','Events collected',number(totals.events)],['episodes','Episodes built',number(totals.episodes)],['classified','LLM classified',number(totals.classified)],['market_labels','Market outcomes',number(totals.market_labels)],['usable_observations','Usable observations',number(totals.usable_observations)],['database','Database size',bytes(databaseSize)]];
 return <div className="metricGrid">{items.map(([key,label,value])=><div className="metric" key={key}><span>{label}{statsInfo[key]&&<i title={statsInfo[key]}>i</i>}</span><b>{value}</b>{key==='wallets_processed'&&<small>{totals.wallets_failed?`${totals.wallets_failed} failed`:`${totals.wallets_partial||0} partial`}</small>}</div>)}</div>
}

function Operations({speed,queues}) {
 const qLabels={episode_builder:"Episode builder",llm_classification:"LLM classification",market_labeling:"Market labeling",analysis:"Analysis"};
 return <section className="ops"><div><div className="sectionTitle">COLLECTION SPEED <span>LIVE</span></div><div className="speedGrid">
  <Kpi value={number(speed.events_per_minute)} unit="events/min"/><Kpi value={number(speed.wallets_per_hour)} unit="wallets/hour"/><Kpi value={number(speed.api_requests_per_second)} unit="API req/sec"/><Kpi value={number(speed.llm_jobs_per_second)} unit="LLM jobs/sec"/>
 </div><div className="apiRow"><span>API</span><b>{number(speed.api?.success)} successful</b><b>{number(speed.api?.cache_hit)} cache hits</b><b className="amber">{number(speed.api?.retry)} retries</b><b className="red">{number(speed.api?.failed)} failed</b></div></div>
 <div className="queuePanel"><div className="sectionTitle">QUEUES / CATCH-UP</div>{Object.entries(speed.eta||{}).map(([k,v])=><div className="queue" key={k}><span>{k} ETA</span><b>{duration(v)}</b></div>)}{Object.entries(qLabels).map(([key,label])=><div className="queue" key={key}><span>{label}{key==="llm_classification"&&queues[key]>0&&speed.llm_jobs_per_second>0&&<small> Р В РІР‚в„ўР вЂ™Р’В· ~{duration(queues[key]/speed.llm_jobs_per_second)}</small>}</span><b className={queues[key]?"pending":"clear"}>{number(queues[key])} pending</b></div>)}</div></section>;
}
function Kpi({value,unit}){return <div className="kpi"><b>{value}</b><span>{unit}</span></div>}

function EntityCards({entities}) {
 return <section><div className="sectionHeading"><div><div className="eyebrow">PIPELINE COVERAGE</div><h2>Entities</h2></div></div><div className="entityCards">{entities.map(e=>{
  const coverage=e.wallets_total?e.wallets_processed/e.wallets_total:0;return <article className="entityCard" key={e.entity_id}><div className="entityName"><i/>{e.name}<span>{Math.round(coverage*100)}%</span></div>
  <div className="miniProgress"><i style={{width:`${coverage*100}%`}}/></div><dl><div><dt>Wallets</dt><dd>{e.wallets_processed} / {e.wallets_total}</dd></div><div><dt>Events</dt><dd>{number(e.events)}</dd></div><div><dt>Episodes</dt><dd>{number(e.episodes)}</dd></div><div><dt>Classified</dt><dd>{number(e.classified)}</dd></div><div><dt>Market labels</dt><dd>{number(e.market_labels)}</dd></div></dl>{e.wallets_failed>0&&<small className="red">{e.wallets_failed} wallet failures</small>}</article>})}</div></section>
}

function PatternCard({pattern,runId}) {
 const rate=pattern.direction==="down"?pattern.negative_rate:pattern.positive_rate;
 return <button className="patternCard" onClick={()=>go(`/runs/${runId}/patterns/${pattern.pattern_key}`)}><div className="patternTop"><span>{pattern.entity_id.replaceAll("-"," ")} Р В РІР‚в„ўР вЂ™Р’В· {pattern.asset_label} Р вЂ™Р’В· {pattern.chain}</span><em className={`maturity ${pattern.maturity}`}>{pattern.maturity}</em></div>
  <small title={pattern.asset_key}>{pattern.asset_name||pattern.asset_key}</small><h3 title={pattern.pattern_label}>{pattern.pattern_label}</h3><small>{pattern.intent_label.replaceAll("_"," ")} Р В РІР‚в„ўР вЂ™Р’В· {horizonName(pattern.horizon_seconds)} outcome</small>
  <div className="outcome"><div><b>{pct(rate)}</b><span>price went {pattern.direction}</span></div><div className={pattern.mean_return<0?"negative":"positive"}><b>{pct(pattern.mean_return)}</b><span>average return</span></div></div>
  <dl className="patternStats"><div><dt>Historical cases</dt><dd>{pattern.n}</dd></div><div><dt>q-value</dt><dd>{pattern.qvalue==null?"pending":pattern.qvalue.toFixed(4)}</dd></div><div><dt>Median</dt><dd>{pct(pattern.median_return)}</dd></div><div><dt>95% CI</dt><dd>{pattern.ci_low==null?"pending":`${pct(pattern.ci_low)} Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ ${pct(pattern.ci_high)}`}</dd></div><div><dt>Holdout accuracy</dt><dd>{pct(pattern.holdout_accuracy)}</dd></div></dl>
  {pattern.n<3&&<div className="insufficient">Low sample Р вЂ™Р’В· n={pattern.n}</div>}
  {pattern.maturity==="EARLY"&&<div className="insufficient">Insufficient sample Р В РІР‚в„ўР вЂ™Р’В· interpret cautiously</div>}
 </button>;
}

function Discoveries({feed,runId,entities=[]}) {
 const [filters,setFilters]=useState({min_n:"3",entity:"",intent:"",kind:"",horizon:"",search:"",sort:"best"});
 const [offset,setOffset]=useState(0),[data,setData]=useState({items:[],total:0}),[error,setError]=useState("");
 const change=(key,value)=>{setFilters({...filters,[key]:value});setOffset(0)};
 useEffect(()=>{let active=true;const load=()=>{const q=new URLSearchParams({...filters,offset,limit:24});if(filters.min_n==="low"){q.set("min_n",1);q.set("max_n",2)}request(`/runs/${runId}/patterns?${q}`).then(d=>active&&setData(d)).catch(e=>active&&setError(e.message))};load();const timer=setInterval(load,15000);return()=>{active=false;clearInterval(timer)}},[runId,filters,offset]);
 return <section><div className="sectionHeading"><div><div className="eyebrow">LIVE DISCOVERIES</div><h2>Pattern Explorer</h2></div><span className="muted">{data.total} hypotheses Р вЂ™Р’В· {filters.min_n==="low"?"Low sample":"Confidence-aware ranking"}</span></div>
 <div className="explorerFilters">
 <label>Sample<select aria-label="Sample" value={filters.min_n} onChange={e=>change("min_n",e.target.value)}>{[["1","All"],["3","n >= 3"],["5","n >= 5"],["10","n >= 10"],["20","n >= 20"],["low","Low sample: n=1-2"]].map(([v,t])=><option key={v} value={v}>{t}</option>)}</select></label>
 <label>Entity<select value={filters.entity} onChange={e=>change("entity",e.target.value)}><option value="">All entities</option>{entities.map(e=><option key={e.entity_id} value={e.entity_id}>{e.name}</option>)}</select></label>
 <label>Intent<input value={filters.intent} placeholder="All intents" onChange={e=>change("intent",e.target.value)}/></label>
 <label>Pattern type<select value={filters.kind} onChange={e=>change("kind",e.target.value)}>{["","ACTION","BIGRAM","TRIGRAM","ENDPOINT","FULL"].map(v=><option key={v} value={v}>{v||"All types"}</option>)}</select></label>
 <label>Horizon<select value={filters.horizon} onChange={e=>change("horizon",e.target.value)}>{["",300,3600,21600,86400,259200].map(v=><option key={v} value={v}>{v?horizonName(v):"All horizons"}</option>)}</select></label>
 <label>Search<input value={filters.search} placeholder="Action, pattern, asset..." onChange={e=>change("search",e.target.value)}/></label>
 <label>Sort<select value={filters.sort} onChange={e=>change("sort",e.target.value)}>{[["best","Best overall"],["sample","Largest sample"],["effect","Largest effect"],["hit","Highest hit rate"],["newest","Recently changed"]].map(([v,t])=><option key={v} value={v}>{t}</option>)}</select></label></div>
 {error&&<div className="errorBox">{error}</div>}<div className="patternGrid">{data.items.map(p=><PatternCard key={p.pattern_key} pattern={p} runId={runId}/>)}</div>
 {!data.items.length&&<p className="emptySmall">No matching patterns yet. Low-sample observations remain available in the Sample filter.</p>}
 <div className="actionBar"><button disabled={!offset} onClick={()=>setOffset(Math.max(0,offset-24))}>Previous</button><span>{data.total?offset+1:0}-{Math.min(offset+24,data.total)} / {data.total}</span><button disabled={offset+24>=data.total} onClick={()=>setOffset(offset+24)}>Next</button></div>
 <div className="feed"><div className="sectionTitle">DISCOVERY FEED</div>{feed.slice(0,15).map(item=><div className="feedItem" key={item.id}><time>{dateTime(item.created_at)}</time><i/><div><b>{item.title}</b><span>{item.detail}</span></div></div>)}</div>
 </section>;
}

function RunDashboard({runId}) {
 const [data,setData]=useState(null),[error,setError]=useState("");
 const load=()=>request(`/runs/${runId}`).then(setData).catch(e=>setError(e.message));
 useEffect(()=>{load();const source=new EventSource(`${API}/runs/${runId}/stream`);source.addEventListener("snapshot",e=>setData(JSON.parse(e.data)));source.onerror=()=>setError("Live connection interrupted; reconnectingР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦");return()=>source.close()},[runId]);
 const control=async action=>{try{await request(`/runs/${runId}/${action}`,{method:"POST"});load()}catch(e){setError(e.message)}};
 if(!data)return <div className="loading">Opening live runР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦ {error}</div>;
 const canPause=["running","queued"].includes(data.run.status), canResume=["paused","failed","stopped"].includes(data.run.status);
 return <><div className="actionBar"><button onClick={()=>go("/runs")}>Р В Р вЂ Р Р†Р вЂљР’В Р РЋРІР‚в„ў All runs</button><div>{canPause&&<button onClick={()=>control("pause")}>Р В Р вЂ Р Р†Р вЂљР’В¦Р В Р вЂ№ Pause</button>}{canResume&&<button className="primary small" onClick={()=>control("resume")}>Р В Р вЂ Р Р†Р вЂљРІР‚СљР вЂ™Р’В¶ Resume</button>}{!["completed","stopped"].includes(data.run.status)&&<button className="danger" onClick={()=>control("stop")}>Р В Р вЂ Р Р†Р вЂљРІР‚СљР вЂ™Р’В  Stop safely</button>}<button onClick={()=>request(`/runs/${runId}/analysis/refresh`,{method:"POST"}).then(load).catch(e=>setError(e.message))}>Refresh statistics</button><button onClick={()=>go(`/runs/${runId}/quality`)}>Data quality</button></div></div>
  {error&&<div className="toast" onClick={()=>setError("")}>{error}</div>}<Progress run={data.run} elapsed={data.elapsed_seconds} eta={data.eta_seconds}/><MetricCards totals={data.totals} databaseSize={data.database_size}/><Operations speed={data.speed} queues={data.queues}/><EntityCards entities={data.entities}/><Discoveries patterns={data.patterns} feed={data.feed} runId={runId} entities={data.entities}/><footer>Analysis updated {data.run.analysis_updated_at?dateTime(data.run.analysis_updated_at):"not yet"} Р В РІР‚в„ўР вЂ™Р’В· Conditional historical estimates, not trading recommendations.</footer></>;
}

function Runs() {
 const [runs,setRuns]=useState([]);useEffect(()=>{request("/runs").then(setRuns)},[]);
 return <section><div className="pageHead"><div><div className="eyebrow">RESEARCH ARCHIVE</div><h1>Analysis runs</h1></div><button className="primary" onClick={()=>go("/")}>+ New analysis</button></div><div className="runsList">{runs.map(run=><button key={run.run_id} onClick={()=>go(`/runs/${run.run_id}`)}><span className={`runNumber ${run.status}`}>#{run.sequence}</span><div><b>{run.status.toUpperCase()}</b><span>{run.from_date} Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ {run.to_date} Р В РІР‚в„ўР вЂ™Р’В· {run.entities.length} entities</span></div><div><b>{number(run.events)} events</b><span>{duration(run.elapsed_seconds)}</span></div><em>Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ</em></button>)}</div></section>;
}

function Quality({runId}) {
 const [data,setData]=useState(null);useEffect(()=>{request(`/runs/${runId}/quality`).then(setData)},[runId]);if(!data)return <div className="loading">Inspecting data qualityР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦</div>;
 return <section><div className="actionBar"><button onClick={()=>go(`/runs/${runId}`)}>Р В Р вЂ Р Р†Р вЂљР’В Р РЋРІР‚в„ў Dashboard</button></div><div className="pageHead"><div><div className="eyebrow">COVERAGE & FAILURES</div><h1>Data quality</h1><p className="muted">Separate absence of evidence from absence of data.</p></div></div><div className="qualityGrid">{data.entities.map(e=><article key={e.entity_id}><h3>{e.name}</h3><dl><div><dt>Discovered wallets</dt><dd>{e.wallets_total}</dd></div><div><dt>Successfully backfilled</dt><dd>{e.wallets_processed}</dd></div><div><dt>Failed / incomplete</dt><dd className={e.wallets_failed?"red":""}>{e.wallets_failed}</dd></div><div><dt>Missing market outcomes</dt><dd>{e.missing_market}</dd></div><div><dt>Unknown assets</dt><dd>{e.unknown_assets}</dd></div><div><dt>Unclassified episodes</dt><dd>{e.unclassified}</dd></div><div><dt>Unsupported chains</dt><dd>{e.unsupported_chains}</dd></div><div><dt>Unavailable horizons</dt><dd>{e.unavailable_market}</dd></div><div><dt>Processing / partial</dt><dd>{e.processing||0} / {e.partial||0}</dd></div><div><dt>Events / episodes</dt><dd>{e.events} / {e.episodes}</dd></div><div><dt>Retries / failures</dt><dd>{e.retries||0} / {e.failures||0}</dd></div><div><dt>Time coverage</dt><dd>{e.coverage_from?new Date(e.coverage_from*1000).toLocaleDateString():"Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ"} Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ {e.coverage_to?new Date(e.coverage_to*1000).toLocaleDateString():"Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ"}</dd></div><div><dt>API success</dt><dd>{e.api_success_rate==null?"Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ":pct(e.api_success_rate)}</dd></div></dl></article>)}</div><div className="feed errors"><div className="sectionTitle">RECENT ERRORS / RETRIES</div>{data.market_failures?.map(r=><div className="errorLine" key={`${r.episode_id}-${r.horizon_seconds}`}><b>{horizonName(r.horizon_seconds)}</b><b>{r.state}</b><span>{r.reason}</span></div>)}{data.requests?.map(r=><div className="errorLine" key={`request-${r.id}`}><time>{dateTime(r.created_at)}</time><b>{r.provider} ? {r.endpoint}</b><span>{r.state}: {r.error} ? attempt {r.attempt}{r.next_retry?` ? next ${dateTime(r.next_retry*1000)}`:""}</span></div>)}{data.errors.length?data.errors.map(e=><div className="errorLine" key={e.id}><time>{dateTime(e.created_at)}</time><b>{e.stage}</b><span>{e.message}</span></div>):<div className="emptySmall">No recorded errors in this run.</div>}</div></section>;
}

function LineChart({points,field,color="#58e6c2",percent=true}) {
 if(!points?.length)return <div className="chartEmpty">Not enough checkpoints yet</div>;
 const values=points.map(p=>+p[field]),min=Math.min(...values),max=Math.max(...values),range=max-min||1;
 const coords=points.map((p,i)=>`${20+i*(560/Math.max(1,points.length-1))},${150-(+p[field]-min)/range*110}`).join(" ");
 return <svg className="lineChart" viewBox="0 0 600 180"><line x1="20" y1="150" x2="580" y2="150"/><polyline points={coords} fill="none" stroke={color} strokeWidth="3"/>{points.map((p,i)=><g key={i}><circle cx={20+i*(560/Math.max(1,points.length-1))} cy={150-(+p[field]-min)/range*110} r="4" fill={color}/><text x={20+i*(560/Math.max(1,points.length-1))} y="171" textAnchor="middle">n={p.n}</text></g>)}</svg>;
}

function EpisodeRow({observation,runId}) {
 const [open,setOpen]=useState(false),[evidence,setEvidence]=useState(null);
 const toggle=async event=>{event.preventDefault();const next=!open;setOpen(next);if(next&&!evidence)setEvidence(await request(`/runs/${runId}/episodes/${observation.episode_id}`));};
 return <details open={open}><summary onClick={toggle}><time>{new Date(observation.episode_ts*1000).toLocaleDateString()}</time><b>{observation.gross_usd?`$${number(observation.gross_usd)}`:"Value unavailable"}</b><span>{observation.motif.replaceAll(">"," Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ ")}</span><em className={observation.return_value<0?"negative":"positive"}>{pct(observation.return_value)}</em></summary>{open&&<pre>{evidence?JSON.stringify(evidence,null,2):"Loading evidenceР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦"}</pre>}</details>;
}

function PatternDetail({runId,patternId}) {
 const [data,setData]=useState(null);useEffect(()=>{request(`/runs/${runId}/patterns/${patternId}`).then(setData)},[runId,patternId]);if(!data)return <div className="loading">Loading evidenceР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦</div>;
 const p=data.pattern, rate=p.direction==="down"?p.negative_rate:p.positive_rate;
 return <section><div className="actionBar"><button onClick={()=>go(`/runs/${runId}`)}>Р В Р вЂ Р Р†Р вЂљР’В Р РЋРІР‚в„ў Dashboard</button></div><div className="patternHero"><div><span>{p.entity_id.replaceAll("-"," ")} Р В РІР‚в„ўР вЂ™Р’В· {p.asset_label} Р В РІР‚в„ўР вЂ™Р’В· {horizonName(p.horizon_seconds)}</span><h1>{p.pattern_label}</h1><p>After this entity performed this sequence, {p.asset_label} was {p.direction === "down" ? "lower" : "higher"} {horizonName(p.horizon_seconds)} later in <b>{pct(rate)}</b> of {p.n} historical observations.</p></div><em className={`maturity ${p.maturity}`}>{p.maturity}</em></div>
 <div className="detailKpis"><Kpi value={pct(p.mean_return)} unit="mean return"/><Kpi value={pct(p.median_return)} unit="median return"/><Kpi value={p.n} unit="observations"/><Kpi value={pct(p.holdout_accuracy)} unit="holdout accuracy"/><Kpi value={p.qvalue==null?"Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ":p.qvalue.toFixed(4)} unit="BH q-value"/></div>
 <div className="chartGrid"><article><div className="sectionTitle">EVOLUTION AS SAMPLE GROWS</div><LineChart points={data.checkpoints} field="mean_return"/></article><article><div className="sectionTitle">DIRECTION RATE</div><LineChart points={data.checkpoints} field="negative_rate" color="#9a8cff"/></article></div>
 <div className="chartGrid secondaryCharts"><article><div className="sectionTitle">RETURN DISTRIBUTION</div><div className="histogram">{data.histogram.map((bin,i)=><span key={i}><b style={{height:`${8+90*bin.count/Math.max(1,...data.histogram.map(x=>x.count))}px`}}/><small>{pct(bin.start)}</small></span>)}</div></article><article><div className="sectionTitle">CHRONOLOGICAL OBSERVATIONS</div><LineChart points={data.observations.map((o,i)=>({n:i+1,value:o.return_value}))} field="value" color="#f2ba5c"/></article></div>
 <article className="trainHoldout"><div className="sectionTitle">TRAIN VS CHRONOLOGICAL HOLDOUT</div><div><span><b>TRAIN Р В РІР‚в„ўР вЂ™Р’В· {p.train_n||0}</b><em>{pct(p.train_n?((p.mean_return*p.n-(p.test_mean_return||0)*(p.test_n||0))/p.train_n):null)}</em></span><i>Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ</i><span><b>HOLDOUT Р В РІР‚в„ўР вЂ™Р’В· {p.test_n||0}</b><em>{pct(p.test_mean_return)}</em></span><span><b>DIRECTION ACCURACY</b><em>{pct(p.holdout_accuracy)}</em></span></div></article>
 <article className="horizonChart"><div className="sectionTitle">MEAN RETURN BY HORIZON</div><div>{data.horizons.map(h=><span key={h.horizon_seconds}><b style={{height:`${Math.min(100,Math.abs(h.mean_return)*1000+8)}px`}} className={h.mean_return<0?"down":"up"}/><em>{pct(h.mean_return)}</em><small>{horizonName(h.horizon_seconds)}</small></span>)}</div></article>
 <details className="advancedStats"><summary>Advanced statistics</summary><dl>{[['n',p.n],['mean',pct(p.mean_return)],['median',pct(p.median_return)],['bootstrap 95% CI',`${pct(p.ci_low)} Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ ${pct(p.ci_high)}`],['p-value',p.sign_pvalue?.toFixed(5)||'Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ'],['q-value',p.qvalue?.toFixed(5)||'Р В Р вЂ Р В РІР‚С™Р Р†Р вЂљРЎСљ'],['train / holdout',`${p.train_n||0} / ${p.test_n||0}`],['holdout accuracy',pct(p.holdout_accuracy)],['shrinkage estimate',pct(p.shrunk_mean)]].map(([a,b])=><div key={a}><dt>{a}</dt><dd>{b}</dd></div>)}</dl></details>
 <div className="sectionHeading"><div><div className="eyebrow">SOURCE EVIDENCE</div><h2>Historical episodes</h2></div></div><div className="observations">{[...data.observations].reverse().map(o=><EpisodeRow key={o.observation_key} observation={o} runId={runId}/>)}</div></section>;
}

function Home(){const[config,setConfig]=useState(null);useEffect(()=>{request("/config").then(setConfig)},[]);if(!config)return <div className="loading">Starting terminalР В Р вЂ Р В РІР‚С™Р вЂ™Р’В¦</div>;return <div className="home"><div className="intro"><div className="eyebrow">INSTITUTIONAL ON-CHAIN INTELLIGENCE</div><h1>Research behavior.<br/><span>Watch evidence mature.</span></h1><p>Historical collection, intent modeling and forward-return analysis in one live, restart-safe workspace.</p><div className="terminalMock"><i/><i/><i/><code>COLLECT Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ EPISODES Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ INTENT Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ OUTCOMES Р В Р вЂ Р Р†Р вЂљР’В Р Р†Р вЂљРІвЂћСћ EVIDENCE</code></div></div><StartForm config={config} onStarted={id=>go(`/runs/${id}`)}/></div>}

export default function App(){const [pathname,setPathname]=useState(location.pathname);useEffect(()=>{const update=()=>setPathname(location.pathname);addEventListener("popstate",update);return()=>removeEventListener("popstate",update)},[]);const path=pathname.split("/").filter(Boolean);let page=<Home/>;if(path[0]==="runs"&&!path[1])page=<Runs/>;else if(path[0]==="runs"&&path[1]&&path[2]==="quality")page=<Quality runId={path[1]}/>;else if(path[0]==="runs"&&path[1]&&path[2]==="patterns")page=<PatternDetail runId={path[1]} patternId={path[3]}/>;else if(path[0]==="runs"&&path[1])page=<RunDashboard runId={path[1]}/>;return <Shell>{page}</Shell>}
