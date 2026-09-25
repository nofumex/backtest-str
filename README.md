# Smartwallet Research Terminal

Live, restart-safe research and backtesting for institutional smart wallets. The browser controls historical collection, episode construction, LLM intent classification, market labeling and incremental statistical analysis.

## Run it

Requirements: Python 3.11+, Node.js 20+ and the credentials below in `.env`:

```dotenv
API_HUB_KEY=...
FREE_LLM_API=...
FREE_LLM_BASE_URL=http://159.194.241.69:3001/v1
FREE_LLM_MODEL=llama-3.3-70b-versatile
```

Development — one command after install:

```bash
npm install
npm run dev
```

Open **http://localhost:5173**. Vite proxies the API to the Python process; both processes are managed by the single npm command.

Production:

```bash
npm install
npm run build
pm2 start ecosystem.config.cjs --only backtest-str
```

Open **http://SERVER_IP:8000**. FastAPI serves the built frontend and owns all background work. Set `PORT` to use another port.

## What the terminal does

- creates durable analysis runs with explicit entity and date scope;
- starts, pauses, resumes and safely stops work at wallet/batch boundaries;
- marks interrupted work as paused after a process/VPS restart so it can be resumed;
- shows real wallet, event, episode, classification, label, API, queue and database counters;
- streams a dashboard snapshot through SSE every two seconds without coupling logs to the pipeline;
- shows entity-level coverage, data-quality gaps, errors and run history;
- builds pattern cards from real completed observations and exposes their source episodes;
- stores sample-size checkpoints and a discovery feed as results evolve;
- keeps the existing developer CLI available (`smartwallet --help`).

## Incremental architecture

The application remains a single FastAPI process with asyncio background tasks. Existing provider, normalizer, episode, LLM and market-labeling functions are reused; the web layer does not implement a second research pipeline.

SQLite stays in WAL mode. Web/run tables live beside the existing research tables:

```text
provider backfill → normalized wallet_events
                         ↓ new rowid watermark per entity/run
              dirty time-window episode rebuild
                         ↓ only unclassified episodes
                 LLM intent classification
                         ↓ only incomplete episodes
                   future market labels
                         ↓ deduplicated run observations
        cheap incremental aggregates + sample checkpoints
                         ↓ periodically / meaningful Δn
              bootstrap, holdout and BH refresh
                         ↓
                    SSE dashboard
```

Cheap statistics update after new observations by rebuilding only affected pattern groups. The system does not rescan the full multi-year dataset every 30 seconds. Expensive checks run when at least 10 observations or 20% sample growth has accumulated, during periodic refreshes and at final drain. Each observation has a run-scoped deterministic key, so resume/retry is idempotent.

Research data and API response cache may be reused across runs, but `analysis_observations`, aggregates, checkpoints, feed and dashboard results are scoped by `run_id`, entity set and date range. Old runs therefore remain attributable and inspectable.

## Statistical maturity

Maturity is a conservative display state, not a trading signal. The exact rules are implemented in `smartwallet.incremental.maturity_for`:

- **EARLY**: `n < 20`, regardless of effect size or p-value.
- **PROMISING**: `n >= 20`; enough for preliminary inspection, but robustness conditions are not met.
- **ESTABLISHING**: `n >= 50`, bootstrap 95% CI excludes zero, train and chronological holdout means have the same sign, and holdout directional accuracy is at least 55%.
- **ROBUST**: `n >= 100`, all establishing conditions hold, Benjamini–Hochberg `q <= 0.05`, and holdout directional accuracy is at least 60%.

Cards always show `n`. Advanced statistics include mean, median, bootstrap CI, sign-test p-value, BH q-value, train/holdout sizes, holdout accuracy and the shrinkage estimate. “Robust” means robust under these historical checks only; it is not investment advice and does not establish causality.

## Data and operations

Default paths:

- SQLite: `data/smartwallet.db`
- immutable raw responses: `data/raw/`
- legacy CLI reports: `data/reports/`

Useful environment overrides include `SMARTWALLET_DB`, `SMARTWALLET_RAW_DIR`, `SMARTWALLET_REPORT_DIR`, `SMARTWALLET_CONCURRENCY`, `SMARTWALLET_HTTP_TIMEOUT` and `PORT`.

Run automated checks with:

```bash
npm test
npm run build
```

Authenticated throughput and full-history duration depend on provider limits, selected entities, cache warmth and VPS resources. They cannot be benchmarked honestly without spending the configured API/LLM credentials; the live terminal reports the actual rates for each run.

More details: [architecture](docs/ARCHITECTURE.md), [provider contract](docs/HUB_CONTRACT.md), [limitations](docs/LIMITATIONS.md), and [validation](docs/VALIDATION.md).
