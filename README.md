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

Development Р Р†Р вЂљРІР‚Сњ one command after install:

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
provider backfill Р Р†РІР‚В РІР‚в„ў normalized wallet_events
                         Р Р†РІР‚В РІР‚Сљ new rowid watermark per entity/run
              dirty time-window episode rebuild
                         Р Р†РІР‚В РІР‚Сљ only unclassified episodes
                 LLM intent classification
                         Р Р†РІР‚В РІР‚Сљ only incomplete episodes
                   future market labels
                         Р Р†РІР‚В РІР‚Сљ deduplicated run observations
        cheap incremental aggregates + sample checkpoints
                         Р Р†РІР‚В РІР‚Сљ periodically / meaningful Р С›РІР‚Сњn
              bootstrap, holdout and BH refresh
                         Р Р†РІР‚В РІР‚Сљ
                    SSE dashboard
```

Cheap statistics update after new observations by rebuilding only affected pattern groups. The system does not rescan the full multi-year dataset every 30 seconds. Expensive checks run when at least 10 observations or 20% sample growth has accumulated, during periodic refreshes and at final drain. Each observation has a run-scoped deterministic key, so resume/retry is idempotent.

Research data and API response cache may be reused across runs, but `analysis_observations`, aggregates, checkpoints, feed and dashboard results are scoped by `run_id`, entity set and date range. Old runs therefore remain attributable and inspectable.

## Statistical maturity

Maturity is a conservative display state, not a trading signal. The exact rules are implemented in `smartwallet.incremental.maturity_for`:

- **EARLY**: `n < 20`, regardless of effect size or p-value.
- **PROMISING**: `n >= 20`; enough for preliminary inspection, but robustness conditions are not met.
- **ESTABLISHING**: `n >= 50`, bootstrap 95% CI excludes zero, train and chronological holdout means have the same sign, and holdout directional accuracy is at least 55%.
- **ROBUST**: `n >= 100`, all establishing conditions hold, BenjaminiР Р†Р вЂљРІР‚СљHochberg `q <= 0.05`, and holdout directional accuracy is at least 60%.

Cards always show `n`. Advanced statistics include mean, median, bootstrap CI, sign-test p-value, BH q-value, train/holdout sizes, holdout accuracy and the shrinkage estimate. Р Р†Р вЂљРЎС™RobustР Р†Р вЂљРЎСљ means robust under these historical checks only; it is not investment advice and does not establish causality.

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


## Production terminal deployment

Ubuntu 24.04 prerequisites: Node.js (supported by the installed Vite version), npm,
Python 3.11+, and `python3-venv`. Install the Python venv package with apt, then:

```sh
npm ci
npm run build
# Set this in a private .env file (chmod 600), never in VITE_* variables:
# SMARTWALLET_AUTH_TOKEN=<a long random password>
# SMARTWALLET_PUBLIC_ORIGIN=https://terminal.example.com
npm start
# Or: pm2 start ecosystem.config.cjs
```

`npm ci` creates `.venv` and installs pip, setuptools, wheel and the editable Python
package there. `npm start`, `npm test`, and PM2 select the same venv. No system pip
or `--break-system-packages` is used. `node scripts/python.mjs --setup` refreshes it.
The listener defaults to `127.0.0.1:8000`. Requests without a configured
`SMARTWALLET_AUTH_TOKEN` are denied. Local-only development can explicitly set `SMARTWALLET_ALLOW_INSECURE_LOCAL=1`; do not use that override behind a proxy. Browser authentication uses HTTP Basic (any
username, configured secret as password); API clients may use a Bearer token.
The password is not embedded in JavaScript or stored in localStorage.

Terminate TLS at a reverse proxy, keep port 8000 closed in the VPS firewall, and
forward Authorization. Example nginx location inside an HTTPS server:

```nginx
location / {
    proxy_pass http://127.0.0.1:8000;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Authorization $http_authorization;
    proxy_buffering off;
    proxy_read_timeout 3600s;
}
```

## Provenance, retries and diagnostic checks

The additive migration in `src/smartwallet/migrations.py` creates `run_events`,
`run_episodes`, `market_horizons`, `classification_jobs`, `request_attempts`,
`wallet_collection_cache`, `assets`, and a migration ledger. Migration 2 backfills five explicit horizon states for existing episodes; existing successful labels become success, and context alone never does. Migration 3 adds a separate partial-wallet counter. Raw responses, prices,
events, episodes and existing classifications are preserved. Legacy entity/date
matches are not treated as proof of run membership. A new collection explicitly
enrolls normalized events; a proven complete wallet-range cache entry can enroll
existing events without HTTP. Raw GET responses have a one-hour cache; historical
DeBank cursor pages are reusable without expiration. Rebuilding episodes only
changes the current run's membership, preserving shared labels/classifications.
Legacy runs without provenance must be recollected/resumed to acquire membership;
the old global statistics cannot be certified as run-specific.

Request diagnostics record provider, endpoint class, attempts, terminal error and
next retry time. HTTP and envelope 429/5xx, transport errors and timeouts use bounded
backoff with jitter; non-transient 4xx stop immediately. `SMARTWALLET_HTTP_RETRIES`
(default 4, capped at 8), `SMARTWALLET_PROVIDER_RPS` (default 4 per provider), and
`SMARTWALLET_WALLET_TIMEOUT` (default 600 seconds) bound collection. Worker slots
advance independently; one failed wallet does not hold the next batch behind it.
LLM jobs have three pipeline attempts; price horizons preserve individual successes
and exhaust after three failed labeling passes. Future horizons remain pending
until their timestamps arrive. Completed collection is not completed analysis.

Cheap statistics run every five seconds in a worker thread, independently of slow LLM and market requests. Expensive tests refresh when n grows by
10 or 20%, with a full refresh at completion or via the Refresh statistics button (POST `/api/runs/{run_id}/analysis/refresh`). Active workers pick up manual requests at their next processing boundary. BH uses the complete run hypothesis family and saved p-values;
untested hypotheses conservatively participate with p=1. Sample filters default
to n>=3; pagination serves 24 patterns. Confidence-aware ranking uses sample
shrinkage, bounded effect, holdout and corrected significance.

```sh
npm test
npm run build
npm run test:ui  # on Linux first: npx playwright install chromium
node scripts/python.mjs scripts/diagnostic_smoke.py
```

The live smoke creates a fresh temporary database and writes only its summary to
`data/reports/production-smoke.json`. Regression tests simulate repeated 429/500,
network failures, permanent 400/404, recovery, exhaustion and independent progress.
A successful diagnostic wallet may legitimately have zero events in the requested
interval; the UI must retain zero counts in that case.
