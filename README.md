# Smart-wallet Intent Backtest

A complete first-run pipeline for the idea: **model what a specific institutional smart wallet appears to be doing, then test whether that exact behavior historically had predictive value**.

The system does not implement `wallet -> Binance = short`. It builds factual action sequences, keeps entity/wallet identity separate from counterparties, asks an LLM for a probability distribution over economic intent, and only afterwards joins future prices for backtesting.

## Sources used by the implementation

The Hub contract is based on `https://hub.arbitron.dev/llms.txt` and the per-route documentation under `https://hub.arbitron.dev/providers/...`. Every Hub call is allowlisted in `src/smartwallet/contracts.py`; undocumented query parameters are rejected before HTTP.

Core surfaces:

- Arkham: entity identity/summary/balances, history, flow, volume, loans, Hypercore, Solana entity subaccounts, search, recent swaps, transaction attribution.
- DeBank: `history/list`, `user/used_chains`, `portfolio/project_list`.
- OKLink: classified address transactions, token transfers, internal transactions, DeFi protocol list, transaction detail/logs.
- Hub RPC: documented safe read methods (`eth_getTransactionReceipt` etc.).
- LI.FI + Rubic: cross-chain status by source transaction hash.
- Stargate: address-level outgoing bridge volume over a date range.
- Jupiter Portfolio: Solana wallet activity/positions/transfers/trades.
- DefiLlama: historical point-in-time token prices.
- OKX Web3: current public funding rate / open interest plus documented historical candles client.
- GeckoTerminal: documented pool discovery and OHLCV client.

## Tracked entities

Default config:

- `wintermute`
- `jump-trading`
- `cumberland`
- `galaxy-digital`
- `amber`

Edit `config/entities.yaml` to change the universe.

## Install

```bash
cd backtest-str-smartwallet
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

Your existing `.env` can be placed in the project root. Required variables:

```dotenv
API_HUB_KEY=...
FREE_LLM_API=...
```

The requested LLM defaults are already configured:

```dotenv
FREE_LLM_BASE_URL=http://159.194.241.69:3001/v1
FREE_LLM_MODEL=llama-3.3-70b-versatile
```

If that gateway exposes the desired chat model under another name, override only `FREE_LLM_MODEL`; the client still uses the user-specified `/v1/chat/completions` surface.

## Verify before a full run

```bash
smartwallet doctor
smartwallet contracts > hub-contract.json
pytest -q
smartwallet doctor --live
```

`doctor --live` performs one documented Arkham entity-summary call. No secret is printed.

## Full pipeline

Two-year-plus example:

```bash
smartwallet run-all --from 2024-01-01
```

Stages executed in order:

1. discover confirmed Arkham wallet seeds;
2. save full entity context (`balances/history/flow/volume/loans/Hypercore`);
3. backfill every discovered EVM wallet through documented DeBank cursor pagination and collect Solana Jupiter surfaces;
4. snapshot indexed wallet context from OKLink (classified tx / token transfers / internals / DeFi protocols) and current top token pools from GeckoTerminal;
5. collect Stargate address bridge-volume context;
6. verify/enrich EVM transactions with Arkham tx + OKLink tx/logs + Hub RPC and check LI.FI/Rubic bridge status;
7. build bounded multi-wallet episodes and hierarchical action/bigram/trigram/endpoint patterns;
8. classify wallet roles and episode intent with `FREE_LLM_API`;
9. build pre-event BTC/ETH market regime from DefiLlama historical prices, then label 5m / 1h / 6h / 1d / 3d future returns and excess returns;
10. capture current OKX funding/OI context;
11. run hierarchical entity/action, entity/action/intent, entity/pattern/intent and wallet/pattern/intent/asset estimates with shrinkage priors, bootstrap CI, sign test, BH q-values and chronological holdout;
12. write CSV, JSON and HTML reports under `data/reports/`.

### Expensive/full vs diagnostic runs

The full command has no wallet/page cap. To test wiring first:

```bash
smartwallet run-all --from 2026-01-01 --max-wallets 5 --max-pages 2 --max-enrich-events 20 --max-market-episodes 30
```

Then remove the caps for the real backfill. For maximum deep transaction enrichment rather than the practical default, also pass `--enrich-min-usd 0`; this can multiply the number of Hub calls on high-frequency market-maker wallets.

The diagnostic cap is applied independently for each configured entity, so a run capped at five wallets still exercises every configured entity. Market price points are deduplicated and persisted in SQLite; normalized events and classifications are idempotent and can resume after interruption. Future labels with unavailable prices are not counted as successful labels.

`run-all` displays a low-overhead live stage dashboard and stage timings. For a reproducible profile, use a clean temporary database and compare emitted stage timings:

```bash
SMARTWALLET_DB=/tmp/smartwallet-diagnostic.db smartwallet run-all --from 2026-01-01 --max-wallets 5 --max-pages 2 --max-enrich-events 20 --max-market-episodes 30
```

This checkout has no credentials, so an authenticated before/after Hub benchmark cannot be honestly reported here. Request counts and stage timings are emitted when credentials are supplied.

## Individual stages

```bash
smartwallet discover
smartwallet snapshot-entities
smartwallet backfill --from 2024-01-01
smartwallet wallet-context
smartwallet enrich --min-usd 100000
smartwallet stargate-context --from 2024-01-01
smartwallet episodes
smartwallet classify
smartwallet label-market
smartwallet backtest --min-n 5
```

## Important design choices

### No whale-copy shortcut

A DeBank event with `cex_id` and outgoing tokens becomes factual `CEX_INTERACTION_OUT`, **not `SELL`**. Sell intent is one probability among several LLM outputs.

### No fake ownership graph

Counterparties are not promoted to owned wallets. Discovery uses only Arkham-confirmed surfaces. This avoids contaminating a market-maker entity with pools, routers and ordinary users.

### No future leakage

The LLM is run before market labeling. Historical entity context is selected only at or before episode start. Wallet profiles sent to historical episode classification contain only events strictly earlier than the episode. Pre-event market regime uses only `t-24h ... t`; future returns live in a separate table.

### Entity + individual-wallet behavior memory

Backtests are emitted at both scopes. A wallet-specific pattern estimate is shrinkage-regularized toward the same entity + intent + asset + horizon prior, so a thin wallet history cannot overpower the broader entity evidence.

### Hierarchical sequence patterns

Each episode emits broad and specific hypotheses: action unigrams, bigrams, trigrams, first→last endpoints and short full sequences. CEX / protocol IDs are retained in pattern tokens. This avoids the useless situation where every long market-maker episode is a unique exact string.

### Reproducibility

Every external Hub response is archived immutably as gzip JSON under `data/raw/<provider>/<date>/`, while normalized state is stored in SQLite/WAL. Re-running normalization/backtesting does not require silently changing the raw evidence.

See `docs/ARCHITECTURE.md`, `docs/HUB_CONTRACT.md` and `docs/LIMITATIONS.md`.
