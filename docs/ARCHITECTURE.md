# Architecture

## Goal

Build a behavioral memory for each tracked institutional entity and each confirmed smart wallet rather than a naive whale-alert rule. The system separates:

1. **factual on-chain action evidence**;
2. **inferred economic intent**;
3. **future market outcome**.

Future market outcomes are never sent to the LLM classifier.

## Data flow

```text
Arkham identity + entity context
        │
        ├── entity / summary / balances
        ├── entity history / flow / volume / loans
        ├── Hypercore entity addresses + positions
        └── Solana entity subaccount ownerAddress seeds
        │
        ▼
Confirmed wallet set
        │
        ├──────── EVM ────────┐
        │                     │
        │                 DeBank
        │             full history/list
        │          + current DeFi positions
        │                     │
        │              conservative actions
        │                     │
        │       OKLink + Hub RPC verification
        │      OKLink address indexed context
        │        LI.FI/Rubic bridge evidence
        │        Stargate address bridge volume
        │       GeckoTerminal token-pool context
        │                     │
        └────── Solana ─── Jupiter raw wallet surfaces
                              │
                              ▼
                         Event warehouse
                              │
                              ▼
                          Episodes
              fixed time window + confirmed own-wallet links
                              │
                              ▼
                   FREE_LLM intent classifier
                              │
                              ▼
        DefiLlama pre-event regime + future price labels
                 + OKX live funding/open interest
                              │
                              ▼
                         Backtesting
         bootstrap CI + sign test + BH q-values
       empirical-Bayes shrinkage + walk-forward test
```

## Identity is not inferred from transaction adjacency

A wallet is included only when it comes from a confirmed Arkham surface used by discovery:

- entity top address;
- Arkham global search result whose `arkhamEntity.id` matches the target entity;
- Hypercore entity `addresses`;
- Solana entity subaccount `ownerAddress` values.

The project deliberately does **not** treat transaction counterparties as owned wallets. Real testing on a high-volume Wintermute market-maker address showed why this creates contamination.

## Deterministic actions vs intent

`normalize.py` intentionally uses conservative labels such as:

- `CEX_INTERACTION_OUT`
- `CEX_INTERACTION_IN`
- `DEFI_INTERACTION_BIDIRECTIONAL`
- `TRANSFER_OUT`
- `TRANSFER_IN`
- `ASSET_EXCHANGE_LIKE`

It never converts a CEX deposit directly into `SELL`. The LLM receives a sequence of factual evidence and outputs a probability distribution across intent labels.

## Episode graph

Events from one wallet are grouped inside a fixed maximum episode window. Continuous market-maker activity cannot create a multi-year component. Separate confirmed wallets of the same entity are merged only when an event explicitly transfers to/from another confirmed wallet and the matching wallet has activity inside the episode window.

## Leakage controls

- LLM classification happens before market labeling.
- LLM input contains only episode evidence and entity-history points at or before episode start.
- Market returns are written to a separate table after classification.
- Backtest split is chronological 70/30 for the simple direction validation.

## Statistical output

For each entity-scope and individual-wallet-scope `(pattern, intent, horizon, asset)` group with sufficient observations:

- N;
- mean / median return;
- standard deviation;
- positive / negative rate;
- bootstrap 95% confidence interval for the median;
- two-sided sign test;
- Benjamini-Hochberg q-value across tested groups;
- empirical-Bayes shrunk mean toward the global return mean;
- 70/30 chronological holdout mean and direction accuracy.

## Storage

The first-run implementation is intentionally single-host and reproducible:

- SQLite/WAL for normalized relational state;
- immutable gzip JSON raw archive for every Hub response;
- CSV/JSON/HTML reports.

This preserves the same logical separation as a production ClickHouse/PostgreSQL/object-storage deployment without requiring a distributed stack before the backtest is validated. Raw responses can be replayed into a larger warehouse later.

## Behavioral pattern hierarchy

An episode contributes to several nested hypotheses instead of only one exact long string:

- `ACTION:` — one factual action with CEX/project/category qualifier when present;
- `BIGRAM:` — consecutive two-action sequence;
- `TRIGRAM:` — consecutive three-action sequence;
- `ENDPOINT:` — first action → last action;
- `FULL:` — complete sequence when the episode has at most six events;
- `REGIME:<risk_on|neutral|risk_off>|...` — the same hypothesis conditioned on pre-event BTC regime.

The statistical estimator shrinks thin groups toward the matching `entity + intent + asset + horizon` prior. Wallet scope therefore inherits information from its parent entity rather than pretending five observations are a stable standalone distribution.

## Pre-event market regime

Only documented DefiLlama historical price calls are needed. BTC/ETH are sampled at `t-24h, t-18h, t-12h, t-6h, t`; the system stores 24h return and a 6h-step log-return volatility proxy. Future prices are requested only after LLM intent classification.
