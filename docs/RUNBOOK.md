# Runbook

## 1. Unpack and use the existing `.env`

`Settings.load()` searches the current directory and parent directories for `.env`, so the project can be unpacked under an existing working folder that already contains `API_HUB_KEY` and `FREE_LLM_API`.

## 2. Install + offline validation

```bash
./scripts/bootstrap.sh
```

Equivalent manual steps:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest -q
smartwallet doctor
```

## 3. Authenticated smoke test

```bash
smartwallet doctor --live
```

This makes one documented Arkham entity-summary call and does not print the key.

## 4. Diagnostic end-to-end run

```bash
smartwallet run-all \
  --from 2026-01-01 \
  --max-wallets 5 \
  --max-pages 2 \
  --max-enrich-events 20 \
  --max-market-episodes 30
```

Inspect `data/smartwallet.db`, `data/raw/`, and `data/reports/`.

## 5. Full historical run

```bash
smartwallet run-all --from 2024-01-01
```

For maximum per-transaction deep enrichment instead of the practical high-value default:

```bash
smartwallet run-all --from 2024-01-01 --enrich-min-usd 0
```

On market-maker wallets this can create a very large number of free Hub reads. The full DeBank history itself is not capped unless `--max-pages` is supplied.

## 6. Re-run analysis without re-downloading history

```bash
smartwallet episodes
smartwallet classify --force
smartwallet label-market
smartwallet backtest --min-n 5
```

Raw Hub envelopes remain archived under `data/raw/` for auditability.
