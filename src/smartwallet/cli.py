from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer

from .config import load_entities
from .contracts import CONTRACTS
from .pipeline.backtest import run_backtest, write_report
from .pipeline.run import (
    backfill_all, build_all_episodes, classify_all, discover_all, enrich_events,
    label_all_markets, run_all, snapshot_all, snapshot_wallet_contexts, stargate_context,
)
from .runtime import Runtime
from .settings import Settings

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Entity-specific smart-wallet intent backtesting")


def _run(coro):
    return asyncio.run(coro)


async def _with_runtime(fn):
    settings = Settings.load()
    rt = Runtime.create(settings)
    try:
        return await fn(rt)
    finally:
        await rt.aclose()


def _print(value) -> None:
    typer.echo(json.dumps(value, ensure_ascii=False, indent=2, default=str))


@app.command()
def doctor(live: bool = typer.Option(False, help="Also execute one documented Arkham entity-summary request")):
    """Validate environment, config and the static Hub contract registry."""
    settings = Settings.load()
    entities = load_entities()
    info = {
        "api_hub_key_present": bool(settings.api_hub_key),
        "free_llm_api_present": bool(settings.llm_api_key),
        "llm_base_url": settings.llm_base_url,
        "llm_model": settings.llm_model,
        "entities": entities,
        "registered_hub_contracts": len(CONTRACTS),
    }
    if live:
        async def f(rt):
            return await rt.arkham.entity_summary(entities[0]["id"])
        info["live_arkham_summary"] = _run(_with_runtime(f))
    _print(info)


@app.command()
def contracts():
    """Print every Hub endpoint the project is allowed to call and its source documentation."""
    _print({k: {"method": v.method, "provider": v.provider, "path": v.path, "query_params": sorted(v.query_params), "docs": v.docs_url} for k, v in CONTRACTS.items()})


@app.command()
def discover():
    _print(_run(_with_runtime(lambda rt: discover_all(rt))))


@app.command("snapshot-entities")
def snapshot_entities():
    _print(_run(_with_runtime(lambda rt: snapshot_all(rt))))


@app.command()
def backfill(
    from_date: str = typer.Option("2024-01-01", "--from", help="UTC start date, ISO-8601"),
    max_wallets: Optional[int] = typer.Option(None),
    max_pages: Optional[int] = typer.Option(None, help="Diagnostic cap; omit for full DeBank pagination"),
):
    _print(_run(_with_runtime(lambda rt: backfill_all(rt, from_date=from_date, max_wallets=max_wallets, max_pages=max_pages))))


@app.command("wallet-context")
def wallet_context(max_wallets: Optional[int] = typer.Option(None), max_tokens: int = typer.Option(5)):
    _print(_run(_with_runtime(lambda rt: snapshot_wallet_contexts(rt, max_wallets=max_wallets, max_tokens=max_tokens))))


@app.command()
def enrich(min_usd: float = typer.Option(100000.0), max_events: Optional[int] = typer.Option(None)):
    _print(_run(_with_runtime(lambda rt: enrich_events(rt, min_usd=min_usd, max_events=max_events))))


@app.command("stargate-context")
def stargate_context_cmd(from_date: str = typer.Option("2024-01-01", "--from"), max_wallets: Optional[int] = typer.Option(None)):
    _print({"wallets": _run(_with_runtime(lambda rt: stargate_context(rt, from_date=from_date, max_wallets=max_wallets)))})


@app.command()
def episodes():
    _print(_run(_with_runtime(lambda rt: build_all_episodes(rt))))


@app.command()
def classify(force: bool = typer.Option(False), roles: bool = typer.Option(True)):
    _print(_run(_with_runtime(lambda rt: classify_all(rt, classify_roles=roles, force=force))))


@app.command("label-market")
def label_market(max_episodes: Optional[int] = typer.Option(None)):
    _print({"labels": _run(_with_runtime(lambda rt: label_all_markets(rt, max_episodes=max_episodes)))})


@app.command()
def backtest(min_n: int = typer.Option(5)):
    settings = Settings.load()
    rt = Runtime.create(settings)
    try:
        run_id, frame = run_backtest(rt.storage, min_n=min_n)
        _print({"run_id": run_id, "rows": len(frame), "reports": write_report(rt.storage, run_id, frame)})
    finally:
        _run(rt.aclose())


@app.command("run-all")
def run_all_cmd(
    from_date: str = typer.Option("2024-01-01", "--from"),
    max_wallets: Optional[int] = typer.Option(None),
    max_pages: Optional[int] = typer.Option(None),
    enrich_min_usd: float = typer.Option(100000.0),
    max_enrich_events: Optional[int] = typer.Option(None),
    max_market_episodes: Optional[int] = typer.Option(None),
    min_backtest_n: int = typer.Option(5),
):
    async def f(rt):
        return await run_all(
            rt,
            from_date=from_date,
            max_wallets=max_wallets,
            max_pages=max_pages,
            enrich_min_usd=enrich_min_usd,
            max_enrich_events=max_enrich_events,
            max_market_episodes=max_market_episodes,
            min_backtest_n=min_backtest_n,
        )
    _print(_run(_with_runtime(f)))


if __name__ == "__main__":
    app()
