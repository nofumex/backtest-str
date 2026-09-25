from __future__ import annotations

import json
import math
import uuid
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import binomtest

from ..storage import Storage, utc_now_iso


def bootstrap_ci(values: np.ndarray, *, samples: int = 2000, seed: int = 42) -> tuple[float, float]:
    if len(values) == 0:
        return math.nan, math.nan
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(samples, len(values)), replace=True)
    med = np.median(draws, axis=1)
    return float(np.quantile(med, 0.025)), float(np.quantile(med, 0.975))


def bh_qvalues(pvalues: list[float]) -> list[float]:
    n = len(pvalues)
    if n == 0:
        return []
    order = np.argsort(np.asarray(pvalues))
    q = np.ones(n)
    running = 1.0
    for rank_rev, idx in enumerate(order[::-1], start=1):
        rank = n - rank_rev + 1
        candidate = min(1.0, pvalues[idx] * n / rank)
        running = min(running, candidate)
        q[idx] = running
    return q.tolist()


def _patterns(evidence_json: str, fallback_motif: str) -> list[str]:
    try:
        evidence = json.loads(evidence_json)
    except Exception:
        evidence = {}
    values = evidence.get("patterns") if isinstance(evidence, dict) else None
    if isinstance(values, list):
        out = sorted({str(v) for v in values if v})
        if out:
            return out
    return ["FULL:" + fallback_motif]


def run_backtest(storage: Storage, *, min_n: int = 5, shrinkage_strength: float = 20.0) -> tuple[str, pd.DataFrame]:
    rows = storage.fetchall(
        """SELECT e.episode_id,e.entity_id,e.start_ts,e.motif,e.evidence_json,e.wallets_json,
                  COALESCE(e.intent_label,'unknown') intent_label,
                  m.asset_key,m.horizon_seconds,m.simple_return,m.excess_vs_btc,m.excess_vs_eth,
                  COALESCE(c.regime,'unknown') regime
           FROM episodes e JOIN market_labels m ON m.episode_id=e.episode_id
           LEFT JOIN episode_market_context c ON c.episode_id=e.episode_id
           WHERE m.simple_return IS NOT NULL ORDER BY e.start_ts"""
    )
    if not rows:
        return str(uuid.uuid4()), pd.DataFrame()

    raw_df = pd.DataFrame(rows)
    # Priors are computed once per episode, before wallet-scope expansion, so a multi-wallet
    # episode cannot accidentally receive extra prior weight.
    prior_cols = ["entity_id", "intent_label", "horizon_seconds", "asset_key"]
    prior_means = raw_df.groupby(prior_cols, dropna=False)["simple_return"].mean().to_dict()
    global_mean = float(raw_df["simple_return"].mean())

    entity_pattern_rows: list[dict[str, Any]] = []
    wallet_pattern_rows: list[dict[str, Any]] = []
    broad_rows: list[dict[str, Any]] = []
    for row in rows:
        try:
            wallets = sorted({str(x) for x in json.loads(row["wallets_json"]) if x})
        except Exception:
            wallets = []
        # Broad hypotheses are one row per episode/asset/horizon. Never expand them by pattern.
        action = str(row["motif"]).split(">", 1)[0] or "unknown"
        broad_rows.append({**row, "scope_type": "entity", "scope_id": row["entity_id"], "action": action, "pattern": action})
        pattern_values: list[str] = []
        for pattern in _patterns(row["evidence_json"], row["motif"]):
            pattern_values.append(pattern)
            if row.get("regime") and row["regime"] != "unknown":
                pattern_values.append(f"REGIME:{row['regime']}|{pattern}")
        for pattern in sorted(set(pattern_values)):
            entity_pattern_rows.append({**row, "scope_type": "entity", "scope_id": row["entity_id"], "pattern": pattern})
            for wallet in wallets:
                wallet_pattern_rows.append({**row, "scope_type": "wallet", "scope_id": wallet, "pattern": pattern})
    broad_df = pd.DataFrame(broad_rows)
    entity_pattern_df = pd.DataFrame(entity_pattern_rows).drop_duplicates(subset=["episode_id", "asset_key", "horizon_seconds", "pattern"])
    wallet_pattern_df = pd.DataFrame(wallet_pattern_rows).drop_duplicates(subset=["episode_id", "asset_key", "horizon_seconds", "scope_id", "pattern"])
    has_patterns = any(
        isinstance((json.loads(r["evidence_json"]) if r["evidence_json"] else {}).get("patterns"), list)
        for r in rows
    )

    # Four nested hypotheses: broad entity/action, then intent, pattern, and wallet/asset.
    # The level is explicit so multiple observations do not create accidental duplicate tests.
    group_specs = [
        ("entity_action", broad_df, ["entity_id", "action", "intent_label", "horizon_seconds", "asset_key"]),
        ("wallet_pattern_intent_asset", wallet_pattern_df, ["entity_id", "scope_id", "pattern", "intent_label", "horizon_seconds", "asset_key"]),
    ]
    if has_patterns:
        group_specs.insert(1, ("entity_action_intent", broad_df, ["entity_id", "action", "intent_label", "horizon_seconds", "asset_key"]))
        group_specs.insert(2, ("entity_pattern_intent", entity_pattern_df, ["entity_id", "pattern", "intent_label", "horizon_seconds", "asset_key"]))
    results: list[dict[str, Any]] = []
    for level, source_df, group_cols in group_specs:
      if source_df.empty:
        continue
      for keys, g in source_df.groupby(group_cols, dropna=False):
        if len(g) < min_n:
            continue
        vals = g["simple_return"].astype(float).to_numpy()
        ci_low, ci_high = bootstrap_ci(vals)
        neg = int(np.sum(vals < 0))
        pos = int(np.sum(vals > 0))
        effective = neg + pos
        p = float(binomtest(min(neg, pos), n=effective, p=0.5, alternative="two-sided").pvalue) if effective else 1.0
        split = max(1, int(len(g) * 0.7))
        train = vals[:split]
        test = vals[split:]
        direction = -1.0 if float(np.mean(train)) < 0 else 1.0
        test_acc = float(np.mean(np.sign(test) == direction)) if len(test) else math.nan
        key_map = dict(zip(group_cols, keys))
        key_map.setdefault("scope_id", key_map.get("entity_id"))
        key_map.setdefault("pattern", "ACTION:any")
        key_map.setdefault("motif", key_map.get("action", "ACTION:any"))
        key_map.setdefault("intent_label", "unknown")
        if "asset_key" not in key_map:
            # An estimate must never silently mix assets or choose the first asset.
            if g["asset_key"].nunique(dropna=False) != 1:
                continue
            key_map["asset_key"] = str(g["asset_key"].iloc[0])
        key_map["scope_type"] = "wallet" if level.startswith("wallet") else "entity"
        key_map["level"] = level
        prior_key = (key_map["entity_id"], key_map["intent_label"], key_map["horizon_seconds"], key_map["asset_key"])
        prior_mean = float(prior_means.get(prior_key, global_mean))
        shrunk = (len(vals) * float(np.mean(vals)) + shrinkage_strength * prior_mean) / (len(vals) + shrinkage_strength)
        results.append({
            **key_map,
            "motif": f"{level}:{key_map.get('pattern', key_map.get('action', key_map['motif']))}",
            "n": len(vals),
            "mean_return": float(np.mean(vals)),
            "median_return": float(np.median(vals)),
            "std_return": float(np.std(vals, ddof=1)) if len(vals) > 1 else 0.0,
            "negative_rate": neg / len(vals),
            "positive_rate": pos / len(vals),
            "ci_low": ci_low,
            "ci_high": ci_high,
            "sign_pvalue": p,
            "prior_mean": prior_mean,
            "shrunk_mean": shrunk,
            "train_n": len(train),
            "test_n": len(test),
            "test_mean_return": float(np.mean(test)) if len(test) else math.nan,
            "test_direction_accuracy": test_acc,
            "hierarchy_level": level,
        })
      
    out = pd.DataFrame(results)
    if out.empty:
        return str(uuid.uuid4()), out
    out["qvalue"] = bh_qvalues(out["sign_pvalue"].astype(float).tolist())
    run_id = str(uuid.uuid4())
    created = utc_now_iso()
    with storage.conn() as db:
        for r in out.to_dict("records"):
            db.execute(
                """INSERT OR REPLACE INTO backtest_results(run_id,entity_id,scope_type,scope_id,motif,intent_label,horizon_seconds,asset_key,n,mean_return,median_return,std_return,
                   negative_rate,positive_rate,ci_low,ci_high,sign_pvalue,qvalue,shrunk_mean,train_n,test_n,test_mean_return,test_direction_accuracy,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, r["entity_id"], r["scope_type"], r["scope_id"], r["motif"], r["intent_label"], int(r["horizon_seconds"]), r["asset_key"], int(r["n"]), r["mean_return"], r["median_return"], r["std_return"], r["negative_rate"], r["positive_rate"], r["ci_low"], r["ci_high"], r["sign_pvalue"], r["qvalue"], r["shrunk_mean"], int(r["train_n"]), int(r["test_n"]), r["test_mean_return"], r["test_direction_accuracy"], created),
            )
    return run_id, out


def write_report(storage: Storage, run_id: str, frame: pd.DataFrame) -> dict[str, str]:
    storage.settings.report_dir.mkdir(parents=True, exist_ok=True)
    csv_path = storage.settings.report_dir / f"backtest-{run_id}.csv"
    json_path = storage.settings.report_dir / f"backtest-{run_id}.json"
    html_path = storage.settings.report_dir / f"backtest-{run_id}.html"
    frame.to_csv(csv_path, index=False)
    json_path.write_text(frame.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    display = frame.copy()
    if not display.empty:
        display = display.sort_values(["n", "qvalue"], ascending=[False, True])
    html = "<html><head><meta charset='utf-8'><title>Smart-wallet backtest</title></head><body>"
    html += f"<h1>Smart-wallet behavior backtest</h1><p>run_id={run_id}</p>"
    html += "<p>Rows are conditional historical estimates, not trading recommendations. qvalue is Benjamini-Hochberg adjusted; shrunk_mean uses an entity+intent+asset+horizon prior.</p>"
    html += display.to_html(index=False, float_format=lambda x: f"{x:.6f}") if not display.empty else "<p>No groups met min_n.</p>"
    html += "</body></html>"
    html_path.write_text(html, encoding="utf-8")
    return {"csv": str(csv_path), "json": str(json_path), "html": str(html_path)}
