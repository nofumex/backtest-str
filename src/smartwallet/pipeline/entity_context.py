from __future__ import annotations

import json
from typing import Any

from ..storage import Storage


def _parse_ts(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        return int(value / 1000) if value > 10_000_000_000 else int(value)
    if isinstance(value, str):
        from datetime import datetime
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            try:
                n = float(value)
                return int(n / 1000) if n > 10_000_000_000 else int(n)
            except ValueError:
                return None
    return None


def _nearest_at_or_before(rows: list[Any], ts: int) -> dict[str, Any] | None:
    best = None
    best_ts = -1
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = _parse_ts(row.get("time"))
        if t is not None and t <= ts and t > best_ts:
            best, best_ts = row, t
    return best


def historical_entity_context(storage: Storage, entity_id: str, chain: str, ts: int) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for kind in ("history", "flow", "volume"):
        row = storage.fetchone(
            "SELECT payload_json FROM entity_snapshots WHERE entity_id=? AND source='arkham' AND kind=? ORDER BY captured_at DESC LIMIT 1",
            (entity_id, kind),
        )
        if not row:
            continue
        payload = json.loads(row["payload_json"])
        chain_rows = payload.get(chain) if isinstance(payload, dict) else None
        if isinstance(chain_rows, list):
            point = _nearest_at_or_before(chain_rows, ts)
            if point:
                out[kind] = point
    return out



def historical_wallet_context(storage: Storage, entity_id: str, wallet: str, chain: str, ts: int) -> dict[str, Any]:
    """Point-in-time Arkham wallet context from time-series snapshots only.

    Current balances/loans are deliberately excluded from historical episodes because their
    capture time may be after the episode and would create leakage.
    """
    out: dict[str, Any] = {}
    for source, key in (("arkham.address_history", "history"), ("arkham.address_flow", "flow")):
        row = storage.fetchone(
            """SELECT payload_json FROM wallet_positions
               WHERE entity_id=? AND address=? AND source=? ORDER BY captured_at DESC LIMIT 1""",
            (entity_id, wallet.lower(), source),
        )
        if not row:
            continue
        payload = json.loads(row["payload_json"])
        chain_rows = payload.get(chain) if isinstance(payload, dict) else None
        if isinstance(chain_rows, list):
            point = _nearest_at_or_before(chain_rows, ts)
            if point:
                out[key] = point
    return out
