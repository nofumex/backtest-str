from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_entities(path: str | Path = "config/entities.yaml") -> list[dict[str, str]]:
    payload: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    rows = payload.get("entities") or []
    out = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("id"):
            continue
        out.append({"id": str(row["id"]), "name": str(row.get("name") or row["id"])})
    if not out:
        raise RuntimeError(f"No entities configured in {path}")
    return out
