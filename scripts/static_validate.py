from __future__ import annotations

import ast
from pathlib import Path

from smartwallet.contracts import CONTRACTS

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "smartwallet"
errors: list[str] = []
used: set[str] = set()
for path in (SRC / "providers").glob("*.py"):
    text = path.read_text(encoding="utf-8")
    if "/run/" in text:
        errors.append(f"direct /run/ literal in {path.relative_to(ROOT)}")
    tree = ast.parse(text, filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "call" and node.args:
            a = node.args[0]
            if isinstance(a, ast.Constant) and isinstance(a.value, str):
                used.add(a.value)
                if a.value not in CONTRACTS:
                    errors.append(f"unregistered endpoint key {a.value!r} in {path.relative_to(ROOT)}")
for key, c in CONTRACTS.items():
    if not c.docs_url.startswith("https://hub.arbitron.dev/providers/"):
        errors.append(f"contract {key} missing Hub docs URL")
if errors:
    raise SystemExit("\n".join(errors))
print(f"OK: {len(CONTRACTS)} documented Hub contracts; {len(used)} are referenced by provider code; no direct provider /run/ literals")
