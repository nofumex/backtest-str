import ast
from pathlib import Path

from smartwallet.contracts import CONTRACTS


def test_every_static_provider_call_key_exists_in_registry():
    root = Path(__file__).parents[1] / "src" / "smartwallet" / "providers"
    missing = []
    used = set()
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or node.func.attr != "call":
                continue
            if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
                continue
            key = node.args[0].value
            used.add(key)
            if key not in CONTRACTS:
                missing.append((path.name, key))
    assert not missing
    assert used
