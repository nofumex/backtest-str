from pathlib import Path


def test_provider_modules_do_not_hardcode_run_routes():
    root = Path(__file__).parents[1] / "src" / "smartwallet" / "providers"
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "/run/" not in text, f"Direct Hub execution path found in {path}; use contracts.py"
