#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
smartwallet doctor
pytest -q
printf '\nBootstrap complete. For a live credential check: smartwallet doctor --live\n'
