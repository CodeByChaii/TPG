#!/bin/sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

. "$PROJECT_ROOT/.venv/bin/activate"

python bam_snapshot.py
python sniper_engine.py
