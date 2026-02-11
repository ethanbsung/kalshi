#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="$ROOT_DIR/src:$PYTHONPATH"
else
  export PYTHONPATH="$ROOT_DIR/src"
fi

exec "$PYTHON_BIN" "$ROOT_DIR/scripts/run_live_stack.py" \
  --status active \
  --quote-source ws \
  --edge-interval-seconds 10 \
  --opportunity-interval-seconds 10 \
  --scoring-every-minutes 60 \
  --report-every-minutes 60 \
  "$@"
