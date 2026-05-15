#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Ensure local calls bypass any global proxy envs
export NO_PROXY="127.0.0.1,localhost"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate

pip -q install -r requirements.txt

PORT="${PORT:-8000}"
HOST="${HOST:-127.0.0.1}"

exec uvicorn app.main:app --host "$HOST" --port "$PORT"
