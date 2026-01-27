#!/usr/bin/env bash
set -euo pipefail

# Optional convenience script for local development.
# Assumes you have an active virtualenv or are using a tool like uv/poetry.

export DTS_DB_PATH="${DTS_DB_PATH:-./var/tasks.db}"
export DTS_MAX_CONCURRENT="${DTS_MAX_CONCURRENT:-4}"
export DTS_SCHED_TICK_MS="${DTS_SCHED_TICK_MS:-200}"
export DTS_LEASE_MS="${DTS_LEASE_MS:-60000}"
export DTS_MAX_ATTEMPTS="${DTS_MAX_ATTEMPTS:-3}"
export DTS_HOST="${DTS_HOST:-127.0.0.1}"
export DTS_PORT="${DTS_PORT:-8000}"
export DTS_LOG_LEVEL="${DTS_LOG_LEVEL:-info}"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

python scripts/init_db.py

# Start API server (scheduler starts via FastAPI lifespan)
exec uvicorn dts.api.app:app --host "$DTS_HOST" --port "$DTS_PORT" --reload
