# Distributed Task Scheduler (SQLite)

Lightweight persistent task orchestration engine with:
- REST API for submitting and observing tasks
- Dependency-aware scheduling (DAG)
- Concurrency limit
- Crash recovery via leases
- SQLite persistence (no external services)

## Architecture
- API: FastAPI
- Storage: SQLite (stdlib `sqlite3`)
- Scheduler: periodic claim loop using atomic DB transactions
- Workers: ThreadPoolExecutor (tasks simulate work via sleep)

## Task lifecycle
States:
- QUEUED: task exists but may be waiting on dependencies
- RUNNING: claimed by scheduler with a lease
- COMPLETED: finished successfully
- FAILED: failed after attempts
- BLOCKED: a dependency failed (optional, if enabled)

A task is runnable when:
- status=QUEUED AND remaining_deps=0
AND there is global concurrency capacity.

## Setup

### Requirements
- Python 3.13+

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
