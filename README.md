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

## What runs the server and the scheduler (wiring)

### 1) What runs the HTTP server?
**Uvicorn** runs the server process. FastAPI is the app framework, but it’s not the process runner.

`scripts/run_dev.sh` ends with:

```bash
exec uvicorn dts.api.app:app --host "$DTS_HOST" --port "$DTS_PORT" --reload
```

So

- `uvicorn` starts an ASGI server process.
- It imports `dts.api.app:app` (the FastAPI application object).
- It serves HTTP on the configured host and port.


You can also run it manually from the repository root:

```bash
uvicorn dts.api.app:app --reload
```

### 2) Where is the FastAPI app defined?

In `src/dts/api/app.py`:

```python
app = FastAPI(
title="Distributed Task Scheduler",
version="0.1.0",
lifespan=lifespan,
)
app.include_router(router)
```

The router is imported from `src/dts/api/routes.py` and defines endpoints such as:


- `/tasks`
- `/tasks/batch`


### 3) Who starts the scheduler?

The scheduler is started by FastAPI’s lifespan startup hook in `src/dts/api/app.py`.


Relevant part of the lifespan function:

```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    configure_logging(settings.log_level)

    db = SQLiteDB(settings.db_path)

    conn = db.connect()
    try:
        apply_migrations(conn, _migrations_dir())
    finally:
        conn.close()

    app.state.settings = settings
    app.state.db = db

    cfg = SchedulerConfig(
        max_concurrent_tasks=settings.max_concurrent_tasks,
        sched_tick_ms=settings.sched_tick_ms,
        lease_ms=settings.lease_ms,
        max_attempts=settings.max_attempts,
    )
    scheduler = Scheduler(db=db, cfg=cfg)
    scheduler.start()
    app.state.scheduler = scheduler

    try:
        yield
    finally:
        scheduler_obj = getattr(app.state, "scheduler", None)
        if scheduler_obj is not None:
            scheduler_obj.stop(timeout_s=5.0)
```

Lifecycle:


- Server starts → FastAPI lifespan enters → migrations run → scheduler starts
- Server shuts down → lifespan exits → scheduler stops

No separate process is starting the scheduler; it’s started inside the API app lifecycle.

### 4) What actually runs the scheduler loop?

Inside `src/dts/engine/scheduler.py`, `Scheduler.start()` creates a background thread.

```python
self._thread = threading.Thread(
    target=self._run_loop,
    name="dts-scheduler",
    daemon=True,
)
self._thread.start()
```

That thread runs `_run_loop()` continuously until stopped.


Inside `_run_loop()`, it ticks:

```python
while not self._stop.is_set():
    t0 = now_ms()

    if (t0 - last_recovery) >= self._cfg.recovery_interval_ms:
        run_recovery(self._db, max_attempts=self._cfg.max_attempts)
        last_recovery = t0

    self._claim_and_dispatch(repo, t0)

    elapsed = now_ms() - t0
    sleep_s = max(0.0, (self._cfg.sched_tick_ms - elapsed) / 1000.0)
    self._stop.wait(timeout=sleep_s)
```

So: the scheduler is a polling loop in a dedicated thread.

### 5) How does it execute tasks concurrently?

The scheduler uses a `ThreadPoolExecutor` sized to the configured concurrency:

```python
self._executor = ThreadPoolExecutor(
    max_workers=cfg.max_concurrent_tasks,
    thread_name_prefix="dts-worker",
)
```

When tasks are claimed, it dispatches them:

```python
for task_id, duration_ms in claimed:
    job = TaskRun(task_id=task_id, duration_ms=duration_ms)
    fut = self._executor.submit(self._worker.run, job)
```

Meaning:

- the scheduler thread decides what to run
- worker threads actually do the work

### 6) Who does the work of a task?

`src/dts/engine/worker.py` defines `Worker.run()`:

```python
time.sleep(job.duration_ms / 1000.0)
self._mark_completed(job.task_id)
```

It updates the DB using `TaskRepo`:

```python
TaskRepo(conn).mark_completed(task_id, now_ms())
```

So work = sleep, then commit state transitions.

### 7) How does the scheduler decide what to run?

`Scheduler._claim_and_dispatch()` uses DB truth:

```python
running = repo.count_running_leased(now)
slots = self._cfg.max_concurrent_tasks - running

claimed = repo.claim_runnable_tasks(now_ms=now, lease_ms=self._cfg.lease_ms, limit=limit)
```

The “atomic claim” is implemented in `src/dts/storage/repo.py`:


- Query runnable tasks: `status = 'QUEUED'` and `remaining_deps = 0`
- Update them to `RUNNING` with a lease inside a `BEGIN IMMEDIATE` transaction


That’s the core of concurrency safety.


## TL;DR (Wiring)


- **Uvicorn** runs the HTTP server: `scripts/run_dev.sh`
- **FastAPI** defines endpoints and lifecycle: `src/dts/api/app.py`
- **Scheduler** starts in the FastAPI lifespan: `scheduler.start()`
- **Scheduler** runs in its own thread: `threading.Thread(target=_run_loop)`
- **Task execution** runs in a `ThreadPoolExecutor` worker pool
- **Coordination** is handled via SQLite state and atomic claims in `TaskRepo`

## Setup

### Requirements
- Python 3.13+

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Configuration Defaults

Defaults are defined in `src/dts/config.py`:

- **DTS_DB_PATH** (default: `./var/tasks.db`)
- **DTS_MAX_CONCURRENT** (default: `3`)
- **DTS_SCHED_TICK_MS** (default: `200`)
- **DTS_LEASE_MS** (default: `60000`)
- **DTS_MAX_ATTEMPTS** (default: `3`)
- **DTS_HOST** (default: `127.0.0.1`)
- **DTS_PORT** (default: `8000`)
- **DTS_LOG_LEVEL** (default: `info`)

> **Note:** `.env.example` is a template only.  
> The application currently reads variables directly from the process environment (via `os.getenv`).

## Run (Dev)

From the repository root:

```bash
bash scripts/run_dev.sh
```

This script:

- Exports default environment variables if not already set
- Runs `scripts/init_db.py` (creates the database and applies migrations)
- Starts `uvicorn` with `--reload` (the scheduler starts via the FastAPI lifespan)

## API

- **Swagger UI:** http://127.0.0.1:8000/docs
- **Health Check:** `GET /healthz`

## API Usage
### Submit a single task

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "id": "task-1",
    "type": "demo",
    "duration_ms": 2000,
    "dependencies": []
  }'
```

### Submit a batch atomically (strict)

Batch endpoint: `POST /tasks/batch`

Rules:
- all IDs must be new
- dependencies must exist in DB or within the batch
- cycles inside the batch are rejected

```bash
curl -X POST http://127.0.0.1:8000/tasks/batch \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {"id":"A","type":"demo","duration_ms":2000,"dependencies":[]},
      {"id":"B","type":"demo","duration_ms":500,"dependencies":["A"]},
      {"id":"C","type":"demo","duration_ms":500,"dependencies":["B"]}
    ]
  }'
```

### Get task status

```bash
curl http://127.0.0.1:8000/tasks/task-1
```

### List tasks

```bash
curl http://127.0.0.1:8000/tasks
```

### Tests
From repo root:

```bash
pytest -q
```

## Persistence & Recovery Model


- State is persisted in SQLite (`tasks`, `deps`).
- When a task is claimed, it transitions to `RUNNING` and receives a lease
(`lease_expires_at = now + lease_ms`).
- Recovery runs periodically (and at startup) to:
- Re-queue tasks whose lease has expired, or
- Mark tasks as `FAILED` if `attempts >= max_attempts`.
