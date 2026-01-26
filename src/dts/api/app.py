# src/dts/api/app.py
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI

from dts.config import load_settings
from dts.engine.scheduler import Scheduler, SchedulerConfig
from dts.logging import configure_logging, get_logger
from dts.storage import SQLiteDB, apply_migrations

from .routes import router

_LOG = get_logger(__name__)


def _migrations_dir() -> Path:
    # Resolve migrations directory relative to repo root.
    # This works when running from project root.
    return Path("migrations")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Lifespan handler (preferred over deprecated @app.on_event).

    Responsible for:
    - loading settings
    - configuring logging
    - running DB migrations
    - starting scheduler
    - stopping scheduler on shutdown
    """
    settings = load_settings()
    configure_logging(settings.log_level)

    db = SQLiteDB(settings.db_path)

    # Run migrations once at startup (idempotent)
    conn = db.connect()
    try:
        apply_migrations(conn, _migrations_dir())
    finally:
        conn.close()

    # Store on app.state for DI
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

    _LOG.info("Startup complete.")

    try:
        yield
    finally:
        # Shutdown
        scheduler_obj = getattr(app.state, "scheduler", None)
        if scheduler_obj is not None:
            scheduler_obj.stop(timeout_s=5.0)
        _LOG.info("Shutdown complete.")


app = FastAPI(
    title="Distributed Task Scheduler",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(router)
