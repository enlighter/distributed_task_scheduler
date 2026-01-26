# src/dts/engine/recovery.py
from __future__ import annotations

import time

from dts.logging import get_logger
from dts.storage import SQLiteDB, TaskRepo

_LOG = get_logger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


def run_recovery(db: SQLiteDB, *, max_attempts: int) -> int:
    """
    Crash recovery:
    - Re-queue RUNNING tasks whose lease has expired (or mark FAILED if out of attempts)

    Returns number of tasks transitioned.
    """
    conn = db.connect()
    try:
        repo = TaskRepo(conn)
        transitioned = repo.recover_stale_running(now_ms(), max_attempts=max_attempts)
        if transitioned:
            _LOG.info("Recovery transitioned %d stale RUNNING task(s).", transitioned)
        return transitioned
    finally:
        conn.close()
