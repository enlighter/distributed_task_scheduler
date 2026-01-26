# src/dts/engine/worker.py
from __future__ import annotations

import time
from dataclasses import dataclass

from dts.logging import get_logger
from dts.storage import SQLiteDB, TaskRepo

_LOG = get_logger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class TaskRun:
    task_id: str
    duration_ms: int


class Worker:
    """
    Executes a single task (simulated via sleep) and commits status transitions to SQLite.

    Design choice:
    - Each worker execution uses its own SQLite connection (thread-safe and avoids sharing
      connections across threads).
    """

    def __init__(self, db: SQLiteDB) -> None:
        self._db = db

    def run(self, job: TaskRun) -> None:
        start = now_ms()
        _LOG.info("Running task %s for %dms", job.task_id, job.duration_ms)

        try:
            time.sleep(job.duration_ms / 1000.0)
        except Exception as e:
            # If something weird happens during sleep/interruption, mark failed.
            self._mark_failed(job.task_id, f"Execution interrupted: {e!r}")
            raise

        # On success:
        self._mark_completed(job.task_id)
        end = now_ms()
        _LOG.info("Completed task %s in %dms", job.task_id, end - start)

    def _mark_completed(self, task_id: str) -> None:
        conn = self._db.connect()
        try:
            TaskRepo(conn).mark_completed(task_id, now_ms())
        finally:
            conn.close()

    def _mark_failed(self, task_id: str, error: str) -> None:
        conn = self._db.connect()
        try:
            TaskRepo(conn).mark_failed(task_id, now_ms(), error=error)
        finally:
            conn.close()
