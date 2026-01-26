# src/dts/engine/scheduler.py
from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

from dts.logging import get_logger
from dts.storage import SQLiteDB, TaskRepo

from .recovery import run_recovery
from .worker import TaskRun, Worker

_LOG = get_logger(__name__)


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class SchedulerConfig:
    """
    Runtime config for the scheduler loop.
    """
    max_concurrent_tasks: int = 3
    sched_tick_ms: int = 200
    lease_ms: int = 60_000
    max_attempts: int = 3

    # How often to run recovery even while the process is alive (ms)
    recovery_interval_ms: int = 5_000

    # Max tasks to claim in a single DB transaction (upper bound)
    claim_batch_size: int = 50

    @property
    def sched_tick_s(self) -> float:
        return self.sched_tick_ms / 1000.0


class Scheduler:
    """
    Scheduler loop:
    - Periodically runs recovery
    - Computes capacity from DB (RUNNING with valid lease)
    - Atomically claims runnable tasks (QUEUED & remaining_deps==0) up to capacity
    - Submits them to a worker pool

    Concurrency semantics:
    - Global concurrency is enforced by DB truth: status=RUNNING with active lease.
    - In-process thread pool also caps actual concurrent execution.
    """

    def __init__(self, db: SQLiteDB, cfg: SchedulerConfig) -> None:
        if cfg.max_concurrent_tasks <= 0:
            raise ValueError("max_concurrent_tasks must be > 0")
        if cfg.sched_tick_ms <= 0:
            raise ValueError("sched_tick_ms must be > 0")

        self._db = db
        self._cfg = cfg

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Executor sized to configured concurrency.
        self._executor = ThreadPoolExecutor(
            max_workers=cfg.max_concurrent_tasks,
            thread_name_prefix="dts-worker",
        )

        self._worker = Worker(db)

    def start(self) -> None:
        """
        Starts the scheduler background thread.
        Safe to call once.
        """
        if self._thread and self._thread.is_alive():
            return

        _LOG.info(
            "Starting scheduler: max_concurrent=%d tick_ms=%d lease_ms=%d",
            self._cfg.max_concurrent_tasks,
            self._cfg.sched_tick_ms,
            self._cfg.lease_ms,
        )
        self._stop.clear()

        # Run an initial recovery pass before scheduling new work.
        run_recovery(self._db, max_attempts=self._cfg.max_attempts)

        self._thread = threading.Thread(target=self._run_loop, name="dts-scheduler", daemon=True)
        self._thread.start()

    def stop(self, *, timeout_s: float = 5.0) -> None:
        """
        Stops the scheduler loop and shuts down the worker pool.

        Note: in-flight tasks continue to run unless you cancel futures.
        For this challenge, we let them finish.
        """
        _LOG.info("Stopping scheduler...")
        self._stop.set()

        if self._thread:
            self._thread.join(timeout=timeout_s)

        self._executor.shutdown(wait=False, cancel_futures=False)
        _LOG.info("Scheduler stopped.")

    def _run_loop(self) -> None:
        last_recovery = 0

        # Dedicated connection for the scheduler loop thread.
        conn = self._db.connect()
        repo = TaskRepo(conn)

        try:
            while not self._stop.is_set():
                t0 = now_ms()

                # Periodic recovery (covers cases where tasks run longer than lease,
                # or if external kill happened and we restarted without full process restart).
                if (t0 - last_recovery) >= self._cfg.recovery_interval_ms:
                    # recovery uses a separate connection intentionally
                    try:
                        run_recovery(self._db, max_attempts=self._cfg.max_attempts)
                    except Exception:
                        _LOG.exception("Recovery pass failed (continuing).")
                    last_recovery = t0

                try:
                    self._claim_and_dispatch(repo, t0)
                except Exception:
                    _LOG.exception("Scheduler iteration failed (continuing).")

                # Tick
                elapsed = now_ms() - t0
                sleep_s = max(0.0, (self._cfg.sched_tick_ms - elapsed) / 1000.0)
                if sleep_s:
                    self._stop.wait(timeout=sleep_s)

        finally:
            conn.close()

    def _claim_and_dispatch(self, repo: TaskRepo, now: int) -> None:
        # Capacity derived from DB truth (important for crash recovery correctness).
        running = repo.count_running_leased(now)
        slots = self._cfg.max_concurrent_tasks - running
        if slots <= 0:
            return

        # Claim tasks up to slots (bounded by batch size).
        limit = min(slots, self._cfg.claim_batch_size)
        claimed = repo.claim_runnable_tasks(now_ms=now, lease_ms=self._cfg.lease_ms, limit=limit)
        if not claimed:
            return

        for task_id, duration_ms in claimed:
            job = TaskRun(task_id=task_id, duration_ms=duration_ms)
            fut = self._executor.submit(self._worker.run, job)
            fut.add_done_callback(self._on_job_done(task_id))

        _LOG.info("Claimed %d task(s); running=%d slots=%d", len(claimed), running, slots)

    def _on_job_done(self, task_id: str):
        def _cb(fut: Future[None]) -> None:
            try:
                fut.result()
            except Exception as e:
                # Worker.run already attempted to mark FAILED, but we log here too.
                _LOG.exception("Task %s execution raised: %r", task_id, e)

        return _cb
