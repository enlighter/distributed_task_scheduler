# tests/test_scheduler_claim.py
import time
from pathlib import Path

from dts.domain.models import TaskCreate
from dts.storage import SQLiteDB, TaskRepo, apply_migrations


def now_ms() -> int:
    return int(time.time() * 1000)


def test_claim_runnable_tasks_is_batched_and_atomic(tmp_path: Path):
    db_path = tmp_path / "tasks.db"
    db = SQLiteDB(db_path)

    # Init schema
    conn = db.connect()
    try:
        apply_migrations(conn, Path("migrations"))
    finally:
        conn.close()

    # Insert 5 runnable tasks (no deps)
    conn = db.connect()
    try:
        repo = TaskRepo(conn)
        t = now_ms()
        for i in range(5):
            repo.create_task(
                TaskCreate(id=f"task-{i}", type="x", duration_ms=1000, dependencies=[]),
                now_ms=t + i,
                default_max_attempts=3,
            )
    finally:
        conn.close()

    # Claim in two rounds
    conn = db.connect()
    try:
        repo = TaskRepo(conn)
        claimed1 = repo.claim_runnable_tasks(now_ms=now_ms(), lease_ms=10_000, limit=3)
        assert len(claimed1) == 3

        claimed2 = repo.claim_runnable_tasks(now_ms=now_ms(), lease_ms=10_000, limit=3)
        assert len(claimed2) == 2

        claimed3 = repo.claim_runnable_tasks(now_ms=now_ms(), lease_ms=10_000, limit=3)
        assert len(claimed3) == 0

        # Ensure those tasks are marked RUNNING
        running = repo.count_running_leased(now_ms())
        assert running == 5
    finally:
        conn.close()
