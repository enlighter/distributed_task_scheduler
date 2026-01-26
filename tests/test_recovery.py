# tests/test_recovery.py
import time
from pathlib import Path

from fastapi.testclient import TestClient

from dts.storage import SQLiteDB, apply_migrations


def _wait_for_completed(client: TestClient, task_id: str, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        r = client.get(f"/tasks/{task_id}")
        if r.status_code == 200 and r.json()["status"] == "COMPLETED":
            return True
        time.sleep(0.05)
    return False


def test_recovery_requeues_stale_running(tmp_path: Path, client_factory):
    db_path = tmp_path / "tasks.db"
    db = SQLiteDB(db_path)

    # Prepare DB with stale RUNNING task
    conn = db.connect()
    try:
        apply_migrations(conn, Path("migrations"))
        now = int(time.time() * 1000)

        conn.execute(
            """
            INSERT INTO tasks(
              id, type, duration_ms, status, remaining_deps,
              attempts, max_attempts, created_at, updated_at,
              started_at, lease_expires_at, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "stale-task",
                "x",
                50,
                "RUNNING",
                0,
                0,
                3,
                now,
                now,
                now,
                now - 1_000,  # expired lease
                "Simulated crash: stale RUNNING",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    # Start app pointing at this DB; recovery should requeue and execute it
    with client_factory(db_path=db_path, overrides={"DTS_MAX_CONCURRENT": "1"}) as client:
        assert _wait_for_completed(client, "stale-task", timeout_s=3.0), "stale-task did not complete"

        t = client.get("/tasks/stale-task").json()
        assert t["status"] == "COMPLETED"
        assert t["attempts"] >= 1
