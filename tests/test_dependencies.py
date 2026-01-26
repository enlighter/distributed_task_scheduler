# tests/test_dependencies.py
import time

from fastapi.testclient import TestClient


def _wait_for_status(client: TestClient, task_id: str, status: str, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        r = client.get(f"/tasks/{task_id}")
        if r.status_code == 200 and r.json()["status"] == status:
            return True
        time.sleep(0.05)
    return False


def test_dependency_blocks_until_completed(client_factory):
    with client_factory(overrides={"DTS_MAX_CONCURRENT": "1"}) as client:
        # A: longer task
        r = client.post(
            "/tasks",
            json={"id": "A", "type": "x", "duration_ms": 300, "dependencies": []},
        )
        assert r.status_code == 201, r.text

        # B depends on A
        r = client.post(
            "/tasks",
            json={"id": "B", "type": "x", "duration_ms": 50, "dependencies": ["A"]},
        )
        assert r.status_code == 201, r.text

        # Immediately after creation, B should show remaining_deps=1 and status QUEUED
        b = client.get("/tasks/B").json()
        assert b["status"] == "QUEUED"
        assert b["remaining_deps"] == 1

        assert _wait_for_status(client, "A", "COMPLETED", timeout_s=3.0), "A did not complete"
        assert _wait_for_status(client, "B", "COMPLETED", timeout_s=3.0), "B did not complete after A"
