# tests/test_api.py
import time

from fastapi.testclient import TestClient


def _wait_until(fn, timeout_s: float = 5.0, poll_s: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if fn():
            return True
        time.sleep(poll_s)
    return False


def test_healthz(client: TestClient):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_submit_get_list_task(client: TestClient):
    payload = {
        "id": "task-api-1",
        "type": "data_processing",
        "duration_ms": 50,
        "dependencies": [],
    }
    r = client.post("/tasks", json=payload)
    assert r.status_code == 201, r.text
    assert r.json()["id"] == "task-api-1"

    r = client.get("/tasks/task-api-1")
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["id"] == "task-api-1"
    assert data["status"] in {"QUEUED", "RUNNING", "COMPLETED"}  # timing-dependent

    r = client.get("/tasks")
    assert r.status_code == 200
    j = r.json()
    assert "tasks" in j and "total" in j
    assert any(t["id"] == "task-api-1" for t in j["tasks"])

    # Eventually should complete
    ok = _wait_until(
        lambda: client.get("/tasks/task-api-1").json()["status"] == "COMPLETED",
        timeout_s=3.0,
    )
    assert ok, "Task did not reach COMPLETED in time"


def test_submit_duplicate_id_returns_409(client: TestClient):
    payload = {
        "id": "task-dupe",
        "type": "t",
        "duration_ms": 10,
        "dependencies": [],
    }
    r1 = client.post("/tasks", json=payload)
    assert r1.status_code == 201, r1.text

    r2 = client.post("/tasks", json=payload)
    assert r2.status_code == 409, r2.text
    body = r2.json()
    assert body["code"] == "CONFLICT"
