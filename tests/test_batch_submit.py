# tests/test_batch_submit.py
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


def test_batch_submit_success_with_internal_dependencies(client_factory):
    # Concurrency=1 makes ordering deterministic-ish, but not required for correctness
    with client_factory(overrides={"DTS_MAX_CONCURRENT": "1"}) as client:
        payload = {
            "tasks": [
                {"id": "BA", "type": "demo", "duration_ms": 150, "dependencies": []},
                {"id": "BB", "type": "demo", "duration_ms": 50, "dependencies": ["BA"]},
                {"id": "BC", "type": "demo", "duration_ms": 50, "dependencies": ["BB"]},
            ]
        }
        r = client.post("/tasks/batch", json=payload)
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["count"] == 3
        assert body["created"] == ["BA", "BB", "BC"]

        # Immediately BB should be waiting (remaining it depends on BA)
        bb = client.get("/tasks/BB").json()
        assert bb["status"] == "QUEUED"
        assert bb["remaining_deps"] == 1

        assert _wait_for_status(client, "BA", "COMPLETED", timeout_s=3.0)
        assert _wait_for_status(client, "BB", "COMPLETED", timeout_s=3.0)
        assert _wait_for_status(client, "BC", "COMPLETED", timeout_s=3.0)


def test_batch_submit_missing_external_dependency_returns_400(client: TestClient):
    # dep "NOPE" is neither in DB nor in batch => 400 DEPENDENCY_ERROR
    r = client.post(
        "/tasks/batch",
        json={
            "tasks": [
                {"id": "M1", "type": "demo", "duration_ms": 10, "dependencies": ["NOPE"]},
            ]
        },
    )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["code"] == "DEPENDENCY_ERROR"
    assert "missing" in body.get("details", {})


def test_batch_submit_duplicate_ids_in_payload_returns_422(client: TestClient):
    # Duplicate IDs are caught by Pydantic model validator -> FastAPI returns 422
    r = client.post(
        "/tasks/batch",
        json={
            "tasks": [
                {"id": "DUP", "type": "demo", "duration_ms": 10, "dependencies": []},
                {"id": "DUP", "type": "demo", "duration_ms": 10, "dependencies": []},
            ]
        },
    )
    assert r.status_code == 422, r.text


def test_batch_submit_cycle_in_batch_returns_400(client: TestClient):
    # A depends on B and B depends on A => cycle
    r = client.post(
        "/tasks/batch",
        json={
            "tasks": [
                {"id": "CA", "type": "demo", "duration_ms": 10, "dependencies": ["CB"]},
                {"id": "CB", "type": "demo", "duration_ms": 10, "dependencies": ["CA"]},
            ]
        },
    )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["code"] == "CYCLE_DETECTED"


def test_batch_submit_conflict_existing_id_returns_409(client: TestClient):
    # Create one task first
    r1 = client.post(
        "/tasks",
        json={"id": "EXISTING", "type": "demo", "duration_ms": 10, "dependencies": []},
    )
    assert r1.status_code == 201, r1.text

    # Now batch includes that existing id => 409
    r2 = client.post(
        "/tasks/batch",
        json={
            "tasks": [
                {"id": "EXISTING", "type": "demo", "duration_ms": 10, "dependencies": []},
                {"id": "NEWONE", "type": "demo", "duration_ms": 10, "dependencies": []},
            ]
        },
    )
    assert r2.status_code == 409, r2.text
    body = r2.json()
    assert body["code"] == "CONFLICT"
    assert "existing" in body.get("details", {})
