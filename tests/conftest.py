# tests/conftest.py
import importlib
import itertools
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

import pytest
from fastapi.testclient import TestClient

_counter = itertools.count(1)

DEFAULT_ENV = {
    "DTS_MAX_CONCURRENT": "2",
    "DTS_SCHED_TICK_MS": "50",
    "DTS_LEASE_MS": "2000",
    "DTS_MAX_ATTEMPTS": "3",
    "DTS_LOG_LEVEL": "warning",
    # server host/port are irrelevant for TestClient, but harmless if set elsewhere
}


def _apply_env(monkeypatch: pytest.MonkeyPatch, db_path: Path, overrides: Optional[dict[str, str]] = None) -> None:
    monkeypatch.setenv("DTS_DB_PATH", str(db_path))
    for k, v in DEFAULT_ENV.items():
        monkeypatch.setenv(k, v)
    if overrides:
        for k, v in overrides.items():
            monkeypatch.setenv(k, v)


@contextmanager
def _client_ctx(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, overrides: Optional[dict[str, str]] = None,
                db_path: Optional[Path] = None) -> Iterator[TestClient]:
    # Unique DB per client instance unless one is provided
    if db_path is None:
        n = next(_counter)
        db_path = tmp_path / f"tasks_{n}.db"

    _apply_env(monkeypatch, db_path, overrides)

    # Import after env is set; reload to avoid cross-test state
    app_mod = importlib.import_module("dts.api.app")
    importlib.reload(app_mod)

    with TestClient(app_mod.app) as client:
        yield client


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """
    Default integration test client.
    Uses DEFAULT_ENV and a fresh sqlite db per test.
    """
    with _client_ctx(monkeypatch, tmp_path) as c:
        yield c


@pytest.fixture()
def client_factory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Factory for tests that need custom settings or a pre-populated DB.

    Usage:
      with client_factory(overrides={"DTS_MAX_CONCURRENT": "1"}) as client:
          ...

      with client_factory(db_path=some_existing_db_path) as client:
          ...
    """

    def _make(*, overrides: Optional[dict[str, str]] = None, db_path: Optional[Path] = None):
        return _client_ctx(monkeypatch, tmp_path, overrides=overrides, db_path=db_path)

    return _make
