from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as e:
        raise ValueError(f"Environment variable {name} must be an int, got: {raw!r}") from e
    return value


def _get_env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw


@dataclass(frozen=True)
class Settings:
    # Database
    db_path: Path

    # Scheduler / execution
    max_concurrent_tasks: int
    sched_tick_ms: int
    lease_ms: int
    max_attempts: int

    # Server (used by dts.main when starting uvicorn programmatically)
    host: str
    port: int
    log_level: str

    @property
    def sched_tick_s(self) -> float:
        return self.sched_tick_ms / 1000.0


def load_settings() -> Settings:
    """
    Loads settings from env vars with sane defaults.

    Env vars:
      - DTS_DB_PATH (default: ./var/tasks.db)
      - DTS_MAX_CONCURRENT (default: 3)
      - DTS_SCHED_TICK_MS (default: 200)
      - DTS_LEASE_MS (default: 60000)
      - DTS_MAX_ATTEMPTS (default: 3)
      - DTS_HOST (default: 127.0.0.1)
      - DTS_PORT (default: 8000)
      - DTS_LOG_LEVEL (default: info)
    """
    db_path = Path(_get_env_str("DTS_DB_PATH", "./var/tasks.db")).expanduser()

    max_concurrent = _get_env_int("DTS_MAX_CONCURRENT", 3)
    if max_concurrent <= 0:
        raise ValueError("DTS_MAX_CONCURRENT must be > 0")

    sched_tick_ms = _get_env_int("DTS_SCHED_TICK_MS", 200)
    if sched_tick_ms <= 0:
        raise ValueError("DTS_SCHED_TICK_MS must be > 0")

    lease_ms = _get_env_int("DTS_LEASE_MS", 60_000)
    if lease_ms <= 0:
        raise ValueError("DTS_LEASE_MS must be > 0")

    max_attempts = _get_env_int("DTS_MAX_ATTEMPTS", 3)
    if max_attempts <= 0:
        raise ValueError("DTS_MAX_ATTEMPTS must be > 0")

    host = _get_env_str("DTS_HOST", "127.0.0.1")
    port = _get_env_int("DTS_PORT", 8000)
    if not (1 <= port <= 65535):
        raise ValueError("DTS_PORT must be between 1 and 65535")

    log_level = _get_env_str("DTS_LOG_LEVEL", "info").lower()

    return Settings(
        db_path=db_path,
        max_concurrent_tasks=max_concurrent,
        sched_tick_ms=sched_tick_ms,
        lease_ms=lease_ms,
        max_attempts=max_attempts,
        host=host,
        port=port,
        log_level=log_level,
    )
