# src/dts/domain/states.py
from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    """
    Internal states stored in the DB.

    Note:
      - We keep the stored states minimal and durable.
      - "WAITING" can be represented as QUEUED with remaining_deps > 0,
        but we don't store WAITING as a separate state.

    Suggested semantics:
      - QUEUED: not yet started; may be runnable (remaining_deps==0) or waiting
      - RUNNING: claimed by a worker/scheduler with a lease
      - COMPLETED: finished successfully
      - FAILED: execution failed (or retries exhausted)
      - BLOCKED: will never run because a dependency failed (optional policy)
    """

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"
