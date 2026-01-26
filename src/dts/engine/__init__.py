# src/dts/engine/__init__.py
"""
Execution engine for DTS.

- scheduler: claim loop + concurrency control
- worker: executes tasks (sleep) and commits results
- recovery: lease expiry / crash recovery
"""

from .scheduler import Scheduler

__all__ = ["Scheduler"]
