# src/dts/api/__init__.py
"""
API layer for DTS (FastAPI).

- app: FastAPI instance + lifecycle hooks
- routes: REST endpoints
- deps: dependency injection helpers
"""

from .app import app

__all__ = ["app"]
