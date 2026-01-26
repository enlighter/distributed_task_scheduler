from __future__ import annotations

import sys
from pathlib import Path

from dts.config import load_settings
from dts.logging import configure_logging, get_logger


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    """
    Programmatic entrypoint.

    Recommended dev command:
      uvicorn dts.api.app:app --reload

    This entrypoint exists so you can also do:
      python -m dts.main
    """
    settings = load_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)

    _ensure_parent_dir(settings.db_path)
    log.info("Starting DTS with DB path: %s", settings.db_path)

    # Import here so config/logging are set before app import side-effects.
    try:
        from dts.api.app import app  # noqa: F401
    except Exception as e:
        log.exception("Failed to import FastAPI app (dts.api.app:app).")
        return 1

    try:
        import uvicorn
    except ImportError:
        log.error("uvicorn is not installed. Install with: pip install uvicorn")
        return 1

    # Start uvicorn
    uvicorn.run(
        "dts.api.app:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        reload=False,  # prefer `uvicorn ... --reload` in dev
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
