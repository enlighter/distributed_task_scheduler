from __future__ import annotations

import logging
import sys
from typing import Optional


def configure_logging(log_level: str = "info") -> None:
    """
    Configures root logging for the service.

    Keeps it simple but production-usable:
    - logs to stdout
    - consistent format
    - avoids double handlers on reload
    """
    level = _parse_level(log_level)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers (common with reload / repeated init)
    if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        for h in list(root.handlers):
            root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Make noisy loggers a bit quieter (tweak as you like)
    logging.getLogger("uvicorn.access").setLevel(max(level, logging.INFO))
    logging.getLogger("uvicorn.error").setLevel(level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    return logging.getLogger(name if name else "dts")


def _parse_level(log_level: str) -> int:
    mapping = {
        "critical": logging.CRITICAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "warn": logging.WARNING,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "trace": logging.DEBUG,  # Python stdlib has no TRACE; map to DEBUG.
    }
    return mapping.get(log_level.lower().strip(), logging.INFO)
