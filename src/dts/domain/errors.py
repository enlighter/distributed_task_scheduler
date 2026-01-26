# src/dts/domain/errors.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class DTSBaseError(Exception):
    """
    Base domain error.

    The API layer can map these to HTTP responses consistently.
    """
    message: str
    code: str = "DTS_ERROR"
    details: Optional[dict[str, Any]] = None

    def __str__(self) -> str:
        return self.message


@dataclass
class ValidationError(DTSBaseError):
    code: str = "VALIDATION_ERROR"


@dataclass
class NotFoundError(DTSBaseError):
    code: str = "NOT_FOUND"


@dataclass
class ConflictError(DTSBaseError):
    code: str = "CONFLICT"


@dataclass
class DependencyError(DTSBaseError):
    code: str = "DEPENDENCY_ERROR"


@dataclass
class CycleDetectedError(DTSBaseError):
    code: str = "CYCLE_DETECTED"
