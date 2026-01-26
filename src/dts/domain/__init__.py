# src/dts/domain/__init__.py
"""
Domain layer for DTS.

- states: TaskStatus enum
- models: Pydantic models for API input/output
- errors: domain-level exceptions
"""

from .states import TaskStatus
from .models import TaskCreate, TaskView, TaskListResponse, ErrorResponse
from .errors import (
    DTSBaseError,
    ValidationError,
    NotFoundError,
    ConflictError,
    DependencyError,
    CycleDetectedError,
)

__all__ = [
    "TaskStatus",
    "TaskCreate",
    "TaskView",
    "TaskListResponse",
    "ErrorResponse",
    "DTSBaseError",
    "ValidationError",
    "NotFoundError",
    "ConflictError",
    "DependencyError",
    "CycleDetectedError",
]
