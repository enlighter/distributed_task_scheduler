"""
Domain layer for DTS.

- states: TaskStatus enum
- models: Pydantic models for API input/output
- errors: domain-level exceptions
"""

from .states import TaskStatus
from .models import (
    ErrorResponse,
    TaskBatchCreate,
    TaskBatchCreateResponse,
    TaskCreate,
    TaskListResponse,
    TaskView,
)
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
    "TaskBatchCreate",
    "TaskBatchCreateResponse",
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
