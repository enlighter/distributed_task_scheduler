from __future__ import annotations

from typing import Annotated, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .states import TaskStatus


TaskId = Annotated[str, Field(min_length=1, max_length=256)]


class TaskCreate(BaseModel):
    """
    API input model for submitting a task.
    """
    model_config = ConfigDict(extra="forbid")

    id: TaskId
    type: Annotated[str, Field(min_length=1, max_length=256)]
    duration_ms: Annotated[int, Field(gt=0, le=86_400_000)]  # up to 24h
    dependencies: list[TaskId] = Field(default_factory=list)

    @field_validator("dependencies")
    @classmethod
    def _validate_dependencies(cls, deps: list[str], info) -> list[str]:
        # No duplicates
        if len(deps) != len(set(deps)):
            raise ValueError("dependencies must not contain duplicates")

        # No self-dependency
        task_id = info.data.get("id")
        if task_id and task_id in deps:
            raise ValueError("task cannot depend on itself")

        return deps


class TaskBatchCreate(BaseModel):
    """
    API input model for submitting a batch of tasks atomically.

    Strict mode:
    - Dependencies must exist either in DB already OR within the same batch.
    - Reject cycles within the batch.
    """
    model_config = ConfigDict(extra="forbid")

    tasks: list[TaskCreate] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_batch_ids_unique(self):
        ids = [t.id for t in self.tasks]
        if len(ids) != len(set(ids)):
            raise ValueError("batch contains duplicate task ids")
        return self


class TaskBatchCreateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    created: list[str]
    count: int


class TaskView(BaseModel):
    """
    API output model for a single task.
    """
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    duration_ms: int

    status: TaskStatus

    # WAITING can be inferred when remaining_deps > 0 and status == QUEUED
    remaining_deps: int

    attempts: int
    max_attempts: int

    created_at: int
    updated_at: int
    started_at: Optional[int] = None
    finished_at: Optional[int] = None

    lease_expires_at: Optional[int] = None
    last_error: Optional[str] = None

    dependencies: list[str] = Field(default_factory=list)


class TaskListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tasks: list[TaskView]
    total: int


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: str
    code: str
    details: dict = Field(default_factory=dict)
