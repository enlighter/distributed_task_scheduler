# src/dts/api/routes.py
from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from dts.domain.errors import (
    ConflictError,
    CycleDetectedError,
    DependencyError,
    DTSBaseError,
    NotFoundError,
    ValidationError,
)
from dts.domain.models import ( 
    ErrorResponse, 
    TaskCreate, 
    TaskListResponse, 
    TaskView, 
    TaskBatchCreate, 
    TaskBatchCreateResponse 
)
from dts.logging import get_logger
from dts.storage import TaskRepo

from .deps import get_repo, get_settings

_LOG = get_logger(__name__)
router = APIRouter()


def now_ms() -> int:
    return int(time.time() * 1000)


def _error_response(err: DTSBaseError, http_status: int) -> JSONResponse:
    payload = ErrorResponse(
        error=err.message,
        code=err.code,
        details=err.details or {},
    ).model_dump()
    return JSONResponse(status_code=http_status, content=payload)


@router.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@router.post("/tasks", response_model=None, status_code=201)
def submit_task(
    task: TaskCreate,
    repo: TaskRepo = Depends(get_repo),
    settings=Depends(get_settings),
):
    """
    Submit a task definition.

    Notes:
    - Dependencies must already exist (simplest rule).
    - Cycle creation is rejected.
    """
    try:
        repo.create_task(task, now_ms=now_ms(), default_max_attempts=settings.max_attempts)
        return {"id": task.id}
    except ConflictError as e:
        return _error_response(e, 409)
    except (DependencyError, CycleDetectedError, ValidationError) as e:
        return _error_response(e, 400)
    except DTSBaseError as e:
        return _error_response(e, 400)
    
@router.post("/tasks/batch", response_model=TaskBatchCreateResponse, status_code=201)
def submit_tasks_batch(
    payload: TaskBatchCreate,
    repo: TaskRepo = Depends(get_repo),
    settings=Depends(get_settings),
):
    """
    Submit a batch of tasks atomically.

    Strict rules:
    - Task IDs must be new.
    - Dependencies must exist in DB or in the batch.
    - Cycles within the batch are rejected.
    """
    try:
        created = repo.create_tasks_batch(
            tasks=payload.tasks,
            now_ms=now_ms(),
            default_max_attempts=settings.max_attempts,
        )
        return TaskBatchCreateResponse(created=created, count=len(created))
    except ConflictError as e:
        return _error_response(e, 409)
    except (DependencyError, CycleDetectedError, ValidationError) as e:
        return _error_response(e, 400)
    except DTSBaseError as e:
        return _error_response(e, 400)


@router.get("/tasks/{task_id}", response_model=TaskView)
def get_task_status(
    task_id: str,
    repo: TaskRepo = Depends(get_repo),
):
    try:
        return repo.get_task(task_id)
    except NotFoundError as e:
        return _error_response(e, 404)


@router.get("/tasks", response_model=TaskListResponse)
def list_tasks(
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    repo: TaskRepo = Depends(get_repo),
):
    tasks, total = repo.list_tasks(limit=limit, offset=offset)
    return TaskListResponse(tasks=tasks, total=total)
