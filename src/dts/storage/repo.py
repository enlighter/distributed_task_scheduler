# src/dts/storage/repo.py
from __future__ import annotations

from asyncio import tasks
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from dts.domain.errors import (
    ConflictError,
    CycleDetectedError,
    DependencyError,
    NotFoundError,
    ValidationError,
)
from dts.domain.models import TaskCreate, TaskView
from dts.domain.states import TaskStatus
from dts.logging import get_logger

from .db import begin_immediate, commit, rollback

_LOG = get_logger(__name__)


@dataclass
class TaskRepo:
    """
    Repository encapsulating all SQL access.

    Important invariants:
    - Task claiming is atomic (BEGIN IMMEDIATE + guarded UPDATE).
    - Dependency unblocking is updated transactionally when tasks complete.
    - Crash recovery re-queues stale RUNNING tasks via lease expiry.
    """
    conn: sqlite3.Connection

    # -------------------------
    # Read operations
    # -------------------------

    def get_task(self, task_id: str) -> TaskView:
        row = self.conn.execute(
            """
            SELECT id, type, duration_ms, status, remaining_deps, attempts, max_attempts,
                   created_at, updated_at, started_at, finished_at, lease_expires_at, last_error
            FROM tasks
            WHERE id = ?;
            """,
            (task_id,),
        ).fetchone()
        if not row:
            raise NotFoundError(f"Task not found: {task_id}", details={"id": task_id})

        deps = self._get_dependencies(task_id)

        return TaskView(
            id=row["id"],
            type=row["type"],
            duration_ms=row["duration_ms"],
            status=TaskStatus(row["status"]),
            remaining_deps=row["remaining_deps"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            dependencies=deps,
        )

    def list_tasks(self, limit: int = 200, offset: int = 0) -> tuple[list[TaskView], int]:
        # Total count
        total = self.conn.execute("SELECT COUNT(*) AS c FROM tasks;").fetchone()["c"]

        rows = self.conn.execute(
            """
            SELECT id, type, duration_ms, status, remaining_deps, attempts, max_attempts,
                   created_at, updated_at, started_at, finished_at, lease_expires_at, last_error
            FROM tasks
            ORDER BY created_at ASC
            LIMIT ? OFFSET ?;
            """,
            (limit, offset),
        ).fetchall()

        tasks: list[TaskView] = []
        for row in rows:
            deps = self._get_dependencies(row["id"])
            tasks.append(
                TaskView(
                    id=row["id"],
                    type=row["type"],
                    duration_ms=row["duration_ms"],
                    status=TaskStatus(row["status"]),
                    remaining_deps=row["remaining_deps"],
                    attempts=row["attempts"],
                    max_attempts=row["max_attempts"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    lease_expires_at=row["lease_expires_at"],
                    last_error=row["last_error"],
                    dependencies=deps,
                )
            )
        return tasks, int(total)

    # -------------------------
    # Write operations
    # -------------------------

    def create_task(self, task: TaskCreate, now_ms: int, default_max_attempts: int) -> None:
        """
        Inserts a task and its dependency edges in a single transaction.

        Behavior:
        - Reject if id exists
        - Reject if dependencies are missing (simplifies logic)
        - Reject if cycle would be created by adding these edges
        - Compute remaining_deps based on which deps are already COMPLETED
        - Set status=QUEUED always; readiness is derived from remaining_deps==0
        """
        try:
            begin_immediate(self.conn)

            # Uniqueness
            existing = self.conn.execute("SELECT 1 FROM tasks WHERE id=?;", (task.id,)).fetchone()
            if existing:
                raise ConflictError(f"Task already exists: {task.id}", details={"id": task.id})

            # Dependencies must exist (simplest rule)
            if task.dependencies:
                missing = self._missing_dependency_ids(task.dependencies)
                if missing:
                    raise DependencyError(
                        "One or more dependencies do not exist",
                        details={"missing": sorted(missing)},
                    )

                # Cycle detection: would adding edges (task.id -> dep) create a cycle?
                # Equivalent: is task.id reachable FROM any dep already? If yes, cycle.
                if self._would_create_cycle(task.id, task.dependencies):
                    raise CycleDetectedError(
                        f"Adding dependencies would create a cycle for task {task.id}",
                        details={"id": task.id, "dependencies": task.dependencies},
                    )

            remaining = self._count_incomplete_dependencies(task.dependencies)

            self.conn.execute(
                """
                INSERT INTO tasks(
                  id, type, duration_ms,
                  status, remaining_deps,
                  attempts, max_attempts,
                  created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    task.id,
                    task.type,
                    task.duration_ms,
                    TaskStatus.QUEUED.value,
                    remaining,
                    0,
                    default_max_attempts,
                    now_ms,
                    now_ms,
                ),
            )

            # Insert edges
            for dep in task.dependencies:
                self.conn.execute(
                    "INSERT INTO deps(task_id, depends_on_id) VALUES (?, ?);",
                    (task.id, dep),
                )

            commit(self.conn)
        except Exception:
            rollback(self.conn)
            raise

    def claim_runnable_tasks(
        self,
        now_ms: int,
        lease_ms: int,
        limit: int,
    ) -> list[tuple[str, int]]:
        """
        Atomically claims up to `limit` runnable tasks and marks them RUNNING.

        Runnable = status=QUEUED AND remaining_deps=0.

        Returns a list of (task_id, duration_ms) claimed.
        """
        if limit <= 0:
            return []

        try:
            begin_immediate(self.conn)

            # Select candidates deterministically.
            rows = self.conn.execute(
                """
                SELECT id, duration_ms
                FROM tasks
                WHERE status = ?
                  AND remaining_deps = 0
                ORDER BY created_at ASC
                LIMIT ?;
                """,
                (TaskStatus.QUEUED.value, limit),
            ).fetchall()

            if not rows:
                commit(self.conn)
                return []

            ids = [r["id"] for r in rows]

            # Mark RUNNING; guard with status/remaining_deps again (safety)
            self.conn.execute(
                f"""
                UPDATE tasks
                SET status = ?,
                    started_at = COALESCE(started_at, ?),
                    updated_at = ?,
                    attempts = attempts + 1,
                    lease_expires_at = ?
                WHERE id IN ({",".join("?" for _ in ids)})
                  AND status = ?
                  AND remaining_deps = 0;
                """,
                (
                    TaskStatus.RUNNING.value,
                    now_ms,
                    now_ms,
                    now_ms + lease_ms,
                    *ids,
                    TaskStatus.QUEUED.value,
                ),
            )

            commit(self.conn)

            return [(r["id"], int(r["duration_ms"])) for r in rows]
        except Exception:
            rollback(self.conn)
            raise

    def mark_completed(self, task_id: str, now_ms: int) -> None:
        """
        Marks task COMPLETED and unblocks dependents by decrementing remaining_deps.
        """
        try:
            begin_immediate(self.conn)

            updated = self.conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    finished_at = ?,
                    lease_expires_at = NULL,
                    last_error = NULL
                WHERE id = ?
                  AND status = ?;
                """,
                (TaskStatus.COMPLETED.value, now_ms, now_ms, task_id, TaskStatus.RUNNING.value),
            ).rowcount

            if updated == 0:
                # Either missing or not RUNNING; treat as not found or conflict
                exists = self.conn.execute("SELECT status FROM tasks WHERE id=?;", (task_id,)).fetchone()
                if not exists:
                    raise NotFoundError(f"Task not found: {task_id}", details={"id": task_id})
                raise ConflictError(
                    "Task is not RUNNING; cannot mark completed",
                    details={"id": task_id, "status": exists["status"]},
                )

            # Decrement remaining_deps of dependents
            self.conn.execute(
                """
                UPDATE tasks
                SET remaining_deps = CASE
                      WHEN remaining_deps > 0 THEN remaining_deps - 1
                      ELSE 0
                    END,
                    updated_at = ?
                WHERE id IN (SELECT task_id FROM deps WHERE depends_on_id = ?)
                  AND status = ?;
                """,
                (now_ms, task_id, TaskStatus.QUEUED.value),
            )

            commit(self.conn)
        except Exception:
            rollback(self.conn)
            raise

    def mark_failed(self, task_id: str, now_ms: int, error: str) -> None:
        """
        Marks task FAILED.

        Note: Dependency failure propagation (BLOCKED) is a policy choice.
        Here we do NOT automatically block dependents; you can add that later
        if you want explicit 'dependency failed' visibility.
        """
        try:
            begin_immediate(self.conn)

            updated = self.conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    finished_at = ?,
                    lease_expires_at = NULL,
                    last_error = ?
                WHERE id = ?
                  AND status = ?;
                """,
                (TaskStatus.FAILED.value, now_ms, now_ms, error, task_id, TaskStatus.RUNNING.value),
            ).rowcount

            if updated == 0:
                exists = self.conn.execute("SELECT status FROM tasks WHERE id=?;", (task_id,)).fetchone()
                if not exists:
                    raise NotFoundError(f"Task not found: {task_id}", details={"id": task_id})
                raise ConflictError(
                    "Task is not RUNNING; cannot mark failed",
                    details={"id": task_id, "status": exists["status"]},
                )

            commit(self.conn)
        except Exception:
            rollback(self.conn)
            raise

    def recover_stale_running(self, now_ms: int, max_attempts: int) -> int:
        """
        Crash recovery:
        - Finds RUNNING tasks with expired lease
        - If attempts < max_attempts => re-queue
        - Else => mark FAILED
        Returns number of tasks transitioned.
        """
        try:
            begin_immediate(self.conn)

            # Requeue those still eligible for retry
            requeued = self.conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    lease_expires_at = NULL,
                    last_error = 'Recovered: lease expired; re-queued'
                WHERE status = ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                  AND attempts < ?;
                """,
                (
                    TaskStatus.QUEUED.value,
                    now_ms,
                    TaskStatus.RUNNING.value,
                    now_ms,
                    max_attempts,
                ),
            ).rowcount

            # Fail those out of attempts
            failed = self.conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    updated_at = ?,
                    finished_at = COALESCE(finished_at, ?),
                    lease_expires_at = NULL,
                    last_error = 'Recovered: lease expired; max attempts reached'
                WHERE status = ?
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                  AND attempts >= ?;
                """,
                (
                    TaskStatus.FAILED.value,
                    now_ms,
                    now_ms,
                    TaskStatus.RUNNING.value,
                    now_ms,
                    max_attempts,
                ),
            ).rowcount

            commit(self.conn)
            return int(requeued + failed)
        except Exception:
            rollback(self.conn)
            raise

    def count_running_leased(self, now_ms: int) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM tasks
            WHERE status = ?
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at > ?;
            """,
            (TaskStatus.RUNNING.value, now_ms),
        ).fetchone()
        return int(row["c"])
    
    def create_tasks_batch(
        self,
        tasks: list[TaskCreate],
        now_ms: int,
        default_max_attempts: int,
    ) -> list[str]:
        """
        Atomically inserts a batch of tasks and dependency edges.

        Strict rules:
        - All task IDs must be new (no existing IDs in DB).
        - Dependencies must exist either in DB OR within the same batch.
        - Cycles within the batch are rejected.

        Returns list of created task IDs (in input order).
        """
        if not tasks:
            raise ValidationError("tasks batch must not be empty")

        batch_ids = [t.id for t in tasks]
        batch_id_set = set(batch_ids)
        if len(batch_ids) != len(batch_id_set):
            raise ValidationError("batch contains duplicate task ids")

        # Validate no self-deps (already checked per TaskCreate) and compute set of all deps
        all_dep_ids: set[str] = set()
        for t in tasks:
            all_dep_ids.update(t.dependencies)

        external_deps = sorted(all_dep_ids - batch_id_set)

        try:
            begin_immediate(self.conn)

            # Ensure none of the task IDs already exist
            existing = self._existing_task_ids(batch_ids)
            if existing:
                raise ConflictError(
                    "One or more task ids already exist",
                    details={"existing": sorted(existing)},
                )

            # Ensure external deps exist in DB
            missing_external = self._missing_dependency_ids(external_deps)
            if missing_external:
                raise DependencyError(
                    "One or more dependencies do not exist",
                    details={"missing": sorted(missing_external)},
                )

            # Detect cycles within the batch graph (task -> depends_on)
            self._assert_no_cycle_within_batch(tasks)

            # Pre-fetch completion status for external deps in ONE query
            external_incomplete = self._external_incomplete_deps(external_deps)

            # Insert tasks first
            # status=QUEUED always; readiness is derived from remaining_deps==0
            for t in tasks:
                remaining = 0
                for dep in t.dependencies:
                    if dep in batch_id_set:
                        remaining += 1  # dep is in batch, not completed yet
                    else:
                        # dep exists in DB
                        if dep in external_incomplete:
                            remaining += 1

                self.conn.execute(
                    """
                    INSERT INTO tasks(
                      id, type, duration_ms,
                      status, remaining_deps,
                      attempts, max_attempts,
                      created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        t.id,
                        t.type,
                        t.duration_ms,
                        TaskStatus.QUEUED.value,
                        remaining,
                        0,
                        default_max_attempts,
                        now_ms,
                        now_ms,
                    ),
                )

            # Insert dependency edges
            for t in tasks:
                for dep in t.dependencies:
                    self.conn.execute(
                        "INSERT INTO deps(task_id, depends_on_id) VALUES (?, ?);",
                        (t.id, dep),
                    )

            commit(self.conn)
            return batch_ids

        except Exception:
            rollback(self.conn)
            raise

    # -------------------------
    # Helpers
    # -------------------------

    def _get_dependencies(self, task_id: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT depends_on_id FROM deps WHERE task_id = ? ORDER BY depends_on_id ASC;",
            (task_id,),
        ).fetchall()
        return [r["depends_on_id"] for r in rows]

    def _missing_dependency_ids(self, dep_ids: Sequence[str]) -> set[str]:
        if not dep_ids:
            return set()
        rows = self.conn.execute(
            f"SELECT id FROM tasks WHERE id IN ({','.join('?' for _ in dep_ids)});",
            tuple(dep_ids),
        ).fetchall()
        found = {r["id"] for r in rows}
        return set(dep_ids) - found

    def _count_incomplete_dependencies(self, dep_ids: Sequence[str]) -> int:
        if not dep_ids:
            return 0
        row = self.conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM tasks
            WHERE id IN ({','.join('?' for _ in dep_ids)})
              AND status != ?;
            """,
            (*dep_ids, TaskStatus.COMPLETED.value),
        ).fetchone()
        return int(row["c"])

    def _would_create_cycle(self, new_task_id: str, dep_ids: Sequence[str]) -> bool:
        """
        Cycle check using recursive CTE.

        Adding edges: new_task_id depends on dep_ids
        creates a cycle iff new_task_id is reachable from any dep_id in the existing graph.

        We ask: starting from dep_ids, can we reach new_task_id by following edges task -> depends_on ?
        Using deps table, we can traverse "task_id -> depends_on_id" downward in dependency direction.
        """
        if not dep_ids:
            return False

        # Recursive traversal of dependency chain from the immediate deps.
        # If we ever encounter new_task_id, we'd create a cycle.
        placeholders = ",".join("?" for _ in dep_ids)
        query = f"""
        WITH RECURSIVE walk(node) AS (
          SELECT depends_on_id
          FROM deps
          WHERE task_id IN ({placeholders})
          UNION
          SELECT d.depends_on_id
          FROM deps d
          JOIN walk w ON d.task_id = w.node
        )
        SELECT 1
        FROM walk
        WHERE node = ?
        LIMIT 1;
        """
        row = self.conn.execute(query, (*dep_ids, new_task_id)).fetchone()
        return row is not None
    
    def _existing_task_ids(self, ids: Sequence[str]) -> set[str]:
        if not ids:
            return set()
        rows = self.conn.execute(
            f"SELECT id FROM tasks WHERE id IN ({','.join('?' for _ in ids)});",
            tuple(ids),
        ).fetchall()
        return {r["id"] for r in rows}

    def _external_incomplete_deps(self, dep_ids: Sequence[str]) -> set[str]:
        """
        Returns the subset of dep_ids that exist in DB and are NOT COMPLETED.
        """
        if not dep_ids:
            return set()
        rows = self.conn.execute(
            f"""
            SELECT id
            FROM tasks
            WHERE id IN ({','.join('?' for _ in dep_ids)})
              AND status != ?;
            """,
            (*dep_ids, TaskStatus.COMPLETED.value),
        ).fetchall()
        return {r["id"] for r in rows}

    def _assert_no_cycle_within_batch(self, tasks: list[TaskCreate]) -> None:
        """
        Detect cycles within the batch DAG using Kahn's algorithm.

        Graph direction: task -> dependency (task depends_on dependency).
        For cycle detection, we consider edges only among nodes in the batch.
        """
        ids = [t.id for t in tasks]
        id_set = set(ids)

        # Build adjacency for edges dep -> dependents (reverse) for Kahn
        dependents: dict[str, list[str]] = defaultdict(list)
        indegree: dict[str, int] = {tid: 0 for tid in ids}

        for t in tasks:
            for dep in t.dependencies:
                if dep in id_set:
                    # dep must be processed before t
                    dependents[dep].append(t.id)
                    indegree[t.id] += 1

        q = deque([tid for tid in ids if indegree[tid] == 0])
        visited = 0

        while q:
            node = q.popleft()
            visited += 1
            for child in dependents.get(node, []):
                indegree[child] -= 1
                if indegree[child] == 0:
                    q.append(child)

        if visited != len(ids):
            raise CycleDetectedError(
                "Batch contains a dependency cycle",
                details={"batch_ids": ids},
            )
        
        # Why this is safe without checking cycles involving existing DB tasks:
        # Because this batch insert only creates new tasks with edges pointing to existing 
        # tasks (or within the batch). Existing tasks cannot already reference these new nodes, 
        # so you cannot form a cycle involving old nodes. The only possible cycle is within 
        # the new nodes, which we detect.
