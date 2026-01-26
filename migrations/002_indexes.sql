-- 002_indexes.sql
-- Performance-oriented indexes

-- Common scheduler query: runnable tasks (QUEUED + remaining_deps==0) ordered by created_at
CREATE INDEX IF NOT EXISTS idx_tasks_runnable
ON tasks(status, remaining_deps, created_at);

-- Recovery + running count query: RUNNING with lease_expires_at comparisons
CREATE INDEX IF NOT EXISTS idx_tasks_running_lease
ON tasks(status, lease_expires_at);

-- Dependency lookups:
-- When a task completes, find its dependents quickly via deps.depends_on_id
CREATE INDEX IF NOT EXISTS idx_deps_depends_on
ON deps(depends_on_id);

-- When reading a task, fetch dependencies quickly via deps.task_id
CREATE INDEX IF NOT EXISTS idx_deps_task_id
ON deps(task_id);

-- For listing tasks chronologically (optional; helps ORDER BY created_at)
CREATE INDEX IF NOT EXISTS idx_tasks_created_at
ON tasks(created_at);
