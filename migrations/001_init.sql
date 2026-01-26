-- 001_init.sql
-- Initial schema for Distributed Task Scheduler (DTS)

-- Track applied migrations (used by storage/migrations.py)
CREATE TABLE IF NOT EXISTS schema_migrations (
  version   INTEGER PRIMARY KEY,
  filename  TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);

-- Core tasks table
CREATE TABLE IF NOT EXISTS tasks (
  id               TEXT PRIMARY KEY,
  type             TEXT NOT NULL,
  duration_ms      INTEGER NOT NULL CHECK (duration_ms > 0),

  status           TEXT NOT NULL
                   CHECK (status IN ('QUEUED','RUNNING','COMPLETED','FAILED','BLOCKED')),

  remaining_deps   INTEGER NOT NULL DEFAULT 0 CHECK (remaining_deps >= 0),

  attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts     INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),

  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  started_at       INTEGER,
  finished_at      INTEGER,

  lease_expires_at INTEGER,
  last_error       TEXT
);

-- Dependency edges: task_id depends on depends_on_id
CREATE TABLE IF NOT EXISTS deps (
  task_id       TEXT NOT NULL,
  depends_on_id TEXT NOT NULL,

  PRIMARY KEY (task_id, depends_on_id),

  FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
  FOREIGN KEY (depends_on_id) REFERENCES tasks(id) ON DELETE RESTRICT
);

-- Optional sanity check trigger: keep updated_at fresh when tasks change
-- (not strictly necessary; repository already sets updated_at)
-- CREATE TRIGGER IF NOT EXISTS trg_tasks_updated_at
-- AFTER UPDATE ON tasks
-- FOR EACH ROW
-- BEGIN
--   UPDATE tasks SET updated_at = strftime('%s','now')*1000 WHERE id = OLD.id;
-- END;
