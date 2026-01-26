# src/dts/storage/migrations.py
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

from dts.logging import get_logger

_LOG = get_logger(__name__)


_MIGRATION_RE = re.compile(r"^(?P<version>\d+)_.*\.sql$")


@dataclass(frozen=True)
class Migration:
    version: int
    filename: str
    path: Path


def apply_migrations(conn: sqlite3.Connection, migrations_dir: Path) -> None:
    """
    Applies SQL migrations from migrations_dir in ascending numeric order.

    We store applied migration versions in schema_migrations.
    Migrations should be idempotent where possible, but the runner ensures
    each version is applied once.

    Expected migration filenames:
      001_init.sql
      002_indexes.sql
      ...

    Usage:
      apply_migrations(conn, Path("migrations"))
    """
    migrations_dir = migrations_dir.resolve()
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Migrations dir not found: {migrations_dir}")

    _ensure_migrations_table(conn)

    applied = _get_applied_versions(conn)
    pending = _load_migrations(migrations_dir)

    to_apply = [m for m in pending if m.version not in applied]
    if not to_apply:
        _LOG.info("No pending migrations.")
        return

    _LOG.info("Applying %d migration(s)...", len(to_apply))
    for m in to_apply:
        sql = m.path.read_text(encoding="utf-8")
        _LOG.info("Applying migration %03d (%s)", m.version, m.filename)
        conn.executescript(sql)
        conn.execute(
            "INSERT INTO schema_migrations(version, filename, applied_at) VALUES (?, ?, strftime('%s','now')*1000);",
            (m.version, m.filename),
        )
    _LOG.info("Migrations applied successfully.")


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations(
          version INTEGER PRIMARY KEY,
          filename TEXT NOT NULL,
          applied_at INTEGER NOT NULL
        );
        """
    )


def _get_applied_versions(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version;").fetchall()
    return {int(r["version"]) for r in rows}


def _load_migrations(migrations_dir: Path) -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        m = _MIGRATION_RE.match(path.name)
        if not m:
            # ignore files that don't match the naming convention
            continue
        version = int(m.group("version"))
        migrations.append(Migration(version=version, filename=path.name, path=path))

    migrations.sort(key=lambda x: x.version)
    return migrations
