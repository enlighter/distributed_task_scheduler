# src/dts/storage/db.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class SQLiteDB:
    """
    SQLite connection factory.

    Notes:
    - Use one connection per thread (recommended).
    - Apply pragmas on each connection.
    - WAL mode improves read/write concurrency significantly for this use case.
    """
    db_path: Path
    timeout_s: float = 5.0

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(
            str(self.db_path),
            timeout=self.timeout_s,
            isolation_level=None,          # we manage transactions manually (BEGIN/COMMIT)
            check_same_thread=True,        # one connection per thread (safe default)
        )
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn

    def _apply_pragmas(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        # Better concurrency
        cur.execute("PRAGMA journal_mode=WAL;")
        # Enforce FK constraints
        cur.execute("PRAGMA foreign_keys=ON;")
        # Reduce spurious 'database is locked'
        cur.execute("PRAGMA busy_timeout=5000;")
        # Good defaults; you can tune
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.close()


def begin_immediate(conn: sqlite3.Connection) -> None:
    """
    Begins a transaction that acquires a RESERVED lock immediately.
    This prevents concurrent writers from proceeding and is useful for atomic claims.
    """
    conn.execute("BEGIN IMMEDIATE;")


def begin_deferred(conn: sqlite3.Connection) -> None:
    """
    Begins a transaction in DEFERRED mode (default). Writes will lock when they occur.
    """
    conn.execute("BEGIN;")


def commit(conn: sqlite3.Connection) -> None:
    conn.execute("COMMIT;")


def rollback(conn: sqlite3.Connection) -> None:
    conn.execute("ROLLBACK;")
