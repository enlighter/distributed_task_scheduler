# src/dts/api/deps.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Generator

from fastapi import Depends, Request

from dts.config import Settings, load_settings
from dts.storage import SQLiteDB, TaskRepo


def get_settings(request: Request) -> Settings:
    """
    Per-request access to settings stored on app.state during startup.
    """
    return request.app.state.settings  # type: ignore[attr-defined]


def get_db(request: Request) -> SQLiteDB:
    """
    Per-request access to SQLiteDB stored on app.state during startup.
    """
    return request.app.state.db  # type: ignore[attr-defined]


def get_conn(
    db: SQLiteDB = Depends(get_db),
) -> Generator[sqlite3.Connection, None, None]:
    """
    Provides a per-request SQLite connection.
    """
    conn = db.connect()
    try:
        yield conn
    finally:
        conn.close()


def get_repo(
    conn: sqlite3.Connection = Depends(get_conn),
) -> TaskRepo:
    """
    Provides a TaskRepo bound to the request connection.
    """
    return TaskRepo(conn)
