#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from dts.config import load_settings
from dts.logging import configure_logging, get_logger
from dts.storage import SQLiteDB, apply_migrations


def main() -> int:
    settings = load_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)

    # Ensure DB directory exists (e.g., ./var)
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    db = SQLiteDB(settings.db_path)
    conn = db.connect()
    try:
        migrations_dir = Path("migrations")
        apply_migrations(conn, migrations_dir)
    finally:
        conn.close()

    log.info("DB initialized at %s", settings.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
