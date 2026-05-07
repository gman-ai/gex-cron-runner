"""Read-only database access helpers.

CRITICAL: this is the ONLY module that opens connections to paisa/advisor/
fetcher SQLite DBs. All callers go through `open_ro()`. Three layers of
read-only enforcement:

1. URI flag `?mode=ro` (per all three sibling-project replies — REQUIRED, not
   optional; bare path triggers write locks).
2. `PRAGMA query_only=1` after connect.
3. No INSERT/UPDATE/DELETE strings in the source (CI grep check in
   `pyproject.toml`).

The atomic-snapshot context manager wraps SELECTs in a `BEGIN`/`COMMIT`
transaction. WAL gives us snapshot isolation for the duration. Per paisa
reply, this is the recommended pattern for cross-table self-consistent reads
in the daily rollup. Live cron skips this (30s drift acceptable).
"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterator

from gex_cron_runner import config

log = logging.getLogger(__name__)


class SnapshotTooSlow(RuntimeError):
    """Raised when a snapshot transaction exceeds SNAPSHOT_FAIL_SEC.

    This is a defense against accidentally holding a long read transaction
    that could prevent paisa's WAL checkpoints. We exit loud rather than
    silently degrade trading-side behavior.
    """


def open_ro(path: str) -> sqlite3.Connection:
    """Open a SQLite connection in read-only mode.

    Required for ALL DBs touched by the cron. Raises if path doesn't exist
    (rather than silently creating an empty DB, which is what bare `connect()`
    would do).

    The `?mode=ro` URI flag is critical — without it, SQLite tries to acquire
    write locks even for SELECT statements and can interfere with the WAL
    writer.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"DB path not found: {path}")
    conn = sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        timeout=10,
        # Don't auto-commit anything — read-only anyway, but explicit
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    # Defense-in-depth: even if URI flag were somehow bypassed, query_only=1
    # blocks INSERT/UPDATE/DELETE at the SQLite engine level.
    conn.execute("PRAGMA query_only=1")
    return conn


@contextmanager
def snapshot(conn: sqlite3.Connection, label: str = "") -> Iterator[sqlite3.Connection]:
    """Atomic-read snapshot via WAL `BEGIN`/`COMMIT`.

    Use ONLY for cross-table consistent reads in the daily rollup. The
    transaction holds a snapshot of the WAL state at BEGIN time; the writer
    can keep adding pages, but our reads see the original state. paisa's
    `wal_autocheckpoint=1000` won't fire while we hold this; keep it short.

    Wall-time is measured. >SNAPSHOT_WARN_SEC = warn. >SNAPSHOT_FAIL_SEC =
    raise SnapshotTooSlow (cron exits, healthcheck fails, INC opens after 3
    misses).

    Live cron should NOT use this — drift is acceptable at 30s cadence.
    """
    t0 = time.monotonic()
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.OperationalError:
            # If we never started a real transaction (e.g., empty body), COMMIT
            # may fail. That's fine — nothing was held.
            pass
        elapsed = time.monotonic() - t0
        if elapsed > config.SNAPSHOT_FAIL_SEC:
            log.error(
                "snapshot %r took %.2fs — exceeds fail threshold (%.1fs); "
                "this could be blocking paisa WAL checkpoints",
                label, elapsed, config.SNAPSHOT_FAIL_SEC,
            )
            raise SnapshotTooSlow(
                f"snapshot {label!r} took {elapsed:.2f}s "
                f"(threshold {config.SNAPSHOT_FAIL_SEC}s)"
            )
        if elapsed > config.SNAPSHOT_WARN_SEC:
            log.warning(
                "snapshot %r took %.2fs — investigate (warn threshold %.1fs)",
                label, elapsed, config.SNAPSHOT_WARN_SEC,
            )
        else:
            log.debug("snapshot %r took %.2fs", label, elapsed)


def attach_ro(conn: sqlite3.Connection, path: str, alias: str) -> None:
    """ATTACH a second SQLite DB as read-only.

    Used by daily_writer to attach alert_log.db onto the paisa connection so
    the cross-DB join (`gex_positions.advisor_alert_id = alert_log.id`) can
    happen in a single SQL statement.

    Note: ATTACH is itself a write to sqlite_master, but with `?mode=ro` on
    the URI it's a read-only attach. Verified safe by paisa reply (§5).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"attach path not found: {path}")
    # Note: parameter binding doesn't work for ATTACH/identifier — but we
    # control both inputs (config constants), no user input.
    conn.execute(f"ATTACH DATABASE 'file:{path}?mode=ro' AS {alias}")
