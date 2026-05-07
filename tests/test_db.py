"""DB module tests. Builds an in-memory SQLite + a tmp-file SQLite to verify
read-only enforcement actually blocks writes.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import time

import pytest

from gex_cron_runner import db


@pytest.fixture
def tmp_db(tmp_path):
    """Create a tiny SQLite file with one table for testing."""
    path = tmp_path / "test.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO t (val) VALUES ('hello'), ('world')")
    conn.commit()
    conn.close()
    return str(path)


def test_open_ro_can_read(tmp_db):
    conn = db.open_ro(tmp_db)
    rows = list(conn.execute("SELECT * FROM t ORDER BY id"))
    assert len(rows) == 2
    assert rows[0]["val"] == "hello"
    conn.close()


def test_open_ro_blocks_writes(tmp_db):
    """The query_only=1 pragma should reject INSERT/UPDATE/DELETE."""
    conn = db.open_ro(tmp_db)
    for sql in (
        "INSERT INTO t (val) VALUES ('bad')",
        "UPDATE t SET val='x' WHERE id=1",
        "DELETE FROM t WHERE id=1",
    ):
        with pytest.raises(sqlite3.OperationalError, match="(read-only|readonly)"):
            conn.execute(sql)
    conn.close()


def test_open_ro_uri_flag_blocks_writes_even_without_pragma():
    """Even if PRAGMA query_only were bypassed, ?mode=ro alone blocks writes."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
        path = tf.name
    try:
        # Set up
        c = sqlite3.connect(path)
        c.execute("CREATE TABLE t (x INT)")
        c.commit()
        c.close()
        # Open via URI flag only (skip our pragma) — should still block
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO t VALUES (1)")
        conn.close()
    finally:
        os.unlink(path)


def test_open_ro_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        db.open_ro(str(tmp_path / "does-not-exist.db"))


def test_snapshot_short_query_does_not_warn(tmp_db, caplog):
    conn = db.open_ro(tmp_db)
    import logging
    caplog.set_level(logging.WARNING, logger="gex_cron_runner.db")
    with db.snapshot(conn, label="quick"):
        list(conn.execute("SELECT * FROM t"))
    # No warning expected for sub-millisecond reads
    warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warns == []
    conn.close()


def test_snapshot_too_slow_raises(tmp_db, monkeypatch):
    """Force the fail threshold to 0 so any snapshot exceeds it."""
    from gex_cron_runner import config
    monkeypatch.setattr(config, "SNAPSHOT_FAIL_SEC", 0.0)
    conn = db.open_ro(tmp_db)
    with pytest.raises(db.SnapshotTooSlow):
        with db.snapshot(conn, label="forced-slow"):
            time.sleep(0.001)  # any non-zero elapsed exceeds 0.0
    conn.close()


def test_attach_ro_works(tmp_db, tmp_path):
    """ATTACH with mode=ro on the URI should work and the attached DB should
    also be read-only."""
    second = tmp_path / "second.db"
    c = sqlite3.connect(str(second))
    c.execute("CREATE TABLE u (id INT, label TEXT)")
    c.execute("INSERT INTO u VALUES (1, 'attached')")
    c.commit()
    c.close()
    conn = db.open_ro(tmp_db)
    db.attach_ro(conn, str(second), "adv")
    row = conn.execute("SELECT label FROM adv.u WHERE id=1").fetchone()
    assert row["label"] == "attached"
    # Writes to attached DB should also fail
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO adv.u VALUES (2, 'bad')")
    conn.close()
