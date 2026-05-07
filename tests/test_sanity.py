"""Sanity check tests against the in-memory fixture DBs."""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from gex_cron_runner import sanity


def test_market_data_version_pin_passes(market_data_db):
    ok, notes = sanity.check_market_data(market_data_db)
    assert ok is True


def test_market_data_version_pin_fails_on_drift(market_data_db):
    market_data_db.execute("UPDATE _schema_version SET version=99")
    ok, notes = sanity.check_market_data(market_data_db)
    assert ok is False
    assert "expected 6" in notes[0]


def test_alert_log_column_count_passes(alert_log_db):
    ok, notes = sanity.check_alert_log(alert_log_db)
    assert ok is True


def test_alert_log_column_count_fails_on_drift(alert_log_db):
    """Drop a column → count is now 40, not 41 → must fail."""
    # SQLite ALTER TABLE DROP COLUMN was added in 3.35; should be available in 3.12 venv
    try:
        alert_log_db.execute("ALTER TABLE alert_log DROP COLUMN ticker")
    except sqlite3.OperationalError:
        pytest.skip("DROP COLUMN not supported in this SQLite version")
    ok, notes = sanity.check_alert_log(alert_log_db)
    assert ok is False
    assert "column count = 40" in notes[0]


def test_paisa_required_tables(paisa_db):
    ok, notes = sanity.check_paisa(paisa_db)
    assert ok is True


def test_paisa_missing_table_fails(paisa_db):
    paisa_db.execute("DROP TABLE bot_state")
    ok, notes = sanity.check_paisa(paisa_db)
    assert ok is False
    assert "bot_state" in notes[0]


def test_paisa_missing_advisor_alert_id_fails(paisa_db):
    """If gex_positions loses advisor_alert_id, cross-DB join is broken — hard fail."""
    # Drop the dependent index first
    paisa_db.execute("DROP INDEX IF EXISTS idx_gex_positions_advisor_alert")
    try:
        paisa_db.execute("ALTER TABLE gex_positions DROP COLUMN advisor_alert_id")
    except sqlite3.OperationalError:
        pytest.skip("DROP COLUMN not supported in this SQLite version")
    ok, notes = sanity.check_paisa(paisa_db)
    assert ok is False
    assert "advisor_alert_id" in notes[0]


def test_run_full_sanity_passes(market_data_db, alert_log_db, paisa_db):
    report = sanity.run_sanity_checks(
        market_data_db, alert_log_db, paisa_db,
        today_et=date(2026, 5, 7),
    )
    assert report.all_ok is True
    # Holiday coverage: our fixture seeds 2027-01-01, so as of 2026-05-07
    # lookahead is ~240 days — plenty
    assert report.holiday_coverage_ok is True


def test_run_full_sanity_holiday_warning_only_is_soft(market_data_db, alert_log_db, paisa_db):
    """Drop most holidays so coverage is < 60d; should still pass overall."""
    market_data_db.execute("DELETE FROM market_holidays WHERE date != '2026-04-03'")
    report = sanity.run_sanity_checks(
        market_data_db, alert_log_db, paisa_db,
        today_et=date(2026, 5, 7),
    )
    # Hard checks all pass
    assert report.all_ok is True
    # Soft holiday warning surfaces
    assert report.holiday_coverage_ok is False
    assert "reseed" in report.holiday_warning.lower()
