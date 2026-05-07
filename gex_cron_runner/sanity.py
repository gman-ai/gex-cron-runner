"""Startup schema-version + column-count + holiday-coverage sanity checks.

Drift = exit 1 (fail loud, fail fast). Better to skip a daily/live publish
than ship JSON with silently-wrong shapes. Health-check ping fail → INC opens
after 3 misses.

Two severity levels:
- `SchemaDriftError`: hard fail (column counts, schema_version mismatch,
  required tables missing). Cron exits 1.
- holiday-coverage warning: soft warn via log + a metadata field in the
  output JSON. Doesn't fail the run.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date

from gex_cron_runner import config, rth

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """Hard schema drift — cron should exit 1."""


@dataclass
class SanityReport:
    market_data_ok: bool
    alert_log_ok: bool
    paisa_ok: bool
    holiday_coverage_ok: bool
    holiday_warning: str | None = None
    notes: list[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.notes is None:
            self.notes = []

    @property
    def all_ok(self) -> bool:
        # holiday coverage is a soft warning, not a hard fail
        return self.market_data_ok and self.alert_log_ok and self.paisa_ok


def check_market_data(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    """Verify market_data.db schema_version matches pin."""
    notes: list[str] = []
    try:
        row = conn.execute("SELECT MAX(version) FROM _schema_version").fetchone()
    except sqlite3.OperationalError as e:
        return False, [f"market_data: _schema_version table missing or unreadable: {e}"]
    if row is None or row[0] is None:
        return False, ["market_data: _schema_version is empty"]
    version = row[0]
    expected = config.SCHEMA_PINS["market_data_version"]
    if version != expected:
        return False, [f"market_data: _schema_version={version}, expected {expected}"]
    notes.append(f"market_data schema version {version} OK")
    return True, notes


def check_alert_log(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    """Verify alert_log table column count matches pin (41 per
    INTEGRATION.md@eec441f)."""
    try:
        cols = conn.execute("PRAGMA table_info(alert_log)").fetchall()
    except sqlite3.OperationalError as e:
        return False, [f"alert_log: table missing or unreadable: {e}"]
    if not cols:
        return False, ["alert_log: PRAGMA table_info returned empty"]
    n = len(cols)
    expected = config.SCHEMA_PINS["alert_log_columns"]
    if n != expected:
        col_names = [c[1] for c in cols]
        return False, [
            f"alert_log: column count = {n}, expected {expected}. "
            f"Schema may have drifted since INTEGRATION.md@eec441f. "
            f"Columns: {col_names}"
        ]
    return True, [f"alert_log column count {n} OK (matches INTEGRATION.md@eec441f pin)"]


def check_paisa(conn: sqlite3.Connection) -> tuple[bool, list[str]]:
    """Verify paisa critical tables are present."""
    required = config.SCHEMA_PINS["paisa_required_tables"]
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    present = {r[0] for r in rows}
    missing = [t for t in required if t not in present]
    if missing:
        return False, [f"paisa: missing required tables: {missing}"]
    notes = [f"paisa critical tables OK ({len(required)} required, all present)"]
    # Soft check: gex_signals should have skip_reason column
    sig_cols = {c[1] for c in conn.execute("PRAGMA table_info(gex_signals)").fetchall()}
    if "skip_reason" not in sig_cols:
        return False, ["paisa: gex_signals.skip_reason column missing"]
    if "advisor_alert_id" not in {c[1] for c in conn.execute("PRAGMA table_info(gex_positions)").fetchall()}:
        return False, ["paisa: gex_positions.advisor_alert_id column missing — cross-DB join broken"]
    return True, notes


def run_sanity_checks(
    conn_market_data: sqlite3.Connection,
    conn_alert_log: sqlite3.Connection,
    conn_paisa: sqlite3.Connection,
    today_et: date | None = None,
) -> SanityReport:
    """Run all schema sanity checks. Returns a SanityReport.

    Caller should `if not report.all_ok: raise SchemaDriftError(...)` to exit
    on hard drift. The holiday-coverage check is a soft warning — surface in
    output JSON metadata but don't fail.
    """
    notes: list[str] = []

    md_ok, md_notes = check_market_data(conn_market_data)
    notes.extend(md_notes)

    al_ok, al_notes = check_alert_log(conn_alert_log)
    notes.extend(al_notes)

    pa_ok, pa_notes = check_paisa(conn_paisa)
    notes.extend(pa_notes)

    # Holiday coverage (soft)
    today = today_et or date.today()
    holidays = rth.load_holidays(conn_market_data)
    hc_ok, hc_warn = rth.is_holiday_coverage_ok(holidays, today)

    report = SanityReport(
        market_data_ok=md_ok,
        alert_log_ok=al_ok,
        paisa_ok=pa_ok,
        holiday_coverage_ok=hc_ok,
        holiday_warning=hc_warn,
        notes=notes,
    )

    for n in notes:
        log.info("sanity: %s", n)
    if hc_warn:
        log.warning("sanity (holiday-coverage soft warn): %s", hc_warn)
    if not report.all_ok:
        log.error("sanity: hard drift detected — cron should exit 1")

    return report
