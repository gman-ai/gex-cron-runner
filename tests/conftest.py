"""Shared fixtures: in-memory SQLite DBs that mirror real schemas, populated
from the SCP'd sample data."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def _make_market_data_db() -> sqlite3.Connection:
    """Build an in-memory market_data.db with version=6 + holidays."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE _schema_version (version INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO _schema_version (version) VALUES (6)")
    conn.execute("""
        CREATE TABLE market_holidays (
            date TEXT PRIMARY KEY,
            exchange TEXT NOT NULL DEFAULT 'NYSE',
            name TEXT
        )
    """)
    # Seed a few real NYSE 2026 holidays + plenty of lookahead
    holidays = [
        ("2026-01-01", "NYSE", "New Year's Day"),
        ("2026-04-03", "NYSE", "Good Friday"),
        ("2026-12-25", "NYSE", "Christmas Day"),
        ("2027-01-01", "NYSE", "New Year's Day"),
    ]
    conn.executemany(
        "INSERT INTO market_holidays VALUES (?,?,?)", holidays
    )
    conn.execute("""
        CREATE TABLE instrument_registry (
            instrument_id INTEGER PRIMARY KEY,
            symbol TEXT,
            asset_type TEXT
        )
    """)
    conn.executemany("INSERT INTO instrument_registry VALUES (?,?,?)", [
        (1, "SPX", "index"),
        (2, "SPY", "etf"),
        (3, "QQQ", "etf"),
        (4, "TSLA", "equity"),
        (5, "NVDA", "equity"),
    ])
    conn.execute("""
        CREATE TABLE scalar_readings (
            id INTEGER PRIMARY KEY,
            instrument_id INTEGER,
            source_id INTEGER,
            ingest_mode TEXT,
            spot REAL,
            stored_at TEXT,
            event_at TEXT,
            session_date TEXT
        )
    """)
    # Seed one fresh spot per ticker
    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc).date().isoformat()
    conn.executemany(
        "INSERT INTO scalar_readings (instrument_id, source_id, ingest_mode, "
        "spot, stored_at, event_at, session_date) VALUES (?,?,?,?,?,?,?)",
        [
            (1, 1, "live", 7337.50, now_iso, now_iso, today),
            (2, 1, "live", 705.20, now_iso, now_iso, today),
            (3, 1, "live", 580.10, now_iso, now_iso, today),
            (4, 1, "live", 308.40, now_iso, now_iso, today),
            (5, 1, "live", 178.20, now_iso, now_iso, today),
        ],
    )
    conn.commit()
    return conn


def _make_alert_log_db() -> sqlite3.Connection:
    """Build an in-memory alert_log.db with the actual 41-column schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT,
            alert_class TEXT,
            date_str TEXT,
            fired_at TEXT,
            spot REAL,
            gex_vol REAL,
            cascade_conviction INTEGER,
            force_align INTEGER,
            session_minute INTEGER,
            time_bucket TEXT,
            dist_to_magnet REAL,
            dist_to_wall REAL,
            charm_accel REAL,
            gexoflow REAL,
            spot_roc_5 REAL,
            cascade_exhausting INTEGER,
            spot_range_pct REAL,
            direction TEXT,
            target_price REAL,
            stop_price REAL,
            send_ok INTEGER,
            outcome_backfilled INTEGER,
            outcome_version INTEGER,
            backfilled_at TEXT,
            mfe REAL,
            mae REAL,
            mfe_minutes INTEGER,
            target_hit INTEGER,
            stop_hit INTEGER,
            target_first INTEGER,
            close_move REAL,
            move_5m REAL,
            move_15m REAL,
            move_30m REAL,
            eod_spot REAL,
            max_gap_seconds INTEGER,
            data_gap_flag INTEGER,
            is_test INTEGER,
            features_json TEXT,
            ticker TEXT DEFAULT 'SPX'
        )
    """)
    conn.execute("CREATE INDEX idx_alert_log_date ON alert_log(date_str)")
    conn.execute("CREATE INDEX idx_alert_log_type ON alert_log(alert_type)")
    conn.execute("CREATE INDEX idx_alert_log_ticker ON alert_log(ticker)")
    # Seed a few representative alerts
    conn.execute(
        """INSERT INTO alert_log
           (alert_type, date_str, fired_at, spot, gex_vol, cascade_conviction,
            force_align, direction, target_price, stop_price, is_test, ticker,
            features_json, target_hit, outcome_backfilled)
           VALUES ('CASCADE_WATCH', '2026-05-06', '2026-05-06T14:14:32+00:00',
                   7048.40, -2410.5, 4, 3, 'DOWN', 7022.00, 7060.00, 0, 'SPX',
                   '{}', 1, 1)"""
    )
    conn.execute(
        """INSERT INTO alert_log
           (alert_type, date_str, fired_at, spot, gex_vol, direction,
            target_price, stop_price, is_test, ticker, features_json,
            stop_hit, outcome_backfilled)
           VALUES ('CHARM_SQUEEZE', '2026-05-06', '2026-05-06T16:08:14+00:00',
                   308.40, -120.0, 'UP', 312.20, 306.80, 0, 'TSLA',
                   '{"vanna_accel": 1.18}', 1, 1)"""
    )
    conn.execute(
        """INSERT INTO alert_log
           (alert_type, date_str, fired_at, is_test, ticker, features_json)
           VALUES ('SHADOW_VANNA_SPIKE', '2026-05-06', '2026-05-06T15:00:00+00:00',
                   0, 'SPX', '{}')"""
    )
    conn.execute(
        """INSERT INTO alert_log
           (alert_type, date_str, fired_at, is_test, ticker, features_json)
           VALUES ('CASCADE_ALERT_EXHAUSTING', '2026-05-06', '2026-05-06T15:30:00+00:00',
                   0, 'SPX', '{}')"""
    )
    conn.commit()
    return conn


def _make_paisa_db() -> sqlite3.Connection:
    """Build an in-memory paisa db with the tables we touch."""
    conn = sqlite3.connect(":memory:")
    # Enable WAL? Not in :memory:. Just make the schemas right.
    conn.execute("""
        CREATE TABLE gex_signals (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            signal_type TEXT,
            direction TEXT,
            spot REAL,
            zero_gamma REAL,
            target REAL,
            gex_vol REAL,
            gex_vol_prev REAL,
            gex_vol_delta REAL,
            gex_oi REAL,
            oi_vol_agree INTEGER,
            reading_gap_seconds INTEGER,
            reading_age_seconds INTEGER,
            readings_today INTEGER,
            executed INTEGER,
            skip_reason TEXT,
            notes TEXT,
            reading_id INTEGER,
            created_at TEXT,
            source TEXT DEFAULT 'gexwatch',
            ticker TEXT DEFAULT 'SPX'
        )
    """)
    conn.execute("""
        CREATE TABLE gex_positions (
            id INTEGER PRIMARY KEY,
            signal_id INTEGER,
            contract_symbol TEXT,
            direction TEXT,
            entry_time TEXT,
            entry_price REAL,
            quantity INTEGER,
            entry_spot REAL,
            target_level REAL,
            first_sell_price REAL,
            first_sell_filled INTEGER,
            first_sell_time TEXT,
            exit_time TEXT,
            exit_price REAL,
            exit_reason TEXT,
            exit_spot REAL,
            decel_count INTEGER,
            poll_count INTEGER,
            pnl REAL,
            status TEXT,
            shadow_mode INTEGER,
            created_at TEXT,
            trade_signal_id INTEGER,
            con_id INTEGER,
            exit_price_source TEXT,
            exit_bid REAL,
            exit_ask REAL,
            exit_mid REAL,
            entry_order_id INTEGER,
            entry_fill_price REAL,
            entry_exec_status TEXT,
            exit_order_id INTEGER,
            exit_exec_status TEXT,
            exit_submitted_at TEXT,
            exit_lmt_fail_count INTEGER,
            exit_cancel_requested_at TEXT,
            entry_order_submitted_at TEXT,
            entry_quote_bid REAL,
            entry_quote_ask REAL,
            entry_quote_mid REAL,
            entry_quote_fetched_at TEXT,
            entry_cancel_requested_at TEXT,
            entry_cancel_status TEXT,
            entry_cancel_attempts INTEGER,
            source TEXT DEFAULT 'gexwatch',
            ticker TEXT DEFAULT 'SPX',
            advisor_alert_id INTEGER,
            advisor_target_price REAL,
            advisor_stop_price REAL,
            tightened_stop_level REAL,
            entry_delta REAL,
            entry_iv REAL,
            fill_quality TEXT,
            fill_vs_mid REAL,
            slippage_pct REAL,
            entry_pricing_mode TEXT,
            entry_limit_price REAL
        )
    """)
    conn.execute("CREATE UNIQUE INDEX idx_gex_positions_advisor_alert ON gex_positions(advisor_alert_id) WHERE advisor_alert_id IS NOT NULL")
    conn.execute("""
        CREATE TABLE gex_execution_log (
            id INTEGER PRIMARY KEY,
            position_id INTEGER,
            action TEXT,
            order_id INTEGER,
            contract_symbol TEXT,
            direction TEXT,
            requested_qty INTEGER,
            limit_price REAL,
            fill_price REAL,
            fill_qty INTEGER,
            status TEXT,
            gate_results TEXT,
            error_message TEXT,
            created_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE pipeline_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE bot_state (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT
        )
    """)
    # Seed from the actual paisa fixtures
    sigs = json.loads((FIXTURES / "cron_sample_signals.json").read_text())
    for s in sigs:
        cols = ",".join(s.keys())
        placeholders = ",".join("?" * len(s))
        conn.execute(f"INSERT INTO gex_signals ({cols}) VALUES ({placeholders})", list(s.values()))
    pos = json.loads((FIXTURES / "cron_sample_positions.json").read_text())
    for p in pos:
        # Some columns in the fixture aren't in our minimal schema if added recently;
        # filter to known columns
        known_cols = {c[1] for c in conn.execute("PRAGMA table_info(gex_positions)").fetchall()}
        filtered = {k: v for k, v in p.items() if k in known_cols}
        cols = ",".join(filtered.keys())
        placeholders = ",".join("?" * len(filtered))
        conn.execute(f"INSERT INTO gex_positions ({cols}) VALUES ({placeholders})", list(filtered.values()))
    conn.commit()
    return conn


@pytest.fixture
def market_data_db():
    return _make_market_data_db()


@pytest.fixture
def alert_log_db():
    return _make_alert_log_db()


@pytest.fixture
def paisa_db():
    return _make_paisa_db()
