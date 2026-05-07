"""SQL queries for daily + live rollup. All read-only, all parameterized.

Centralized here so the daily/live writers stay focused on orchestration and
the SQL is easy to review for the read-only contract.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from gex_cron_runner import config


# ---------------------------------------------------------------------------
# Daily queries (run inside snapshot transaction, source: alert_log + paisa)
# ---------------------------------------------------------------------------

def query_alerts_emitted(conn_paisa, date_iso: str) -> list[sqlite3.Row]:
    """All production alerts for date D (in ET via date_str).

    Filter: is_test=0 AND alert_type NOT LIKE 'SHADOW_%' AND alert_type NOT IN
    ('STRUCTURE_BREAK', 'STRUCTURE_BRIEF') — all three required per gex-advisor reply.

    Includes both entry-types and exit-types; caller computes
    `entry_type_excluded` from this list.
    """
    sql = f"""
        SELECT
            id, alert_type, alert_class, date_str, fired_at, ticker,
            spot, gex_vol, cascade_conviction, force_align,
            charm_accel, dist_to_magnet, dist_to_wall, cascade_exhausting,
            direction, target_price, stop_price,
            target_hit, stop_hit, mfe, mae, outcome_backfilled,
            features_json, is_test
        FROM adv.alert_log
        WHERE date_str = ?
          AND {config.PROD_ALERT_FILTER_SQL}
        ORDER BY fired_at ASC
    """
    return list(conn_paisa.execute(sql, (date_iso,)))


def query_upstream_received_count(conn_paisa, date_iso: str) -> int:
    """Count of gex_signals (any executed value) where source='advisor' for date D."""
    row = conn_paisa.execute(
        "SELECT COUNT(*) FROM gex_signals "
        "WHERE date(timestamp) = ? AND source = 'advisor'",
        (date_iso,),
    ).fetchone()
    return int(row[0]) if row else 0


def query_skip_counts(conn_paisa, date_iso: str) -> dict[str, int]:
    """skip_reason → count for advisor-source signals that were skipped on D.

    Only `executed=-1` rows (skipped) — `executed=0` is transient (paisa reply
    correction) and `executed=1` is opened. We exclude executed=0 to avoid
    double-counting transient state.
    """
    rows = conn_paisa.execute(
        "SELECT skip_reason, COUNT(*) FROM gex_signals "
        "WHERE date(timestamp) = ? AND source = 'advisor' AND executed = -1 "
        "AND skip_reason IS NOT NULL "
        "GROUP BY skip_reason",
        (date_iso,),
    ).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def query_positions_for_date(conn_paisa, date_iso: str) -> list[sqlite3.Row]:
    """All advisor-source positions opened on date D, real fills only.

    Use status filter (per paisa reply: status='open' / 'closed' / 'void' / 'partial').
    Filter shadow_mode=0 for real IBKR fills (shadow_mode=1 = synthetic sim).
    Hold minutes derived in SQL via julianday() math.
    """
    rows = conn_paisa.execute("""
        SELECT
            id, signal_id, contract_symbol, direction,
            entry_time, entry_price, entry_fill_price, entry_exec_status,
            entry_pricing_mode,
            exit_time, exit_price, exit_reason, exit_exec_status,
            quantity, status, shadow_mode,
            advisor_alert_id, source, ticker, pnl,
            entry_delta, fill_quality, slippage_pct, con_id,
            CASE
              WHEN exit_time IS NOT NULL AND entry_time IS NOT NULL THEN
                CAST(ROUND((julianday(exit_time) - julianday(entry_time)) * 1440) AS INTEGER)
              ELSE NULL
            END AS hold_minutes
        FROM gex_positions
        WHERE date(entry_time) = ? AND shadow_mode = 0
        ORDER BY entry_time ASC
    """, (date_iso,))
    return list(rows)


def query_session_mode_at_eod(conn_paisa, date_iso: str) -> str | None:
    """Best-effort: last `current_mode` from gex_signals notes on date D.

    Note: this is approximated. The authoritative source is the heartbeat
    log (parsed by log_parser.parse_current_mode for live mode). For the
    daily rollup, we just record whether the session mode was clean. If
    we have no signal of degradation, we record 'FULLY_LIVE'.
    """
    # In v1, just return 'FULLY_LIVE' for the daily rollup (we don't have a
    # historical mode field). Future: parse end-of-day log entries.
    return "FULLY_LIVE"


# ---------------------------------------------------------------------------
# Live queries (no snapshot transaction; 30s drift acceptable)
# ---------------------------------------------------------------------------

def query_tracked_tickers(conn_market_data) -> dict[str, int]:
    """instrument_registry ⨝ scalar_readings for last 7d → {symbol: instrument_id}.

    Per fetcher reply: don't hardcode the ticker list; query at startup.
    """
    rows = conn_market_data.execute("""
        SELECT instrument_id, symbol FROM instrument_registry
        WHERE instrument_id IN (
            SELECT DISTINCT instrument_id FROM scalar_readings
            WHERE ingest_mode = 'live'
              AND session_date >= date('now', '-7 days')
        )
        ORDER BY symbol
    """).fetchall()
    return {r[1]: int(r[0]) for r in rows}


def query_latest_spot(conn_market_data, instrument_id: int) -> dict[str, Any] | None:
    """Latest spot for one ticker, with stored_at + age_seconds.

    Per fetcher reply (canonical query):
      source_id=1 (classic/zero/majors), ingest_mode='live', ORDER BY event_at DESC.
    Includes age_seconds via julianday() math.
    """
    row = conn_market_data.execute("""
        SELECT spot, stored_at, event_at,
               (julianday('now') - julianday(stored_at)) * 86400 AS age_seconds
        FROM scalar_readings
        WHERE instrument_id = ?
          AND source_id = 1
          AND ingest_mode = 'live'
        ORDER BY event_at DESC
        LIMIT 1
    """, (instrument_id,)).fetchone()
    if row is None:
        return None
    return {
        "spot": float(row[0]) if row[0] is not None else None,
        "stored_at": row[1],
        "event_at": row[2],
        "age_seconds": float(row[3]) if row[3] is not None else None,
    }


def query_alerts_recent_30m(conn_paisa) -> list[sqlite3.Row]:
    """Last 30 min of prod alerts. Two date_str values per gex-advisor reply
    (handles UTC/ET boundary at midnight)."""
    sql = f"""
        SELECT
            id, alert_type, alert_class, date_str, fired_at, ticker,
            spot, gex_vol, cascade_conviction, force_align,
            charm_accel, dist_to_magnet, dist_to_wall, cascade_exhausting,
            direction, target_price, stop_price,
            target_hit, stop_hit, outcome_backfilled,
            features_json
        FROM adv.alert_log
        WHERE date_str IN (date('now', '-1 day'), date('now'))
          AND fired_at > datetime('now', '-30 minutes')
          AND {config.PROD_ALERT_FILTER_SQL}
        ORDER BY fired_at DESC
    """
    return list(conn_paisa.execute(sql))


def query_open_positions(conn_paisa) -> list[sqlite3.Row]:
    """Confirmed-live open positions only.

    Per paisa reply: filter by status='open' (NOT exit_time IS NULL — voids
    have exit_time set with status='void'). Also filter entry_exec_status='filled'
    to skip the 1–75s 'unknown' transient state during IBKR reconcile.
    """
    rows = conn_paisa.execute("""
        SELECT
            id, signal_id, contract_symbol, direction,
            entry_time, entry_price, entry_fill_price,
            advisor_alert_id, source, ticker, status, shadow_mode,
            entry_pricing_mode, con_id
        FROM gex_positions
        WHERE status = 'open'
          AND entry_exec_status = 'filled'
          AND shadow_mode = 0
        ORDER BY entry_time ASC
    """)
    return list(rows)


def query_session_so_far(conn_paisa, today_iso: str) -> dict[str, Any]:
    """Aggregate today's session: alerts emitted, positions opened/closed,
    P&L, etc. Used in live.json's session_so_far block."""
    # Alerts emitted today (production)
    alerts_row = conn_paisa.execute(f"""
        SELECT COUNT(*) FROM adv.alert_log
        WHERE date_str = ? AND {config.PROD_ALERT_FILTER_SQL}
    """, (today_iso,)).fetchone()
    alerts_emitted = int(alerts_row[0]) if alerts_row else 0

    # Position aggregates
    pos_agg = conn_paisa.execute("""
        SELECT
            SUM(CASE WHEN entry_exec_status = 'filled' THEN 1 ELSE 0 END) AS opened,
            SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed,
            SUM(CASE WHEN status = 'closed' AND pnl > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN status = 'closed' AND pnl < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN status = 'closed' AND pnl = 0 THEN 1 ELSE 0 END) AS break_even,
            COALESCE(SUM(CASE WHEN status = 'closed' THEN pnl ELSE 0 END), 0) AS pnl
        FROM gex_positions
        WHERE date(entry_time) = ? AND shadow_mode = 0
    """, (today_iso,)).fetchone()

    return {
        "alerts_emitted": alerts_emitted,
        "positions_opened": int(pos_agg["opened"] or 0),
        "positions_closed": int(pos_agg["closed"] or 0),
        "wins": int(pos_agg["wins"] or 0),
        "losses": int(pos_agg["losses"] or 0),
        "break_even": int(pos_agg["break_even"] or 0),
        "paper_pnl_usd": float(pos_agg["pnl"] or 0.0),
    }
