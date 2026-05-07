"""JSON v1 builders for daily and live rollup payloads.

Builds plain dicts and validates against the inline jsonschema spec. Caller
serializes via `json.dumps(..., separators=(",", ":"))` for compact output.

Schema lock: schema_version=1. Any breaking change bumps to 2 and udayx
client must handle both.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

import jsonschema

# ---------------------------------------------------------------------------
# Daily JSON v1 schema (subset — focus on top-level shape; sub-objects are
# permissive to keep iteration fast). Strict-validation can come later.
# ---------------------------------------------------------------------------
DAILY_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "schema_version", "date", "tz", "market_status", "generated_at",
        "source_commits", "account", "alerts", "funnel", "execution",
    ],
    "properties": {
        "schema_version": {"const": 1},
        "date": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
        "tz": {"const": "America/New_York"},
        "market_status": {"enum": ["open", "closed_weekend", "closed_holiday"]},
        "rth_window_et": {
            "anyOf": [
                {"type": "null"},
                {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 2},
            ],
        },
        "generated_at": {"type": "string"},
        "source_commits": {
            "type": "object",
            "required": ["paisamaker", "gex_advisor", "cron_runner"],
        },
        "account": {
            "type": "object",
            "required": ["broker", "mode"],
            "properties": {"broker": {"const": "IBKR"}, "mode": {"const": "paper"}},
        },
        "alerts": {"type": "object"},
        "funnel": {"type": "array"},
        "execution": {"type": "object"},
        "metadata": {"type": "object"},
    },
}


# ---------------------------------------------------------------------------
# Live JSON v1 schema
# ---------------------------------------------------------------------------
LIVE_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": [
        "schema_version", "mode", "date", "tz", "is_rth", "generated_at",
        "next_refresh_seconds", "source_commits",
    ],
    "properties": {
        "schema_version": {"const": 1},
        "mode": {"const": "live"},
        "date": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
        "tz": {"const": "America/New_York"},
        "is_rth": {"type": "boolean"},
        "rth_window_et": {
            "anyOf": [
                {"type": "null"},
                {"type": "array", "items": {"type": "string"}, "minItems": 2, "maxItems": 2},
            ],
        },
        "generated_at": {"type": "string"},
        "next_refresh_seconds": {"type": "integer"},
        "source_commits": {"type": "object"},
        "spot": {"type": "object"},
        "alerts_recent_30m": {"type": "array"},
        "open_positions": {"type": "array"},
        "session_so_far": {"type": "object"},
    },
}


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_daily(
    *,
    date_iso: str,
    market_status: str,
    rth_window_et: tuple[str, str] | None,
    source_commits: dict[str, str],
    alerts_block: dict[str, Any],
    funnel: list[dict[str, Any]],
    execution: dict[str, Any],
    holiday_warning: str | None = None,
    bundle_1_warning: bool = False,
) -> dict[str, Any]:
    """Build the daily JSON dict. Caller passes already-computed sub-blocks."""
    payload: dict[str, Any] = {
        "schema_version": 1,
        "date": date_iso,
        "tz": "America/New_York",
        "market_status": market_status,
        "rth_window_et": list(rth_window_et) if rth_window_et else None,
        "generated_at": _now_iso_utc(),
        "source_commits": source_commits,
        "account": {"broker": "IBKR", "mode": "paper"},
        "alerts": alerts_block,
        "funnel": funnel,
        "execution": execution,
    }
    metadata: dict[str, Any] = {}
    if holiday_warning:
        metadata["holiday_coverage_warning"] = holiday_warning
    if bundle_1_warning:
        metadata["bundle_1_caveat"] = (
            "Some rendered alerts have fired_at < 2026-05-05T18:00:00Z; "
            "chain-enrichment columns may be None or stale."
        )
    if metadata:
        payload["metadata"] = metadata
    validate_daily(payload)
    return payload


def build_live(
    *,
    today_iso: str,
    is_rth: bool,
    rth_window_et: tuple[str, str] | None,
    source_commits: dict[str, str],
    spot: dict[str, dict[str, Any]] | None = None,
    alerts_recent_30m: list[dict[str, Any]] | None = None,
    open_positions: list[dict[str, Any]] | None = None,
    session_so_far: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the live.json dict.

    When `is_rth=False`, only the metadata fields are populated (per plan §6b
    'Outside RTH' note). The cron writes one such payload at RTH close to flip
    the client.
    """
    payload: dict[str, Any] = {
        "schema_version": 1,
        "mode": "live",
        "date": today_iso,
        "tz": "America/New_York",
        "is_rth": is_rth,
        "rth_window_et": list(rth_window_et) if rth_window_et else None,
        "generated_at": _now_iso_utc(),
        "next_refresh_seconds": 30,
        "source_commits": source_commits,
    }
    if is_rth:
        payload["spot"] = spot or {}
        payload["alerts_recent_30m"] = alerts_recent_30m or []
        payload["open_positions"] = open_positions or []
        payload["session_so_far"] = session_so_far or {
            "alerts_emitted": 0,
            "funnel": [],
            "positions_opened": 0,
            "positions_closed": 0,
            "wins": 0, "losses": 0, "break_even": 0,
            "paper_pnl_usd": 0.0,
            "ibkr_disconnects": 0,
            "current_mode": "UNKNOWN",
            "kill_switch_state": "UNKNOWN",
            "trades": [],
        }
    else:
        # Closed-market lite payload — fields client uses to fall through to rollup
        payload["spot"] = {}
        payload["alerts_recent_30m"] = []
        payload["open_positions"] = []
        payload["session_so_far"] = {
            "alerts_emitted": 0,
            "funnel": [],
            "positions_opened": 0,
            "positions_closed": 0,
            "wins": 0, "losses": 0, "break_even": 0,
            "paper_pnl_usd": 0.0,
            "ibkr_disconnects": 0,
            "current_mode": None,
            "kill_switch_state": None,
            "trades": [],
        }
    validate_live(payload)
    return payload


def build_closed_market_daily(
    *,
    date_iso: str,
    market_status: str,
    source_commits: dict[str, str],
) -> dict[str, Any]:
    """Convenience: build the minimal closed-market daily payload."""
    return build_daily(
        date_iso=date_iso,
        market_status=market_status,
        rth_window_et=None,
        source_commits=source_commits,
        alerts_block={
            "emitted_prod": 0, "is_test_excluded": 0,
            "by_type": {}, "tickers": [], "rows": [],
        },
        funnel=[],
        execution={
            "positions_opened": 0, "positions_closed": 0, "positions_void": 0,
            "wins": 0, "losses": 0, "break_even": 0,
            "paper_pnl_usd": 0.0, "max_drawdown_usd": 0.0,
            "ibkr_disconnects": 0,
            "end_of_session_mode": None,
            "eod_flatten_count": 0, "trades": [],
        },
    )


def validate_daily(payload: dict[str, Any]) -> None:
    """Raise jsonschema.ValidationError on shape mismatch."""
    jsonschema.validate(payload, DAILY_SCHEMA)


def validate_live(payload: dict[str, Any]) -> None:
    jsonschema.validate(payload, LIVE_SCHEMA)


def trigger_features_for_alert(alert_row: dict[str, Any]) -> dict[str, Any]:
    """Extract the 3 trigger features for an alert based on alert_type + ticker.

    Per gex-advisor reply (handoff/03 §5):
    - SPX uses 3-force (force_align column).
    - Non-SPX uses 5-force (features_json.$.force_align_5).
    - Some features live in features_json (vanna_accel, gamma_reclaim_confirmed,
      regime_strength_slope, gamma_reclaim_exit_reason).
    """
    import json as _json
    from gex_cron_runner import config

    alert_type = alert_row.get("alert_type", "")
    ticker = alert_row.get("ticker", "SPX")
    features_json_str = alert_row.get("features_json") or "{}"
    try:
        features_json = _json.loads(features_json_str)
    except _json.JSONDecodeError:
        features_json = {}

    # Pick recipe key
    if alert_type == "CASCADE_WATCH":
        recipe_key = "CASCADE_WATCH_SPX" if ticker == "SPX" else "CASCADE_WATCH_NON_SPX"
    else:
        recipe_key = alert_type
    recipe = config.TRIGGER_FEATURES.get(recipe_key, [])

    out: dict[str, Any] = {}
    for name, source in recipe:
        if source == "col":
            v = alert_row.get(name)
        else:  # "json"
            v = features_json.get(name)
        if v is not None:
            out[name] = v
    return out


def alert_row_for_json(row: dict[str, Any]) -> dict[str, Any]:
    """Convert an alert_log row dict into the JSON output shape."""
    return {
        "id": row["id"],
        "fired_at": row.get("fired_at"),
        "ticker": row.get("ticker"),
        "alert_type": row.get("alert_type"),
        "direction": row.get("direction"),
        "spot": row.get("spot"),
        "target_price": row.get("target_price"),
        "stop_price": row.get("stop_price"),
        "triggers": trigger_features_for_alert(row),
        "outcome": _alert_outcome(row),
    }


def _alert_outcome(row: dict[str, Any]) -> str:
    """Per gex-advisor reply: outcome semantics."""
    if row.get("target_hit") == 1:
        return "target_hit"
    if row.get("stop_hit") == 1:
        return "stop_hit"
    if row.get("outcome_backfilled") == 0 and row.get("target_hit") is None and row.get("stop_hit") is None:
        return "pending"
    return "unresolved"


def trade_row_for_json(pos: dict[str, Any], alert: dict[str, Any] | None) -> dict[str, Any]:
    """Convert a gex_positions row into the JSON output shape.

    Per paisa reply: hold_minutes derived from julianday() math; right/strike
    parsed from contract_symbol; pnl is the only P&L column (filter shadow_mode=0
    for real fills).
    """
    contract_symbol = pos.get("contract_symbol", "")
    right, strike = _parse_contract(contract_symbol)
    return {
        "entry_time": pos.get("entry_time"),
        "exit_time": pos.get("exit_time"),
        "hold_minutes": pos.get("hold_minutes"),  # caller derives in SQL
        "ticker": pos.get("ticker"),
        "right": right,
        "strike": strike,
        "expiry": _parse_expiry(contract_symbol, pos.get("entry_time")),
        "entry_px": pos.get("entry_fill_price") or pos.get("entry_price"),
        "exit_px": pos.get("exit_price"),
        "qty": pos.get("quantity", 1),
        "pnl_paper": pos.get("pnl"),
        "exit_reason": pos.get("exit_reason"),
        "source": pos.get("source"),
        "alert_id": pos.get("advisor_alert_id"),
        "dispatched": pos.get("entry_exec_status") == "filled",
    }


def _parse_contract(symbol: str) -> tuple[str | None, int | None]:
    """Parse 'SPXW 7305P' or 'QQQ 694P' into (right, strike)."""
    if not symbol:
        return None, None
    parts = symbol.strip().split()
    if not parts:
        return None, None
    last = parts[-1]
    # Strike+right is the last token, e.g., '7305P' or '694C'
    if not last:
        return None, None
    right_char = last[-1].upper()
    if right_char not in ("C", "P"):
        return None, None
    try:
        strike = int(last[:-1])
    except ValueError:
        return None, None
    return right_char, strike


def _parse_expiry(symbol: str, entry_time_iso: str | None) -> str | None:
    """0DTE: expiry == date(entry_time). Per paisa schema, expiry isn't stored
    as a separate column. Derive from entry_time (UTC ISO 8601 → ET date).
    For non-0DTE in the future we'd need actual expiry; for v1 assume 0DTE.
    """
    if not entry_time_iso:
        return None
    try:
        dt = datetime.fromisoformat(entry_time_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    # Convert UTC entry_time to ET date (0DTE expiry is the trading-day date in ET)
    from zoneinfo import ZoneInfo
    return dt.astimezone(ZoneInfo("America/New_York")).date().isoformat()
