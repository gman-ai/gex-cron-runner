"""Regular Trading Hours (RTH) and holiday derivation.

Reimplemented in this repo per market-data-fetcher reply (don't import from
fetcher; ~30 LOC standalone). Uses ZoneInfo for proper DST handling. Holidays
read from market_data.db; half-days from the cron-owned HALF_DAY_CLOSE_ET map
(market_holidays schema doesn't carry a close_time column).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from gex_cron_runner import config

ET = ZoneInfo("America/New_York")
RTH_OPEN = time(9, 30)
RTH_FULL_CLOSE = time(16, 0)


@dataclass(frozen=True)
class RthState:
    is_rth: bool
    close_time_et: str | None  # "16:00" or "13:00" on half-days; None when not RTH
    reason: str  # human-readable reason for the state ("weekend", "holiday X", "RTH", "post-close", etc.)


def load_holidays(conn_market_data: sqlite3.Connection) -> set[str]:
    """Load NYSE full-day holidays from market_data.db.market_holidays.

    Returns a set of ISO date strings ('2026-12-25'). Per fetcher reply, the
    table is keyed by date PRIMARY KEY with TEXT values in ISO 8601.
    """
    rows = conn_market_data.execute(
        "SELECT date FROM market_holidays WHERE exchange = 'NYSE'"
    ).fetchall()
    return {r[0] for r in rows}


def half_day_close(d: date) -> str | None:
    """Return half-day close time HH:MM ET, or None for full-day sessions."""
    return config.HALF_DAY_CLOSE_ET.get(d.isoformat())


def get_rth_state(now_et: datetime, holidays: set[str]) -> RthState:
    """Determine if we're inside RTH right now.

    Args:
        now_et: A timezone-aware datetime in ET (use `datetime.now(ET)`).
        holidays: Set of ISO date strings from `load_holidays()`.

    Returns:
        RthState with is_rth boolean and close-time hint.
    """
    if now_et.tzinfo is None or now_et.utcoffset().total_seconds() != ET.utcoffset(now_et).total_seconds():
        # Defensive: caller passed a naive or wrong-tz datetime. Reject.
        raise ValueError(f"now_et must be tz-aware in ET, got tzinfo={now_et.tzinfo}")

    today = now_et.date()
    iso = today.isoformat()

    # Weekend
    if today.weekday() >= 5:  # 5=Sat, 6=Sun
        return RthState(False, None, f"weekend ({today.strftime('%A').lower()})")

    # Full-day holiday
    if iso in holidays:
        return RthState(False, None, f"holiday ({iso})")

    # Half-day close override
    half = half_day_close(today)
    close_str = half if half else "16:00"
    hh, mm = map(int, close_str.split(":"))
    close_t = time(hh, mm)

    now_t = now_et.time()
    if now_t < RTH_OPEN:
        return RthState(False, close_str, f"pre-open (RTH starts {close_str} early-close: {RTH_OPEN.strftime('%H:%M')})" if half else "pre-open")
    if now_t > close_t:
        return RthState(False, close_str, "post-close")
    # Inside RTH window
    return RthState(True, close_str, "RTH")


def is_holiday_coverage_ok(holidays: set[str], today_et: date, lookahead_days: int = 60) -> tuple[bool, str | None]:
    """Check that market_holidays coverage extends past today + lookahead.

    Returns (ok, warning_message). `ok=False` means startup should warn loud
    (not fail) — a stale holiday table won't crash the cron, but it could
    cause us to render "open" for a date that's actually a holiday.
    """
    if not holidays:
        return False, "market_holidays table is empty"
    max_date = max(holidays)
    deadline = (today_et.toordinal() + lookahead_days)
    max_ordinal = date.fromisoformat(max_date).toordinal()
    if max_ordinal < deadline:
        return False, (
            f"market_holidays MAX(date)={max_date} is < today + {lookahead_days}d. "
            f"Annual reseed needed (per market-data-fetcher reply)."
        )
    return True, None
