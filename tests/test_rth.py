"""RTH derivation tests. Pure logic — no DB."""
from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from gex_cron_runner import config, rth

ET = ZoneInfo("America/New_York")


def at(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=ET)


HOLIDAYS_EMPTY: set[str] = set()
HOLIDAYS_WITH_JULY4 = {"2026-07-03", "2026-12-25", "2027-01-01"}


def test_weekend_is_not_rth():
    # 2026-05-09 is a Saturday
    sat = at(2026, 5, 9, 12, 0)
    state = rth.get_rth_state(sat, HOLIDAYS_EMPTY)
    assert state.is_rth is False
    assert "weekend" in state.reason


def test_inside_rth_on_a_weekday():
    weekday = at(2026, 5, 7, 14, 30)
    state = rth.get_rth_state(weekday, HOLIDAYS_EMPTY)
    assert state.is_rth is True
    assert state.close_time_et == "16:00"
    assert state.reason == "RTH"


def test_pre_open():
    early = at(2026, 5, 7, 8, 0)
    state = rth.get_rth_state(early, HOLIDAYS_EMPTY)
    assert state.is_rth is False
    assert "pre-open" in state.reason


def test_post_close():
    late = at(2026, 5, 7, 17, 0)
    state = rth.get_rth_state(late, HOLIDAYS_EMPTY)
    assert state.is_rth is False
    assert "post-close" in state.reason


def test_holiday_blocks_rth():
    holiday = at(2026, 12, 25, 11, 0)
    state = rth.get_rth_state(holiday, HOLIDAYS_WITH_JULY4)
    assert state.is_rth is False
    assert "holiday" in state.reason


def test_half_day_short_close(monkeypatch):
    """If the half-day map says 13:00 close, 13:30 should not be RTH."""
    monkeypatch.setitem(config.HALF_DAY_CLOSE_ET, "2026-07-03", "13:00")
    weekday = at(2026, 7, 3, 13, 30)  # 13:30 ET on a half-day
    state = rth.get_rth_state(weekday, HOLIDAYS_EMPTY)
    assert state.is_rth is False
    assert state.close_time_et == "13:00"
    assert "post-close" in state.reason


def test_half_day_during_window(monkeypatch):
    monkeypatch.setitem(config.HALF_DAY_CLOSE_ET, "2026-07-03", "13:00")
    weekday = at(2026, 7, 3, 12, 0)  # noon, before 13:00 close
    state = rth.get_rth_state(weekday, HOLIDAYS_EMPTY)
    assert state.is_rth is True
    assert state.close_time_et == "13:00"


def test_naive_datetime_rejected():
    naive = datetime(2026, 5, 7, 14, 30)
    with pytest.raises(ValueError, match="tz-aware"):
        rth.get_rth_state(naive, HOLIDAYS_EMPTY)


def test_holiday_coverage_ok():
    today = date(2026, 5, 7)
    holidays = {"2027-01-01", "2027-12-25"}  # plenty of lookahead
    ok, msg = rth.is_holiday_coverage_ok(holidays, today)
    assert ok is True
    assert msg is None


def test_holiday_coverage_stale():
    today = date(2026, 12, 1)
    holidays = {"2026-12-25"}  # only 24 days lookahead, < 60
    ok, msg = rth.is_holiday_coverage_ok(holidays, today, lookahead_days=60)
    assert ok is False
    assert "reseed" in msg


def test_holiday_coverage_empty():
    today = date(2026, 5, 7)
    ok, msg = rth.is_holiday_coverage_ok(set(), today)
    assert ok is False
    assert "empty" in msg


def test_dst_transition_handled():
    """DST starts second Sunday in March, ends first Sunday in November.
    ZoneInfo handles this — verify a March RTH timestamp lands correctly.
    """
    # 2026-03-08 is the DST start (2nd Sunday). 2026-03-09 (Mon) at 10am should be EDT.
    monday_after_dst = at(2026, 3, 9, 10, 0)
    state = rth.get_rth_state(monday_after_dst, HOLIDAYS_EMPTY)
    assert state.is_rth is True
    # UTC offset is -4 (EDT) not -5 (EST)
    assert monday_after_dst.utcoffset().total_seconds() == -4 * 3600
