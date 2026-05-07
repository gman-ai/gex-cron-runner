"""Schema builder tests + jsonschema validation."""
from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from gex_cron_runner import schema

FIXTURES = Path(__file__).parent / "fixtures"


def test_build_daily_validates():
    p = schema.build_daily(
        date_iso="2026-05-06",
        market_status="open",
        rth_window_et=("09:30", "16:00"),
        source_commits={"paisamaker": "abc", "gex_advisor": "def", "cron_runner": "ghi"},
        alerts_block={"emitted_prod": 0, "is_test_excluded": 0, "by_type": {}, "tickers": [], "rows": []},
        funnel=[],
        execution={"positions_opened": 0, "positions_closed": 0, "positions_void": 0,
                   "wins": 0, "losses": 0, "break_even": 0,
                   "paper_pnl_usd": 0.0, "max_drawdown_usd": 0.0,
                   "ibkr_disconnects": 0, "end_of_session_mode": "FULLY_LIVE",
                   "eod_flatten_count": 0, "trades": []},
    )
    assert p["schema_version"] == 1
    assert p["account"] == {"broker": "IBKR", "mode": "paper"}


def test_build_daily_rejects_wrong_account_mode():
    """Try to publish a real-money payload — schema must reject."""
    with pytest.raises(jsonschema.ValidationError):
        # Bypass build_daily's hardcoded paper to test the validator
        bad = {
            "schema_version": 1,
            "date": "2026-05-06",
            "tz": "America/New_York",
            "market_status": "open",
            "generated_at": "2026-05-07T04:05:00Z",
            "source_commits": {"paisamaker": "x", "gex_advisor": "y", "cron_runner": "z"},
            "account": {"broker": "IBKR", "mode": "live"},  # NOT paper
            "alerts": {}, "funnel": [], "execution": {},
        }
        schema.validate_daily(bad)


def test_build_closed_market_daily():
    p = schema.build_closed_market_daily(
        date_iso="2026-05-09",
        market_status="closed_weekend",
        source_commits={"paisamaker": "x", "gex_advisor": "y", "cron_runner": "z"},
    )
    assert p["market_status"] == "closed_weekend"
    assert p["funnel"] == []
    assert p["execution"]["trades"] == []


def test_build_live_rth_true():
    p = schema.build_live(
        today_iso="2026-05-07",
        is_rth=True,
        rth_window_et=("09:30", "16:00"),
        source_commits={"paisamaker": "x", "gex_advisor": "y", "cron_runner": "z"},
        spot={"SPX": {"px": 7337.5, "as_of": "14:30:00"}},
        alerts_recent_30m=[],
        open_positions=[],
        session_so_far={"alerts_emitted": 0, "funnel": [], "positions_opened": 0,
                        "positions_closed": 0, "wins": 0, "losses": 0, "break_even": 0,
                        "paper_pnl_usd": 0.0, "ibkr_disconnects": 0,
                        "current_mode": "FULLY_LIVE", "kill_switch_state": "RUNNING", "trades": []},
    )
    assert p["is_rth"] is True
    assert p["spot"]["SPX"]["px"] == 7337.5


def test_build_live_rth_false_minimal():
    p = schema.build_live(
        today_iso="2026-05-09",
        is_rth=False,
        rth_window_et=None,
        source_commits={"paisamaker": "x", "gex_advisor": "y", "cron_runner": "z"},
    )
    assert p["is_rth"] is False
    assert p["spot"] == {}
    assert p["session_so_far"]["current_mode"] is None


def test_alert_row_for_json_spx_uses_3_force():
    row = {
        "id": 1, "fired_at": "2026-05-07T15:00:00+00:00", "ticker": "SPX",
        "alert_type": "CASCADE_WATCH", "direction": "DOWN",
        "spot": 7337.0, "target_price": 7320.0, "stop_price": 7345.0,
        "cascade_conviction": 4, "gex_vol": -2410.0, "force_align": 3,
        "features_json": "{}", "target_hit": 1, "stop_hit": None,
        "outcome_backfilled": 1,
    }
    out = schema.alert_row_for_json(row)
    assert out["triggers"]["cascade_conviction"] == 4
    assert out["triggers"]["gex_vol"] == -2410.0
    assert out["triggers"]["force_align"] == 3  # 3-force for SPX
    assert "force_align_5" not in out["triggers"]
    assert out["outcome"] == "target_hit"


def test_alert_row_for_json_non_spx_uses_5_force_from_json():
    row = {
        "id": 2, "fired_at": "2026-05-07T15:00:00+00:00", "ticker": "QQQ",
        "alert_type": "CASCADE_WATCH", "direction": "DOWN",
        "spot": 580.0, "cascade_conviction": 3, "gex_vol": -1200.0,
        "features_json": '{"force_align_5": 4}',
        "target_hit": None, "stop_hit": None, "outcome_backfilled": 0,
    }
    out = schema.alert_row_for_json(row)
    assert out["triggers"]["force_align_5"] == 4
    assert "force_align" not in out["triggers"]
    assert out["outcome"] == "pending"


def test_alert_row_for_json_charm_squeeze_pulls_vanna_from_json():
    row = {
        "id": 3, "fired_at": "2026-05-07T15:00:00+00:00", "ticker": "TSLA",
        "alert_type": "CHARM_SQUEEZE", "direction": "UP",
        "charm_accel": 1.5, "dist_to_magnet": 0.18,
        "features_json": '{"vanna_accel": 1.42}',
        "target_hit": None, "stop_hit": 1, "outcome_backfilled": 1,
    }
    out = schema.alert_row_for_json(row)
    assert out["triggers"]["vanna_accel"] == 1.42  # from JSON
    assert out["triggers"]["charm_accel"] == 1.5    # from column
    assert out["triggers"]["dist_to_magnet"] == 0.18
    assert out["outcome"] == "stop_hit"


def test_parse_contract_spx_put():
    right, strike = schema._parse_contract("SPXW 7305P")
    assert right == "P"
    assert strike == 7305


def test_parse_contract_qqq_call():
    right, strike = schema._parse_contract("QQQ 580C")
    assert right == "C"
    assert strike == 580


def test_parse_contract_garbage():
    assert schema._parse_contract("") == (None, None)
    assert schema._parse_contract("nonsense") == (None, None)


def test_trade_row_for_json_with_real_fixture():
    pos_rows = json.loads((FIXTURES / "cron_sample_positions.json").read_text())
    # Take the filled-closed advisor SPX trade (#172)
    p172 = next(p for p in pos_rows if p["id"] == 172)
    p172["hold_minutes"] = 25.0  # would normally be derived in SQL
    p172["ticker"] = "SPX"  # add ticker (in fixture but worth verifying)
    out = schema.trade_row_for_json(p172, alert=None)
    assert out["right"] == "P"
    assert out["strike"] == 7300
    assert out["pnl_paper"] == 10.0
    assert out["exit_reason"] == "exit_filled"
    assert out["source"] == "advisor"
    assert out["alert_id"] == 31954
    assert out["dispatched"] is True


def test_trade_row_for_json_void():
    pos_rows = json.loads((FIXTURES / "cron_sample_positions.json").read_text())
    p166 = next(p for p in pos_rows if p["id"] == 166)
    p166["hold_minutes"] = 2.0
    p166["ticker"] = "SPX"
    out = schema.trade_row_for_json(p166, alert=None)
    assert out["pnl_paper"] == 0.0
    assert out["exit_reason"] == "entry_timeout"  # void marker
    assert out["dispatched"] is False  # entry_exec_status='failed_cancelled'


def test_metadata_warnings_surface():
    p = schema.build_daily(
        date_iso="2026-05-06",
        market_status="open",
        rth_window_et=("09:30", "16:00"),
        source_commits={"paisamaker": "a", "gex_advisor": "b", "cron_runner": "c"},
        alerts_block={"emitted_prod": 0, "is_test_excluded": 0, "by_type": {}, "tickers": [], "rows": []},
        funnel=[],
        execution={"positions_opened": 0, "positions_closed": 0, "positions_void": 0,
                   "wins": 0, "losses": 0, "break_even": 0,
                   "paper_pnl_usd": 0.0, "max_drawdown_usd": 0.0,
                   "ibkr_disconnects": 0, "end_of_session_mode": None,
                   "eod_flatten_count": 0, "trades": []},
        holiday_warning="2027 holidays not seeded",
        bundle_1_warning=True,
    )
    assert "holiday_coverage_warning" in p["metadata"]
    assert "bundle_1_caveat" in p["metadata"]
