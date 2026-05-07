"""Funnel reconstruction tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from gex_cron_runner import funnel


FIXTURES = Path(__file__).parent / "fixtures"


def test_seven_stages_returned():
    stages = funnel.build_funnel(
        alerts_emitted=10, entry_type_excluded=0,
        upstream_received=10, skip_counts={},
        positions_filled=10, positions_void=0,
    )
    assert len(stages) == 7
    expected_names = [
        "alerts_emitted", "upstream_received", "dispatcher",
        "strike_qualified", "drift_passed", "executor_accepted", "entry_filled",
    ]
    assert [s["stage"] for s in stages] == expected_names


def test_arithmetic_sums_clean_path():
    """No drops anywhere → every stage passes through."""
    stages = funnel.build_funnel(
        alerts_emitted=5, entry_type_excluded=0,
        upstream_received=5, skip_counts={},
        positions_filled=5, positions_void=0,
    )
    errors = funnel.validate_arithmetic(stages)
    assert errors == []
    # All count_ins should be 5
    assert all(s["count_in"] == 5 for s in stages)


def test_dispatcher_drops():
    """3 alerts dropped at dispatcher (cooldown=2, rearm_distance=1)."""
    stages = funnel.build_funnel(
        alerts_emitted=10, entry_type_excluded=0,
        upstream_received=10,
        skip_counts={"cooldown": 2, "rearm_distance": 1},
        positions_filled=7, positions_void=0,
    )
    dispatcher = stages[2]
    assert dispatcher["stage"] == "dispatcher"
    assert dispatcher["count_in"] == 10
    assert dispatcher["count_dropped"] == 3
    assert dispatcher["sub"]["cooldown"] == 2
    assert dispatcher["sub"]["rearm_distance"] == 1
    # Other dispatcher keys default to 0
    assert dispatcher["sub"]["max_positions"] == 0

    errors = funnel.validate_arithmetic(stages)
    assert errors == []


def test_entry_type_excluded_in_upstream_band():
    """4 alerts of exit-type are excluded; 1 lost to upstream (freshness/etc)."""
    stages = funnel.build_funnel(
        alerts_emitted=10, entry_type_excluded=4,
        upstream_received=5,  # 10 - 4 (entry-type) - 1 (other) = 5
        skip_counts={},
        positions_filled=5, positions_void=0,
    )
    upstream = stages[1]
    assert upstream["stage"] == "upstream_received"
    assert upstream["count_dropped"] == 5  # 4 entry-type + 1 other
    assert upstream["sub"]["entry_type_excluded"] == 4
    assert upstream["sub"]["freshness_or_other"] == 1


def test_unknown_skip_reason_marks_dispatcher_unstructured():
    """A skip_reason we don't know about → dispatcher.structured=false +
    sub.unstructured count."""
    stages = funnel.build_funnel(
        alerts_emitted=5, entry_type_excluded=0,
        upstream_received=5,
        skip_counts={"cooldown": 1, "BRAND_NEW_REASON": 2},
        positions_filled=2, positions_void=0,
    )
    dispatcher = stages[2]
    assert dispatcher["structured"] is False
    assert dispatcher["sub"]["unstructured"] == 2
    assert dispatcher["sub"]["cooldown"] == 1
    # Total dropped at dispatcher = 1 cooldown + 2 unstructured = 3
    assert dispatcher["count_dropped"] == 3


def test_void_drops_at_entry_filled():
    """5 made it to executor_accepted; 1 voided on LMT timeout."""
    stages = funnel.build_funnel(
        alerts_emitted=5, entry_type_excluded=0,
        upstream_received=5, skip_counts={},
        positions_filled=4, positions_void=1,
    )
    entry_filled = stages[6]
    assert entry_filled["stage"] == "entry_filled"
    assert entry_filled["count_in"] == 5
    assert entry_filled["count_dropped"] == 1
    assert entry_filled["lmt_timeout_seconds"] == 45


def test_zero_alerts_day():
    stages = funnel.build_funnel(
        alerts_emitted=0, entry_type_excluded=0,
        upstream_received=0, skip_counts={},
        positions_filled=0, positions_void=0,
    )
    assert all(s["count_in"] == 0 for s in stages)
    assert all(s["count_dropped"] == 0 for s in stages)
    assert funnel.validate_arithmetic(stages) == []


def test_all_observed_skip_reasons_bucket_correctly():
    """Use the actual skip_reason values from paisa's prod sample data."""
    # From the paisa reply: 9 observed skip_reasons + counts (5 weeks of data).
    # We use small numbers here for unit-test clarity.
    skip_counts = {
        "rearm_distance": 5,
        "no_qualifying_strike": 3,
        "open_chop_suppression": 2,
        "cooldown": 2,
        "max_positions_ticker": 1,
        "executor_rejected": 1,
        "spot_drift": 1,
        "premium_drift": 0,
        "qualify_failed": 1,
    }
    stages = funnel.build_funnel(
        alerts_emitted=30, entry_type_excluded=2,
        upstream_received=20, skip_counts=skip_counts,
        positions_filled=4, positions_void=0,
    )
    # Verify each band picked up the right reasons
    dispatcher = stages[2]
    assert dispatcher["sub"]["cooldown"] == 2
    assert dispatcher["sub"]["rearm_distance"] == 5
    assert dispatcher["sub"]["max_positions_ticker"] == 1
    assert dispatcher["sub"]["open_chop_suppression"] == 2
    assert dispatcher["count_dropped"] == 10

    strike = stages[3]
    assert strike["sub"]["no_qualifying_strike"] == 3
    assert strike["sub"]["qualify_failed"] == 1
    assert strike["count_dropped"] == 4

    drift = stages[4]
    assert drift["sub"]["spot_drift"] == 1
    assert drift["sub"]["premium_drift"] == 0
    assert drift["count_dropped"] == 1

    executor = stages[5]
    assert executor["sub"]["executor_rejected"] == 1
    assert executor["count_dropped"] == 1


def test_filter_hygiene_reasons():
    """stale_pend_cleanup should be filtered out before passing to build_funnel."""
    raw = {"cooldown": 1, "stale_pend_cleanup": 2, "spot_drift": 1}
    filtered = funnel.filter_skip_counts_for_funnel(raw)
    assert "stale_pend_cleanup" not in filtered
    assert filtered == {"cooldown": 1, "spot_drift": 1}


def test_against_actual_paisa_signals_fixture():
    """Use the real cron_sample_signals.json from paisa to exercise band mapping."""
    rows = json.loads((FIXTURES / "cron_sample_signals.json").read_text())
    # Each row has executed=-1 and a skip_reason. Group them.
    skip_counts: dict[str, int] = {}
    for r in rows:
        sr = r.get("skip_reason")
        if sr:
            skip_counts[sr] = skip_counts.get(sr, 0) + 1

    # Pretend they all came from the advisor lane on the same day
    n = len(rows)
    stages = funnel.build_funnel(
        alerts_emitted=n + 5, entry_type_excluded=2,
        upstream_received=n, skip_counts=skip_counts,
        positions_filled=0, positions_void=0,
    )
    # All known reasons → all stages structured
    assert all(s["structured"] for s in stages)
    # Funnel arithmetic should sum
    assert funnel.validate_arithmetic(stages) == []
