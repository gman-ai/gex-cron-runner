"""7-stage gate funnel reconstruction.

The funnel reads as a sequential pipeline matching paisamaker's actual code
path: alert fires → reaches paisa (via gex_signals) → passes dispatcher →
finds a strike → drift gates pass → executor accepts → LMT fills.

Stages (per plan §3 + cross-project verification):

  1. alerts_emitted        — alert_log rows with prod filter
  2. upstream_received     — gex_signals rows where source='advisor'
                              (drops = entry-type-excluded + freshness/etc upstream losses)
  3. dispatcher            — skip_reason ∈ {cooldown, rearm_distance, max_positions, ...}
  4. strike_qualified      — skip_reason ∈ {no_qualifying_strike, no_fresh_quote, ...}
  5. drift_passed          — skip_reason ∈ {spot_drift, premium_drift, ...}
  6. executor_accepted     — skip_reason ∈ {executor_disconnected, ibkr_not_live, ...}
  7. entry_filled          — drops = positions where entry_exec_status='failed_cancelled'

Per paisa reply: only 9 of 18 defined skip_reasons have ever fired in
production. Cron reserves all 18 buckets; never-observed ones default to 0.
Unknown skip_reasons (schema drift) surface under dispatcher.sub.unstructured
and mark that stage `structured: false`.
"""
from __future__ import annotations

import logging
from typing import Any

from gex_cron_runner import config

log = logging.getLogger(__name__)


def build_funnel(
    *,
    alerts_emitted: int,
    entry_type_excluded: int,
    upstream_received: int,
    skip_counts: dict[str, int],
    positions_filled: int,
    positions_void: int,
) -> list[dict[str, Any]]:
    """Build the 7-stage funnel array.

    Args:
        alerts_emitted: Count of `alert_log` rows matching PROD_ALERT_FILTER for date D.
        entry_type_excluded: Subset of `alerts_emitted` where alert_type ∈
            EXIT_ALERT_TYPES (CASCADE_ALERT_EXHAUSTING, GAMMA_RECLAIM_EXIT).
            These never reach paisa as entries.
        upstream_received: Count of `gex_signals` rows where source='advisor'
            for date D (regardless of executed value).
        skip_counts: SELECT skip_reason, COUNT(*) FROM gex_signals WHERE
            source='advisor' AND date(timestamp)=D AND executed=-1 GROUP BY
            skip_reason. Hygiene reasons (stale_pend_cleanup) are filtered
            out before this dict is built.
        positions_filled: Count of gex_positions where source='advisor' AND
            date(entry_time)=D AND entry_exec_status='filled' AND shadow_mode=0.
        positions_void: Count where entry_exec_status='failed_cancelled'.

    Returns:
        A list of 7 stage dicts, each with `stage`, `count_in`, `count_dropped`,
        `structured` keys. Stages 2-6 also have a `sub` map. Stage 7 has
        `lmt_timeout_seconds`.
    """
    stages: list[dict[str, Any]] = []

    # Stage 1: alerts_emitted (no drops; pure emission count)
    stages.append({
        "stage": "alerts_emitted",
        "count_in": alerts_emitted,
        "count_dropped": 0,
        "structured": True,
    })

    # Stage 2: upstream_received
    # count_in = stage 1 pass-through (alerts_emitted)
    # count_dropped = alerts_emitted - upstream_received (everything that didn't reach paisa)
    # sub: entry_type_excluded (known) + freshness_or_other (residual)
    upstream_dropped = max(0, alerts_emitted - upstream_received)
    freshness_or_other = max(0, upstream_dropped - entry_type_excluded)
    stages.append({
        "stage": "upstream_received",
        "count_in": alerts_emitted,
        "count_dropped": upstream_dropped,
        "structured": True,
        "sub": {
            "entry_type_excluded": entry_type_excluded,
            "freshness_or_other": freshness_or_other,
        },
    })

    # Detect unknown skip_reasons before building bands (schema drift signal)
    unknown_reasons = {
        k: v for k, v in skip_counts.items()
        if k not in config.ALL_KNOWN_SKIP_REASONS
    }

    cumulative = upstream_received

    def make_stage(name: str, keys: tuple[str, ...]) -> dict[str, Any]:
        nonlocal cumulative
        sub = {k: skip_counts.get(k, 0) for k in keys}
        dropped = sum(sub.values())
        s: dict[str, Any] = {
            "stage": name,
            "count_in": cumulative,
            "count_dropped": dropped,
            "structured": True,
            "sub": sub,
        }
        cumulative -= dropped
        return s

    dispatcher = make_stage("dispatcher", config.DISPATCHER_KEYS)

    # If there are unknown reasons, surface them under dispatcher's sub.unstructured
    # and mark this stage `structured: false` so the UI renders [unstructured].
    # Choosing dispatcher because it's the broadest band; better than silently
    # losing the count.
    if unknown_reasons:
        unknown_total = sum(unknown_reasons.values())
        log.warning(
            "unknown skip_reasons (will render as [unstructured] under dispatcher band): %r",
            unknown_reasons,
        )
        dispatcher["sub"]["unstructured"] = unknown_total
        dispatcher["count_dropped"] += unknown_total
        dispatcher["structured"] = False
        cumulative -= unknown_total

    stages.append(dispatcher)
    stages.append(make_stage("strike_qualified", config.STRIKE_KEYS))
    stages.append(make_stage("drift_passed", config.DRIFT_KEYS))
    stages.append(make_stage("executor_accepted", config.EXEC_KEYS))

    # Stage 7: entry_filled
    # count_in = cumulative (whatever passed all earlier bands)
    # count_dropped = positions_void
    # The arithmetic invariant: cumulative >= positions_filled (some signals
    # may still be in-flight at observation time) — but for a daily rollup
    # at 00:05 ET this should be exact since all RTH activity is settled.
    stages.append({
        "stage": "entry_filled",
        "count_in": cumulative,
        "count_dropped": positions_void,
        "structured": True,
        "lmt_timeout_seconds": 45,
    })

    return stages


def validate_arithmetic(stages: list[dict[str, Any]]) -> list[str]:
    """Verify that count_in[i+1] == count_in[i] - count_dropped[i] for all i.

    Returns a list of error messages (empty if OK). Used by the writer for a
    soft warning — funnel arithmetic CAN diverge by 1-2 due to transient
    `executed=0` rows in gex_signals (per paisa reply, ~seconds), but
    larger gaps suggest a counting bug.
    """
    errors: list[str] = []
    for i in range(len(stages) - 1):
        expected = stages[i]["count_in"] - stages[i]["count_dropped"]
        actual = stages[i + 1]["count_in"]
        if expected != actual:
            errors.append(
                f"funnel arithmetic: stage[{i}] {stages[i]['stage']!r} "
                f"count_in - count_dropped = {expected}, "
                f"but stage[{i+1}] {stages[i+1]['stage']!r} count_in = {actual}"
            )
    return errors


def filter_skip_counts_for_funnel(skip_counts: dict[str, int]) -> dict[str, int]:
    """Drop hygiene-band reasons (stale_pend_cleanup) from raw skip_counts
    before passing to build_funnel.

    Hygiene reasons are operational recovery paths, not real "drops" from
    the user's perspective. Per paisa reply, they should be excluded from
    the funnel but kept observable in the raw skip_counts dict for ops review.
    """
    return {k: v for k, v in skip_counts.items() if k not in config.HYGIENE_KEYS}
