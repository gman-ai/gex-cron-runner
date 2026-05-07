"""Configuration constants and env loaders.

No side effects on import. Anything that requires env vars (PATs, UUIDs) is
loaded lazily so unit tests can import the module without secrets present.

Source-of-truth references (per cross-project handoff):
- gex-advisor/INTEGRATION.md@eec441f
- market-data-fetcher/docs/cross-project/{DATA_CONTRACTS,LATENCY_BUDGET}.md
- /Users/ukumar/Projects/career/market/cron-runner-handoff/99-synthesis.md
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Database paths on the Hetzner VM. Use these with `?mode=ro` URI flag.
# Verified by all three sibling-project replies (handoff/01,02,03).
# ---------------------------------------------------------------------------
DB_PATHS: dict[str, str] = {
    "market_data": "/opt/market-data/data/market_data.db",
    "paisamaker": "/opt/paisamaker-app/data/discord_signals.db",
    "alert_log": "/opt/gex-advisor/data/alert_log.db",
}

LOG_PATHS: dict[str, str] = {
    "gex_watch": "/opt/paisamaker-app/logs/gex_watch.log",
}

# ---------------------------------------------------------------------------
# Schema pins. Cron startup verifies these. Mismatch = exit 1 (fail loud).
# ---------------------------------------------------------------------------
SCHEMA_PINS = {
    # market_data.db has _schema_version table; expect version 6 per fetcher reply.
    "market_data_version": 6,
    # alert_log.db schema lives in alert_log.py:114 init_db() (no _schema_version
    # table). Pin column count = 41 per gex-advisor INTEGRATION.md@eec441f.
    "alert_log_columns": 41,
    # paisamaker has no _schema_version; pin presence of critical tables instead.
    "paisa_required_tables": [
        "gex_signals",
        "gex_positions",
        "gex_execution_log",
        "bot_state",
        "pipeline_state",
    ],
}

# ---------------------------------------------------------------------------
# Output destinations on the VM (Phase 3 install paths).
# ---------------------------------------------------------------------------
STATE_DIR = Path(os.environ.get("GEX_CRON_STATE_DIR", "/var/lib/gex-cron"))
OUTPUT_DIRS: dict[str, Path] = {
    "staging": STATE_DIR / "staging",
    "published_daily": STATE_DIR / "published" / "daily",
    "published_live": STATE_DIR / "published" / "live",
    "udayx_clone": STATE_DIR / "udayx-clone",
    "state": STATE_DIR / "state",
    "logs": STATE_DIR / "logs",
}

# udayx repo target (git push destination for daily JSON)
UDAYX_REPO_URL = "https://github.com/gman-ai/udayx.com.git"
UDAYX_DAILY_PATH = "public/trading/daily"  # path inside udayx repo

# Cloudflare R2 bucket for live.json
R2_BUCKET = "udayx-live"
R2_LIVE_KEY = "live.json"
R2_RCLONE_REMOTE = os.environ.get("R2_RCLONE_REMOTE", "udayx-r2")  # rclone config name


# ---------------------------------------------------------------------------
# Staleness thresholds (ladder mirrored in udayx UI).
# ---------------------------------------------------------------------------
SPOT_AGE_AMBER_SEC = 60   # >60s = grey out individual ticker
SPOT_AGE_GREY_SEC = 300   # >300s = upstream broken per fetcher reply

# Heartbeat parsing for current_mode
HEARTBEAT_STALE_MIN = 15  # >15 min without an `ibkr: mode=` line = UNKNOWN

# Snapshot transaction wall-time alert threshold
SNAPSHOT_WARN_SEC = 2.0  # log warn if any snapshot exceeds this
SNAPSHOT_FAIL_SEC = 5.0  # exit 1 if any snapshot exceeds this (defensive)


# ---------------------------------------------------------------------------
# Skip-reason → funnel-band mapping (single source of truth).
# Per paisa reply: only 9 of 18 defined values have ever fired in production
# (rearm_distance dominates ~62%). Cron reserves all 18 buckets; never-observed
# ones default to 0 in the JSON output.
# ---------------------------------------------------------------------------
DISPATCHER_KEYS: tuple[str, ...] = (
    "cooldown",
    "rearm_distance",
    "max_positions",
    "max_positions_ticker",
    "open_chop_suppression",
    "duplicate_contract",
)
STRIKE_KEYS: tuple[str, ...] = (
    "no_qualifying_strike",
    "no_fresh_quote",
    "qualify_failed",
    "spread_widened_after_refresh",
)
DRIFT_KEYS: tuple[str, ...] = (
    "spot_drift",
    "spot_drift_hysteresis",
    "premium_drift",
)
EXEC_KEYS: tuple[str, ...] = (
    "executor_disconnected",
    "executor_rejected",
    "ibkr_not_live",
    "entry_exception",
)
HYGIENE_KEYS: tuple[str, ...] = (
    # Excluded from funnel rendering — operational recovery path, not a real drop.
    "stale_pend_cleanup",
)

ALL_KNOWN_SKIP_REASONS: frozenset[str] = frozenset(
    DISPATCHER_KEYS + STRIKE_KEYS + DRIFT_KEYS + EXEC_KEYS + HYGIENE_KEYS
)


# ---------------------------------------------------------------------------
# Production filter for alert_log queries (three clauses, ALL required per
# gex-advisor reply).
# ---------------------------------------------------------------------------
PROD_ALERT_FILTER_SQL = (
    "is_test = 0 "
    "AND alert_type NOT LIKE 'SHADOW_%' "
    "AND alert_type NOT IN ('STRUCTURE_BREAK', 'STRUCTURE_BRIEF')"
)

# Entry-eligible alert types (4 — adds CASCADE_ALERT vs original 3)
ENTRY_ALERT_TYPES: tuple[str, ...] = (
    "CASCADE_WATCH",
    "CASCADE_ALERT",
    "CHARM_SQUEEZE",
    "GAMMA_RECLAIM",
)
# Exit-only alert types — present in alert_log but not in entry funnel
EXIT_ALERT_TYPES: tuple[str, ...] = (
    "CASCADE_ALERT_EXHAUSTING",
    "GAMMA_RECLAIM_EXIT",
)


# ---------------------------------------------------------------------------
# Trigger features per alert type. Per gex-advisor reply, several feature
# names live in features_json (not as top-level columns) — JSON extract
# required.
# Format: (feature_name, source) where source is "col" or "json".
# ---------------------------------------------------------------------------
TRIGGER_FEATURES: dict[str, list[tuple[str, str]]] = {
    "CASCADE_WATCH_SPX": [
        ("cascade_conviction", "col"),
        ("gex_vol", "col"),
        ("force_align", "col"),  # 3-force for SPX
    ],
    "CASCADE_WATCH_NON_SPX": [
        ("cascade_conviction", "col"),
        ("gex_vol", "col"),
        ("force_align_5", "json"),  # 5-force from features_json
    ],
    "CASCADE_ALERT": [
        ("cascade_conviction", "col"),
        ("gex_vol", "col"),
        ("cascade_exhausting", "col"),
    ],
    "CHARM_SQUEEZE": [
        ("charm_accel", "col"),
        ("vanna_accel", "json"),
        ("dist_to_magnet", "col"),
    ],
    "GAMMA_RECLAIM": [
        ("gex_vol", "col"),
        ("gamma_reclaim_confirmed", "json"),
        ("regime_strength_slope", "json"),
    ],
    "GAMMA_RECLAIM_EXIT": [
        ("gex_vol", "col"),
        ("dist_to_wall", "col"),
        ("gamma_reclaim_exit_reason", "json"),
    ],
    "CASCADE_ALERT_EXHAUSTING": [
        ("cascade_exhausting", "col"),
        ("gex_vol", "col"),
        ("cascade_conviction", "col"),
    ],
}

# Tier C tickers — conviction capped at 3, no CASCADE_ALERT possible
TIER_C_TICKERS: frozenset[str] = frozenset({"TSLA", "NVDA", "AMD"})


# ---------------------------------------------------------------------------
# Bundle 1 cutoff — rows before this had broken chain enrichment columns
# (dist_to_wall, dist_to_magnet, charm_accel may be None/stale).
# ---------------------------------------------------------------------------
BUNDLE_1_CUTOFF_UTC = "2026-05-05T18:00:00Z"


# ---------------------------------------------------------------------------
# Half-day close map. NOT in market_holidays schema (per fetcher reply);
# gex-cron-runner owns this. Empty in 2026-Q2; populate before next half-day.
# Format: ISO date string → close time HH:MM ET.
# ---------------------------------------------------------------------------
HALF_DAY_CLOSE_ET: dict[str, str] = {
    # "2026-11-27": "13:00",  # day after Thanksgiving
    # "2026-12-24": "13:00",  # Christmas Eve
}


# ---------------------------------------------------------------------------
# Lazily-loaded secrets. Functions, not module constants, so import doesn't
# fail in test environments without the env vars set.
# ---------------------------------------------------------------------------
def get_daily_hc_uuid() -> str:
    """Healthchecks.io UUID for the daily cron. Loaded from env."""
    v = os.environ.get("DAILY_HC_UUID")
    if not v:
        raise RuntimeError("DAILY_HC_UUID not set")
    return v


def get_live_hc_uuid() -> str:
    """Healthchecks.io UUID for the live cron. Loaded from env."""
    v = os.environ.get("LIVE_HC_UUID")
    if not v:
        raise RuntimeError("LIVE_HC_UUID not set")
    return v


def get_github_pat() -> str:
    """GitHub PAT for udayx repo push. Loaded from env."""
    v = os.environ.get("GITHUB_PAT")
    if not v:
        raise RuntimeError("GITHUB_PAT not set")
    return v


def get_git_author() -> tuple[str, str]:
    """Git author identity for commits. Defaults to founder@getmyagentnow.com."""
    name = os.environ.get("GIT_AUTHOR_NAME", "Uday Kumar")
    email = os.environ.get("GIT_AUTHOR_EMAIL", "founder@getmyagentnow.com")
    return name, email
