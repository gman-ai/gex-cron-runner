"""Parse paisa's gex_watch.log for live state.

Per paisa reply: `current_mode`, `kill_switch_state` and `ibkr_disconnects` are
NOT in any DB. They live in process memory + heartbeat log lines. The file is
single, append-only (verified ~16.6 MB at 5 weeks, no rotation), so a simple
seek-to-end read covers the recent state.

Real log format (verified on VM 2026-05-07):

    [90m15:51:08[0m [32m[    INFO][0m [90m<module>[0m - <message>

ANSI color codes wrap the timestamp + log level. We strip them before regex.

Heartbeat line example:
    ...GEX HEARTBEAT | ... | ibkr: mode=FULLY_LIVE exec=ok chain=ok | ...

Disconnect line:
    ...paisamaker.execution.ibkr_client - Disconnected from IBKR

"""
from __future__ import annotations

import logging
import os
import re
import subprocess
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from gex_cron_runner import config

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

# ANSI escape stripper
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
# `[HH:MM:SS]` style line prefix (after ANSI strip)
_TS_PREFIX_RE = re.compile(r"^(\d{2}:\d{2}:\d{2})")
# `ibkr: mode=FULLY_LIVE exec=ok chain=ok` pattern
_MODE_RE = re.compile(r"ibkr:\s+mode=(\w+)\s+exec=\w+\s+chain=\w+")

# How much to read from EOF for current_mode (heartbeats every ~10 min;
# 16 KB easily covers the last ~30 min of log).
_MODE_TAIL_BYTES = 16 * 1024
# How much to read for ibkr_disconnects today (~3 MB covers ~6 days; safety
# margin for log growth).
_DISCONNECT_TAIL_BYTES = 3 * 1024 * 1024


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


def _read_tail(path: str, max_bytes: int) -> str:
    """Read up to max_bytes from the END of file. Returns empty string if
    path missing or unreadable."""
    try:
        size = os.stat(path).st_size
        with open(path, "rb") as f:
            f.seek(max(0, size - max_bytes), os.SEEK_SET)
            data = f.read()
        return data.decode("utf-8", errors="replace")
    except (FileNotFoundError, PermissionError) as e:
        log.warning("log read failed for %s: %s", path, e)
        return ""


def parse_current_mode(log_path: str | None = None) -> str:
    """Return the latest `ibkr: mode=` value from the heartbeat log.

    Returns one of: FULLY_LIVE, EXECUTION_DEGRADED, DATA_ONLY, RECONNECT_WARMUP, UNKNOWN.

    UNKNOWN is returned when:
    - The log file is missing or empty.
    - No heartbeat line found in the tail window.
    - The latest heartbeat is >HEARTBEAT_STALE_MIN minutes old (15 by default).
    """
    path = log_path or config.LOG_PATHS["gex_watch"]
    tail = _read_tail(path, _MODE_TAIL_BYTES)
    if not tail:
        return "UNKNOWN"

    # Walk lines backwards, find the most recent ibkr: mode=
    last_mode: str | None = None
    last_hms: str | None = None
    for raw in reversed(tail.splitlines()):
        line = _strip_ansi(raw)
        m = _MODE_RE.search(line)
        if not m:
            continue
        last_mode = m.group(1)
        ts_m = _TS_PREFIX_RE.match(line)
        if ts_m:
            last_hms = ts_m.group(1)
        break

    if last_mode is None:
        return "UNKNOWN"

    # Verify the heartbeat is fresh (<= HEARTBEAT_STALE_MIN)
    if last_hms:
        if _line_age_minutes(last_hms) > config.HEARTBEAT_STALE_MIN:
            log.info("current_mode heartbeat stale (line ts=%s); returning UNKNOWN", last_hms)
            return "UNKNOWN"

    return last_mode


def _line_age_minutes(hms: str) -> float:
    """Compute age in minutes between a HH:MM:SS line prefix (in ET) and now ET.

    The log lines have no date prefix. We assume the latest line in the tail
    window is from today (or yesterday at worst). If the line's HH:MM:SS is
    later than now's HH:MM:SS, treat as yesterday — return 24h+ stale.
    """
    try:
        hh, mm, ss = map(int, hms.split(":"))
    except (ValueError, AttributeError):
        return float("inf")  # malformed → treat as stale
    now = datetime.now(ET)
    line_today_minutes = hh * 60 + mm + ss / 60.0
    now_minutes = now.hour * 60 + now.minute + now.second / 60.0
    if line_today_minutes > now_minutes:
        # Line is "in the future" relative to now → must be from yesterday
        return (24 * 60) - line_today_minutes + now_minutes
    return now_minutes - line_today_minutes


def count_ibkr_disconnects_today(
    log_path: str | None = None,
    journal_unit: str | None = None,
) -> int:
    """Count 'Disconnected from IBKR' events from today (ET).

    Primary: try journalctl (proper timestamps, no day-boundary heuristics).
    Fallback: parse the tee'd file with HH:MM:SS bucketing.

    The journal unit name is unknown until Phase 3 deploy verifies it on the
    VM. Pass `journal_unit` explicitly if known; otherwise fallback to file.
    """
    if journal_unit:
        try:
            n = _count_disconnects_journalctl(journal_unit)
            if n is not None:
                return n
        except Exception as e:
            log.warning("journalctl path failed: %s — falling back to file", e)

    return _count_disconnects_file(log_path or config.LOG_PATHS["gex_watch"])


def _count_disconnects_journalctl(unit: str) -> int | None:
    """Run `journalctl -u <unit> --since 'today 00:00' | grep -c 'Disconnected from IBKR'`.

    Returns None if journalctl is unavailable or returns non-zero (caller falls back).
    """
    today_et = datetime.now(ET).strftime("%Y-%m-%d 00:00:00")
    result = subprocess.run(
        ["journalctl", "-u", unit, "--since", today_et, "--no-pager", "-q"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        log.debug("journalctl unit=%s rc=%d stderr=%s", unit, result.returncode, result.stderr[:200])
        return None
    return result.stdout.count("Disconnected from IBKR")


def _count_disconnects_file(path: str) -> int:
    """Fallback: parse the tee'd file. Walk backwards from EOF, count
    'Disconnected from IBKR' lines whose HH:MM:SS prefix indicates today's ET.
    """
    tail = _read_tail(path, _DISCONNECT_TAIL_BYTES)
    if not tail:
        return 0

    now = datetime.now(ET)
    now_minutes = now.hour * 60 + now.minute + now.second / 60.0

    count = 0
    # The tail buffer goes from earlier → later. Walk lines backwards (newest first)
    # and stop as soon as we cross the day boundary (HH:MM > now's HH:MM, since
    # we're going backwards in time, that means yesterday).
    for raw in reversed(tail.splitlines()):
        line = _strip_ansi(raw)
        ts_m = _TS_PREFIX_RE.match(line)
        if not ts_m:
            continue
        try:
            hh, mm, ss = map(int, ts_m.group(1).split(":"))
        except ValueError:
            continue
        line_minutes = hh * 60 + mm + ss / 60.0
        # Detect day boundary: if line_minutes is greater than now_minutes,
        # we've walked back past 00:00 ET into yesterday. Stop.
        if line_minutes > now_minutes + 0.5:  # 0.5 min tolerance for clock skew
            break
        if "Disconnected from IBKR" in line:
            count += 1

    return count


def get_kill_switch_state(conn_paisa) -> str:
    """Read pipeline_state.exec_halt_level (rare path).

    Per paisa reply: normal operation = empty table. Treat absent row as
    RUNNING; `value='1'` as HALTED.
    """
    try:
        row = conn_paisa.execute(
            "SELECT value FROM pipeline_state WHERE key = 'exec_halt_level'"
        ).fetchone()
    except Exception as e:
        log.warning("pipeline_state read failed: %s", e)
        return "UNKNOWN"
    if row is None:
        return "RUNNING"
    return "HALTED" if str(row[0]) == "1" else "RUNNING"


def get_mkt_forced_active(conn_paisa) -> bool:
    """Read bot_state.gex_entry_mkt_forced (PR #222, 2026-05-07).

    Returns True iff the override flag is set. If table/row missing (older
    paisa version), returns False (feature absent).
    """
    try:
        row = conn_paisa.execute(
            "SELECT value FROM bot_state WHERE key = 'gex_entry_mkt_forced'"
        ).fetchone()
    except Exception as e:
        log.debug("bot_state read failed (table may be absent on older paisa): %s", e)
        return False
    if row is None:
        return False
    return str(row[0]) == "1"
