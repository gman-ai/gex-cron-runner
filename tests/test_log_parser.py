"""Log parser tests with realistic ANSI-coded line samples."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from gex_cron_runner import log_parser

ET = ZoneInfo("America/New_York")

# Real line format captured from VM 2026-05-07 (ANSI codes preserved)
HEARTBEAT_LINE = (
    "\x1b[90m15:51:08\x1b[0m \x1b[32m[    INFO]\x1b[0m \x1b[90mpaisamaker.paisamaker.gex.watch\x1b[0m"
    " - GEX HEARTBEAT | ticks=260 evals=217 sigs=6 pos_o=6 pos_c=0 | skip: ts=0 stale=0 gap=0 warm=0"
    " win=43 cool=30 rearm=76 stitch=0 chop=0 drift=0 neg_sep=0 | warmup: total=680 fresh=680"
    " | last_reading=2026-05-07T19:51:07Z spot=7341.38 zg=7342.5 dist=-1.1 gex_vol=-108279.86"
    " | api: classic=260/0/0 flow=260/0/0 rows=260 stitch=0 ignored=0 orderflow=OK"
    " | 5F: zvanna=-755.1 zcharm=-2551.8 net_dex=2567.6 zcvr=-265.0 gexoflow=408.3"
    " | chain: ok=6/6 fail=0 insuf_q=0 no_strike=0 sat=0"
    " | ibkr: mode=FULLY_LIVE exec=ok chain=ok"
    " | exit_q: att=11 conid_miss=0 sym_miss=0 svc_unavail=0 synthetic=0"
    " | chain_data: ok=1300 dd=0 err=0 ins=1300 upd=0 drsk=0 orph=0 bp=0 sz=112M"
)
DISCONNECT_LINE = (
    "\x1b[90m15:13:36\x1b[0m \x1b[32m[    INFO]\x1b[0m \x1b[90mpaisamaker.paisamaker.execution.ibkr_client\x1b[0m"
    " - Disconnected from IBKR"
)


def test_strip_ansi():
    assert log_parser._strip_ansi(HEARTBEAT_LINE).startswith("15:51:08 ")
    assert "\x1b" not in log_parser._strip_ansi(HEARTBEAT_LINE)


def test_mode_regex_matches_real_line():
    line = log_parser._strip_ansi(HEARTBEAT_LINE)
    m = log_parser._MODE_RE.search(line)
    assert m is not None
    assert m.group(1) == "FULLY_LIVE"


def test_parse_current_mode_finds_latest(tmp_path):
    """Write a log with three heartbeats; parser should return the LAST one."""
    log_file = tmp_path / "watch.log"
    # Use HH:MM that's recent enough to pass freshness check
    now = datetime.now(ET)
    fresh_hms = f"{now.hour:02d}:{now.minute:02d}:00"
    lines = [
        f"\x1b[90m{fresh_hms}\x1b[0m [INFO] - older heartbeat | ibkr: mode=DATA_ONLY exec=ok chain=ok\n",
        f"\x1b[90m{fresh_hms}\x1b[0m [INFO] - latest heartbeat | ibkr: mode=FULLY_LIVE exec=ok chain=ok\n",
    ]
    log_file.write_text("".join(lines))
    assert log_parser.parse_current_mode(str(log_file)) == "FULLY_LIVE"


def test_parse_current_mode_stale_returns_unknown(tmp_path):
    """A heartbeat from 2 hours ago should yield UNKNOWN (>15 min stale)."""
    log_file = tmp_path / "watch.log"
    now = datetime.now(ET)
    # 2 hours ago
    stale_hour = (now.hour - 2) % 24
    stale_hms = f"{stale_hour:02d}:{now.minute:02d}:00"
    log_file.write_text(
        f"\x1b[90m{stale_hms}\x1b[0m [INFO] - stale | ibkr: mode=FULLY_LIVE exec=ok chain=ok\n"
    )
    assert log_parser.parse_current_mode(str(log_file)) == "UNKNOWN"


def test_parse_current_mode_missing_file():
    assert log_parser.parse_current_mode("/nonexistent/path") == "UNKNOWN"


def test_parse_current_mode_no_heartbeat(tmp_path):
    log_file = tmp_path / "watch.log"
    log_file.write_text("\x1b[90m15:00:00\x1b[0m [INFO] - boring line\n")
    assert log_parser.parse_current_mode(str(log_file)) == "UNKNOWN"


def test_count_disconnects_file_path(tmp_path):
    """Two disconnects today → count 2; one yesterday → not counted."""
    log_file = tmp_path / "watch.log"
    now = datetime.now(ET)
    # Yesterday: HH > now's HH (will be detected as past day boundary going back)
    yesterday_hour = (now.hour + 5) % 24  # 5 hours "later" than now → yesterday
    yesterday_hms = f"{yesterday_hour:02d}:00:00"
    today_hms_1 = f"{(now.hour - 1) % 24:02d}:30:00"
    today_hms_2 = f"{now.hour:02d}:00:00"
    if yesterday_hour > now.hour:  # yesterday's HH is "later" looking — good
        lines = [
            f"\x1b[90m{yesterday_hms}\x1b[0m [INFO] - Disconnected from IBKR\n",  # yesterday
            f"\x1b[90m{today_hms_1}\x1b[0m [INFO] - Disconnected from IBKR\n",   # today
            f"\x1b[90m{today_hms_2}\x1b[0m [INFO] - Disconnected from IBKR\n",   # today
        ]
        log_file.write_text("".join(lines))
        assert log_parser._count_disconnects_file(str(log_file)) == 2


def test_count_disconnects_empty_file(tmp_path):
    log_file = tmp_path / "watch.log"
    log_file.write_text("")
    assert log_parser._count_disconnects_file(str(log_file)) == 0


def test_count_disconnects_no_disconnect_lines(tmp_path):
    log_file = tmp_path / "watch.log"
    log_file.write_text("\x1b[90m15:00:00\x1b[0m [INFO] - normal heartbeat\n")
    assert log_parser._count_disconnects_file(str(log_file)) == 0


def test_get_kill_switch_state_empty(paisa_db):
    """No row = RUNNING (per paisa reply)."""
    assert log_parser.get_kill_switch_state(paisa_db) == "RUNNING"


def test_get_kill_switch_state_halted(paisa_db):
    paisa_db.execute("INSERT INTO pipeline_state (key, value) VALUES ('exec_halt_level', '1')")
    paisa_db.commit()
    assert log_parser.get_kill_switch_state(paisa_db) == "HALTED"


def test_get_mkt_forced_active_off(paisa_db):
    """Default — no row in bot_state → False."""
    assert log_parser.get_mkt_forced_active(paisa_db) is False


def test_get_mkt_forced_active_on(paisa_db):
    paisa_db.execute(
        "INSERT INTO bot_state (key, value, updated_at) VALUES ('gex_entry_mkt_forced', '1', '2026-05-07T15:00Z')"
    )
    paisa_db.commit()
    assert log_parser.get_mkt_forced_active(paisa_db) is True
