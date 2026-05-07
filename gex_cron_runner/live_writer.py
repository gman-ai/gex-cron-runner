"""Cron #2 — live rollup writer.

Runs every 30s during RTH (per systemd timer). Reads paisa + alert_log +
market_data DBs read-only (NO snapshot transaction; 30s drift acceptable).
Builds live.json and rclone-pushes to Cloudflare R2.

Outside RTH, writes a single `is_rth: false` payload to flip the client back
to rollup mode, then exits. Subsequent firings during non-RTH no-op quickly
(<5ms).

Hard timeout via systemd `TimeoutStartSec=20s` — if a run takes >20s, it's
killed (prevents pile-up at 30s cadence).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from gex_cron_runner import (
    config, db, funnel, health, log_parser, queries, r2_push, rth, sanity, schema,
)

ET = ZoneInfo("America/New_York")
log = logging.getLogger("gex_cron_runner.live_writer")


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _git_sha(path: str) -> str:
    import subprocess
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=path, capture_output=True, text=True, timeout=2,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return "unknown"


def _source_commits() -> dict[str, str]:
    cron_path = str(Path(__file__).parent.parent)
    return {
        "paisamaker": _git_sha("/opt/paisamaker-app") if os.path.isdir("/opt/paisamaker-app") else "vm-only",
        "gex_advisor": _git_sha("/opt/gex-advisor") if os.path.isdir("/opt/gex-advisor") else "vm-only",
        "cron_runner": _git_sha(cron_path),
    }


def build_live_payload(*, journal_unit: str | None = None) -> dict:
    """Run live queries against the VM DBs and build the live JSON dict.

    Skips snapshot transaction (30s drift acceptable; live cron must finish
    fast to avoid 30s timer pile-up).
    """
    today_et = datetime.now(ET).date()
    today_iso = today_et.isoformat()

    conn_md = db.open_ro(config.DB_PATHS["market_data"])
    conn_pa = db.open_ro(config.DB_PATHS["paisamaker"])
    conn_al = db.open_ro(config.DB_PATHS["alert_log"])

    try:
        # Sanity (cheap; cached pragmas reused across runs by SQLite)
        report = sanity.run_sanity_checks(conn_md, conn_al, conn_pa, today_et=today_et)
        if not report.all_ok:
            raise sanity.SchemaDriftError(
                "sanity check failed: " + " | ".join(report.notes)
            )

        # ATTACH alert_log onto paisa for cross-DB queries
        db.attach_ro(conn_pa, config.DB_PATHS["alert_log"], "adv")

        # is_rth check via market_holidays + clock
        holidays = rth.load_holidays(conn_md)
        state = rth.get_rth_state(datetime.now(ET), holidays)

        if not state.is_rth:
            return schema.build_live(
                today_iso=today_iso,
                is_rth=False,
                rth_window_et=None,
                source_commits=_source_commits(),
            )

        # --- inside RTH: build full live payload ---

        # Spot per ticker (instrument_registry → instrument_id → scalar_readings)
        ticker_map = queries.query_tracked_tickers(conn_md)
        spot: dict[str, dict] = {}
        for symbol, instrument_id in ticker_map.items():
            row = queries.query_latest_spot(conn_md, instrument_id)
            if row and row["spot"] is not None:
                spot[symbol] = {
                    "px": row["spot"],
                    "as_of": row["stored_at"],
                }

        # Recent alerts in last 30 min (production filter)
        alerts_recent = queries.query_alerts_recent_30m(conn_pa)
        alerts_recent_30m = [schema.alert_row_for_json(dict(a)) for a in alerts_recent]

        # Open positions (confirmed-live filter)
        open_pos_rows = queries.query_open_positions(conn_pa)
        open_positions = []
        for p in open_pos_rows:
            d = dict(p)
            right, strike = schema._parse_contract(d.get("contract_symbol", ""))
            open_positions.append({
                "ticker": d.get("ticker"),
                "right": right,
                "strike": strike,
                "expiry": schema._parse_expiry(d.get("contract_symbol", ""), d.get("entry_time")),
                "entry_time": d.get("entry_time"),
                "entry_px": d.get("entry_fill_price") or d.get("entry_price"),
                "current_px": None,            # v1 omits unrealized P&L
                "unrealized_pnl_usd": None,    # v1 omits
                "alert_id": d.get("advisor_alert_id"),
                "source": d.get("source"),
            })

        # Session-so-far totals
        ssf = queries.query_session_so_far(conn_pa, today_iso)

        # Live state from log parsing + paisa tables
        current_mode = log_parser.parse_current_mode()
        kill_switch = log_parser.get_kill_switch_state(conn_pa)
        ibkr_disconnects = log_parser.count_ibkr_disconnects_today(journal_unit=journal_unit)
        # mkt_forced_active: read from bot_state if present; not surfaced in v1 schema_version=1
        # but logged for diagnostics
        try:
            mkt_forced = log_parser.get_mkt_forced_active(conn_pa)
            log.debug("mkt_forced_active=%s", mkt_forced)
        except Exception:
            pass

        # Build today's funnel using session_so_far counts
        # We need: alerts_emitted today, entry_type_excluded, upstream_received,
        # skip_counts, positions_filled today, positions_void today.
        all_alerts_today = queries.query_alerts_emitted(conn_pa, today_iso)
        entry_type_excluded = sum(
            1 for a in all_alerts_today
            if a["alert_type"] in config.EXIT_ALERT_TYPES
        )
        upstream_received = queries.query_upstream_received_count(conn_pa, today_iso)
        skip_counts = funnel.filter_skip_counts_for_funnel(
            queries.query_skip_counts(conn_pa, today_iso)
        )
        positions_today = queries.query_positions_for_date(conn_pa, today_iso)
        pos_filled = sum(1 for p in positions_today if p["entry_exec_status"] == "filled" and p["source"] == "advisor")
        pos_void = sum(1 for p in positions_today if p["entry_exec_status"] == "failed_cancelled" and p["source"] == "advisor")
        funnel_stages = funnel.build_funnel(
            alerts_emitted=len(all_alerts_today),
            entry_type_excluded=entry_type_excluded,
            upstream_received=upstream_received,
            skip_counts=skip_counts,
            positions_filled=pos_filled,
            positions_void=pos_void,
        )

        session_so_far = {
            **ssf,
            "funnel": funnel_stages,
            "ibkr_disconnects": ibkr_disconnects,
            "current_mode": current_mode,
            "kill_switch_state": kill_switch,
            "trades": [],  # closed trades today — light version, full list in daily rollup
        }

        return schema.build_live(
            today_iso=today_iso,
            is_rth=True,
            rth_window_et=(state.close_time_et and ("09:30", state.close_time_et)) or ("09:30", "16:00"),
            source_commits=_source_commits(),
            spot=spot,
            alerts_recent_30m=alerts_recent_30m,
            open_positions=open_positions,
            session_so_far=session_so_far,
        )

    finally:
        conn_md.close()
        conn_pa.close()
        conn_al.close()


def _atomic_write(target: Path, payload: dict) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"), default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="gex-cron-runner: live rollup writer")
    parser.add_argument("--journal-unit", default=None,
                        help="systemd unit name to journalctl for ibkr_disconnects (default: file fallback)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip rclone push + HC ping; print JSON to stdout.")
    parser.add_argument("--print-json", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    try:
        payload = build_live_payload(journal_unit=args.journal_unit)
    except sanity.SchemaDriftError as e:
        log.error("HARD FAIL: %s", e)
        if not args.dry_run:
            try:
                health.ping(config.get_live_hc_uuid(), fail=True, message=str(e)[:200])
            except RuntimeError:
                pass
        return 1
    except Exception as e:
        log.exception("live writer failed: %s", e)
        if not args.dry_run:
            try:
                health.ping(config.get_live_hc_uuid(), fail=True, message=f"{type(e).__name__}: {e}"[:200])
            except RuntimeError:
                pass
        return 1

    if args.print_json or args.dry_run:
        print(json.dumps(payload, indent=2, default=str))

    if args.dry_run:
        log.info("dry-run complete; no write, no push")
        return 0

    # Write locally
    out_file = config.OUTPUT_DIRS["published_live"] / "live.json"
    _atomic_write(out_file, payload)

    # rclone push to R2
    try:
        r2_push.copyto_r2(
            local_file=out_file,
            rclone_remote=config.R2_RCLONE_REMOTE,
            bucket=config.R2_BUCKET,
            key=config.R2_LIVE_KEY,
        )
    except r2_push.R2PushError as e:
        # Soft-fail: log but don't crash. The 30s timer retries.
        log.warning("r2 push failed (soft): %s", e)

    # HC ping
    health.ping(config.get_live_hc_uuid())
    log.debug("live rollup complete (is_rth=%s)", payload["is_rth"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
