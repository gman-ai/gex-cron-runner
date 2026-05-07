"""Cron #1 — daily rollup writer.

Runs at 00:05 ET. Reads paisa + alert_log + market_data DBs read-only via a
WAL snapshot transaction, builds the daily JSON, and pushes to the udayx
repo's `public/trading/daily/{date}.json`.

Exit codes:
  0 = success (HC ping sent)
  1 = sanity drift, query failure, or push failure (HC fail-ping sent;
      systemd Restart=on-failure with 60s backoff)

Dry-run mode (--dry-run): runs the full pipeline but skips git push and HC
ping. Used for smoke tests on the VM before enabling the timer.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from gex_cron_runner import (
    config, db, funnel, git_push, health, queries, rth, sanity, schema,
)

ET = ZoneInfo("America/New_York")
log = logging.getLogger("gex_cron_runner.daily_writer")


def _setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def yesterday_et() -> date:
    return (datetime.now(ET) - timedelta(days=1)).date()


def _git_sha(path: str) -> str:
    """Best-effort source SHA for traceability. Returns 'unknown' if not a git dir."""
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


def _alerts_block(alerts_rows, entry_type_excluded_count: int) -> dict:
    """Build the daily.alerts dict from query rows."""
    by_type: dict[str, int] = {}
    tickers: set[str] = set()
    rows_out: list[dict] = []
    is_test_excluded_count = 0  # we already filtered is_test=0; this is for the JSON metadata

    for r in alerts_rows:
        d = dict(r)
        by_type[d["alert_type"]] = by_type.get(d["alert_type"], 0) + 1
        if d.get("ticker"):
            tickers.add(d["ticker"])
        rows_out.append(schema.alert_row_for_json(d))

    return {
        "emitted_prod": len(alerts_rows),
        "is_test_excluded": is_test_excluded_count,  # always 0 since query filters
        "by_type": by_type,
        "tickers": sorted(tickers),
        "rows": rows_out,
    }


def _execution_block(positions, current_mode: str, ibkr_disconnects: int) -> dict:
    """Build the daily.execution dict + closed-trade rows."""
    closed_trades = []
    voids = 0
    opened = 0
    closed = 0
    wins = 0
    losses = 0
    break_even = 0
    pnl_total = 0.0
    max_dd = 0.0
    running_pnl = 0.0

    # Sort by entry_time for sequential walk (max_drawdown computation)
    sorted_pos = sorted(positions, key=lambda p: p["entry_time"])
    for p in sorted_pos:
        d = dict(p)
        if d.get("status") == "void":
            voids += 1
            continue
        if d.get("entry_exec_status") == "filled":
            opened += 1
        if d.get("status") == "closed":
            closed += 1
            pnl = float(d.get("pnl") or 0.0)
            running_pnl += pnl
            if pnl > 0: wins += 1
            elif pnl < 0: losses += 1
            else: break_even += 1
            pnl_total += pnl
            max_dd = min(max_dd, running_pnl)
            closed_trades.append(schema.trade_row_for_json(d, alert=None))

    return {
        "positions_opened": opened,
        "positions_closed": closed,
        "positions_void": voids,
        "wins": wins,
        "losses": losses,
        "break_even": break_even,
        "paper_pnl_usd": round(pnl_total, 2),
        "max_drawdown_usd": round(max_dd, 2),
        "ibkr_disconnects": ibkr_disconnects,
        "end_of_session_mode": current_mode,
        "eod_flatten_count": 0,  # paisa doesn't expose this; placeholder
        "trades": closed_trades,
    }


def build_daily_payload(target_date: date, *, dry_run: bool = False) -> dict:
    """Run all queries against the VM DBs and build the daily JSON dict.

    Returns the JSON-serializable dict. Caller writes to disk + pushes.
    """
    iso = target_date.isoformat()

    # 1. Open RO connections
    conn_md = db.open_ro(config.DB_PATHS["market_data"])
    conn_pa = db.open_ro(config.DB_PATHS["paisamaker"])
    conn_al = db.open_ro(config.DB_PATHS["alert_log"])

    try:
        # 2. Sanity checks (hard fail on drift)
        report = sanity.run_sanity_checks(conn_md, conn_al, conn_pa, today_et=date.today())
        if not report.all_ok:
            raise sanity.SchemaDriftError(
                "sanity check failed: " + " | ".join(report.notes)
            )

        # 3. Closed-market early return
        holidays = rth.load_holidays(conn_md)
        if iso in holidays or target_date.weekday() >= 5:
            log.info("date %s is closed (weekend or holiday) — emitting closed-market payload", iso)
            return schema.build_closed_market_daily(
                date_iso=iso,
                market_status="closed_holiday" if iso in holidays else "closed_weekend",
                source_commits=_source_commits(),
            )

        # 4. ATTACH alert_log onto paisa connection for cross-DB join
        db.attach_ro(conn_pa, config.DB_PATHS["alert_log"], "adv")

        # 5. Snapshot read of all data we need
        with db.snapshot(conn_pa, label=f"daily-rollup-{iso}"):
            alerts = queries.query_alerts_emitted(conn_pa, iso)
            entry_type_excluded = sum(
                1 for a in alerts
                if a["alert_type"] in config.EXIT_ALERT_TYPES
            )
            upstream_received = queries.query_upstream_received_count(conn_pa, iso)
            raw_skip_counts = queries.query_skip_counts(conn_pa, iso)
            skip_counts = funnel.filter_skip_counts_for_funnel(raw_skip_counts)
            positions = queries.query_positions_for_date(conn_pa, iso)

        # 6. Compute funnel
        positions_filled = sum(
            1 for p in positions if p["entry_exec_status"] == "filled" and p["source"] == "advisor"
        )
        positions_void = sum(
            1 for p in positions if p["entry_exec_status"] == "failed_cancelled" and p["source"] == "advisor"
        )
        funnel_stages = funnel.build_funnel(
            alerts_emitted=len(alerts),
            entry_type_excluded=entry_type_excluded,
            upstream_received=upstream_received,
            skip_counts=skip_counts,
            positions_filled=positions_filled,
            positions_void=positions_void,
        )
        arith_errors = funnel.validate_arithmetic(funnel_stages)
        for err in arith_errors:
            log.warning("funnel arithmetic: %s", err)

        # 7. Bundle 1 caveat detection
        bundle_1_warning = any(
            (a["fired_at"] or "") < config.BUNDLE_1_CUTOFF_UTC
            for a in alerts
        )

        # 8. Build payload + metadata
        funnel_warnings: list[str] = list(arith_errors)
        # If paisa received more signals than alert_log emitted (filtered prod),
        # surface the divergence in metadata. This happens when paisa's advisor
        # poller has a different view of the alert universe than our prod filter.
        if upstream_received > len(alerts):
            note = (
                f"upstream_received ({upstream_received}) > alerts_emitted ({len(alerts)}). "
                f"paisa picked up signals not visible in alert_log under prod filter "
                f"(SHADOW_/STRUCTURE_BREAK exclusion). Funnel starts from upstream_received."
            )
            log.warning(note)
            funnel_warnings.append(note)

        payload = schema.build_daily(
            date_iso=iso,
            market_status="open",
            rth_window_et=("09:30", "16:00"),
            source_commits=_source_commits(),
            alerts_block=_alerts_block(alerts, entry_type_excluded),
            funnel=funnel_stages,
            execution=_execution_block(positions, current_mode="FULLY_LIVE", ibkr_disconnects=0),
            holiday_warning=report.holiday_warning,
            bundle_1_warning=bundle_1_warning,
        )
        if funnel_warnings:
            payload.setdefault("metadata", {})["funnel_warnings"] = funnel_warnings
            schema.validate_daily(payload)  # re-validate after metadata edit
        return payload
    finally:
        conn_md.close()
        conn_pa.close()
        conn_al.close()


def _source_commits() -> dict[str, str]:
    """Collect short SHAs for traceability. paisa/advisor SHAs are cron-side
    best-effort — for true authoritative SHAs we'd need to read them from the
    deployed install, which we skip in v1."""
    cron_path = str(Path(__file__).parent.parent)  # repo root
    return {
        "paisamaker": _git_sha("/opt/paisamaker-app") if os.path.isdir("/opt/paisamaker-app") else "vm-only",
        "gex_advisor": _git_sha("/opt/gex-advisor") if os.path.isdir("/opt/gex-advisor") else "vm-only",
        "cron_runner": _git_sha(cron_path),
    }


def _atomic_write(target: Path, payload: dict) -> None:
    """tmp → fsync → rename pattern."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f, separators=(",", ":"), default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="gex-cron-runner: daily rollup writer")
    parser.add_argument("--date", help="Target date YYYY-MM-DD (default: yesterday ET)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Skip git push + HC ping; print JSON to stdout instead of writing.")
    parser.add_argument("--print-json", action="store_true",
                        help="Print payload to stdout (in addition to or instead of writing).")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)

    target = date.fromisoformat(args.date) if args.date else yesterday_et()
    log.info("daily rollup for %s (dry_run=%s)", target.isoformat(), args.dry_run)

    try:
        payload = build_daily_payload(target, dry_run=args.dry_run)
    except sanity.SchemaDriftError as e:
        log.error("HARD FAIL: %s", e)
        if not args.dry_run:
            try:
                health.ping(config.get_daily_hc_uuid(), fail=True, message=str(e)[:200])
            except RuntimeError:
                pass  # HC UUID not configured yet
        return 1
    except Exception as e:
        log.exception("daily rollup failed: %s", e)
        if not args.dry_run:
            try:
                health.ping(config.get_daily_hc_uuid(), fail=True, message=f"{type(e).__name__}: {e}"[:200])
            except RuntimeError:
                pass
        return 1

    # Output handling
    if args.print_json or args.dry_run:
        print(json.dumps(payload, indent=2, default=str))

    if args.dry_run:
        log.info("dry-run complete; no write, no push")
        return 0

    # Write to local published dir (atomic)
    out_dir = config.OUTPUT_DIRS["published_daily"]
    out_file = out_dir / f"{target.isoformat()}.json"
    _atomic_write(out_file, payload)
    log.info("wrote %s", out_file)

    # Push to udayx repo (skipped in dry-run)
    pat = config.get_github_pat()
    git_push.ensure_clone(
        config.OUTPUT_DIRS["udayx_clone"],
        config.UDAYX_REPO_URL,
        pat=pat,
    )
    git_push.push_daily_json(
        clone_dir=config.OUTPUT_DIRS["udayx_clone"],
        relative_path=f"{config.UDAYX_DAILY_PATH}/{target.isoformat()}.json",
        src_file=out_file,
        commit_message=f"daily-rollup: {target.isoformat()}",
        pat=pat,
    )

    # HC ping
    health.ping(config.get_daily_hc_uuid())
    log.info("daily rollup complete for %s", target.isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
