# gex-cron-runner

Daily + live trading-rollup writer for **udayx.com/trading**. Reads three SQLite databases on the Hetzner VM read-only and publishes JSON outputs:

- **daily/{YYYY-MM-DD}.json** — committed once at 00:15 ET to the `udayx.com` repo's `public/trading/daily/` path.
- **live.json** — overwritten every 30s during RTH and pushed to a Cloudflare R2 public bucket.

## Trading-system non-interference contract

The cron is a **passive observer**. Read-only at three layers:
1. SQLite URI flag `?mode=ro` (required by all three upstream projects, not optional).
2. `PRAGMA query_only=1` after connect.
3. Code review pattern — no `INSERT/UPDATE/DELETE` strings in source.

Plus systemd resource caps (`CPUQuota=20%`, `MemoryMax=128M`, `IOSchedulingClass=idle`, `Nice=10–15`), filesystem hardening (`ProtectSystem=strict`, `ReadOnlyPaths` for the three sibling app dirs), no listening sockets, and dedicated `rollup` user.

If anything looks wrong, kill is one command:

```bash
sudo systemctl stop gex-rollup-{daily,live,live-close}.timer
```

## Cross-project source-of-truth pins

| Project | Reference | Pin |
|---|---|---|
| gex-advisor | `INTEGRATION.md` at repo root | SHA `eec441f`, schema column count = 41 |
| market-data-fetcher | `docs/cross-project/{DATA_CONTRACTS,LATENCY_BUDGET}.md` | (no SHA pin given; re-read on each phase) |
| paisamaker | schema in code (`paisamaker/gex/db.py`) | `bot_state` table added 2026-05-07 PR #222 / `be6f567` |

Cron startup runs `sanity.run_sanity_checks()` which verifies these — drift = exit 1.

## Local development

```bash
# Setup
python3.12 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# Run tests
.venv/bin/pytest -v
```

The test suite uses fixture data exported from paisamaker (saved in `tests/fixtures/`).
Tests run against in-memory SQLite mocking the real schemas. **No tests touch
the live VM DBs.**

## VM dry-run (read-only smoke test)

```bash
# From local laptop, ssh to VM and run the cron in dry-run mode
ssh root@<vm> "cd /tmp/gex-cron-runner && \
  .venv/bin/python -m gex_cron_runner.daily_writer --dry-run --date=2026-05-06"
```

The `--dry-run` flag skips git push and Healthchecks.io ping. The DBs are
opened read-only; no writes are made anywhere.

Expected baseline diff after a dry-run (vs. before):
- paisa MainPID + NRestarts: unchanged
- discord_signals.db file size (main DB): unchanged
- WAL file may have grown a few KB from paisa's own writes (not ours)
- No new listening ports, no new processes

## Deployment (on the VM)

Status: daily rollups ran in production through 2026-05-07. The timers are
currently paused.

Pre-flight:
1. Repo checked out at `/opt/gex-cron-runner` with a venv at `.venv/`.
2. Dedicated `rollup` user in the `systemd-journal` group (for `journalctl` log parsing).
3. Env files in `/etc/rollup/`:
   - `github.env`: `GITHUB_PAT`, a fine-grained PAT scoped to `gman-ai/udayx.com`.
   - `r2.env`: rclone credentials for the Cloudflare R2 bucket (token write-scoped to `udayx-live`).
   - `healthchecks.env`: `DAILY_HC_UUID` and `LIVE_HC_UUID` (Healthchecks.io checks `gex-rollup-daily`, `gex-rollup-live`).

Install and start the daily cron:

```bash
sudo cp systemd/gex-rollup-* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now gex-rollup-daily.timer
```

Add the live cron once the daily runs are clean:

```bash
sudo systemctl enable --now gex-rollup-live.timer gex-rollup-live-close.timer
```

## Module layout

```
gex_cron_runner/
├── config.py          # paths, schema pins, skip-reason → band map, half-day map
├── db.py              # RO connection helpers + WAL snapshot context manager
├── rth.py             # is_rth_now() with ZoneInfo + holiday/half-day handling
├── sanity.py          # startup schema-version + column-count + holiday-coverage checks
├── log_parser.py      # current_mode + ibkr_disconnects from gex_watch.log
├── funnel.py          # 7-stage gate funnel reconstruction from skip_reason GROUP BY
├── schema.py          # JSON v1 builders for daily + live (jsonschema validated)
├── queries.py         # All SQL queries — centralized for read-only review
├── health.py          # Healthchecks.io ping helpers (soft-fail)
├── git_push.py        # daily JSON → udayx repo via git push (HTTPS PAT)
├── r2_push.py         # live.json → Cloudflare R2 via rclone (--bwlimit=1M)
├── daily_writer.py    # Cron #1 entrypoint
└── live_writer.py     # Cron #2 entrypoint
```

## Known follow-ups (post-v1)

- **Half-day map** (`HALF_DAY_CLOSE_ET` in `config.py`): populate each year before the first half-day session.
- **`current_px`** for unrealized P&L on open positions: omitted from live.json v1 (not persisted; chain-snapshot lookup is v2 work).
- **2027 holiday reseed** in market_data.db: warn at startup if MAX(date) < today + 60d.
- **`gex_watch.log` rotation**: not currently rotated (single 16 MB file at 5 weeks); add logrotate when file >150 MB/yr becomes uncomfortable.
- **Funnel divergence metadata** when paisa picks up signals not in alert_log under prod filter (e.g., SHADOW_ alerts that map to CASCADE_WATCH internally) — currently surfaced via `metadata.funnel_warnings`; UI can render.

## Source-of-truth references

- Upstream contracts: see [Cross-project source-of-truth pins](#cross-project-source-of-truth-pins) above.
- This repo's tests: `tests/test_*.py` against `tests/fixtures/cron_sample_*.json` from paisamaker.
