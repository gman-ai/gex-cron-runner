"""gex-cron-runner — daily + live rollup writer for udayx.com/trading.

Reads paisamaker / gex-advisor / market-data-fetcher SQLite DBs read-only and
publishes JSON to udayx.com (daily, via git push) and Cloudflare R2 (live, via
rclone). Runs as a dedicated unprivileged user on the Hetzner VM.

Non-interference promise: this package is a passive observer. Read-only at
three layers (URI flag, query_only pragma, code review). Holds short snapshot
transactions only when needed for cross-table consistency. Resource-capped via
systemd. Outbound-only. See plan §15 for the full guarantees.
"""

__version__ = "0.1.0"
