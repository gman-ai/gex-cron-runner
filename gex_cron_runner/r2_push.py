"""Push live.json to Cloudflare R2 via rclone.

Bandwidth-capped (`--bwlimit=1M`) to avoid contending with paisa's IBKR socket
during RTH. Cache-Control header set so CDN refreshes between polls
(`max-age=20` is 5s less than poll interval).

Soft-fails: a transient R2 outage shouldn't crash the cron — the next 30s
firing retries. The staleness ladder on the udayx UI handles user-facing
fallback.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)


class R2PushError(RuntimeError):
    pass


def copyto_r2(
    local_file: Path,
    *,
    rclone_remote: str,  # e.g., "udayx-r2"
    bucket: str,         # e.g., "udayx-live"
    key: str,            # e.g., "live.json"
    cache_control: str = "public,max-age=20",
    bwlimit: str = "1M",
    timeout: int = 15,
    dry_run: bool = False,
) -> None:
    """rclone copyto local_file remote:bucket/key with bandwidth cap + headers.

    Why `copyto` (not `copy`): copyto allows renaming the destination — we
    always upload to a single `live.json` key.

    On dry_run=True, logs the rclone command but doesn't execute.
    """
    dest = f"{rclone_remote}:{bucket}/{key}"
    cmd = [
        "rclone", "copyto", str(local_file), dest,
        f"--bwlimit={bwlimit}",
        f"--header=Cache-Control: {cache_control}",
        "--no-traverse",  # don't list bucket — we know the destination key
        f"--timeout={timeout}s",
        "--retries=1",     # we have our own retry via the 30s cron cadence
        "--low-level-retries=1",
    ]
    if dry_run:
        log.info("[dry_run] would run: %s", " ".join(cmd))
        return

    log.debug("rclone: %s", " ".join(cmd))
    try:
        subprocess.run(
            cmd, check=True, capture_output=True, text=True, timeout=timeout + 5,
        )
        log.info("uploaded %s → %s", local_file, dest)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr or ""
        log.warning("rclone copyto failed: %s", stderr[:300])
        # Soft-fail: don't crash; next 30s will retry.
        raise R2PushError(f"rclone copyto failed: {stderr[:200]}")
    except subprocess.TimeoutExpired as e:
        log.warning("rclone copyto timed out after %ds", timeout + 5)
        raise R2PushError(f"rclone copyto timed out after {timeout + 5}s")
