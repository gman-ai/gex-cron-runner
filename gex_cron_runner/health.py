"""Healthchecks.io ping helpers.

Soft-fails: a network blip on the ping shouldn't fail the cron's exit code,
because that would cascade into INC-004 even though the actual work succeeded.
We log warn and move on. Repeated unhealthy cron runs are detected by HC.io
itself (3 missed pings → alert).
"""
from __future__ import annotations

import logging
from urllib import request
from urllib.error import URLError

log = logging.getLogger(__name__)

HC_BASE = "https://hc-ping.com"


def ping(uuid: str, *, fail: bool = False, message: str | None = None,
         timeout: float = 5.0, dry_run: bool = False) -> bool:
    """Ping Healthchecks.io with the given UUID.

    `fail=True` sends to /{uuid}/fail (signals an unhealthy run).
    Returns True on HTTP 2xx, False otherwise (logged but not raised).
    `dry_run=True` skips the network call entirely.
    """
    url = f"{HC_BASE}/{uuid}/fail" if fail else f"{HC_BASE}/{uuid}"
    if dry_run:
        log.debug("[dry_run] would ping %s", url)
        return True
    try:
        body = (message or "").encode("utf-8") if message else None
        req = request.Request(url, data=body, method="POST" if body else "GET")
        with request.urlopen(req, timeout=timeout) as resp:
            ok = 200 <= resp.status < 300
            if not ok:
                log.warning("healthcheck ping returned %d for %s", resp.status, url)
            return ok
    except (URLError, TimeoutError, OSError) as e:
        # Soft-fail: don't take down the cron over a transient HC.io blip
        log.warning("healthcheck ping failed (soft): %s — %s", url, e)
        return False
