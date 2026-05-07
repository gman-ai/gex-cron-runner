"""Push the daily JSON to the udayx repo via git.

The cron maintains a persistent local clone of the udayx repo at
`/var/lib/gex-cron/udayx-clone/`. Each daily run does:
  1. git fetch + git rebase against origin/main (pick up any concurrent pushes)
  2. cp the new daily JSON into public/trading/daily/{D}.json
  3. git add (path-scoped — only this file)
  4. git commit
  5. git push origin main (with retry on transient network errors)

Authentication: HTTPS with a fine-grained PAT scoped to read+write contents on
gman-ai/udayx.com. PAT loaded from /etc/rollup/github.env at cron start.

Dry-run mode: skips the push (and optionally the commit). Used in tests +
smoke tests.
"""
from __future__ import annotations

import logging
import shutil
import subprocess
import time
from pathlib import Path

from gex_cron_runner import config

log = logging.getLogger(__name__)


class GitPushError(RuntimeError):
    """Non-recoverable push error after retries exhausted."""


def _run_git(args: list[str], cwd: Path, env: dict | None = None,
             check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess:
    """Run `git <args>` in cwd. Raises CalledProcessError if check=True and rc!=0."""
    cmd = ["git"] + args
    log.debug("git: cwd=%s cmd=%s", cwd, args)
    return subprocess.run(
        cmd, cwd=str(cwd), env=env, check=check, capture_output=True, text=True,
        timeout=timeout,
    )


def ensure_clone(clone_dir: Path, repo_url: str, pat: str | None = None) -> None:
    """Ensure a local clone exists at clone_dir. If missing, clone fresh.

    First-run pattern: this is called on every cron startup; if the dir is
    populated and points to the right remote, we re-use it.
    """
    if (clone_dir / ".git").exists():
        # Verify remote points where we expect
        result = _run_git(["remote", "get-url", "origin"], cwd=clone_dir, check=False)
        if result.returncode == 0 and repo_url in result.stdout.strip():
            log.debug("clone already present at %s", clone_dir)
            return
        log.warning("clone at %s has wrong remote; recloning", clone_dir)
        shutil.rmtree(clone_dir)

    clone_dir.parent.mkdir(parents=True, exist_ok=True)
    auth_url = repo_url
    if pat:
        # Insert PAT into HTTPS URL: https://founder:PAT@github.com/...
        if repo_url.startswith("https://"):
            auth_url = repo_url.replace("https://", f"https://founder:{pat}@", 1)
    log.info("cloning %s → %s", repo_url, clone_dir)
    subprocess.run(
        ["git", "clone", auth_url, str(clone_dir)],
        check=True, capture_output=True, text=True, timeout=120,
    )
    # Set git author on the clone
    name, email = config.get_git_author()
    _run_git(["config", "user.name", name], cwd=clone_dir)
    _run_git(["config", "user.email", email], cwd=clone_dir)


def push_daily_json(
    clone_dir: Path,
    relative_path: str,  # e.g., "public/trading/daily/2026-05-06.json"
    src_file: Path,
    *,
    commit_message: str,
    pat: str | None = None,
    dry_run: bool = False,
    max_retries: int = 2,
) -> None:
    """Copy src_file → clone_dir/relative_path, commit, push.

    On dry_run=True, runs everything except the final `git push`.
    """
    target = clone_dir / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)

    # 1. git fetch + rebase to be current with main
    if not dry_run:
        _run_git(["fetch", "origin", "main"], cwd=clone_dir)
        _run_git(["reset", "--hard", "origin/main"], cwd=clone_dir)
        # Note: hard reset is safe because the cron's clone is path-scoped —
        # only commits we made would get clobbered, and they're already on origin.

    # 2. Copy file
    shutil.copy2(src_file, target)

    # 3. git add + commit (path-scoped — only the target file)
    _run_git(["add", relative_path], cwd=clone_dir)
    # Check if there's anything to commit
    diff_result = _run_git(
        ["diff", "--cached", "--quiet"], cwd=clone_dir, check=False,
    )
    if diff_result.returncode == 0:
        log.info("no changes to commit (file unchanged): %s", relative_path)
        return

    _run_git(["commit", "-m", commit_message], cwd=clone_dir)

    # 4. git push with retry
    if dry_run:
        log.info("[dry_run] would push %s", relative_path)
        return

    last_err: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            _run_git(["push", "origin", "main"], cwd=clone_dir, timeout=60)
            log.info("pushed %s (attempt %d)", relative_path, attempt + 1)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            stderr = e.stderr if isinstance(e.stderr, str) else (e.stderr or b"").decode("utf-8", "replace")
            log.warning("git push attempt %d failed: %s", attempt + 1, stderr[:300])
            if attempt < max_retries:
                backoff = 10 * (3 ** attempt)  # 10s, 30s
                log.info("sleeping %ds before retry", backoff)
                time.sleep(backoff)
        except subprocess.TimeoutExpired as e:
            last_err = e
            log.warning("git push attempt %d timed out", attempt + 1)
            if attempt < max_retries:
                time.sleep(10 * (3 ** attempt))

    raise GitPushError(f"git push failed after {max_retries + 1} attempts: {last_err}")
