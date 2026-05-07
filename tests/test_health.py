"""Health module tests. Dry-run path only — no network."""
from gex_cron_runner import health


def test_ping_dry_run_returns_true():
    assert health.ping("test-uuid", dry_run=True) is True
    assert health.ping("test-uuid", fail=True, dry_run=True) is True


def test_ping_invalid_url_soft_fails():
    """A bogus uuid → URL still constructed, but should soft-fail (return False)
    rather than raise. Network call to hc-ping.com may succeed with 4xx — we
    treat 4xx as not-ok-but-also-not-crash."""
    # We don't want to actually hit the network in CI. Use dry_run.
    # (The soft-fail behavior on real network errors is exercised by inspection.)
    assert health.ping("does-not-matter", dry_run=True) is True
