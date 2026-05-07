"""git_push tests. Pure logic only — no network, no real git."""
from gex_cron_runner.git_push import _url_matches


def test_plain_url_matches_itself():
    assert _url_matches(
        "https://github.com/gman-ai/udayx.com.git",
        "https://github.com/gman-ai/udayx.com.git",
    )


def test_pat_embedded_url_matches_plain():
    """The on-disk URL has a PAT in userinfo; target URL doesn't. Should match."""
    assert _url_matches(
        "https://founder:github_pat_xxxxx@github.com/gman-ai/udayx.com.git",
        "https://github.com/gman-ai/udayx.com.git",
    )


def test_different_repos_dont_match():
    assert not _url_matches(
        "https://github.com/gman-ai/udayx.com.git",
        "https://github.com/gman-ai/gex-cron-runner.git",
    )


def test_different_hosts_dont_match():
    assert not _url_matches(
        "https://gitlab.com/gman-ai/udayx.com.git",
        "https://github.com/gman-ai/udayx.com.git",
    )


def test_trailing_slash_normalized():
    assert _url_matches(
        "https://github.com/gman-ai/udayx.com.git/",
        "https://github.com/gman-ai/udayx.com.git",
    )
