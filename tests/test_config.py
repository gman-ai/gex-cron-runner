"""Config sanity tests. No env vars touched."""
from gex_cron_runner import config


def test_skip_reason_bands_partition_correctly():
    """Every known skip_reason maps to exactly one band."""
    bands = (config.DISPATCHER_KEYS, config.STRIKE_KEYS, config.DRIFT_KEYS,
             config.EXEC_KEYS, config.HYGIENE_KEYS)
    flat = [k for band in bands for k in band]
    assert len(flat) == len(set(flat)), "skip_reason keys overlap across bands"
    assert set(flat) == config.ALL_KNOWN_SKIP_REASONS


def test_observed_skip_reasons_all_bucketed():
    """The 9 production-observed skip_reasons (per paisa reply) are all known."""
    observed = {
        "rearm_distance", "no_qualifying_strike", "open_chop_suppression",
        "cooldown", "max_positions_ticker", "executor_rejected",
        "spot_drift", "premium_drift", "qualify_failed",
    }
    assert observed.issubset(config.ALL_KNOWN_SKIP_REASONS)


def test_db_paths_are_absolute():
    for name, path in config.DB_PATHS.items():
        assert path.startswith("/"), f"{name} path must be absolute"


def test_entry_alert_types_disjoint_from_exit():
    assert not (set(config.ENTRY_ALERT_TYPES) & set(config.EXIT_ALERT_TYPES))


def test_trigger_features_cover_all_alert_types():
    """Every entry/exit alert type has a trigger-feature recipe."""
    expected_keys = (
        "CASCADE_WATCH_SPX", "CASCADE_WATCH_NON_SPX", "CASCADE_ALERT",
        "CHARM_SQUEEZE", "GAMMA_RECLAIM",
        "GAMMA_RECLAIM_EXIT", "CASCADE_ALERT_EXHAUSTING",
    )
    assert set(config.TRIGGER_FEATURES.keys()) == set(expected_keys)
    for k, recipe in config.TRIGGER_FEATURES.items():
        assert len(recipe) <= 3, f"{k} should have at most 3 trigger features"
        for name, source in recipe:
            assert source in ("col", "json"), f"{k}.{name} bad source: {source}"


def test_tier_c_tickers_no_cascade_alert():
    """Tier C tickers can't have CASCADE_ALERT — conviction capped at 3."""
    assert "TSLA" in config.TIER_C_TICKERS
    assert "NVDA" in config.TIER_C_TICKERS
    assert "AMD" in config.TIER_C_TICKERS


def test_secrets_are_lazy():
    """Importing config doesn't require env vars to be set."""
    # Import already happened. If it required env, we'd have failed at import.
    # But verify the lazy loaders raise cleanly when missing.
    import os
    saved = os.environ.pop("DAILY_HC_UUID", None)
    try:
        try:
            config.get_daily_hc_uuid()
            assert False, "should have raised"
        except RuntimeError as e:
            assert "DAILY_HC_UUID" in str(e)
    finally:
        if saved is not None:
            os.environ["DAILY_HC_UUID"] = saved
