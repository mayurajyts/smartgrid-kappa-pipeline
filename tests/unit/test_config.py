"""Tests for environment-driven configuration.

WHY THIS IS TESTED
------------------
Two properties of `common/config.py` are architectural commitments rather than
implementation details, and both are easy to break silently later:

1. Values come from the ENVIRONMENT. The moment a default gets baked in where a
   variable should be read, the "reproducible from a clean clone" and "same code
   for live and replay" claims stop being true — and nothing crashes to tell
   you.

2. Invalid configuration FAILS LOUDLY AT STARTUP rather than being coerced to
   something plausible. A job that silently accepts a bad log level or a
   negative sim-day length starts up healthy and then writes wrong numbers into
   the billing tables.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from common.config import (
    KafkaSettings,
    LoggingSettings,
    PostgresSettings,
    Settings,
    SimClockSettings,
    get_settings,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Settings are cached process-wide, so the cache must be cleared around any
    test that patches the environment."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestEnvironmentOverrides:
    def test_kafka_bootstrap_is_read_from_environment(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-1:9092")
        assert KafkaSettings().kafka_bootstrap_servers == "broker-1:9092"

    def test_topic_names_are_configurable(self, monkeypatch):
        """Topic names must be config: a schema bump to `.v2` or a replay
        against a shadow topic must not require editing source."""
        monkeypatch.setenv("TOPIC_METER_READINGS", "meter.readings.v2")
        assert KafkaSettings().topic_meter_readings == "meter.readings.v2"

    def test_sim_day_length_is_configurable(self, monkeypatch):
        monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "600")
        assert SimClockSettings().sim_day_real_seconds == 600

    def test_sim_start_date_is_configurable(self, monkeypatch):
        monkeypatch.setenv("SIM_START_DATE", "2026-03-15")
        assert SimClockSettings().sim_start_date.isoformat() == "2026-03-15"

    def test_blank_anchor_means_unset(self, monkeypatch):
        """`.env` ships SIM_ANCHOR_REAL= with no value, so the empty string must
        resolve to None rather than raising — otherwise every service refuses to
        start before `make up` has stamped it."""
        monkeypatch.setenv("SIM_ANCHOR_REAL", "")
        assert SimClockSettings().sim_anchor_real is None

    def test_numeric_values_are_coerced_from_strings(self, monkeypatch):
        """Environment variables are always strings; the typed settings layer
        exists precisely so the rest of the codebase never does int() by hand."""
        monkeypatch.setenv("TOPIC_METER_PARTITIONS", "12")
        partitions = KafkaSettings().topic_meter_partitions
        assert partitions == 12
        assert isinstance(partitions, int)

    def test_composed_settings_expose_every_section(self):
        settings = Settings()
        assert settings.sim.sim_day_real_seconds > 0
        assert settings.kafka.topic_meter_readings
        assert settings.postgres.postgres_db
        assert settings.objectstore.s3_curated_bucket
        assert settings.logging.log_level


class TestFailFastValidation:
    def test_zero_sim_day_length_is_rejected(self, monkeypatch):
        """A zero-length simulated day would make the compression ratio a
        division by zero at the first clock read."""
        monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "0")
        with pytest.raises(ValidationError):
            SimClockSettings()

    def test_negative_sim_day_length_is_rejected(self, monkeypatch):
        monkeypatch.setenv("SIM_DAY_REAL_SECONDS", "-300")
        with pytest.raises(ValidationError):
            SimClockSettings()

    def test_zero_partitions_is_rejected(self, monkeypatch):
        monkeypatch.setenv("TOPIC_METER_PARTITIONS", "0")
        with pytest.raises(ValidationError):
            KafkaSettings()

    def test_unknown_log_level_is_rejected(self, monkeypatch):
        """Silently defaulting an unrecognised level would leave an operator
        believing DEBUG was on while it was not."""
        monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
        with pytest.raises(ValidationError):
            LoggingSettings()

    def test_unknown_log_format_is_rejected(self, monkeypatch):
        monkeypatch.setenv("LOG_FORMAT", "xml")
        with pytest.raises(ValidationError):
            LoggingSettings()

    def test_log_level_is_normalised_to_upper_case(self, monkeypatch):
        """Accept the convenient spelling, store the canonical one — this value
        is passed straight to the stdlib logging module."""
        monkeypatch.setenv("LOG_LEVEL", "debug")
        assert LoggingSettings().log_level == "DEBUG"


class TestConnectionStrings:
    def test_dsn_is_assembled_from_parts(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_PORT", "6543")
        monkeypatch.setenv("POSTGRES_DB", "grid")
        monkeypatch.setenv("POSTGRES_USER", "svc")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        assert PostgresSettings().dsn == "postgresql://svc:secret@db.internal:6543/grid"

    def test_jdbc_url_targets_the_same_database_as_the_dsn(self, monkeypatch):
        """The Spark sink (JDBC) and the API (psycopg) must be unable to drift
        onto different stores — both URLs derive from the same fields."""
        monkeypatch.setenv("POSTGRES_HOST", "db.internal")
        monkeypatch.setenv("POSTGRES_PORT", "6543")
        monkeypatch.setenv("POSTGRES_DB", "grid")
        settings = PostgresSettings()
        assert settings.jdbc_url == "jdbc:postgresql://db.internal:6543/grid"
        assert "db.internal:6543/grid" in settings.dsn


class TestCaching:
    def test_get_settings_returns_the_same_instance(self):
        """Cached because Spark calls into user code on every micro-batch;
        re-parsing the environment every 5 seconds would be pure waste."""
        assert get_settings() is get_settings()

    def test_cache_clear_picks_up_a_new_environment(self, monkeypatch):
        first = get_settings().kafka.topic_meter_readings
        monkeypatch.setenv("TOPIC_METER_READINGS", "changed.topic.v1")
        get_settings.cache_clear()
        assert get_settings().kafka.topic_meter_readings == "changed.topic.v1"
        assert first != "changed.topic.v1"
