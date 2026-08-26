"""Tests for the settings surface."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from finflow.config import (
    Environment,
    LogLevel,
    MissingSettingError,
    Settings,
    get_settings,
)


def test_defaults_are_local(settings: Settings) -> None:
    assert settings.env is Environment.LOCAL
    assert settings.log_level is LogLevel.INFO
    assert settings.is_local


def test_reads_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINFLOW_ENV", "prod")
    monkeypatch.setenv("FINFLOW_LOG_LEVEL", "DEBUG")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.env is Environment.PROD
    assert s.log_level is LogLevel.DEBUG
    assert not s.is_local


def test_raw_dir_derives_from_data_dir(settings: Settings) -> None:
    assert settings.raw_dir == settings.data_dir / "raw"


def test_secrets_are_not_exposed_by_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINFLOW_FRED_API_KEY", "super-secret-value")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert "super-secret-value" not in repr(s)
    assert "super-secret-value" not in str(s)
    assert s.require("fred_api_key") == "super-secret-value"


def test_require_raises_actionable_error_when_missing(settings: Settings) -> None:
    with pytest.raises(MissingSettingError) as exc:
        settings.require("fred_api_key")
    assert "FINFLOW_FRED_API_KEY" in str(exc.value)
    assert exc.value.field == "fred_api_key"


def test_settings_are_frozen(settings: Settings) -> None:
    with pytest.raises(ValidationError):
        settings.env = Environment.PROD


def test_invalid_retry_count_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINFLOW_HTTP_MAX_RETRIES", "99")
    with pytest.raises(ValueError):
        Settings(_env_file=None)  # type: ignore[call-arg]


def test_tilde_in_paths_is_expanded() -> None:
    s = Settings(_env_file=None, data_dir=Path("~/finflow-data"))  # type: ignore[call-arg]
    assert "~" not in str(s.data_dir)


def test_get_settings_is_cached() -> None:
    get_settings.cache_clear()
    assert get_settings() is get_settings()
    get_settings.cache_clear()
