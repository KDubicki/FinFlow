"""Tests for logging configuration."""

from __future__ import annotations

import json

import pytest

from finflow.config import Settings
from finflow.logging import configure_logging, get_logger


def test_configure_is_idempotent(settings: Settings) -> None:
    configure_logging(settings)
    configure_logging(settings)
    assert get_logger("finflow.test") is not None


def test_json_output_is_machine_readable(
    settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(settings.model_copy(update={"log_json": True}))
    get_logger("finflow.test").info("ingest_complete", symbol="GLD", rows=1234)

    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert payload["event"] == "ingest_complete"
    assert payload["symbol"] == "GLD"
    assert payload["rows"] == 1234
    assert payload["level"] == "info"
    assert payload["logger"] == "finflow.test"
    assert "timestamp" in payload


def test_level_filtering_suppresses_debug(
    settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(settings.model_copy(update={"log_json": True}))
    get_logger("finflow.test").debug("should_not_appear")
    assert "should_not_appear" not in capsys.readouterr().err
