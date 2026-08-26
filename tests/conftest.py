"""Shared fixtures.

The environment is scrubbed of ``FINFLOW_*`` variables for every test so that a
developer's real ``.env`` can never change a test outcome.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from finflow.config import Settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove inherited FinFlow settings so tests are hermetic."""
    for key in list(os_environ_keys()):
        if key.startswith("FINFLOW_"):
            monkeypatch.delenv(key, raising=False)


def os_environ_keys() -> Iterator[str]:
    import os

    yield from os.environ


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings pointed at a throwaway directory, ignoring any local .env."""
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_dir=tmp_path / "data",
        duckdb_path=tmp_path / "data" / "test.duckdb",
    )
