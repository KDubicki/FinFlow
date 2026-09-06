"""The nightly copy that leaves the machine.

``PROJECT.md`` §11.3 calls the ops store the one genuinely irreplaceable piece
of state, and an untested restore a hope with a cron schedule. So the restore
path is exercised here, along with the two retention tiers and the refusal to
run at all without a destination on a second device.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from finflow.adapters.ops.backup import backup, latest_backup, prune
from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.config import Settings
from finflow.contracts.sources import SourceKey
from finflow.entrypoints.cli import backup as backup_cli
from finflow.ports.ops_store import Watermark

NOW = dt.datetime(2026, 9, 6, 2, 0, tzinfo=dt.UTC)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    store = SqliteOpsStore(tmp_path / "data" / "ops.sqlite")
    store.save_watermark(Watermark(SourceKey.STOOQ, "GLD", dt.date(2026, 9, 4), NOW, 42))
    (tmp_path / "data" / "raw" / "source=stooq").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "source=stooq" / "data.parquet").write_bytes(b"not really")
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_dir=tmp_path / "data",
        backup_dir=tmp_path / "second-device",
        log_json=True,
    )


class TestRetention:
    def test_the_monthly_tier_survives_the_daily_window(self, tmp_path: Path) -> None:
        # Thirty daily copies of a database that was corrupted six weeks ago are
        # thirty copies of a corrupted database. The monthly tier is what is
        # left to restore from.
        SqliteOpsStore(tmp_path / "ops.sqlite")
        for month in range(1, 5):
            for day in (1, 15, 28):
                backup(
                    tmp_path / "ops.sqlite",
                    tmp_path / "backups",
                    now=dt.datetime(2026, month, day, tzinfo=dt.UTC),
                )

        prune(tmp_path / "backups", keep_daily=2, keep_monthly=3)
        kept = sorted(p.name for p in (tmp_path / "backups").glob("ops-*"))

        # The two most recent, plus the first archive of each of the last three
        # months — with the overlap counted once.
        assert kept == [
            "ops-20260201T000000Z.sqlite.gz",
            "ops-20260301T000000Z.sqlite.gz",
            "ops-20260401T000000Z.sqlite.gz",
            "ops-20260415T000000Z.sqlite.gz",
            "ops-20260428T000000Z.sqlite.gz",
        ]


class TestTheCommand:
    def test_it_refuses_to_run_without_a_second_device(self, tmp_path: Path) -> None:
        settings = Settings(_env_file=None, data_dir=tmp_path)  # type: ignore[call-arg]
        assert backup_cli.main([], settings=settings) == 2

    def test_a_backup_lands_and_the_raw_zone_is_mirrored(self, settings: Settings) -> None:
        assert backup_cli.main([], settings=settings) == 0
        assert settings.backup_dir is not None
        assert latest_backup(settings.backup_dir / "ops") is not None
        # rsync may be absent on a stripped-down runner; the mirror is skipped
        # rather than failing the job, so its presence is not asserted here.

    def test_the_check_verifies_without_touching_the_live_store(
        self, settings: Settings, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The monthly restore drill, as a command rather than a note in a
        # runbook nobody opens.
        backup_cli.main([], settings=settings)
        assert backup_cli.main(["--check"], settings=settings) == 0
        assert "verified" in capsys.readouterr().out
        assert not (settings.data_dir / "ops-restore-check.sqlite").exists()

    def test_a_restore_brings_the_store_back(self, settings: Settings) -> None:
        backup_cli.main(["--skip-raw"], settings=settings)
        (settings.data_dir / "ops.sqlite").unlink()

        assert backup_cli.main(["--restore"], settings=settings) == 0
        restored = SqliteOpsStore(settings.data_dir / "ops.sqlite")
        mark = restored.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None and mark.row_count == 42

    def test_a_restore_with_no_backup_fails_loudly(self, settings: Settings) -> None:
        assert backup_cli.main(["--restore"], settings=settings) == 1

    def test_a_configured_encryption_key_with_no_binary_fails_rather_than_writing_plaintext(
        self, settings: Settings, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A backup that is quietly unencrypted is worse than no backup, because
        # it is trusted.
        import subprocess

        def no_age(*args: object, **kwargs: object) -> object:
            raise FileNotFoundError("age")

        monkeypatch.setattr(subprocess, "run", no_age)
        encrypted = settings.model_copy(update={"age_recipient": "age1exampleexample"})

        assert backup_cli.main([], settings=encrypted) == 1
        assert settings.backup_dir is not None
        assert list((settings.backup_dir / "ops").glob("*")) == []
