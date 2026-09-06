"""The whole pipeline, once, offline.

Everything real except the vendor and the chat: a synthetic source, the actual
raw zone on disk, the actual DuckDB warehouse, the actual dbt build, the actual
ops store. The clock is frozen, so the run is reproducible — which is what makes
"the same day twice produces one message" an assertion rather than an anecdote.

These are the acceptance criteria of M4 that can be checked in CI. The ones that
cannot — seven consecutive green days, a digest a human finds readable — are
what the soak period is for.
"""

from __future__ import annotations

import datetime as dt
import textwrap
from pathlib import Path

import pytest

from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.config import Settings
from finflow.entrypoints.cli import daily
from tests.fakes import FrozenClock

NOW = dt.datetime(2026, 3, 2, 5, 30, tzinfo=dt.UTC)

REGISTRY = textwrap.dedent(
    """
    instruments:
      - symbol: GLD
        name: SPDR Gold Shares
        asset_class: commodity
        sub_class: precious_metals
        exchange: ARCA
        currency: USD
        calendar: XNYS
        inception: 2004-11-18
        backfill_start: 2025-01-02
        sources: { synthetic: GLD }
        costs: { commission_bps: 2, spread_bps: 2 }
        enabled: true

      - symbol: IAU
        name: iShares Gold Trust
        asset_class: commodity
        sub_class: precious_metals
        exchange: ARCA
        currency: USD
        calendar: XNYS
        inception: 2005-01-21
        backfill_start: 2025-01-02
        sources: { synthetic: IAU }
        costs: { commission_bps: 2, spread_bps: 2 }
        enabled: true
    """
).strip()

UNIVERSES = textwrap.dedent(
    """
    universes:
      precious_metals:
        description: Gold, for a test that must stay small
        members: [GLD, IAU]
        benchmark: GLD
    """
).strip()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """A whole installation in a temporary directory.

    Its own two-instrument registry rather than the project's: the run has to
    ingest every enabled instrument from ``backfill_start``, and a test that
    fetched twenty years for eight symbols would be a test nobody runs.
    """
    registry_dir = tmp_path / "instruments"
    registry_dir.mkdir()
    (registry_dir / "commodities.yml").write_text(REGISTRY, encoding="utf-8")
    (registry_dir / "universes.yml").write_text(UNIVERSES, encoding="utf-8")

    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        data_dir=tmp_path / "data",
        registry_dir=registry_dir,
        log_json=True,
    )


def run(settings: Settings, *args: str, at: dt.datetime = NOW) -> int:
    return daily.main(["--offline", "--dry-run", *args], settings=settings, clock=FrozenClock(at))


@pytest.mark.slow
class TestTheDailyRun:
    def test_one_run_ingests_builds_decides_and_speaks(
        self, settings: Settings, capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert run(settings) == 0
        printed = capsys.readouterr().out

        ops = SqliteOpsStore(settings.data_dir / "ops.sqlite")
        (last,) = ops.runs(limit=1)
        assert last.status == "succeeded"
        assert last.snapshot_id is not None

        # A decision was recorded and a digest was produced. Both, every day:
        # the digest is unconditional, which is what makes its absence mean
        # something.
        assert ops.decisions()
        assert "FinFlow 2026-03-02" in printed
        assert "run " in printed

    def test_the_warehouse_is_rebuilt_from_the_raw_zone(self, settings: Settings) -> None:
        # "Deleting the local warehouse and re-running rebuilds it from the raw
        # zone on disk" — the acceptance criterion, run rather than asserted.
        assert run(settings) == 0
        warehouse = settings.data_dir / "warehouse.duckdb"
        assert warehouse.exists()
        warehouse.unlink()

        assert run(settings, "--skip-ingest", at=NOW + dt.timedelta(hours=6)) == 0
        assert warehouse.exists()

    def test_a_second_run_on_the_same_data_sends_no_second_message(
        self, settings: Settings
    ) -> None:
        # The ≥95% no-action target, enforced. An unchanged instruction must
        # produce no message at all, or the channel gets muted within a month.
        assert run(settings) == 0
        ops = SqliteOpsStore(settings.data_dir / "ops.sqlite")
        after_first = len(ops.sent()) + len(ops.pending())

        assert run(settings, "--skip-ingest", at=NOW + dt.timedelta(hours=6)) == 0
        assert len(ops.sent()) + len(ops.pending()) == after_first

    def test_the_run_is_deterministic_under_a_frozen_clock(self, settings: Settings) -> None:
        assert run(settings) == 0
        ops = SqliteOpsStore(settings.data_dir / "ops.sqlite")
        first = ops.decisions()[0]

        assert run(settings, "--skip-ingest", at=NOW) == 0
        assert ops.decisions()[0].decision_id == first.decision_id

    def test_a_second_process_exits_rather_than_writing_concurrently(
        self, settings: Settings
    ) -> None:
        # PROJECT.md §11.6: a manual backfill started while the scheduled run is
        # in flight is the realistic collision, because a backfill is what you
        # start when something looks wrong.
        from finflow.entrypoints.cli.locking import ExclusiveLock

        with ExclusiveLock(settings.data_dir / "pipeline.lock"):
            assert run(settings) == 3

    def test_a_broken_source_produces_a_failure_message_not_silence(
        self,
        settings: Settings,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # "Deliberately breaking the Stooq client causes a failure message, not
        # silence." Broken here at the wiring, which is the only place a client
        # is constructed.
        from finflow.contracts.errors import SourceUnavailable

        def explode(*_args: object, **_kwargs: object) -> object:
            raise SourceUnavailable("vendor is down", source="synthetic")

        monkeypatch.setattr(daily, "build_sources", explode)
        assert run(settings) == 1

        printed = capsys.readouterr().out
        assert "FinFlow" in printed
        assert "vendor is down" in printed
        assert "No instruction is issued today." in printed

    def test_a_position_recorded_by_hand_shows_up_in_the_digest(
        self, settings: Settings, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from finflow.ports.ops_store import ActualPosition

        assert run(settings) == 0
        ops = SqliteOpsStore(settings.data_dir / "ops.sqlite")
        ops.save_position(ActualPosition("GLD", units=10, updated_at=NOW))
        capsys.readouterr()

        assert run(settings, "--skip-ingest", at=NOW + dt.timedelta(hours=6)) == 0
        printed = capsys.readouterr().out
        # Whether the strategy wants GLD depends on the synthetic series, so the
        # assertion is that the holding is reported at all — the difference
        # between an alerting system and a useful one (PROJECT.md §7.6).
        assert "Held" in printed
        assert "GLD 10" in printed
