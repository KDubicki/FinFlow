"""Evaluation under user controls, and the message it all ends up in.

Two things are being defended here, and neither is about arithmetic:

- **The digest stays quiet.** A system that says "no action" on most days is one
  that gets read on the days it does not (``PROJECT.md`` §1.2). An unchanged
  target must therefore produce no message at all.
- **Nothing is suppressed silently.** A pause, a mute or a hold changes what is
  issued, and every one of them has to appear in the digest with its reason
  (§7.7) — a control the user forgot they set is otherwise indistinguishable
  from a bug.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.application.daily_digest import BuildDigest, RunFacts
from finflow.application.evaluate_strategies import (
    EvaluateStrategies,
    EvaluationOutcome,
    FeaturesUnavailable,
)
from finflow.domain.decision import Decision, TargetPosition
from finflow.domain.drift import Action, compute_drift
from finflow.domain.messages import FAILED, OK, Digest, Step, render_digest
from finflow.domain.strategy import SMA_CROSS
from finflow.ports.ops_store import HOLD_SCOPE, ActualPosition, Control, ControlKind
from finflow.registry import load_registry
from finflow.registry.models import Registry
from tests.fakes import FrozenClock

TODAY = dt.date(2026, 9, 6)
NOW = dt.datetime(2026, 9, 6, 5, 30, tzinfo=dt.UTC)
REGISTRY_DIR = Path(__file__).resolve().parents[1] / "instruments"
UNIVERSE = ("GLD", "IAU", "SLV", "GDX")


@pytest.fixture
def registry() -> Registry:
    return load_registry(REGISTRY_DIR)


class FakeWarehouse:
    """The mart, without a database.

    The evaluator reads exactly one table through the ``Warehouse`` port, so a
    dozen rows in a frame prove more about the decision than a DuckDB file would
    — and the real store is exercised end to end by ``test_daily.py``.
    """

    def __init__(self, frame: pl.DataFrame, *, tables: tuple[str, ...] = ("fct_ohlcv_daily",)):
        self._frame = frame
        self._tables = tables

    def query(self, sql: str, **params: object) -> pl.DataFrame:
        if "count(*)" in sql:
            return pl.DataFrame({"n": [0]})
        if "max(date)" in sql:
            return (
                self._frame.group_by("symbol")
                .agg(pl.col("date").max(), pl.col("close").last())
                .sort("symbol")
            )
        wanted = {
            part.strip().strip("'") for part in sql.split("IN (")[-1].split(")")[0].split(",")
        }
        return self._frame.filter(pl.col("symbol").is_in(wanted)).sort(["symbol", "date"])

    def execute(self, sql: str, **params: object) -> None:
        raise AssertionError("evaluation must never write to the warehouse")

    def register(self, name: str, frame: pl.DataFrame) -> None:
        raise AssertionError("evaluation must never write to the warehouse")

    def unregister(self, name: str) -> None:
        raise AssertionError("evaluation must never write to the warehouse")

    def tables(self) -> tuple[str, ...]:
        return self._tables


def bars(trend: dict[str, float], *, days: int = 120) -> pl.DataFrame:
    """One rising or falling series per symbol, ending on TODAY."""
    rows = []
    for symbol, slope in trend.items():
        for i in range(days):
            rows.append(
                {
                    "symbol": symbol,
                    "date": TODAY - dt.timedelta(days=days - 1 - i),
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0 + slope * i,
                    "volume": 1_000.0,
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture
def ops(tmp_path: Path) -> SqliteOpsStore:
    return SqliteOpsStore(tmp_path / "ops.sqlite")


def evaluate(
    ops: SqliteOpsStore,
    registry: Registry,
    frame: pl.DataFrame,
    tables: tuple[str, ...] = ("fct_ohlcv_daily",),
) -> EvaluationOutcome:
    return EvaluateStrategies(
        warehouse=FakeWarehouse(frame, tables=tables),
        ops_store=ops,
        registry=registry,
        clock=FrozenClock(NOW),
        strategies=(SMA_CROSS,),
    ).run(run_id="run-1", snapshot_id="snap-1")


class TestEvaluation:
    def test_a_new_target_is_recorded_and_queued(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        outcome = evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)))
        assert outcome.queued == 1
        assert len(ops.decisions()) == 1
        assert len(ops.pending()) == 1

    def test_an_unchanged_target_is_recorded_but_not_re_sent(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # The guard on the >=95% no-action target. "Hold GLD" every morning for
        # six weeks is how a channel gets muted.
        frame = bars(dict.fromkeys(UNIVERSE, 1.0))
        evaluate(ops, registry, frame)
        second = evaluate(ops, registry, frame)
        assert second.queued == 0
        assert "unchanged" in " ".join(second.withheld)
        assert len(ops.pending()) == 1

    def test_a_paused_strategy_keeps_computing_and_says_so(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        ops.set_control(Control(ControlKind.PAUSE, SMA_CROSS.id, set_at=NOW))
        outcome = evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)))

        assert outcome.queued == 0
        assert ops.pending() == ()
        # The counterfactual survives, which is the whole point of a pause.
        assert len(ops.decisions()) == 1
        assert any("paused" in reason for reason in outcome.withheld)

    def test_a_hold_suspends_instructions_and_names_its_end_date(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        ops.set_control(
            Control(ControlKind.HOLD, HOLD_SCOPE, until=dt.date(2026, 10, 1), set_at=NOW)
        )
        outcome = evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)))
        assert outcome.queued == 0
        assert any("2026-10-01" in reason for reason in outcome.withheld)

    def test_a_muted_instrument_leaves_the_target_and_is_listed(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        ops.set_control(Control(ControlKind.MUTE, "GLD", until=dt.date(2026, 10, 1), set_at=NOW))
        outcome = evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)))
        decision = outcome.results[0].decision

        assert "GLD" not in [p.symbol for p in decision.positions]
        assert any("GLD" in item for item in outcome.withheld)

    def test_an_expired_control_stops_applying(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        ops.set_control(Control(ControlKind.MUTE, "GLD", until=dt.date(2026, 9, 1), set_at=NOW))
        outcome = evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)))
        assert "GLD" in [p.symbol for p in outcome.results[0].decision.positions]

    def test_a_missing_mart_refuses_to_decide(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # No instruction from a warehouse that did not build. This is the
        # failure the trust ladder drops a rung for.
        with pytest.raises(FeaturesUnavailable, match="dbt build did not run"):
            evaluate(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0)), tables=())


class TestDrift:
    def _decision(self, **weights: float) -> Decision:
        return Decision(
            strategy_id="s",
            strategy_version="v1",
            as_of=TODAY,
            data_as_of=TODAY,
            positions=tuple(TargetPosition(s, w) for s, w in weights.items()),
        )

    def test_matching_holdings_produce_no_action(self) -> None:
        lines = compute_drift(
            self._decision(GLD=0.5, SLV=0.5),
            holdings={"GLD": 10, "SLV": 20},
            prices={"GLD": 100.0, "SLV": 50.0},
        )
        assert all(line.action is Action.HOLD for line in lines)

    def test_drift_inside_the_band_is_still_no_action(self) -> None:
        lines = compute_drift(
            self._decision(GLD=0.5, SLV=0.5),
            holdings={"GLD": 11, "SLV": 20},
            prices={"GLD": 100.0, "SLV": 50.0},
            band_pp=5.0,
        )
        assert all(line.action is Action.HOLD for line in lines)

    def test_a_target_you_do_not_hold_is_always_an_open(self) -> None:
        # Never suppressed by the band: "you are not holding what the strategy
        # says to hold" is exactly the message that must get through.
        (line,) = compute_drift(self._decision(GLD=1.0), holdings={}, prices={"GLD": 100.0})
        assert line.action is Action.OPEN

    def test_a_position_the_strategy_has_exited_is_always_a_close(self) -> None:
        (line,) = compute_drift(self._decision(), holdings={"GLD": 5}, prices={"GLD": 100.0})
        assert line.action is Action.CLOSE

    def test_a_holding_with_no_price_is_reported_rather_than_dropped(self) -> None:
        lines = compute_drift(self._decision(GLD=1.0), holdings={"XYZ": 3}, prices={"GLD": 10.0})
        assert {line.symbol for line in lines} == {"GLD", "XYZ"}


class TestDigest:
    def _digest(
        self,
        ops: SqliteOpsStore,
        registry: Registry,
        frame: pl.DataFrame,
        failures: tuple[str, ...] = (),
    ) -> Digest:
        outcome = evaluate(ops, registry, frame)
        return BuildDigest(
            ops_store=ops,
            registry=registry,
            clock=FrozenClock(NOW),
            warehouse=FakeWarehouse(frame),
        ).run(
            RunFacts(
                run_id="run-1",
                steps=(Step("ingest"), Step("build")),
                snapshot_id="snap-1",
                failures=failures,
            ),
            evaluation=outcome,
        )

    def test_a_quiet_day_says_no_action_near_the_top(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        digest = self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, -1.0)))
        text = render_digest(digest)
        assert "No action." in text.splitlines()[2]

    def test_a_readable_digest_fits_on_a_phone(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # "Readable in ten seconds" made falsifiable: a screenful, not an essay.
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, -1.0))))
        assert len(text.splitlines()) <= 24
        assert max(len(line) for line in text.splitlines()) <= 72

    def test_a_position_to_open_appears_as_an_instruction(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0))))
        assert "OPEN GLD" in text

    def test_holdings_are_reported_against_the_target(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        for symbol in UNIVERSE:
            ops.save_position(ActualPosition(symbol, units=10, updated_at=NOW))
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0))))
        assert "Held" in text
        assert "GLD 10" in text

    def test_a_failed_step_blocks_the_instruction_and_says_so(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        digest = self._digest(
            ops,
            registry,
            bars(dict.fromkeys(UNIVERSE, 1.0)),
            failures=("build: dbt build failed",),
        )
        text = render_digest(digest)
        assert digest.status == FAILED
        assert "No instruction is issued today." in text
        assert "build: dbt build failed" in text

    def test_the_freshest_date_is_stated_and_stragglers_are_named(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # Only the universes that are behind are listed. Naming all of them puts
        # a 95-character line on a phone and buries the one that is stale.
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0))))
        assert "fresh to 2026-09-06" in text
        assert "Behind" in text
        assert "equity_core never" in text
        assert "precious_metals" not in text.split("Behind")[1]

    def test_a_weekend_is_not_stale(self, ops: SqliteOpsStore, registry: Registry) -> None:
        # NOW is a Sunday, and the freshest bar is Friday's. Calendar days would
        # call that two days old and cry "degraded" every weekend, which teaches
        # the reader to ignore the word by the second Monday.
        friday = bars(dict.fromkeys(UNIVERSE, 1.0)).filter(pl.col("date") <= dt.date(2026, 9, 4))
        digest = self._digest(ops, registry, friday)
        assert digest.sessions_behind == 0
        assert digest.status != "degraded"

    def test_a_stale_pipeline_is_flagged_as_degraded(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        stale = bars(dict.fromkeys(UNIVERSE, 1.0)).with_columns(
            pl.col("date") - pl.duration(days=5)
        )
        digest = self._digest(ops, registry, stale)
        text = render_digest(digest)
        assert digest.status == "degraded"
        assert "trading sessions behind" in text

    def test_every_control_in_force_is_named(self, ops: SqliteOpsStore, registry: Registry) -> None:
        ops.set_control(Control(ControlKind.MUTE, "SLV", until=dt.date(2026, 10, 1), set_at=NOW))
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0))))
        assert "Withheld" in text
        assert "mute SLV until 2026-10-01" in text

    def test_the_run_and_snapshot_are_always_on_the_message(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        text = render_digest(self._digest(ops, registry, bars(dict.fromkeys(UNIVERSE, 1.0))))
        assert "run run-1 · snapshot snap-1" in text

    def test_a_full_disk_is_warned_about_before_it_bites(self) -> None:
        from dataclasses import replace

        digest = Digest(as_of=TODAY, run_id="r", steps=(Step("ingest", OK),))
        assert "Disk" not in render_digest(replace(digest, disk_used_pct=40.0))
        assert "Disk 81% full" in render_digest(replace(digest, disk_used_pct=81.0))
