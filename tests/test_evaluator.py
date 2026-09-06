"""``decide`` — the one evaluation path.

The important test here is prefix stability. ``PROJECT.md`` §7.3 argues that it
subsumes the whole lookahead family — a forward-shifted join, a ``shift(-1)``, a
full-sample z-score, a global rank, an ``ffill`` that reaches backwards — and
that is why it is asserted over a sweep of dates rather than spot-checked once.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from finflow.domain.decision import Decision
from finflow.domain.evaluator import FeatureError, decide
from finflow.domain.strategy import SMA_CROSS, Strategy

START = dt.date(2026, 1, 1)


def series(symbol: str, closes: list[float]) -> list[dict[str, object]]:
    return [
        {"symbol": symbol, "date": START + dt.timedelta(days=i), "close": close}
        for i, close in enumerate(closes)
    ]


def frame(*groups: list[dict[str, object]]) -> pl.DataFrame:
    rows = [row for group in groups for row in group]
    return pl.DataFrame(rows)


RISING = [100.0 + i for i in range(120)]
FALLING = [200.0 - i for i in range(120)]


class TestDeciding:
    def test_a_rising_series_is_held_and_a_falling_one_is_not(self) -> None:
        decision = decide(
            frame(series("GLD", RISING), series("SLV", FALLING)),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "SLV"],
        )
        assert [p.symbol for p in decision.positions] == ["GLD"]
        assert decision.weight_of("GLD") == 1.0
        assert decision.weight_of("SLV") == 0.0

    def test_two_firing_instruments_share_the_book_equally(self) -> None:
        decision = decide(
            frame(series("GLD", RISING), series("IAU", RISING)),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "IAU"],
        )
        assert [p.weight for p in decision.positions] == [0.5, 0.5]

    def test_an_instrument_short_of_warmup_is_withheld_rather_than_assumed_false(self) -> None:
        # A decision made on a null is a decision made on nothing. Reporting it
        # is the difference between "nothing fired" and "we never looked".
        decision = decide(
            frame(series("GLD", RISING), series("SLV", FALLING[:10])),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "SLV"],
        )
        reasons = {w.symbol: w.reason for w in decision.withheld}
        assert "insufficient history" in reasons["SLV"]

    def test_a_universe_member_with_no_bars_at_all_is_named(self) -> None:
        decision = decide(
            frame(series("GLD", RISING)),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "IAU"],
        )
        assert [w.symbol for w in decision.withheld] == ["IAU"]
        assert decision.evaluated == 1

    def test_a_muted_instrument_is_excluded_but_still_recorded(self) -> None:
        decision = decide(
            frame(series("GLD", RISING), series("IAU", RISING)),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "IAU"],
            excluded={"IAU": "muted until 2026-12-01"},
        )
        assert [p.symbol for p in decision.positions] == ["GLD"]
        assert [w.describe() for w in decision.withheld] == ["IAU: muted until 2026-12-01"]

    def test_max_positions_drops_the_excess_and_says_which(self) -> None:
        capped = Strategy(
            id="capped",
            universe="precious_metals",
            signal=SMA_CROSS.signal,
            max_positions=1,
        )
        decision = decide(
            frame(series("GLD", RISING), series("IAU", RISING)),
            capped,
            START + dt.timedelta(days=119),
            universe=["GLD", "IAU"],
        )
        assert [p.symbol for p in decision.positions] == ["GLD"]
        assert any("max_positions" in w.reason for w in decision.withheld)

    def test_an_empty_universe_produces_a_flat_decision_not_a_crash(self) -> None:
        empty = pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date, "close": pl.Float64})
        decision = decide(empty, SMA_CROSS, START, universe=[])
        assert decision.is_flat
        assert decision.data_as_of is None

    def test_a_frame_missing_a_column_fails_by_name(self) -> None:
        with pytest.raises(FeatureError, match="date"):
            decide(pl.DataFrame({"symbol": ["GLD"]}), SMA_CROSS, START, universe=["GLD"])

    def test_a_frame_with_no_columns_at_all_names_the_missing_mart(self) -> None:
        # The shape a missing dbt build produces. The error has to say that,
        # not "column 'date' not found".
        with pytest.raises(FeatureError, match="no columns"):
            decide(pl.DataFrame(), SMA_CROSS, START, universe=["GLD"])


class TestPrefixStability:
    """`decide(features[:D], D) == decide(features_full, D)` for every D."""

    def _full(self) -> pl.DataFrame:
        return frame(series("GLD", RISING), series("SLV", FALLING))

    @pytest.mark.parametrize("offset", [55, 60, 75, 90, 100, 110, 119])
    def test_future_bars_cannot_reach_a_past_decision(self, offset: int) -> None:
        full = self._full()
        as_of = START + dt.timedelta(days=offset)
        truncated = full.filter(pl.col("date") <= as_of)

        from_full = decide(full, SMA_CROSS, as_of, universe=["GLD", "SLV"])
        from_prefix = decide(truncated, SMA_CROSS, as_of, universe=["GLD", "SLV"])
        assert from_full.decision_id == from_prefix.decision_id

    def test_bars_after_as_of_cannot_change_the_decision(self) -> None:
        # The property holds because `decide` truncates the frame itself rather
        # than trusting the caller to. This is the test that fails the day
        # somebody removes that filter as redundant: the tampered future is
        # violent enough to flip both averages if it were ever visible.
        as_of = START + dt.timedelta(days=80)
        honest = self._full()
        tampered = honest.with_columns(
            pl.when(pl.col("date") > as_of)
            .then(pl.col("close") * 1000)
            .otherwise(pl.col("close"))
            .alias("close")
        )
        assert (
            decide(honest, SMA_CROSS, as_of, universe=["GLD", "SLV"]).decision_id
            == decide(tampered, SMA_CROSS, as_of, universe=["GLD", "SLV"]).decision_id
        )


class TestPurity:
    def test_deciding_twice_gives_the_same_answer(self) -> None:
        features = frame(series("GLD", RISING))
        as_of = START + dt.timedelta(days=119)
        first = decide(features, SMA_CROSS, as_of, universe=["GLD"])
        second = decide(features, SMA_CROSS, as_of, universe=["GLD"])
        assert first == second

    def test_the_input_frame_is_not_mutated(self) -> None:
        features = frame(series("GLD", RISING))
        before = features.clone()
        decide(features, SMA_CROSS, START + dt.timedelta(days=119), universe=["GLD"])
        assert features.equals(before)

    def test_a_decision_survives_a_payload_round_trip(self) -> None:
        decision = decide(
            frame(series("GLD", RISING), series("SLV", FALLING)),
            SMA_CROSS,
            START + dt.timedelta(days=119),
            universe=["GLD", "SLV", "IAU"],
            snapshot_id="abc123",
        )
        assert Decision.from_payload(decision.to_payload()) == decision
