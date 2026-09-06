"""The AST, its type system and its version hash.

These are the properties M7's parser will have to preserve. Asserting them now,
against a hand-built tree, is what makes "the surface syntax arrives later and
nothing changes underneath" a claim with a test behind it.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from finflow.domain.strategy import (
    SMA_CROSS,
    Compare,
    Indicator,
    Price,
    Strategy,
    StrategyTypeError,
    ValueType,
)


class TestTypes:
    def test_a_comparison_of_two_indicators_is_a_predicate(self) -> None:
        node = Compare(Indicator("sma", Price(), 20), ">", Indicator("sma", Price(), 50))
        assert node.value_type() is ValueType.SERIES_BOOL

    def test_a_float_is_rejected_as_a_signal(self) -> None:
        # The trap PROJECT.md §7.2 names: `filter: "sma(close, 20)"`, a float
        # where a predicate is required. Caught at construction, not at 05:00.
        with pytest.raises(StrategyTypeError, match="predicate"):
            Strategy(id="bad", universe="precious_metals", signal=Indicator("sma", Price(), 20))

    def test_a_predicate_is_rejected_as_an_indicator_operand(self) -> None:
        predicate = Compare(Price(), ">", Price("open"))
        with pytest.raises(StrategyTypeError, match="Series"):
            Indicator("sma", predicate, 10)

    def test_an_unknown_function_is_refused(self) -> None:
        with pytest.raises(StrategyTypeError, match="unknown function"):
            Indicator("rsi", Price(), 14)

    def test_an_unknown_column_is_refused(self) -> None:
        with pytest.raises(StrategyTypeError, match="unknown price column"):
            Price("adjusted_close")

    def test_equality_is_not_in_the_grammar(self) -> None:
        # Float equality in a financial expression is a bug, so it is not
        # expressible rather than merely discouraged.
        with pytest.raises(StrategyTypeError, match="unknown operator"):
            Compare(Price(), "==", Price("open"))

    def test_a_nonsense_window_is_refused(self) -> None:
        with pytest.raises(StrategyTypeError, match="positive"):
            Indicator("sma", Price(), 0)

    def test_an_unknown_api_version_fails_loudly(self) -> None:
        with pytest.raises(StrategyTypeError, match="apiVersion"):
            Strategy(
                id="x",
                universe="precious_metals",
                signal=Compare(Price(), ">", Price("open")),
                api_version="finflow/v99",
            )


class TestVersionHash:
    def test_the_same_tree_hashes_the_same_way(self) -> None:
        assert (
            SMA_CROSS.version
            == Strategy(
                id="sma_cross_precious_metals",
                universe="precious_metals",
                signal=Compare(
                    Indicator("sma", Price("close"), 20), ">", Indicator("sma", Price("close"), 50)
                ),
            ).version
        )

    def test_renaming_a_strategy_does_not_change_its_version(self) -> None:
        # The id is identity, not behaviour. A rename must not invalidate a
        # stored backtest (PROJECT.md §7.4).
        renamed = Strategy(
            id="something_else", universe=SMA_CROSS.universe, signal=SMA_CROSS.signal
        )
        assert renamed.version == SMA_CROSS.version

    def test_changing_a_window_changes_the_version(self) -> None:
        edited = Strategy(
            id=SMA_CROSS.id,
            universe=SMA_CROSS.universe,
            signal=Compare(Indicator("sma", Price(), 21), ">", Indicator("sma", Price(), 50)),
        )
        assert edited.version != SMA_CROSS.version

    def test_changing_the_universe_changes_the_version(self) -> None:
        # It changes what is actually evaluated, so it must change run identity.
        moved = Strategy(id=SMA_CROSS.id, universe="equity_core", signal=SMA_CROSS.signal)
        assert moved.version != SMA_CROSS.version

    def test_the_version_is_short_and_stable_across_processes(self) -> None:
        # A digest line carries it, so it has to be short; a stored run keys on
        # it, so it must not involve id() or hash() of anything.
        assert len(SMA_CROSS.version) == 12
        assert SMA_CROSS.version == "5656daa1778f"


class TestCompilation:
    def _bars(self, closes: list[float]) -> pl.DataFrame:
        start = dt.date(2026, 1, 1)
        return pl.DataFrame(
            {
                "date": [start + dt.timedelta(days=i) for i in range(len(closes))],
                "close": closes,
            }
        )

    def test_sma_matches_the_hand_computed_average(self) -> None:
        frame = self._bars([1.0, 2.0, 3.0, 4.0, 5.0])
        out = frame.with_columns(Indicator("sma", Price(), 3).to_expr().alias("sma"))
        assert out["sma"].to_list() == [None, None, 2.0, 3.0, 4.0]

    def test_ema_uses_the_recursive_form(self) -> None:
        # adjust=False: the form every charting package means by "EMA".
        frame = self._bars([1.0, 2.0, 3.0])
        out = frame.with_columns(Indicator("ema", Price(), 2).to_expr().alias("ema"))
        # alpha = 2/(2+1); 1, then 1 + 2/3*(2-1), then that + 2/3*(3-that)
        assert out["ema"].to_list() == pytest.approx([1.0, 1.6666666667, 2.5555555556])

    def test_warmup_is_reported_so_a_null_never_becomes_a_decision(self) -> None:
        assert SMA_CROSS.warmup == 50
