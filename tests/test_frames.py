"""Frame contracts.

The bounds here are not opinions about markets. They are what stops a vendor
error page, a parse slip or a units mistake becoming a price bar — the failure
mode that ends with a decision made on a number that was never a price.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest
from patito.exceptions import DataFrameValidationError

from finflow.contracts.frames import (
    PROVENANCE,
    FrameContractError,
    MacroObservation,
    OhlcvBar,
    RawOhlcv,
    ohlcv_consistency_errors,
    validate_frame,
)

DAY = dt.date(2024, 1, 2)


def bars(**overrides: object) -> pl.DataFrame:
    base: dict[str, list[object]] = {
        "symbol": ["GLD"],
        "date": [DAY],
        "open": [190.0],
        "high": [192.0],
        "low": [189.0],
        "close": [191.0],
        "volume": [1_000_000.0],
    }
    base.update({k: [v] for k, v in overrides.items()})
    return pl.DataFrame(base)


class TestOhlcvSchema:
    def test_a_well_formed_bar_validates(self) -> None:
        OhlcvBar.validate(bars())

    @pytest.mark.parametrize("column", ["open", "high", "low", "close"])
    def test_a_non_positive_price_is_rejected(self, column: str) -> None:
        with pytest.raises(DataFrameValidationError):
            OhlcvBar.validate(bars(**{column: 0.0}))

    def test_negative_volume_is_rejected(self) -> None:
        with pytest.raises(DataFrameValidationError):
            OhlcvBar.validate(bars(volume=-1.0))

    def test_zero_volume_is_allowed(self) -> None:
        # A real and unremarkable event in a thin ETF on a quiet day.
        OhlcvBar.validate(bars(volume=0.0))

    def test_a_string_where_a_price_belongs_is_rejected(self) -> None:
        # This is the shape a half-parsed HTML page arrives in. Patito lets it
        # through as a Polars ComputeError rather than a validation error, so
        # only the wrapper gives it a single, catchable failure mode.
        with pytest.raises(FrameContractError, match="OhlcvBar"):
            validate_frame(OhlcvBar, bars(close="n/a"))

    def test_the_wrapper_collapses_both_failure_modes(self) -> None:
        validate_frame(OhlcvBar, bars())
        with pytest.raises(FrameContractError):
            validate_frame(OhlcvBar, bars(close=-1.0))  # a bound
        with pytest.raises(FrameContractError):
            validate_frame(OhlcvBar, bars(close="n/a"))  # a dtype
        with pytest.raises(FrameContractError):
            validate_frame(OhlcvBar, bars().drop("volume"))  # a missing column

    def test_an_empty_symbol_is_rejected(self) -> None:
        with pytest.raises(DataFrameValidationError):
            OhlcvBar.validate(bars(symbol=""))

    def test_a_missing_column_is_rejected(self) -> None:
        with pytest.raises(DataFrameValidationError):
            OhlcvBar.validate(bars().drop("volume"))


class TestProvenance:
    def test_the_raw_form_adds_exactly_the_provenance_columns(self) -> None:
        assert set(RawOhlcv.columns) - set(OhlcvBar.columns) == set(PROVENANCE)

    def test_a_stamped_frame_validates(self) -> None:
        stamped = bars().with_columns(
            pl.lit("stooq").alias("source"),
            pl.lit(dt.datetime(2026, 8, 27, 5, 12)).alias("ingested_at"),
            pl.lit("run-1").alias("ingestion_run_id"),
        )
        RawOhlcv.validate(stamped)

    def test_an_unstamped_frame_is_not_a_raw_frame(self) -> None:
        with pytest.raises(DataFrameValidationError):
            RawOhlcv.validate(bars())


class TestMacroSchema:
    def test_an_observation_validates(self) -> None:
        MacroObservation.validate(
            pl.DataFrame(
                {
                    "series_id": ["DFII10"],
                    "observation_date": [DAY],
                    "value": [1.85],
                    "vintage_date": [None],
                },
                schema_overrides={"vintage_date": pl.Date},
            )
        )

    def test_a_vintage_date_is_optional_because_most_series_are_never_revised(
        self,
    ) -> None:
        assert MacroObservation.model_fields["vintage_date"].default is None

    def test_a_negative_value_is_allowed(self) -> None:
        # Real yields and spreads go negative; a positivity bound here would be
        # a modelling assumption smuggled into a contract.
        MacroObservation.validate(
            pl.DataFrame(
                {
                    "series_id": ["DFII10"],
                    "observation_date": [DAY],
                    "value": [-0.75],
                    "vintage_date": [None],
                },
                schema_overrides={"vintage_date": pl.Date},
            )
        )


class TestCrossColumnConsistency:
    def test_a_clean_frame_reports_nothing(self) -> None:
        assert ohlcv_consistency_errors(bars()) == []

    def test_an_empty_frame_reports_nothing(self) -> None:
        assert ohlcv_consistency_errors(bars().clear()) == []

    def test_high_below_low_is_reported_with_the_date(self) -> None:
        problems = ohlcv_consistency_errors(bars(high=180.0, low=185.0, open=182.0, close=183.0))
        assert any("high < low" in p for p in problems)
        assert all(str(DAY) in p for p in problems)

    def test_close_outside_the_range_is_reported(self) -> None:
        problems = ohlcv_consistency_errors(bars(close=250.0))
        assert any("close outside [low, high]" in p for p in problems)

    def test_open_outside_the_range_is_reported(self) -> None:
        problems = ohlcv_consistency_errors(bars(open=1.0))
        assert any("open outside [low, high]" in p for p in problems)

    def test_a_duplicated_date_is_reported(self) -> None:
        problems = ohlcv_consistency_errors(pl.concat([bars(), bars()]))
        assert any("duplicated date" in p for p in problems)

    def test_the_count_of_offending_rows_is_reported(self) -> None:
        many = pl.concat([bars(close=250.0, date=dt.date(2024, 1, d)) for d in (2, 3, 4)])
        problems = ohlcv_consistency_errors(many)
        assert any(p.startswith("3 row(s)") for p in problems)
