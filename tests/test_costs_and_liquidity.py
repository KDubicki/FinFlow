"""What a trade costs, and whether it can be filled at all.

``PROJECT.md`` §5.7 names the failure precisely: a flat 3 bps across GDXJ, SIL,
UNG and PDBC understates the round trip by five to ten times and manufactures
several percent a year of return that cannot be earned. These are the tests that
make the alternative real rather than aspirational.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from finflow.contracts.instruments import AssetClass
from finflow.domain.costs import (
    DEFAULTS,
    REFERENCE_VOL,
    CostFloor,
    floor_for,
    round_trip_bps,
    slippage_bps,
)
from finflow.domain.liquidity import below_floor, median_dollar_volume


class TestCostFloors:
    def test_every_asset_class_has_a_default(self) -> None:
        # A missing entry would fall back to something, and the something would
        # be whatever the caller happened to pass.
        assert set(DEFAULTS) == set(AssetClass)

    def test_the_fallback_is_pessimistic_rather_than_free(self) -> None:
        # The asymmetry is the point: overstating costs loses a marginal
        # strategy, understating them ships one that cannot work.
        floor = floor_for(AssetClass.COMMODITY)
        assert floor.spread_bps >= floor_for(AssetClass.EQUITY).spread_bps

    def test_a_round_trip_pays_commission_twice_and_the_spread_once(self) -> None:
        assert CostFloor(commission_bps=2, spread_bps=4).round_trip_bps == 8


class TestSlippage:
    def test_a_quiet_day_costs_half_the_spread(self) -> None:
        assert slippage_bps(spread_bps=4, realized_vol=REFERENCE_VOL) == 2.0

    def test_calm_does_not_earn_a_discount(self) -> None:
        # Market makers have a floor too, and a model that rewarded quiet
        # markets would make every low-vol backtest look better than it was.
        assert slippage_bps(spread_bps=4, realized_vol=0.0001) == 2.0

    def test_a_volatile_day_costs_more(self) -> None:
        # The whole reason slippage is computed rather than stored: spreads
        # widen exactly when signals fire.
        calm = slippage_bps(spread_bps=4, realized_vol=REFERENCE_VOL)
        stressed = slippage_bps(spread_bps=4, realized_vol=REFERENCE_VOL * 3)
        assert stressed == pytest.approx(calm * 3)

    def test_size_relative_to_volume_adds_impact(self) -> None:
        without = slippage_bps(spread_bps=4, realized_vol=REFERENCE_VOL)
        with_impact = slippage_bps(
            spread_bps=4,
            realized_vol=REFERENCE_VOL,
            trade_value_usd=1_000_000,
            adv_usd=4_000_000,
        )
        assert with_impact > without

    def test_impact_grows_with_the_square_root_of_participation(self) -> None:
        # Four times the participation costs twice the impact, not four times.
        # The one robust empirical regularity in this area, and the reason a
        # linear model would overstate a small account's costs badly.
        base = slippage_bps(
            spread_bps=0, realized_vol=0.02, trade_value_usd=1_000, adv_usd=1_000_000
        )
        quadrupled = slippage_bps(
            spread_bps=0, realized_vol=0.02, trade_value_usd=4_000, adv_usd=1_000_000
        )
        assert quadrupled == pytest.approx(base * 2)

    def test_negative_inputs_are_refused(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            slippage_bps(spread_bps=-1, realized_vol=0.01)

    def test_a_thin_instrument_costs_multiples_of_a_liquid_one(self) -> None:
        liquid = round_trip_bps(CostFloor(2, 1), realized_vol=0.01)
        thin = round_trip_bps(CostFloor(2, 10), realized_vol=0.03)
        assert thin > liquid * 5


class TestLiquidity:
    def _bars(self, **volumes: list[float]) -> pl.DataFrame:
        rows = []
        for symbol, series in volumes.items():
            for i, volume in enumerate(series):
                rows.append(
                    {
                        "symbol": symbol,
                        "date": dt.date(2026, 1, 1) + dt.timedelta(days=i),
                        "close": 100.0,
                        "volume": volume,
                    }
                )
        return pl.DataFrame(rows)

    def test_dollar_volume_is_close_times_volume(self) -> None:
        traded = median_dollar_volume(self._bars(GLD=[1_000.0] * 5))
        assert traded == {"GLD": 100_000.0}

    def test_one_rebalance_day_does_not_lift_a_thin_fund_over_the_floor(self) -> None:
        # Median rather than mean, deliberately. One index-rebalance day on
        # GDXJ can hold a twenty-day mean above the floor for a month — which is
        # precisely the month a naive gate would wave through.
        spiky = [1_000.0] * 19 + [10_000_000.0]
        traded = median_dollar_volume(self._bars(GDXJ=spiky))
        assert traded["GDXJ"] == 100_000.0

    def test_only_the_recent_window_counts(self) -> None:
        # A fund that used to trade is not a fund that trades.
        dying = [1_000_000.0] * 40 + [10.0] * 20
        traded = median_dollar_volume(self._bars(UNG=dying), window=20)
        assert traded["UNG"] == 1_000.0

    def test_a_symbol_below_its_floor_is_gated_with_a_readable_reason(self) -> None:
        gated = below_floor({"UNG": 500_000.0}, {"UNG": 2_000_000.0})
        assert "below liquidity floor" in gated["UNG"]
        assert "$0.5M" in gated["UNG"] and "$2M" in gated["UNG"]

    def test_a_symbol_above_its_floor_is_not_gated(self) -> None:
        assert below_floor({"SPY": 5e9}, {"SPY": 50e6}) == {}

    def test_an_instrument_with_no_floor_is_never_gated(self) -> None:
        assert below_floor({"X": 1.0}, {"X": None}) == {}

    def test_missing_volume_data_is_not_treated_as_illiquidity(self) -> None:
        # "We do not know" and "nothing traded" are different answers, and only
        # the second should gate an instrument out. The gap itself is the
        # calendar check's business.
        assert below_floor({}, {"SPY": 50e6}) == {}

    def test_a_frame_without_volume_yields_nothing_rather_than_zero(self) -> None:
        frame = pl.DataFrame({"symbol": ["SPY"], "date": [dt.date(2026, 1, 1)], "close": [1.0]})
        assert median_dollar_volume(frame) == {}
