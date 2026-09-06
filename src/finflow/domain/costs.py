"""What a trade actually costs.

``PROJECT.md`` §5.7 is blunt about why this is a domain module rather than a
constant somewhere: a flat 3 bps assumption across GDXJ, SIL, UNG and PDBC
understates the true round trip by five to ten times and **manufactures alpha
that cannot be earned**. A backtest that clears its costs by 2% a year on a
universe whose real costs are 4% is not a strategy, it is an arithmetic error
with a chart.

Three pieces, in order of how much they matter:

1. **Per-instrument floors**, carried in the registry. A strategy may raise
   them, never lower them.
2. **Asset-class defaults**, applied when an instrument does not state its own,
   so the *cheap* mistake is forgetting to be specific rather than forgetting to
   have costs at all.
3. **Slippage scaled by realized volatility**, computed at evaluation time
   rather than stored — because spreads widen exactly when signals fire, and a
   stored average spread is measured on the days nothing was happening.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from finflow.contracts.instruments import AssetClass


@dataclass(frozen=True, slots=True)
class CostFloor:
    """One-way costs, in basis points of notional."""

    commission_bps: float
    spread_bps: float

    @property
    def round_trip_bps(self) -> float:
        """What a full in-and-out costs, ignoring slippage.

        Both legs pay commission; the spread is paid once as the half-spread on
        each side, which is the same arithmetic and worth stating so nobody
        halves it twice.
        """
        return 2 * self.commission_bps + self.spread_bps


DEFAULTS: dict[AssetClass, CostFloor] = {
    AssetClass.EQUITY: CostFloor(commission_bps=2.0, spread_bps=2.0),
    AssetClass.COMMODITY: CostFloor(commission_bps=2.0, spread_bps=5.0),
    AssetClass.RATES: CostFloor(commission_bps=2.0, spread_bps=2.0),
    AssetClass.CREDIT: CostFloor(commission_bps=2.0, spread_bps=4.0),
    AssetClass.CURRENCY: CostFloor(commission_bps=2.0, spread_bps=3.0),
}
"""Defaults per asset class, deliberately pessimistic.

These are floors for an instrument that has not said anything about itself, and
the asymmetry is the point: overstating costs loses a marginal strategy that
might have worked, while understating them ships one that cannot. Only the first
mistake is recoverable.

A liquid instrument states its own, lower, numbers in the registry — which is
the same shape as the rest of the design, where the general case is safe and the
specific case is earned.
"""

REFERENCE_VOL = 0.01
"""Daily realized volatility at which quoted spreads hold: roughly 16% annual.

Below it, spreads do not tighten much further — market makers have a floor too —
so the model does not reward calm. Above it, they widen roughly in proportion,
which is what the scaling below encodes.
"""

IMPACT_COEFFICIENT = 0.5
"""Square-root-law coefficient for market impact.

Impact grows with the square root of participation rather than linearly, which
is the one robust empirical regularity in this area. The coefficient itself is a
conservative placeholder: M8 measures it against realized fills, and until then
it is stated here rather than buried in a backtest so that changing it is a
visible decision.
"""


def floor_for(asset_class: AssetClass) -> CostFloor:
    """The default floor for an asset class."""
    return DEFAULTS.get(asset_class, CostFloor(commission_bps=2.0, spread_bps=5.0))


def slippage_bps(
    *,
    spread_bps: float,
    realized_vol: float,
    trade_value_usd: float | None = None,
    adv_usd: float | None = None,
) -> float:
    """Expected one-way slippage, in basis points.

    Two terms:

    - **Half the spread, scaled by volatility.** A signal that fires on a 3%
      day is filled into a wider market than the same signal on a quiet one, so
      the quoted spread is the *best* case rather than the expected one.
    - **Impact**, when the trade's size relative to average daily volume is
      known. Square-root in participation, scaled by the day's volatility.
      Negligible for a personal account in SPY; the reason GDXJ and UNG are in
      this universe at all is that it is not negligible there.

    Never returns less than half the quoted spread: a fill better than the touch
    is luck, and luck does not belong in a cost model.
    """
    if spread_bps < 0 or realized_vol < 0:
        raise ValueError("costs and volatility are non-negative quantities")

    half_spread = spread_bps / 2.0
    scale = max(1.0, realized_vol / REFERENCE_VOL)
    slippage = half_spread * scale

    if trade_value_usd and adv_usd and adv_usd > 0:
        participation = trade_value_usd / adv_usd
        vol_bps = realized_vol * 10_000
        slippage += IMPACT_COEFFICIENT * vol_bps * math.sqrt(participation)

    return slippage


def round_trip_bps(
    floor: CostFloor,
    *,
    realized_vol: float = REFERENCE_VOL,
    trade_value_usd: float | None = None,
    adv_usd: float | None = None,
) -> float:
    """Everything one in-and-out costs, slippage included.

    The number a backtest must clear before it has found anything. Reported per
    trade rather than annualized, because turnover is the strategy's property
    and cost is the instrument's — multiplying them together too early is how a
    high-turnover strategy hides its bill.
    """
    slip = slippage_bps(
        spread_bps=floor.spread_bps,
        realized_vol=realized_vol,
        trade_value_usd=trade_value_usd,
        adv_usd=adv_usd,
    )
    return 2 * floor.commission_bps + 2 * slip
