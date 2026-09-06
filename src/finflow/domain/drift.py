"""Target versus actual — the line that makes the digest worth reading.

``PROJECT.md`` §7.6: what a person needs each morning is not "GLD triggered" but
*this is what you should hold, this is what you do hold, here is the difference,
and here is whether it is worth trading*. The last clause is the one that keeps
the message quiet: a drift inside the rebalance band produces no instruction, so
most days say no action, which is what stops the digest being muted.

Actual weights are computed **within recorded holdings** — the marked value of
what the user has told the system they own. There is no cash balance and no
broker link, so an absolute weight of the whole account is not knowable here,
and inventing one would put a confident number on a guess. Once every holding is
recorded, the two definitions coincide.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from finflow.domain.decision import Decision


class Action(StrEnum):
    """What the drift on one instrument implies."""

    HOLD = "hold"
    BUY = "buy"
    SELL = "sell"
    CLOSE = "close"
    OPEN = "open"


@dataclass(frozen=True, slots=True)
class DriftLine:
    """One instrument's target, actual, and the gap between them."""

    symbol: str
    target_weight: float
    actual_weight: float
    actual_units: float
    price: float | None
    action: Action

    @property
    def drift_pp(self) -> float:
        """The gap, in percentage points of the portfolio."""
        return (self.actual_weight - self.target_weight) * 100.0

    @property
    def needs_action(self) -> bool:
        """True when this line is asking the user to do something."""
        return self.action is not Action.HOLD

    def describe(self) -> str:
        """One line for the digest."""
        if self.action is Action.HOLD:
            return f"{self.symbol}  {self.actual_weight:.0%} (target {self.target_weight:.0%})"
        return (
            f"{self.action.upper()} {self.symbol}  "
            f"{self.actual_weight:.0%} -> {self.target_weight:.0%} "
            f"({self.drift_pp:+.0f} pp)"
        )


def compute_drift(
    decision: Decision,
    *,
    holdings: Mapping[str, float],
    prices: Mapping[str, float],
    band_pp: float = 5.0,
) -> tuple[DriftLine, ...]:
    """Compare a target portfolio with what the user says they hold.

    ``band_pp`` is the rebalance band in percentage points: drift smaller than
    this is reported as a hold rather than an instruction. It exists to protect
    the ≥95% no-action target, which is a tracked number rather than an
    aspiration (``PROJECT.md`` §1.2).

    An instrument with units but no price is reported at zero weight rather than
    dropped — a holding the system cannot value is exactly the thing a person
    needs to be told about.
    """
    symbols = sorted({p.symbol for p in decision.positions} | set(holdings))
    values = {
        symbol: holdings.get(symbol, 0.0) * prices.get(symbol, 0.0)
        for symbol in symbols
        if symbol in prices
    }
    total = sum(values.values())

    lines: list[DriftLine] = []
    for symbol in symbols:
        units = holdings.get(symbol, 0.0)
        target = decision.weight_of(symbol)
        actual = (values.get(symbol, 0.0) / total) if total > 0 else 0.0
        lines.append(
            DriftLine(
                symbol=symbol,
                target_weight=target,
                actual_weight=actual,
                actual_units=units,
                price=prices.get(symbol),
                action=_action(target, actual, units, band_pp),
            )
        )
    return tuple(lines)


def _action(target: float, actual: float, units: float, band_pp: float) -> Action:
    """Classify one gap.

    Opening and closing are called out separately from buying and selling
    because they are the two that always deserve attention: a position the user
    does not hold at all, and one the target no longer contains. Neither is
    protected by the band — a band that could suppress "you are holding
    something the strategy has exited" would be actively harmful.
    """
    if target > 0 and units == 0:
        return Action.OPEN
    if target == 0 and units > 0:
        return Action.CLOSE
    if abs(actual - target) * 100.0 <= band_pp:
        return Action.HOLD
    return Action.BUY if actual < target else Action.SELL
