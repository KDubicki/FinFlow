"""Can this instrument absorb the trade the rule is asking for?

``PROJECT.md`` §5.7 makes ``min_adv_usd`` a **data-quality gate rather than a
preference**: below the floor, no signal is emitted for that instrument that
day. The reasoning is the same as the cost floor's and just as unglamorous — a
target you cannot fill without moving the price is not a target, and a backtest
that fills it anyway is measuring a market that was not there.

Median rather than mean dollar volume, deliberately. One index-rebalance day on
GDXJ can lift a twenty-day mean above the floor for a month, which is precisely
the month a naive gate would wave through.
"""

from __future__ import annotations

from collections.abc import Mapping

import polars as pl

DEFAULT_WINDOW = 20
"""Sessions in the lookback. A month of trading: long enough that one quiet week
does not gate an instrument out, short enough to notice a fund that is dying."""


def median_dollar_volume(
    features: pl.DataFrame, *, window: int = DEFAULT_WINDOW
) -> dict[str, float]:
    """Median ``close * volume`` per symbol over the most recent sessions.

    Expects the long feature frame — one row per symbol per date — already
    truncated to the evaluation date by the caller. Symbols with no volume
    column, or none recorded, are absent from the result rather than reported as
    zero: "we do not know" and "there was no trading" are different answers, and
    only the second one should gate an instrument out.
    """
    required = {"symbol", "date", "close", "volume"}
    if features.is_empty() or not required <= set(features.columns):
        return {}

    recent = (
        features.sort(["symbol", "date"])
        .group_by("symbol")
        .tail(window)
        .with_columns((pl.col("close") * pl.col("volume")).alias("_dollar_volume"))
        .group_by("symbol")
        .agg(pl.col("_dollar_volume").median().alias("median"))
    )
    return {
        str(row["symbol"]): float(row["median"])
        for row in recent.iter_rows(named=True)
        if row["median"] is not None
    }


def below_floor(traded: Mapping[str, float], floors: Mapping[str, float | None]) -> dict[str, str]:
    """Symbols that fail their liquidity floor, mapped to why.

    Shaped as ``{symbol: reason}`` so it drops straight into the evaluator's
    ``excluded`` argument — which means a gated instrument is *recorded as
    withheld with its reason* rather than quietly missing from the target
    (``PROJECT.md`` §7.7).
    """
    gated: dict[str, str] = {}
    for symbol, floor in floors.items():
        if floor is None:
            continue
        observed = traded.get(symbol)
        if observed is None:
            # No volume data is not evidence of illiquidity. The gap itself is
            # reported by the calendar check, which is the test that owns it.
            continue
        if observed < floor:
            # Terse on purpose: this line lands in a digest read on a phone,
            # and the full numbers are in the log for anyone who wants them.
            gated[symbol] = f"below liquidity floor (${observed / 1e6:.1f}M vs ${floor / 1e6:.0f}M)"
    return gated
