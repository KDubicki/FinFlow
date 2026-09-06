"""``decide`` — the single evaluation path, live and backtest alike.

``PROJECT.md`` §7.3 makes this the highest-risk claim in the design and then
removes the risk by refusing to have two implementations:

    decide(features, ast, as_of)      -> Decision      pure; reads only up to as_of
    simulate(decisions, bars, costs)  -> fills          needs the NEXT bar; backtest only

Live evaluation is ``decide`` with ``as_of`` set to today. A backtest is the same
call over a range of dates. ``simulate`` arrives with the engine in M7; this
module is the half that runs every morning from M4 onwards, and it is the final
one rather than a placeholder.

Two properties are enforced here rather than trusted:

- **Prefix stability.** The frame is truncated to ``as_of`` *inside* this
  function, so no caller can forget to and no future join can reach past it. The
  property test asserts ``decide(features[:D], D) == decide(features_full, D)``
  over random dates, which subsumes the whole lookahead family (§7.3).
- **No ambient anything.** No clock, no IO, no configuration. ``as_of`` is an
  argument, which is what makes "evaluate as of 2019-06-03" a call rather than a
  rewrite.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date

import polars as pl

from finflow.domain.decision import Decision, Scope, TargetPosition, Withheld
from finflow.domain.strategy import Strategy

REQUIRED_COLUMNS = ("symbol", "date")
SIGNAL = "_signal"


class FeatureError(ValueError):
    """The feature frame cannot support this strategy.

    A programming error rather than a data condition: a missing column means the
    mart and the AST disagree, and continuing would produce a confident decision
    from the wrong inputs.
    """


def decide(
    features: pl.DataFrame,
    strategy: Strategy,
    as_of: date,
    *,
    universe: Sequence[str] = (),
    excluded: Mapping[str, str] | None = None,
    substitutions: Mapping[str, str] | None = None,
    snapshot_id: str | None = None,
) -> Decision:
    """Evaluate one strategy as of one date and return its target portfolio.

    ``features`` is long — one row per symbol per date — and is expected to hold
    the universe's bars and nothing else; resolving a universe name to symbols
    needs the registry, which sits outside this layer.

    ``universe`` is the membership as of ``as_of``, passed separately so that a
    member with no rows at all is *visible* as withheld rather than quietly
    absent. That distinction is the difference between "nothing fired" and "we
    never looked", and only one of them is an incident.

    ``excluded`` maps a symbol to the reason it may not be held — a ``/mute``, a
    liquidity floor, a tradeability filter. Excluded symbols are still evaluated
    and still recorded as withheld, because the counterfactual is what makes the
    override reviewable later (``PROJECT.md`` §7.7).

    ``substitutions`` maps a symbol to the line to buy *instead* of it. Under
    PRIIPs an EU retail account cannot buy SPY however liquid it is, so a target
    naming SPY is research unless it also names what to purchase (§5.7). The
    substitution rides on the position rather than being looked up at delivery,
    so the instruction is self-contained.
    """
    excluded = dict(excluded or {})
    substitutions = dict(substitutions or {})
    _require_columns(features, strategy)

    members = tuple(universe)
    if not members and not features.is_empty():
        # No membership supplied: fall back to whatever the frame holds. Honest
        # for an ad-hoc evaluation, and never used by the daily run, which
        # resolves membership from the registry as of the evaluation date.
        members = tuple(sorted(set(features["symbol"].to_list())))

    history = features.filter(pl.col("date") <= as_of).sort(["symbol", "date"])
    withheld: list[Withheld] = []

    latest = _latest_signal(history, strategy)
    seen = set(latest["symbol"].to_list()) if not latest.is_empty() else set()

    for symbol in members:
        if symbol not in seen:
            withheld.append(Withheld(symbol=symbol, reason="no bars on or before this date"))

    firing: list[str] = []
    for row in latest.iter_rows(named=True):
        symbol = str(row["symbol"])
        signal = row[SIGNAL]
        if signal is None:
            withheld.append(
                Withheld(
                    symbol=symbol,
                    reason=f"insufficient history — the rule needs {strategy.warmup} bars",
                )
            )
            continue
        if symbol in excluded:
            withheld.append(Withheld(symbol=symbol, reason=excluded[symbol]))
            continue
        if signal:
            firing.append(symbol)

    firing.sort()
    if strategy.max_positions is not None and len(firing) > strategy.max_positions:
        # Alphabetical rather than arbitrary: with no ranking function in the
        # grammar yet, the only defensible tie-break is a deterministic one, and
        # the digest says which names were dropped and why.
        for symbol in firing[strategy.max_positions :]:
            withheld.append(Withheld(symbol=symbol, reason="over max_positions for this strategy"))
        firing = firing[: strategy.max_positions]

    weight = 1.0 / len(firing) if firing else 0.0
    return Decision(
        strategy_id=strategy.id,
        strategy_version=strategy.version,
        as_of=as_of,
        data_as_of=_max_date(history),
        positions=tuple(
            TargetPosition(symbol, weight, substitutions.get(symbol)) for symbol in firing
        ),
        withheld=tuple(withheld),
        scope=Scope.STRATEGY,
        snapshot_id=snapshot_id,
        universe=strategy.universe,
        evaluated=len(seen),
    )


def _require_columns(features: pl.DataFrame, strategy: Strategy) -> None:
    """Fail early and by name when the frame cannot support the AST."""
    if features.is_empty() and not features.columns:
        raise FeatureError(
            f"{strategy.id}: the feature frame has no columns — the mart is missing "
            f"or the build step did not run"
        )
    missing = [column for column in REQUIRED_COLUMNS if column not in features.columns]
    if missing:
        raise FeatureError(
            f"{strategy.id}: feature frame is missing {', '.join(missing)}; "
            f"got {', '.join(features.columns)}"
        )


def _latest_signal(history: pl.DataFrame, strategy: Strategy) -> pl.DataFrame:
    """The signal on each symbol's freshest bar at or before ``as_of``.

    ``over("symbol")`` rather than a group-by-and-join: the rolling windows must
    see one instrument's history in date order and nothing else, and a plain
    rolling over a concatenated long frame would silently average across the
    boundary between two symbols.
    """
    if history.is_empty():
        return history.with_columns(pl.lit(None, dtype=pl.Boolean).alias(SIGNAL)).head(0)

    signal = strategy.signal.to_expr().over("symbol").alias(SIGNAL)
    return (
        history.with_columns(signal)
        .group_by("symbol")
        .agg(pl.all().sort_by("date").last())
        .sort("symbol")
    )


def _max_date(history: pl.DataFrame) -> date | None:
    """The freshest bar the decision actually saw."""
    if history.is_empty():
        return None
    value = history["date"].max()
    return value if isinstance(value, date) else None
