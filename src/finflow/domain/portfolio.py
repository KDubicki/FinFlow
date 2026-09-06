"""Netting several strategies into one instruction.

``PROJECT.md`` §7.6: four strategies running at once will overlap, and emitting
four independent alerts pushes portfolio construction onto the reader at exactly
the moment they are least equipped to do it. So a portfolio step sits after
evaluation and produces one target for the account.

What is here is the netting itself, under **equal capital allocation across
strategies**. The gross and single-name caps, the minimum trade size and the
per-strategy weights of ``portfolio.yml`` arrive with M7; until then there is one
strategy running and netting is a pass-through, so an interim rule that is
obvious and stated beats a configuration file with one entry in it.

The netted object is a ``Decision`` like any other, with ``scope: portfolio``
and the reserved strategy id — one table, one outbox, one delivery path, so
nothing downstream needs to know which kind it is holding.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from finflow.domain.decision import (
    PORTFOLIO_SCOPE_ID,
    Decision,
    Scope,
    TargetPosition,
    Withheld,
)


def net(decisions: Sequence[Decision], *, as_of: date, snapshot_id: str | None = None) -> Decision:
    """Combine per-strategy decisions into one account-level target.

    Capital is split evenly across the strategies that produced a decision, and
    each strategy's weights are scaled by its share. Two strategies that both
    want to be fully invested in GLD therefore produce one 100% GLD target, not
    a 200% one — the gross exposure of the account is what the reader acts on.

    The per-strategy decisions are *not* replaced. They stay recorded, because
    they are the counterfactual that ``/pause`` and the decision journal depend
    on (§7.7).
    """
    if not decisions:
        return Decision(
            strategy_id=PORTFOLIO_SCOPE_ID,
            strategy_version="empty",
            as_of=as_of,
            data_as_of=None,
            scope=Scope.PORTFOLIO,
            snapshot_id=snapshot_id,
        )
    if len(decisions) == 1:
        only = decisions[0]
        return Decision(
            strategy_id=PORTFOLIO_SCOPE_ID,
            strategy_version=only.strategy_version,
            as_of=as_of,
            data_as_of=only.data_as_of,
            positions=only.positions,
            withheld=only.withheld,
            scope=Scope.PORTFOLIO,
            snapshot_id=snapshot_id,
            universe=only.universe,
            evaluated=only.evaluated,
        )

    share = 1.0 / len(decisions)
    weights: dict[str, float] = {}
    withheld: list[Withheld] = []
    data_dates = [d.data_as_of for d in decisions if d.data_as_of is not None]

    for decision in decisions:
        for position in decision.positions:
            weights[position.symbol] = weights.get(position.symbol, 0.0) + position.weight * share
        withheld.extend(decision.withheld)

    return Decision(
        strategy_id=PORTFOLIO_SCOPE_ID,
        # A version derived from its inputs: the netted target changes whenever
        # any contributing strategy does, and the id has to move with it.
        strategy_version="+".join(sorted({d.strategy_version for d in decisions}))[:64],
        as_of=as_of,
        # The oldest contributing bar, not the newest: the account's target is
        # only as fresh as the stalest thing that went into it.
        data_as_of=min(data_dates) if data_dates else None,
        positions=tuple(
            TargetPosition(symbol, round(weight, 6))
            for symbol, weight in sorted(weights.items())
            if weight > 0
        ),
        withheld=tuple(withheld),
        scope=Scope.PORTFOLIO,
        snapshot_id=snapshot_id,
        evaluated=sum(d.evaluated for d in decisions),
    )
