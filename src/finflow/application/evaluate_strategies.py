"""Evaluate every strategy and decide what, if anything, to say.

This is the use case that turns a warehouse into a system. It reads features,
calls the one ``decide()`` of ``PROJECT.md`` §7.3, records every decision, and
enqueues the ones worth telling somebody about.

Two rules shape it, and both are about not being muted:

- **A decision is recorded every day; a message is queued only when the
  instruction changes.** Sending "hold GLD" every morning for six weeks trains
  the reader to ignore the channel, and the digest already reports the standing
  target. The ≥95% no-action target is a tracked number, not an aspiration
  (``PROJECT.md`` §1.2).
- **Nothing is suppressed silently.** A paused strategy still computes and still
  records, and the digest lists what was withheld and why (§7.7). A control the
  user forgot they set is otherwise indistinguishable from a bug.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date

import polars as pl

from finflow.domain.decision import Decision
from finflow.domain.evaluator import decide
from finflow.domain.strategy import DEFAULT_STRATEGIES, Strategy
from finflow.logging import get_logger
from finflow.ports.clock import Clock
from finflow.ports.ops_store import ControlKind, OpsStore
from finflow.ports.warehouse import Warehouse
from finflow.registry.models import Registry

log = get_logger(__name__)

FEATURE_TABLE = "fct_ohlcv_daily"
FEATURE_COLUMNS = ("symbol", "date", "open", "high", "low", "close", "volume")

_SAFE_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")


class FeaturesUnavailable(RuntimeError):
    """The mart the evaluator reads is missing or unreadable.

    Its own class because the response is specific: issue no instruction, say so
    in the digest, and do not fall back to stale features. A decision made from
    a table that is not there is the failure mode the trust ladder is designed
    around (``PROJECT.md`` §15).
    """


@dataclass
class StrategyResult:
    """What happened to one strategy this run."""

    strategy: Strategy
    decision: Decision
    queued: bool = False
    reason: str = ""
    """Why nothing was queued, when nothing was."""


@dataclass
class EvaluationOutcome:
    """What the evaluation step did, for the digest and for ``pipeline_runs``."""

    as_of: date
    results: list[StrategyResult] = field(default_factory=list)
    withheld: list[str] = field(default_factory=list)

    @property
    def evaluated(self) -> int:
        """How many strategies ran."""
        return len(self.results)

    @property
    def queued(self) -> int:
        """How many decisions were put on the outbox."""
        return sum(1 for result in self.results if result.queued)

    def summary(self) -> str:
        """One line for a log or a digest."""
        return f"{self.evaluated} strategies evaluated, {self.queued} queued"


class EvaluateStrategies:
    """Run the strategies, record the decisions, queue what changed."""

    def __init__(
        self,
        *,
        warehouse: Warehouse,
        ops_store: OpsStore,
        registry: Registry,
        clock: Clock,
        strategies: Sequence[Strategy] = DEFAULT_STRATEGIES,
    ) -> None:
        self._warehouse = warehouse
        self._ops = ops_store
        self._registry = registry
        self._clock = clock
        self._strategies = tuple(strategies)

    def run(
        self, *, run_id: str, snapshot_id: str | None = None, as_of: date | None = None
    ) -> EvaluationOutcome:
        """Evaluate every strategy as of ``as_of``, defaulting to today."""
        today = as_of or self._clock.today()
        now = self._clock.now()
        outcome = EvaluationOutcome(as_of=today)

        controls = self._ops.controls(today=today)
        muted = {
            control.scope: f"muted until {control.until}"
            for control in controls
            if control.kind is ControlKind.MUTE
        }
        paused = {control.scope for control in controls if control.kind is ControlKind.PAUSE}
        holding = next((c for c in controls if c.kind is ControlKind.HOLD), None)

        for strategy in self._strategies:
            members = [i.symbol for i in self._registry.universe(strategy.universe, today)]
            decision = decide(
                self._features(members),
                strategy,
                today,
                universe=members,
                excluded=muted,
                snapshot_id=snapshot_id,
            )
            # Recorded before any decision about delivery, so the counterfactual
            # survives a pause, a hold, and a crash in the delivery step.
            self._ops.save_decision(decision, run_id=run_id, now=now)
            result = StrategyResult(strategy=strategy, decision=decision)

            if strategy.id in paused:
                result.reason = f"{strategy.id} is paused — decision recorded, not issued"
            elif holding is not None:
                result.reason = f"rebalancing on hold until {holding.until}"
            elif not self._instruction_changed(decision):
                result.reason = "target unchanged since the last message"
            else:
                result.queued = self._ops.enqueue(decision, now=now)
                if not result.queued:
                    # Already on the outbox: an earlier run today produced this
                    # exact decision and it is waiting, or was delivered.
                    result.reason = "already queued"

            if result.reason:
                outcome.withheld.append(result.reason)
            outcome.withheld.extend(item.describe() for item in decision.withheld)
            outcome.results.append(result)

            log.info(
                "strategy_evaluated",
                strategy=strategy.id,
                version=strategy.version,
                decision=decision.decision_id,
                positions=len(decision.positions),
                queued=result.queued,
                reason=result.reason,
            )

        return outcome

    def _instruction_changed(self, decision: Decision) -> bool:
        """True when this target differs from the last one we told the user about.

        Compared on the *instruction* — the strategy version and the target
        weights — rather than on the decision id, which also moves when the
        underlying bar date does. Otherwise an unchanged portfolio would produce
        a fresh message every single morning.
        """
        last = self._ops.last_enqueued(decision.strategy_id)
        if last is None:
            return True
        previous = Decision.from_payload(last.payload)
        if previous.strategy_version != decision.strategy_version:
            return True
        return [p.canonical() for p in sorted(previous.positions, key=lambda p: p.symbol)] != [
            p.canonical() for p in sorted(decision.positions, key=lambda p: p.symbol)
        ]

    def _features(self, symbols: Sequence[str]) -> pl.DataFrame:
        """Read the bars for one universe out of the mart.

        Symbols are re-validated against the registry's own pattern before they
        reach the SQL text. They arrive from the registry, which already
        enforces it, so this is a second lock on a door that is already
        closed — worth having because the alternative is a string-formatted
        query whose safety depends on a validator three modules away.
        """
        if not symbols:
            return pl.DataFrame(schema={"symbol": pl.String, "date": pl.Date})

        unsafe = [symbol for symbol in symbols if not _SAFE_SYMBOL.match(symbol)]
        if unsafe:
            raise FeaturesUnavailable(f"refusing to query for {unsafe}: not registry symbols")

        if FEATURE_TABLE not in self._warehouse.tables():
            raise FeaturesUnavailable(
                f"{FEATURE_TABLE} is not in the warehouse — the dbt build did not run. "
                f"No instruction can be issued from a mart that is not there."
            )

        listed = ", ".join(f"'{symbol}'" for symbol in symbols)
        return self._warehouse.query(
            f"SELECT {', '.join(FEATURE_COLUMNS)} FROM {FEATURE_TABLE} "
            f"WHERE symbol IN ({listed}) ORDER BY symbol, date"
        )
