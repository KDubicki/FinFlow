"""Assembling the daily digest.

The digest is sent at a fixed time whether or not anything happened, and that
unconditional quality is the point: a message that only arrives when something
fires is indistinguishable from a pipeline that has been dead for a week.

Everything here is gathering; the wording lives in ``domain.messages`` and the
target-versus-actual arithmetic in ``domain.drift``. This module's only real
opinion is that **a missing number is reported, never guessed** — a freshness it
could not read comes out as "never", not as today.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from finflow.application.evaluate_strategies import FEATURE_TABLE, EvaluationOutcome
from finflow.domain import portfolio
from finflow.domain.calendars import sessions_behind
from finflow.domain.decision import Decision
from finflow.domain.drift import DriftLine, compute_drift
from finflow.domain.messages import Digest, Step
from finflow.logging import get_logger
from finflow.ports.clock import Clock
from finflow.ports.ops_store import OpsStore
from finflow.ports.warehouse import Warehouse
from finflow.registry.models import Registry

log = get_logger(__name__)

DEFAULT_BAND_PP = 5.0
"""Rebalance band, in percentage points. Drift smaller than this is a hold.

Five points is chosen to protect the ≥95% no-action target rather than derived
from a cost model; M5 replaces it with one built from the per-instrument spreads
already in the registry.
"""


@dataclass
class RunFacts:
    """The numbers the run itself knows, handed in rather than re-derived."""

    run_id: str
    steps: tuple[Step, ...] = ()
    bars_ingested: int = 0
    partitions_written: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    failures: tuple[str, ...] = ()
    snapshot_id: str | None = None
    notes: tuple[str, ...] = ()


class BuildDigest:
    """Gather everything the daily message reports."""

    def __init__(
        self,
        *,
        ops_store: OpsStore,
        registry: Registry,
        clock: Clock,
        warehouse: Warehouse | None = None,
        band_pp: float = DEFAULT_BAND_PP,
    ) -> None:
        self._ops = ops_store
        self._registry = registry
        self._clock = clock
        self._warehouse = warehouse
        self._band_pp = band_pp

    def run(
        self,
        facts: RunFacts,
        *,
        evaluation: EvaluationOutcome | None = None,
        delivered: int = 0,
        extra_withheld: Sequence[str] = (),
    ) -> Digest:
        """Build the digest for today's run."""
        today = self._clock.today()
        decisions = [result.decision for result in evaluation.results] if evaluation else []
        target = portfolio.net(decisions, as_of=today, snapshot_id=facts.snapshot_id)

        latest = self._latest_bars()
        drift = self._drift(target, latest)
        withheld = [*(evaluation.withheld if evaluation else []), *extra_withheld]
        controls = [f"{control.describe()}" for control in self._ops.controls(today=today)]

        return Digest(
            as_of=today,
            run_id=facts.run_id,
            steps=facts.steps,
            bars_ingested=facts.bars_ingested,
            partitions_written=facts.partitions_written,
            freshest=self._freshness(latest),
            checks_passed=facts.checks_passed,
            checks_failed=facts.checks_failed,
            restatements=self._restatements(),
            strategies_evaluated=evaluation.evaluated if evaluation else 0,
            decisions_delivered=delivered,
            drift=drift,
            withheld=tuple(dict.fromkeys([*withheld, *controls])),
            failures=tuple(facts.failures),
            sessions_behind=self._sessions_behind(target.data_as_of, today),
            snapshot_id=facts.snapshot_id,
            notes=tuple(facts.notes),
        )

    def _sessions_behind(self, data_as_of: date | None, today: date) -> int:
        """How many completed sessions have no data.

        Measured on the venue most of the registry trades on. That is exactly
        right while the universe is US-listed and becomes wrong the day it is
        not, which is why M5 — the milestone that widens the universe — owns
        making freshness per instrument.
        """
        if data_as_of is None:
            return 0
        calendars = Counter(i.calendar for i in self._registry.instruments)
        venue = calendars.most_common(1)[0][0] if calendars else "XNYS"
        return sessions_behind(venue, data_as_of, today)

    # ---- gathering -------------------------------------------------------

    def _latest_bars(self) -> dict[str, tuple[date, float]]:
        """The freshest close per symbol, as far as the mart knows.

        Returns an empty mapping rather than raising when the warehouse is
        absent: a digest that fails to send because it could not read a price is
        strictly worse than one that says a price is missing.
        """
        if self._warehouse is None:
            return {}
        try:
            if FEATURE_TABLE not in self._warehouse.tables():
                return {}
            frame = self._warehouse.query(
                f"SELECT symbol, max(date) AS date, "
                f"arg_max(close, date) AS close FROM {FEATURE_TABLE} GROUP BY symbol"
            )
        except Exception as exc:  # the digest must still send
            log.warning("digest_prices_unavailable", error=str(exc))
            return {}
        return {
            str(row["symbol"]): (row["date"], float(row["close"]))
            for row in frame.iter_rows(named=True)
            if isinstance(row["date"], date)
        }

    def _freshness(
        self, latest: dict[str, tuple[date, float]]
    ) -> tuple[tuple[str, date | None], ...]:
        """Freshest bar per universe.

        Per universe rather than overall because a stale commodities feed and a
        stale rates feed are different incidents with different responses, and
        one overall maximum hides the one that is behind.
        """
        rows: list[tuple[str, date | None]] = []
        for name in self._registry.universe_names:
            members = [i.symbol for i in self._registry.universe(name)]
            dates = [latest[symbol][0] for symbol in members if symbol in latest]
            rows.append((name, min(dates) if dates else None))
        return tuple(rows)

    def _restatements(self) -> int:
        """How many keys a vendor changed its mind about in this build."""
        if self._warehouse is None or "dq_restatements" not in self._warehouse.tables():
            return 0
        try:
            frame = self._warehouse.query("SELECT count(*) AS n FROM dq_restatements")
        except Exception as exc:  # see _latest_bars
            log.warning("digest_restatements_unavailable", error=str(exc))
            return 0
        return int(frame["n"][0]) if not frame.is_empty() else 0

    def _drift(
        self, target: Decision, latest: dict[str, tuple[date, float]]
    ) -> tuple[DriftLine, ...]:
        """Target versus what the user says they hold."""
        holdings = {position.symbol: position.units for position in self._ops.positions()}
        prices = {symbol: price for symbol, (_, price) in latest.items()}
        return compute_drift(target, holdings=holdings, prices=prices, band_pp=self._band_pp)
