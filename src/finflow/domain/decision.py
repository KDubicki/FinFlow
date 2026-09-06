"""The ``Decision`` entity — one per strategy per evaluation.

``PROJECT.md`` §9.4 argues this out: a per-instrument alert key cannot represent
a rebalance, and deduplicating a rotation instrument-by-instrument delivers half
of it when the worker dies mid-batch. So the unit of both storage and delivery
is the whole target portfolio, and a single-instrument entry is just a decision
with one position.

The identity of a decision is **its content**, not the run that produced it.
That one choice is what makes exactly-once delivery achievable with an
append-only outbox and no coordination:

- Re-running the pipeline on the same data recomputes the same id, the outbox
  insert is ignored, and nothing is sent twice.
- A mid-morning retry that finally gets the late vendor's bar produces
  *different* content, so it gets a new id and is delivered — which is the
  behaviour you want, and the behaviour a run-id-based key would not give.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any

PORTFOLIO_SCOPE_ID = "portfolio"
"""Reserved ``strategy_id`` for the netted account-level decision of §7.6.

A literal rather than NULL because the outbox's uniqueness constraint is
``(strategy_id, strategy_version, decision_id)``, and in SQL two NULLs are not
equal — a nullable column there would silently stop deduplicating exactly the
decision that matters most.
"""


class Scope(StrEnum):
    """Whose portfolio a decision describes."""

    STRATEGY = "strategy"
    PORTFOLIO = "portfolio"


@dataclass(frozen=True, slots=True)
class TargetPosition:
    """One line of a target portfolio: how much of the account to hold."""

    symbol: str
    weight: float

    def __post_init__(self) -> None:
        if not 0.0 <= self.weight <= 1.0:
            raise ValueError(f"{self.symbol}: weight {self.weight} outside [0, 1]")

    def canonical(self) -> dict[str, Any]:
        """A stable rendering. Weights are rounded so that float noise from a
        different Polars version cannot change a decision id."""
        return {"symbol": self.symbol, "weight": round(self.weight, 6)}


@dataclass(frozen=True, slots=True)
class Withheld:
    """Something the decision deliberately left out, and why.

    Recorded on the decision rather than logged, because ``PROJECT.md`` §7.7 is
    categorical: nothing is suppressed silently. A control the user forgot they
    set is indistinguishable from a bug unless the digest can list it.
    """

    reason: str
    symbol: str | None = None

    def canonical(self) -> dict[str, Any]:
        """A stable rendering."""
        return {"symbol": self.symbol, "reason": self.reason}

    def describe(self) -> str:
        """One line for the digest."""
        return f"{self.symbol}: {self.reason}" if self.symbol else self.reason


@dataclass(frozen=True, slots=True)
class Decision:
    """A full target portfolio, as of one evaluation date.

    ``as_of`` is the date the decision was made *for*; ``data_as_of`` is the
    freshest bar it actually saw. They differ whenever the pipeline is behind,
    and carrying both is what lets a message say it is stale rather than
    sounding confident (``PROJECT.md`` §7.3).
    """

    strategy_id: str
    strategy_version: str
    as_of: date
    data_as_of: date | None
    positions: tuple[TargetPosition, ...] = ()
    withheld: tuple[Withheld, ...] = ()
    scope: Scope = Scope.STRATEGY
    snapshot_id: str | None = None
    universe: str = ""
    evaluated: int = 0
    """How many instruments the rule was actually evaluated over — a decision
    holding nothing because the universe was empty is a different animal from
    one holding nothing because nothing fired."""

    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        total = sum(p.weight for p in self.positions)
        if total > 1.0 + 1e-9:
            raise ValueError(f"target weights sum to {total:.4f}, above 1.0")
        symbols = [p.symbol for p in self.positions]
        if len(set(symbols)) != len(symbols):
            raise ValueError(f"repeated symbol in target portfolio: {sorted(symbols)}")
        object.__setattr__(self, "decision_id", self._identity())

    def _identity(self) -> str:
        """Content address: everything that changes what the user is told.

        ``snapshot_id`` is excluded on purpose. It changes on every build, so
        including it would make every run a new decision and defeat the outbox's
        deduplication entirely — the message says which snapshot it came from,
        but the snapshot does not define the instruction.
        """
        payload = json.dumps(
            {
                "scope": str(self.scope),
                "strategy_id": self.strategy_id,
                "strategy_version": self.strategy_version,
                "as_of": self.as_of.isoformat(),
                "data_as_of": self.data_as_of.isoformat() if self.data_as_of else None,
                "positions": [
                    p.canonical() for p in sorted(self.positions, key=lambda p: p.symbol)
                ],
                "withheld": [
                    w.canonical()
                    for w in sorted(self.withheld, key=lambda w: (w.symbol or "", w.reason))
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @property
    def is_flat(self) -> bool:
        """True when the target portfolio holds nothing."""
        return not self.positions

    def weight_of(self, symbol: str) -> float:
        """The target weight for one symbol, zero when it is not held."""
        return next((p.weight for p in self.positions if p.symbol == symbol), 0.0)

    def staleness_days(self, today: date) -> int:
        """Calendar days between the freshest bar and ``today``.

        Calendar rather than trading days deliberately: the question the digest
        answers is "how old is this?", and a reader at 06:00 on a Tuesday counts
        in days off a wall calendar.
        """
        return (today - self.data_as_of).days if self.data_as_of else -1

    def to_payload(self) -> dict[str, Any]:
        """The JSON body written to the outbox.

        Self-contained: delivery must not have to re-query anything, because a
        row may be sent by a later run than the one that wrote it, against a
        warehouse that has been rebuilt since.
        """
        return {
            "decision_id": self.decision_id,
            "scope": str(self.scope),
            "strategy_id": self.strategy_id,
            "strategy_version": self.strategy_version,
            "universe": self.universe,
            "as_of": self.as_of.isoformat(),
            "data_as_of": self.data_as_of.isoformat() if self.data_as_of else None,
            "snapshot_id": self.snapshot_id,
            "evaluated": self.evaluated,
            "positions": [p.canonical() for p in self.positions],
            "withheld": [w.canonical() for w in self.withheld],
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Decision:
        """Rebuild a decision from its stored payload.

        The round trip is asserted by a test: an outbox row that cannot be read
        back is an alert that can never be delivered, and it would be discovered
        during an incident rather than in CI.
        """
        data_as_of = payload.get("data_as_of")
        return cls(
            strategy_id=str(payload["strategy_id"]),
            strategy_version=str(payload["strategy_version"]),
            as_of=date.fromisoformat(str(payload["as_of"])),
            data_as_of=date.fromisoformat(str(data_as_of)) if data_as_of else None,
            positions=tuple(
                TargetPosition(str(p["symbol"]), float(p["weight"]))
                for p in payload.get("positions", [])
            ),
            withheld=tuple(
                Withheld(str(w["reason"]), w["symbol"]) for w in payload.get("withheld", [])
            ),
            scope=Scope(str(payload.get("scope", Scope.STRATEGY))),
            snapshot_id=payload.get("snapshot_id"),
            universe=str(payload.get("universe", "")),
            evaluated=int(payload.get("evaluated", 0)),
        )
