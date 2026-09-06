"""The strategy AST — the artifact the surface syntax will compile to.

``PROJECT.md`` §7.2 is explicit that the expression strings are a *surface
syntax* and the stable artifact is the typed AST. So the AST arrives first, and
the first rule is hand-constructed rather than parsed. M7 adds the parser that
produces exactly this shape; nothing here is thrown away when it does.

Three node types, deliberately:

``Price``      a column of the feature frame          -> ``Series[float]``
``Indicator``  a whitelisted function over a node     -> ``Series[float]``
``Compare``    two float series related by an operator -> ``Series[bool]``

That is the smallest set that expresses an SMA cross, and it already carries the
two properties that matter later:

- **No ``eval``, ever.** Functions come from a closed registry, operators from a
  closed set. There is no path from a string to a call.
- **The AST is what the version hash keys on**, not the text. Reformatting does
  not invalidate a stored backtest; changing a ``20`` to a ``21`` does
  (``PROJECT.md`` §7.4).

The minimal type system is here from the start too. It is what catches
``filter: sma(close, 20)`` — a float where a predicate is required — at load
time rather than at 05:00 on a Tuesday.
"""

from __future__ import annotations

import hashlib
import json
import operator
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

import polars as pl

API_VERSION = "finflow/v1"
"""The strategy contract version (``PROJECT.md`` §7.4). An unknown one fails."""


class ValueType(StrEnum):
    """What a node evaluates to.

    Small on purpose: ``Scalar`` and ``CrossSection[float]`` join it when a node
    needs them, not before (``PROJECT.md`` §4.6).
    """

    SERIES_FLOAT = "Series[float]"
    SERIES_BOOL = "Series[bool]"


class StrategyTypeError(ValueError):
    """A node was handed an operand of the wrong type.

    Raised at construction, so an ill-typed strategy cannot be built at all —
    which is what makes "caught at load time" true rather than aspirational.
    """


@runtime_checkable
class Node(Protocol):
    """One node of a strategy expression."""

    def value_type(self) -> ValueType:
        """The type this node evaluates to."""
        ...

    def to_expr(self) -> pl.Expr:
        """Compile to a Polars expression over one instrument's sorted history.

        The expression assumes rows for a single symbol in ascending date order.
        Partitioning across symbols is the evaluator's job, not the node's —
        a node that knew about ``symbol`` could not be reused for a scalar
        macro series.
        """
        ...

    def canonical(self) -> dict[str, Any]:
        """A stable, JSON-serialisable rendering, for hashing and storage."""
        ...


# Columns a ``Price`` node may reference. A closed set rather than "any column":
# an unknown column is a typo that would otherwise surface as a Polars error
# during the 05:30 run instead of at construction.
PRICE_COLUMNS = ("open", "high", "low", "close", "volume")


@dataclass(frozen=True, slots=True)
class Price:
    """A column of the feature frame."""

    column: str = "close"

    def __post_init__(self) -> None:
        if self.column not in PRICE_COLUMNS:
            raise StrategyTypeError(
                f"unknown price column {self.column!r}; available: {', '.join(PRICE_COLUMNS)}"
            )

    def value_type(self) -> ValueType:
        """Prices are float series."""
        return ValueType.SERIES_FLOAT

    def to_expr(self) -> pl.Expr:
        """The column itself."""
        return pl.col(self.column)

    def canonical(self) -> dict[str, Any]:
        """``{"node": "price", "column": "close"}``."""
        return {"node": "price", "column": self.column}


def _sma(operand: pl.Expr, window: int) -> pl.Expr:
    return operand.rolling_mean(window_size=window)


def _ema(operand: pl.Expr, window: int) -> pl.Expr:
    # ``adjust=False`` is the recursive form every charting package means by
    # "EMA". The adjusted form differs on the first few dozen bars, which is
    # exactly the region a known-answer test compares.
    return operand.ewm_mean(span=window, adjust=False)


INDICATORS: dict[str, Callable[[pl.Expr, int], pl.Expr]] = {"sma": _sma, "ema": _ema}
"""The typed whitelist. New entries are a backlog item, never a mid-milestone
yes — the DSL becoming the project is a named risk."""


@dataclass(frozen=True, slots=True)
class Indicator:
    """A whitelisted function of a float series over a lookback window."""

    name: str
    operand: Node
    window: int

    def __post_init__(self) -> None:
        if self.name not in INDICATORS:
            raise StrategyTypeError(
                f"unknown function {self.name!r}; available: {', '.join(sorted(INDICATORS))}"
            )
        if self.operand.value_type() is not ValueType.SERIES_FLOAT:
            raise StrategyTypeError(
                f"{self.name}() takes {ValueType.SERIES_FLOAT}, got {self.operand.value_type()}"
            )
        if self.window < 1:
            raise StrategyTypeError(f"{self.name}() window must be positive, got {self.window}")

    def value_type(self) -> ValueType:
        """Indicators return float series."""
        return ValueType.SERIES_FLOAT

    def to_expr(self) -> pl.Expr:
        """The compiled rolling expression."""
        return INDICATORS[self.name](self.operand.to_expr(), self.window)

    @property
    def warmup(self) -> int:
        """Bars needed before this indicator produces a value.

        Reported rather than inferred at evaluation time: a decision made on an
        indicator that has not warmed up is a decision made on a null, and the
        evaluator refuses to make it.
        """
        return _warmup(self.operand) + self.window

    def canonical(self) -> dict[str, Any]:
        """``{"node": "indicator", "name": "sma", ...}``."""
        return {
            "node": "indicator",
            "name": self.name,
            "window": self.window,
            "operand": self.operand.canonical(),
        }


COMPARISONS: dict[str, Callable[[pl.Expr, pl.Expr], pl.Expr]] = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}
"""Ordering comparisons only. Equality on floats is a bug in a financial
expression, so the grammar does not offer it."""


@dataclass(frozen=True, slots=True)
class Compare:
    """Two float series related by an ordering operator."""

    left: Node
    op: str
    right: Node

    def __post_init__(self) -> None:
        if self.op not in COMPARISONS:
            raise StrategyTypeError(
                f"unknown operator {self.op!r}; available: {', '.join(COMPARISONS)}"
            )
        for side, node in (("left", self.left), ("right", self.right)):
            if node.value_type() is not ValueType.SERIES_FLOAT:
                raise StrategyTypeError(
                    f"{side} operand of {self.op!r} must be {ValueType.SERIES_FLOAT}, "
                    f"got {node.value_type()}"
                )

    def value_type(self) -> ValueType:
        """Comparisons return boolean series."""
        return ValueType.SERIES_BOOL

    def to_expr(self) -> pl.Expr:
        """The compiled comparison."""
        return COMPARISONS[self.op](self.left.to_expr(), self.right.to_expr())

    @property
    def warmup(self) -> int:
        """The longer of the two sides' warmups."""
        return max(_warmup(self.left), _warmup(self.right))

    def canonical(self) -> dict[str, Any]:
        """``{"node": "compare", "op": ">", ...}``."""
        return {
            "node": "compare",
            "op": self.op,
            "left": self.left.canonical(),
            "right": self.right.canonical(),
        }


def _warmup(node: Node) -> int:
    """Bars this node needs before it yields a value. Zero for a bare column."""
    return int(getattr(node, "warmup", 0))


@dataclass(frozen=True, slots=True)
class Strategy:
    """A named rule over a universe, plus how it sizes what it holds.

    Equal weighting across the instruments whose signal is true is the whole
    sizing model in M4. It is stated as a field rather than assumed so that the
    day a second scheme exists, the change is a value rather than a rewrite —
    and so that the version hash already covers it.
    """

    id: str
    universe: str
    signal: Node
    description: str = ""
    weighting: str = "equal"
    max_positions: int | None = None
    api_version: str = API_VERSION

    def __post_init__(self) -> None:
        if self.api_version != API_VERSION:
            raise StrategyTypeError(
                f"{self.id}: unsupported apiVersion {self.api_version!r}, expected {API_VERSION}"
            )
        if self.signal.value_type() is not ValueType.SERIES_BOOL:
            raise StrategyTypeError(
                f"{self.id}: signal must be a predicate ({ValueType.SERIES_BOOL}), "
                f"got {self.signal.value_type()} — a float is not a rule"
            )
        if self.weighting != "equal":
            raise StrategyTypeError(f"{self.id}: unknown weighting {self.weighting!r}")
        if self.max_positions is not None and self.max_positions < 1:
            raise StrategyTypeError(f"{self.id}: max_positions must be positive")

    @property
    def warmup(self) -> int:
        """Bars of history the signal needs before it means anything."""
        return _warmup(self.signal)

    def canonical(self) -> dict[str, Any]:
        """Everything that affects what is evaluated, and nothing else.

        The strategy *id* is deliberately absent: it is the identity, not the
        behaviour. Renaming a strategy must not invalidate its stored runs,
        while changing its universe or its rule must.
        """
        return {
            "apiVersion": self.api_version,
            "universe": self.universe,
            "weighting": self.weighting,
            "max_positions": self.max_positions,
            "signal": self.signal.canonical(),
        }

    @property
    def version(self) -> str:
        """The AST hash — the ``strategy_version`` carried on every message.

        Part of the outbox key (``PROJECT.md`` §9.4), because without it an
        edited strategy's new signal for the same bar collides with the old
        key and is suppressed as a duplicate: a silent miss.
        """
        payload = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


# ---- the first rule ------------------------------------------------------

SMA_CROSS = Strategy(
    id="sma_cross_precious_metals",
    universe="precious_metals",
    description="Hold the metals whose 20-day average is above their 50-day average.",
    signal=Compare(
        left=Indicator("sma", Price("close"), 20),
        op=">",
        right=Indicator("sma", Price("close"), 50),
    ),
)
"""The M4 rule, hand-built rather than parsed.

Narrow on purpose — the milestone is about the pipeline running unattended, not
about the rule being good. What it is *not* is throwaway: it goes through the
same ``decide()`` the compiler will target, so the evaluation path in production
on day one is the final one.
"""

DEFAULT_STRATEGIES: tuple[Strategy, ...] = (SMA_CROSS,)
"""What the daily run evaluates until strategy documents exist (M7)."""
