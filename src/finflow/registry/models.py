"""The registry value object.

Everything here is frozen. The ``Registry`` is constructed once at the
composition root and injected (``PROJECT.md`` §5.1) rather than imported as a
module-level singleton, which is the shape it drifts into by default and which
makes "evaluate against the registry as it was in March" impossible.
"""

from __future__ import annotations

from datetime import date, datetime
from types import MappingProxyType
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from finflow.contracts.instruments import AssetClass, Frequency, ReturnBasis
from finflow.contracts.sources import SourceKey, SourceKeyField
from finflow.domain.calendars import is_known_calendar
from finflow.domain.costs import CostFloor, floor_for
from finflow.registry.errors import RegistryValidationError

Symbol = Annotated[str, Field(pattern=r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")]
"""An uppercase ticker. Deliberately strict: a lowercase symbol in one file and
an uppercase one in another is a duplicate the uniqueness check would miss."""


class _Frozen(BaseModel):
    """Base for every registry model: immutable, strict, no unknown fields."""

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)


class Costs(_Frozen):
    """Per-instrument cost floor.

    A flat assumption across the universe understates the round trip on thin
    ETFs by five to ten times and manufactures alpha that cannot be earned
    (``PROJECT.md`` §5.7). A strategy may raise these, never lower them.
    Slippage is not stored: it is derived from realized volatility at
    evaluation time, because spreads widen exactly when signals fire.

    Omitting the block is allowed and means "use the asset-class default"
    (``domain.costs``). Omitting *costs entirely* is not: the default is
    deliberately pessimistic, so the cheap mistake is forgetting to be specific
    rather than forgetting to have costs at all.
    """

    commission_bps: float = Field(ge=0, le=100)
    spread_bps: float = Field(gt=0, le=500)
    """Strictly positive: a zero spread is not a market, and it is the value
    that turns a marginal backtest into a profitable-looking one."""


class Instrument(_Frozen):
    """One tradeable instrument. Adding one of these is the whole workflow."""

    symbol: Symbol
    name: str = Field(min_length=1)
    asset_class: AssetClass
    sub_class: str | None = None
    exchange: str = Field(min_length=1)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    calendar: str
    inception: date
    backfill_start: date
    delisted: date | None = None
    sources: dict[SourceKeyField, str] = Field(min_length=1)
    return_basis: ReturnBasis = ReturnBasis.PRICE
    distribution_yield_hint: float | None = Field(default=None, ge=0, le=1)
    costs: Costs | None = None
    min_adv_usd: float | None = Field(default=None, ge=0)
    """Median dollar volume below which no signal is emitted for this
    instrument on that day (``PROJECT.md`` §5.7). A liquidity floor, not a
    preference: a target you cannot fill without moving the price is not a
    target."""

    ucits: bool = False
    """True when this line is itself UCITS-domiciled, and therefore purchasable
    from an EU retail account. Stated rather than inferred from the symbol: the
    consequence of getting it wrong is a target portfolio that cannot be
    executed, which is the whole reason M5 exists."""

    ucits_equivalent: Symbol | None = None
    """The UCITS line to buy instead of this one, when there is one. Must name a
    registered instrument with ``ucits: true`` — a mapping to a fund the system
    does not ingest is a mapping nobody can act on."""

    enabled: bool = True
    tags: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not is_known_calendar(self.calendar):
            raise ValueError(
                f"{self.symbol}: calendar {self.calendar!r} is not recognised by "
                f"exchange_calendars (try XNYS, XNAS, XLON)"
            )
        if self.backfill_start < self.inception:
            raise ValueError(
                f"{self.symbol}: backfill_start {self.backfill_start} precedes "
                f"inception {self.inception} — history before inception does not exist"
            )
        if self.delisted is not None and self.delisted <= self.inception:
            raise ValueError(
                f"{self.symbol}: delisted {self.delisted} is not after inception {self.inception}"
            )
        if self.ucits and self.ucits_equivalent is not None:
            raise ValueError(
                f"{self.symbol}: a UCITS line has no ucits_equivalent — it is the equivalent"
            )
        if self.ucits_equivalent == self.symbol:
            raise ValueError(f"{self.symbol}: ucits_equivalent points at itself")
        if self.return_basis is not ReturnBasis.PRICE:
            # PROJECT.md §6.4: the field exists so a distributions feed is
            # additive, but no source supplies one yet, so claiming total return
            # would be a wrong number that looks authoritative.
            raise ValueError(
                f"{self.symbol}: return_basis 'total' is not supported — the MVP is "
                f"price-return everywhere until a distributions source exists (§6.4)"
            )
        return self

    @property
    def cost_floor(self) -> CostFloor:
        """The effective floor: this instrument's own, or its class's default."""
        if self.costs is None:
            return floor_for(self.asset_class)
        return CostFloor(commission_bps=self.costs.commission_bps, spread_bps=self.costs.spread_bps)

    @property
    def tradeable_eu(self) -> bool:
        """Whether an EU retail account can buy *this* line.

        Under PRIIPs it cannot buy SPY, GLD, TLT or the SPDR sectors, however
        liquid they are, because they publish no KID. Having a UCITS equivalent
        does not make the US line purchasable — it names what to buy instead —
        and conflating the two produces a portfolio that looks executable and
        is not.
        """
        return self.ucits

    @property
    def is_live(self) -> bool:
        """True when this instrument should still be ingested.

        ``enabled`` and ``delisted`` are separate on purpose: the first is a
        choice, the second is a fact, and neither removes the row (§5.2).
        """
        return self.enabled and self.delisted is None


class UniverseMember(_Frozen):
    """Membership of one instrument in a universe, optionally date-effective.

    ``from`` is a Python keyword, so it is accepted under its YAML name and
    stored as ``valid_from``.
    """

    symbol: Symbol
    valid_from: date | None = Field(default=None, alias="from")
    valid_to: date | None = Field(default=None, alias="to")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.valid_from and self.valid_to and self.valid_to <= self.valid_from:
            raise ValueError(
                f"{self.symbol}: membership ends {self.valid_to}, "
                f"on or before it starts {self.valid_from}"
            )
        return self

    def covers(self, as_of: date) -> bool:
        """True when this membership is in force on ``as_of``."""
        if self.valid_from is not None and as_of < self.valid_from:
            return False
        return not (self.valid_to is not None and as_of >= self.valid_to)

    @classmethod
    def parse(cls, raw: Any) -> UniverseMember:
        """Accept either ``GLD`` or ``{symbol: XLRE, from: 2015-10-08}``."""
        if isinstance(raw, str):
            return cls(symbol=raw)
        if isinstance(raw, dict):
            return cls.model_validate(raw)
        raise ValueError(
            f"universe member must be a symbol or a mapping with a symbol key, got {raw!r}"
        )


class Universe(_Frozen):
    """A named group of instruments that strategies and models reference."""

    name: str = Field(min_length=1)
    description: str | None = None
    members: tuple[UniverseMember, ...] = Field(min_length=1)
    benchmark: Symbol

    @model_validator(mode="after")
    def _check(self) -> Self:
        seen = [m.symbol for m in self.members]
        duplicates = sorted({s for s in seen if seen.count(s) > 1})
        if duplicates:
            raise ValueError(f"{self.name}: repeated member(s) {', '.join(duplicates)}")
        return self


class MacroSeries(_Frozen):
    """A macro driver: a level with a release lag and possibly a revision history.

    ``release_lag_days`` is what stops the pipeline using March's CPI print in a
    decision made on 1 March (``PROJECT.md`` §6.3).
    """

    id: str = Field(min_length=1, pattern=r"^[a-z0-9_]+$")
    source: SourceKeyField
    source_id: str = Field(min_length=1)
    unit: str
    frequency: Frequency
    release_lag_days: int = Field(ge=0, le=400)
    revised: bool
    vintage_aware: bool = False
    transform: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.vintage_aware and not self.revised:
            raise ValueError(
                f"{self.id}: vintage_aware requires revised — a series that is never "
                f"restated has no vintages to read"
            )
        return self


class RegistryCommit(_Frozen):
    """Where the registry came from, resolved once at load time.

    ``committed_at`` is the git commit date, not the pipeline run date: it is
    what ``dim_instrument`` uses for SCD2 ``valid_from`` (``PROJECT.md`` §9.2),
    and a backfill run in November must not stamp an August change with
    November.
    """

    sha: str | None = None
    committed_at: datetime | None = None
    dirty: bool = False

    @property
    def is_reproducible(self) -> bool:
        """True when this registry state can be recovered from git alone."""
        return self.sha is not None and not self.dirty


class Registry(_Frozen):
    """Every instrument, universe and macro series, validated as a whole."""

    instruments: tuple[Instrument, ...] = Field(default=())
    universes: tuple[Universe, ...] = Field(default=())
    macro: tuple[MacroSeries, ...] = Field(default=())
    commit: RegistryCommit = RegistryCommit()

    # Indexes, built once. Private attributes stay assignable on a frozen model,
    # which is what lets the lookups be O(1) without giving up immutability.
    _by_symbol: dict[str, Instrument] = PrivateAttr(default_factory=dict)
    _by_universe: dict[str, Universe] = PrivateAttr(default_factory=dict)
    _by_macro_id: dict[str, MacroSeries] = PrivateAttr(default_factory=dict)

    def model_post_init(self, _context: Any, /) -> None:
        self._by_symbol = {i.symbol: i for i in self.instruments}
        self._by_universe = {u.name: u for u in self.universes}
        self._by_macro_id = {m.id: m for m in self.macro}

    @model_validator(mode="after")
    def _check(self) -> Self:
        symbols = [i.symbol for i in self.instruments]
        duplicates = sorted({s for s in symbols if symbols.count(s) > 1})
        if duplicates:
            raise ValueError(
                f"duplicate symbol(s) across the registry: {', '.join(duplicates)} — "
                f"a symbol may be defined in exactly one file"
            )

        known = set(symbols)
        for universe in self.universes:
            missing = sorted({m.symbol for m in universe.members} - known)
            if missing:
                raise ValueError(
                    f"universe {universe.name!r} references unknown instrument(s): "
                    f"{', '.join(missing)}"
                )
            if universe.benchmark not in known:
                raise ValueError(
                    f"universe {universe.name!r} has unknown benchmark {universe.benchmark!r}"
                )

        # A UCITS mapping the system does not ingest is a mapping nobody can
        # act on: the digest would say "buy CSPX.UK instead" for a line with no
        # price series, no cost floor and no way to check it tracks.
        for instrument in self.instruments:
            equivalent = instrument.ucits_equivalent
            if equivalent is None:
                continue
            target = self._by_symbol.get(equivalent) if self._by_symbol else None
            target = target or next((i for i in self.instruments if i.symbol == equivalent), None)
            if target is None:
                raise ValueError(
                    f"{instrument.symbol}: ucits_equivalent {equivalent!r} is not a registered "
                    f"instrument — register the UCITS line or leave the mapping null"
                )
            if not target.ucits:
                raise ValueError(
                    f"{instrument.symbol}: ucits_equivalent {equivalent!r} is not marked "
                    f"`ucits: true`"
                )

        macro_ids = [m.id for m in self.macro]
        macro_duplicates = sorted({m for m in macro_ids if macro_ids.count(m) > 1})
        if macro_duplicates:
            raise ValueError(f"duplicate macro series id(s): {', '.join(macro_duplicates)}")
        return self

    # ---- Queries ---------------------------------------------------------

    @property
    def symbols(self) -> tuple[str, ...]:
        """Every registered symbol, in file order."""
        return tuple(self._by_symbol)

    @property
    def universe_names(self) -> tuple[str, ...]:
        """Every registered universe name."""
        return tuple(self._by_universe)

    def instrument(self, symbol: str) -> Instrument:
        """Return one instrument, or fail naming what is available."""
        try:
            return self._by_symbol[symbol]
        except KeyError:
            raise RegistryValidationError(
                f"unknown instrument {symbol!r}; registered: {', '.join(sorted(self._by_symbol))}"
            ) from None

    def tradeable_eu(self) -> tuple[Instrument, ...]:
        """Every line an EU retail account can actually buy.

        The research universe and the live universe are reported separately
        (``PROJECT.md`` §5.7) precisely because they differ: a backtest over SPY
        is honest research and a dishonest instruction.
        """
        return tuple(i for i in self.instruments if i.tradeable_eu)

    def ucits_line(self, symbol: str) -> Instrument | None:
        """The UCITS line to buy instead of ``symbol``, if one is mapped.

        Returns the instrument rather than the ticker so the caller has its
        costs and its history, which is what "verify the mapping" needs.
        """
        instrument = self.instrument(symbol)
        if instrument.tradeable_eu:
            return instrument
        if instrument.ucits_equivalent is None:
            return None
        return self._by_symbol.get(instrument.ucits_equivalent)

    def enabled(self) -> tuple[Instrument, ...]:
        """Instruments that should still be ingested — enabled and not delisted."""
        return tuple(i for i in self.instruments if i.is_live)

    def universe(self, name: str, as_of: date | None = None) -> tuple[Instrument, ...]:
        """Resolve a universe's members, as of a date.

        Membership is resolved as of the evaluation date, never as of today
        (``PROJECT.md`` §5.3): a backtest of ``sectors`` starting in 2010 holds
        nine members, not eleven. ``as_of=None`` means "every member that has
        ever belonged" and is the honest default, because the alternative would
        be reading the clock — which this layer must not do.
        """
        try:
            universe = self._by_universe[name]
        except KeyError:
            raise RegistryValidationError(
                f"unknown universe {name!r}; registered: {', '.join(sorted(self._by_universe))}"
            ) from None
        return tuple(
            self._by_symbol[member.symbol]
            for member in universe.members
            if as_of is None or member.covers(as_of)
        )

    def sources_for(self, symbol: str) -> MappingProxyType[SourceKey, str]:
        """The vendor symbol per source for one instrument.

        Returned read-only so a caller cannot mutate the registry by holding
        onto the result.
        """
        return MappingProxyType(dict(self.instrument(symbol).sources))

    def macro_series(self, series_id: str) -> MacroSeries:
        """Return one macro series by its registry id."""
        try:
            return self._by_macro_id[series_id]
        except KeyError:
            raise RegistryValidationError(
                f"unknown macro series {series_id!r}; registered: "
                f"{', '.join(sorted(self._by_macro_id))}"
            ) from None
