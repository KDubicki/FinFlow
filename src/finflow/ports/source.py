"""The source seam.

"Adding a source is one interface" is the project's most-repeated claim, so the
interface is stated rather than implied. A client fetches and raises; it does
not retry, does not know where data is stored, and does not stamp provenance.
Everything else is the ingestion service's job.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, runtime_checkable

import polars as pl

from finflow.contracts.sources import SourceKey


@dataclass(frozen=True)
class SourceCapabilities:
    """What a source can do, so callers do not have to special-case vendors."""

    key: SourceKey
    supports_ohlcv: bool
    supports_macro: bool
    requires_auth: bool
    vintage_aware: bool = False
    """True when the source can return a series as of a past publication date."""

    max_requests_per_day: int | None = None
    """Vendor quota, where one is documented. None means undocumented, not
    unlimited — Stooq's per-IP cap is real and simply unpublished."""

    def supports(self, *, macro: bool) -> bool:
        """True when this source can serve the requested grain."""
        return self.supports_macro if macro else self.supports_ohlcv


@runtime_checkable
class SourceClient(Protocol):
    """Fetches raw data for one symbol over one date range.

    Contract, asserted for every implementation by the conformance suite in
    ``tests/test_source_conformance.py``:

    - ``fetch`` returns a frame matching ``OhlcvBar`` or ``MacroObservation``,
      or raises from the taxonomy in ``contracts.errors`` — never anything else.
    - An empty range returns an empty frame with the right columns. It is not
      an error: a fund that had not listed yet simply has no bars.
    - ``fetch`` never retries internally and never sleeps beyond its own rate
      limiter. Back-off is the ingestion service's decision (``PROJECT.md`` §6.7).
    - ``fetch`` is free of side effects: it writes nothing and mutates nothing.
    """

    def capabilities(self) -> SourceCapabilities:
        """Describe what this source supports."""
        ...

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Return observations for ``symbol`` between ``start`` and ``end``, inclusive.

        ``symbol`` is the *vendor's* identifier, resolved from the registry by
        the caller, not the platform symbol.
        """
        ...
