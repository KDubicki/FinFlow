"""Frame contracts.

Pydantic is the right tool for a single record and the wrong one for 300k rows.
These are Patito schemas: one definition, validated over a whole Polars frame in
a single vectorized pass — dtypes, nullability, ranges and uniqueness
(``PROJECT.md`` §9.1).

Two grains, each in two forms. A client returns the *bare* frame; the ingestion
service stamps provenance onto it and writes the *raw* form. Keeping the
stamping out of the clients is what lets a new source be written without knowing
how the landing zone is laid out.
"""

from __future__ import annotations

import datetime as dt

import patito as pt
import polars as pl


class OhlcvBar(pt.Model):
    """A daily bar, as a ``SourceClient`` returns it.

    The bounds are contract checks rather than opinions about markets: a
    negative price or a high below its low is a parse error wearing a number's
    clothing, and admitting one is how an error page becomes a bar.
    """

    symbol: str = pt.Field(min_length=1)
    date: dt.date
    open: float = pt.Field(gt=0)
    high: float = pt.Field(gt=0)
    low: float = pt.Field(gt=0)
    close: float = pt.Field(gt=0)
    volume: float = pt.Field(ge=0)


class MacroObservation(pt.Model):
    """One macro reading, as a ``SourceClient`` returns it.

    ``vintage_date`` is null for series that are never revised. For a
    ``vintage_aware`` series it is the ALFRED realtime date, which makes the
    grain ``(series_id, observation_date, vintage_date)`` and is what allows a
    point-in-time read (``PROJECT.md`` §6.3).
    """

    series_id: str = pt.Field(min_length=1)
    observation_date: dt.date
    value: float
    vintage_date: dt.date | None = None


# Columns the ingestion service stamps onto every frame before it lands.
PROVENANCE = ("source", "ingested_at", "ingestion_run_id")


class RawOhlcv(OhlcvBar):
    """A bar as it lands in the raw zone, with provenance attached."""

    source: str = pt.Field(min_length=1)
    ingested_at: dt.datetime
    ingestion_run_id: str = pt.Field(min_length=1)


class RawMacro(MacroObservation):
    """A macro reading as it lands in the raw zone, with provenance attached."""

    source: str = pt.Field(min_length=1)
    ingested_at: dt.datetime
    ingestion_run_id: str = pt.Field(min_length=1)


def ohlcv_consistency_errors(frame: pl.DataFrame) -> list[str]:
    """Cross-column checks Patito's per-column rules cannot express.

    Separate from the schema on purpose: these are relationships between
    columns, and stating them as their own function keeps the failure message
    specific enough to act on.
    """
    if frame.is_empty():
        return []

    problems: list[str] = []
    checks = {
        "high < low": pl.col("high") < pl.col("low"),
        "close outside [low, high]": (pl.col("close") > pl.col("high"))
        | (pl.col("close") < pl.col("low")),
        "open outside [low, high]": (pl.col("open") > pl.col("high"))
        | (pl.col("open") < pl.col("low")),
    }
    for label, predicate in checks.items():
        offending = frame.filter(predicate)
        if not offending.is_empty():
            first = offending.row(0, named=True)
            problems.append(f"{len(offending)} row(s) with {label}, first on {first['date']}")

    duplicated = frame.filter(frame.select("date").is_duplicated())
    if not duplicated.is_empty():
        dates = sorted({str(d) for d in duplicated["date"].to_list()})[:3]
        problems.append(f"{len(duplicated)} duplicated date(s), e.g. {', '.join(dates)}")
    return problems
