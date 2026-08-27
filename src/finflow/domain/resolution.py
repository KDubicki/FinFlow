"""Resolving many raw partitions into one opinion per key.

The raw zone keeps every fetch (ADR 0006), so bronze has to decide which of
several readings of the same ``(symbol, date)`` is current. The rule is simple —
the latest ``ingested_at`` wins — but two things around it are not, and both
live here because they are decisions rather than plumbing:

- A **restatement** is a later reading that *disagrees* with an earlier one. It
  is recorded rather than absorbed, because silently accepting it makes a
  backtest irreproducible and hides a corporate action.
- Rows that fail the frame contract are **quarantined with a reason**, not
  dropped. A dropped row is a gap nobody can explain later.

Pure functions over frames: no IO, no clock, no warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

# `source` is part of the key on purpose. An instrument declaring two vendors
# produces two independent readings of the same bar, and they are not competing
# opinions to be collapsed here -- the primary source wins at the mart layer and
# the secondary only ever produces a reconciliation flag (``PROJECT.md`` §6.6).
# Without `source` in the key, the two vendors overwrite each other and every
# row looks restated.
OHLCV_KEY = ("symbol", "date", "source")
MACRO_KEY = ("series_id", "observation_date", "vintage_date", "source")

# Columns whose disagreement between two readings counts as a restatement.
OHLCV_VALUES = ("open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class Resolution:
    """The outcome of resolving raw partitions for one grain."""

    resolved: pl.DataFrame
    """One row per key, carrying the latest opinion."""

    quarantined: pl.DataFrame
    """Rows that failed the contract, with a reason and their provenance."""

    restatements: pl.DataFrame
    """Keys whose value changed between two readings, with both run ids."""

    @property
    def counts(self) -> dict[str, int]:
        """Row counts, for the digest and for ``pipeline_runs``."""
        return {
            "resolved": len(self.resolved),
            "quarantined": len(self.quarantined),
            "restatements": len(self.restatements),
        }


def resolve_ohlcv(raw: pl.DataFrame) -> Resolution:
    """Resolve raw OHLCV partitions into bronze.

    ``raw`` is the concatenation of every partition for the instruments being
    loaded, so it may contain the same ``(symbol, date, source)`` several times
    with different ``ingested_at``. The latest reading from each source wins;
    choosing *between* sources is a mart-layer decision.
    """
    if raw.is_empty():
        return Resolution(raw.clone(), _empty_quarantine(raw), _empty_restatements())

    clean, quarantined = _quarantine_invalid(raw)
    if clean.is_empty():
        return Resolution(clean, quarantined, _empty_restatements())

    ordered = clean.sort("ingested_at")
    resolved = ordered.group_by(list(OHLCV_KEY)).last()
    restatements = _find_restatements(ordered, list(OHLCV_KEY), list(OHLCV_VALUES))
    return Resolution(resolved.sort(list(OHLCV_KEY)), quarantined, restatements)


def resolve_macro(raw: pl.DataFrame) -> Resolution:
    """Resolve raw macro partitions into bronze.

    The key includes ``vintage_date``, so two vintages of the same observation
    are two rows rather than a restatement — that is the whole point of an
    ALFRED read (``PROJECT.md`` §6.3).
    """
    if raw.is_empty():
        return Resolution(raw.clone(), _empty_quarantine(raw), _empty_restatements())

    ordered = raw.sort("ingested_at")
    resolved = ordered.group_by(list(MACRO_KEY)).last()
    restatements = _find_restatements(ordered, list(MACRO_KEY), ["value"])
    return Resolution(
        resolved.sort(["series_id", "observation_date"]),
        _empty_quarantine(raw),
        restatements,
    )


def _quarantine_invalid(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split rows that break the OHLCV relationships from those that do not.

    Per row rather than per frame: one bad bar in a thirty-year history should
    cost that bar, not the instrument.
    """
    bad = (
        (pl.col("high") < pl.col("low"))
        | (pl.col("close") > pl.col("high"))
        | (pl.col("close") < pl.col("low"))
        | (pl.col("open") > pl.col("high"))
        | (pl.col("open") < pl.col("low"))
        | (pl.col("close") <= 0)
        | (pl.col("volume") < 0)
    )
    reason = (
        pl.when(pl.col("high") < pl.col("low"))
        .then(pl.lit("high < low"))
        .when((pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low")))
        .then(pl.lit("close outside [low, high]"))
        .when((pl.col("open") > pl.col("high")) | (pl.col("open") < pl.col("low")))
        .then(pl.lit("open outside [low, high]"))
        .when(pl.col("close") <= 0)
        .then(pl.lit("close <= 0"))
        .otherwise(pl.lit("volume < 0"))
        .alias("rejection_reason")
    )
    quarantined = raw.filter(bad).with_columns(reason)
    return raw.filter(~bad), quarantined


def _find_restatements(ordered: pl.DataFrame, key: list[str], values: list[str]) -> pl.DataFrame:
    """Find keys whose values changed between consecutive readings.

    A key read twice with the same numbers is not a restatement — that is the
    ordinary case, since every incremental run re-fetches the last loaded day
    on purpose. Only a *disagreement* is reported.
    """
    if len(ordered) == len(ordered.select(key).unique()):
        return _empty_restatements()

    previous = [pl.col(v).shift(1).over(key).alias(f"_prev_{v}") for v in values]
    changed = pl.any_horizontal([(pl.col(v) != pl.col(f"_prev_{v}")) for v in values])

    flagged = ordered.with_columns(
        *previous,
        pl.col("ingestion_run_id").shift(1).over(key).alias("_prev_run"),
    ).filter(pl.col("_prev_run").is_not_null() & changed)
    if flagged.is_empty():
        return _empty_restatements()

    return flagged.select(
        pl.concat_str([pl.col(k).cast(pl.String) for k in key], separator="|").alias("entity_key"),
        pl.col("_prev_run").alias("old_run_id"),
        pl.col("ingestion_run_id").alias("new_run_id"),
        pl.concat_str(
            [pl.format("{}={}->{}", pl.lit(v), pl.col(f"_prev_{v}"), pl.col(v)) for v in values],
            separator=", ",
        ).alias("changes"),
    )


def _empty_quarantine(like: pl.DataFrame) -> pl.DataFrame:
    """An empty quarantine shaped like the input.

    Guarded against a frame with no columns at all: `with_columns` on a
    zero-column frame produces a *one-row* frame, which would report a
    quarantined row that does not exist.
    """
    if not like.columns:
        return pl.DataFrame(schema={"rejection_reason": pl.String})
    return like.clear().with_columns(pl.lit(None, dtype=pl.String).alias("rejection_reason"))


def _empty_restatements() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "entity_key": pl.String,
            "old_run_id": pl.String,
            "new_run_id": pl.String,
            "changes": pl.String,
        }
    )
