"""Projecting the registry into frames the warehouse can join.

The registry is the *only* writer of ``dim_instrument`` (``PROJECT.md`` §9.2):
nothing downstream may insert an instrument it discovered in a feed. Exporting
it as tables is what lets dbt build the dimensions while keeping that rule.
"""

from __future__ import annotations

import polars as pl

from finflow.registry.models import Registry


def to_frames(registry: Registry) -> dict[str, pl.DataFrame]:
    """Every registry table, keyed by the warehouse table name."""
    return {
        "registry_instruments": _instruments(registry),
        "registry_universe_members": _members(registry),
        "registry_macro_series": _macro(registry),
    }


def _instruments(registry: Registry) -> pl.DataFrame:
    # `valid_from` is the git commit date of the registry change, not the run
    # date: a backfill in November must not stamp an August change with
    # November (PROJECT.md §9.2).
    valid_from = registry.commit.committed_at
    return pl.DataFrame(
        [
            {
                "symbol": i.symbol,
                "name": i.name,
                "asset_class": str(i.asset_class),
                "sub_class": i.sub_class,
                "exchange": i.exchange,
                "currency": i.currency,
                "calendar": i.calendar,
                "inception": i.inception,
                "backfill_start": i.backfill_start,
                "delisted": i.delisted,
                "return_basis": str(i.return_basis),
                "commission_bps": i.costs.commission_bps,
                "spread_bps": i.costs.spread_bps,
                "min_adv_usd": i.min_adv_usd,
                "ucits_equivalent": i.ucits_equivalent,
                "tradeable_eu": i.ucits_equivalent is not None,
                "enabled": i.enabled,
                "primary_source": next(iter(i.sources), None),
                "registry_commit": registry.commit.sha,
                "valid_from": valid_from,
            }
            for i in registry.instruments
        ],
        # Explicit types for every nullable column. A column that happens to be
        # all-null infers as Null and lands in DuckDB as INTEGER, which the mart
        # contract then rejects -- correctly, but the fix belongs here.
        schema_overrides={
            "valid_from": pl.Datetime(time_zone="UTC"),
            "inception": pl.Date,
            "backfill_start": pl.Date,
            "delisted": pl.Date,
            "sub_class": pl.String,
            "min_adv_usd": pl.Float64,
            "ucits_equivalent": pl.String,
            "primary_source": pl.String,
            "registry_commit": pl.String,
        },
    )


def _members(registry: Registry) -> pl.DataFrame:
    rows = [
        {
            "universe": u.name,
            "description": u.description,
            "benchmark_symbol": u.benchmark,
            "symbol": m.symbol,
            "valid_from": m.valid_from,
            "valid_to": m.valid_to,
        }
        for u in registry.universes
        for m in u.members
    ]
    return pl.DataFrame(
        rows,
        schema_overrides={
            "valid_from": pl.Date,
            "valid_to": pl.Date,
            "description": pl.String,
        },
    )


def _macro(registry: Registry) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "series_id": m.id,
                "source_id": m.source_id,
                "source": str(m.source),
                "unit": m.unit,
                "frequency": str(m.frequency),
                "release_lag_days": m.release_lag_days,
                "revised": m.revised,
                "vintage_aware": m.vintage_aware,
            }
            for m in registry.macro
        ]
    )
