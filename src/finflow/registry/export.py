"""Projecting the registry into frames the warehouse can join.

The registry is the *only* writer of ``dim_instrument`` (``PROJECT.md`` §9.2):
nothing downstream may insert an instrument it discovered in a feed. Exporting
it as tables is what lets dbt build the dimensions while keeping that rule.
"""

from __future__ import annotations

import polars as pl

from finflow.registry.models import Registry

INSTRUMENT_SCHEMA: dict[str, pl.DataType] = {
    "symbol": pl.String(),
    "name": pl.String(),
    "asset_class": pl.String(),
    "sub_class": pl.String(),
    "exchange": pl.String(),
    "currency": pl.String(),
    "calendar": pl.String(),
    "inception": pl.Date(),
    "backfill_start": pl.Date(),
    "delisted": pl.Date(),
    "return_basis": pl.String(),
    "commission_bps": pl.Float64(),
    "spread_bps": pl.Float64(),
    "min_adv_usd": pl.Float64(),
    "ucits_equivalent": pl.String(),
    "tradeable_eu": pl.Boolean(),
    "enabled": pl.Boolean(),
    "primary_source": pl.String(),
    "registry_commit": pl.String(),
    "valid_from": pl.Datetime(time_zone="UTC"),
}

MEMBER_SCHEMA: dict[str, pl.DataType] = {
    "universe": pl.String(),
    "description": pl.String(),
    "benchmark_symbol": pl.String(),
    "symbol": pl.String(),
    "valid_from": pl.Date(),
    "valid_to": pl.Date(),
}

MACRO_SCHEMA: dict[str, pl.DataType] = {
    "series_id": pl.String(),
    "source_id": pl.String(),
    "source": pl.String(),
    "unit": pl.String(),
    "frequency": pl.String(),
    "release_lag_days": pl.Int64(),
    "revised": pl.Boolean(),
    "vintage_aware": pl.Boolean(),
}
"""Schemas stated in full rather than inferred.

An empty registry section is normal — a slice with no macro series, a fresh
install with no universes — and an inferred frame with no rows has no *columns*
either, which lands in the warehouse as a placeholder table and fails every
model downstream with an error naming the wrong thing. Stating the schema makes
the empty case produce a well-formed empty table, which is what the models
expect.
"""


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
        schema=INSTRUMENT_SCHEMA,
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
    return pl.DataFrame(rows, schema=MEMBER_SCHEMA)


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
        ],
        schema=MACRO_SCHEMA,
    )
