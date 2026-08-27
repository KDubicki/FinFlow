"""Load the raw zone into bronze, resolving to one opinion per key.

This is the step that turns an append-only pile of partitions into something
queryable. It reads every partition for the instruments being loaded, resolves
to the latest reading per key, and writes three tables rather than one:

- ``bronze_ohlcv`` / ``bronze_macro`` — the resolved rows, stamped with the
  snapshot they came from.
- ``bronze_quarantine`` — rows that broke the contract, with the reason and the
  payload's provenance. A dropped row is a gap nobody can explain later.
- ``dq_restatements`` — keys whose values a later fetch changed, with both run
  ids, because absorbing a restatement silently makes a backtest irreproducible.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field

import polars as pl

from finflow.domain.layout import RAW_PREFIX, parse_raw_key, raw_prefix
from finflow.domain.resolution import resolve_macro, resolve_ohlcv
from finflow.logging import get_logger
from finflow.ports.object_store import ObjectStore
from finflow.ports.warehouse import Warehouse

log = get_logger(__name__)

OHLCV_SOURCES = ("stooq", "twelvedata", "synthetic")
MACRO_SOURCES = ("fred",)


@dataclass
class BuildOutcome:
    """What one load did."""

    counts: dict[str, int] = field(default_factory=dict)
    partitions_read: int = 0

    @property
    def rows(self) -> int:
        """Rows landed in bronze."""
        return self.counts.get("ohlcv_resolved", 0) + self.counts.get("macro_resolved", 0)

    def summary(self) -> str:
        """One line for a log or a digest."""
        return (
            f"{self.rows} bronze rows from {self.partitions_read} partitions, "
            f"{self.counts.get('ohlcv_quarantined', 0)} quarantined, "
            f"{self.counts.get('ohlcv_restatements', 0)} restatements"
        )


class BuildWarehouse:
    """Read the raw zone and materialise the bronze layer."""

    def __init__(self, *, object_store: ObjectStore, warehouse: Warehouse) -> None:
        self._objects = object_store
        self._warehouse = warehouse

    def run(self, *, snapshot_id: str | None = None) -> BuildOutcome:
        """Rebuild bronze from every partition currently in the raw zone.

        A full rebuild rather than an incremental one: at this data size it takes
        seconds, and it is what keeps "the warehouse is disposable" true rather
        than aspirational. A rebuild that is only ever done in theory is one that
        fails the first time it is needed.
        """
        outcome = BuildOutcome()
        keys = self._objects.list(raw_prefix())
        outcome.partitions_read = len(keys)

        ohlcv_raw = self._read(keys, OHLCV_SOURCES)
        macro_raw = self._read(keys, MACRO_SOURCES)

        ohlcv = resolve_ohlcv(ohlcv_raw)
        macro = resolve_macro(macro_raw)

        self._write("bronze_ohlcv", ohlcv.resolved, snapshot_id)
        self._write("bronze_macro", macro.resolved, snapshot_id)
        self._write(
            "bronze_quarantine",
            _align_quarantine(ohlcv.quarantined),
            snapshot_id,
        )
        self._write(
            "dq_restatements",
            pl.concat([ohlcv.restatements, macro.restatements], how="vertical"),
            snapshot_id,
        )

        outcome.counts = {
            **{f"ohlcv_{k}": v for k, v in ohlcv.counts.items()},
            **{f"macro_{k}": v for k, v in macro.counts.items()},
        }
        log.info("warehouse_built", snapshot_id=snapshot_id, **outcome.counts)
        return outcome

    def _read(self, keys: tuple[str, ...], sources: tuple[str, ...]) -> pl.DataFrame:
        """Concatenate every partition belonging to the given sources."""
        frames = []
        for key in keys:
            if not key.startswith(f"{RAW_PREFIX}/"):
                continue
            partition = parse_raw_key(key)
            if partition.source not in sources:
                continue
            frames.append(pl.read_parquet(io.BytesIO(self._objects.get(key))))
        if not frames:
            return pl.DataFrame()
        # `diagonal_relaxed` so a schema that gained a column between runs still
        # concatenates, with the older partitions carrying nulls. The alternative
        # is a rebuild that fails on history it already accepted.
        return pl.concat(frames, how="diagonal_relaxed")

    def _write(self, table: str, frame: pl.DataFrame, snapshot_id: str | None) -> None:
        """Replace one bronze table with the resolved frame."""
        if frame.is_empty():
            self._warehouse.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT 1 WHERE false")
            return
        if snapshot_id is not None and "snapshot_id" not in frame.columns:
            frame = frame.with_columns(pl.lit(snapshot_id).alias("snapshot_id"))
        staging = f"_incoming_{table}"
        self._warehouse.register(staging, frame)
        self._warehouse.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {staging}")
        # Unregistered immediately: a registered frame is a view, and leaving it
        # behind puts staging names in the warehouse's table list where dbt and
        # the rebuild test would both see them.
        self._warehouse.unregister(staging)


def _align_quarantine(frame: pl.DataFrame) -> pl.DataFrame:
    """Keep the columns worth inspecting, and drop the rest.

    The whole offending row is retained rather than a summary, because the
    question asked of quarantine is always "what exactly arrived?".
    """
    return frame
