#!/usr/bin/env python
"""Measure what this system actually holds, and how long a rebuild takes.

``PROJECT.md`` §2 opens with a scale table, and the argument that follows it —
that this is a single-machine problem and every technology choice should be made
on that basis — is only as good as the numbers in it. Estimates are how a
project talks itself into Spark. So the table is measured, by this script,
and the output says how it was produced.

    uv run python scripts/measure_scale.py            # against ./data
    uv run python scripts/measure_scale.py --data-dir ./ci-data

Reports rows per table, on-disk size per zone, and the wall time of a **full
rebuild from the raw zone** — the number that decides whether "the warehouse is
disposable" is a design property or a slogan.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from finflow.adapters.storage import LocalObjectStore
from finflow.adapters.warehouse import DuckDBWarehouse
from finflow.application.build_warehouse import BuildWarehouse
from finflow.registry import load_registry

REPO_ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="measure-scale", description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT / "data")
    parser.add_argument(
        "--skip-rebuild",
        action="store_true",
        help="Report sizes only. The rebuild takes the warehouse's write lock.",
    )
    return parser


def directory_bytes(path: Path) -> int:
    """Total size on disk, following the layout rather than the filesystem."""
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def human(size: int) -> str:
    """Bytes as something a table can carry."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size / 1:.1f} {unit}"
        size //= 1024
    return f"{size} GB"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    data_dir: Path = args.data_dir
    registry = load_registry(REPO_ROOT / "instruments")

    warehouse_path = data_dir / "warehouse.duckdb"
    if not warehouse_path.exists():
        print(f"no warehouse at {warehouse_path} — run `make daily` or `make build` first")
        return 1

    print(
        f"Registry      {len(registry.instruments)} instruments, "
        f"{len(registry.universes)} universes, {len(registry.macro)} macro series"
    )
    print()
    print("On disk")
    for label, path in (
        ("raw zone", data_dir / "raw"),
        ("manifests", data_dir / "manifests"),
        ("warehouse", warehouse_path),
        ("serving snapshot", data_dir / "serving.duckdb"),
        ("ops store", data_dir / "ops.sqlite"),
    ):
        print(f"  {label:<18} {human(directory_bytes(path)):>10}")

    partitions = len(list((data_dir / "raw").rglob("*.parquet")))
    print(f"  {'raw partitions':<18} {partitions:>10}")

    print()
    print("Rows")
    with DuckDBWarehouse(warehouse_path, read_only=True) as warehouse:
        for table in warehouse.tables():
            if table.startswith("_"):
                continue
            count = warehouse.query(f"SELECT count(*) AS n FROM {table}")["n"][0]
            print(f"  {table:<28} {count:>12,}")

    if args.skip_rebuild:
        return 0

    print()
    started = time.perf_counter()
    with DuckDBWarehouse(warehouse_path) as warehouse:
        outcome = BuildWarehouse(
            object_store=LocalObjectStore(data_dir),
            warehouse=warehouse,
            registry=registry,
        ).run(snapshot_id="scale-measurement")
    elapsed = time.perf_counter() - started
    print(f"Full bronze rebuild from the raw zone: {elapsed:.1f}s ({outcome.rows:,} rows)")
    print("  (dbt's transforms run on top of this; `make build` times the whole path)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
