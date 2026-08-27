"""``finflow backfill`` — fetch raw data for the registered universe.

Holds an exclusive lock for the duration. A manual backfill started while the
scheduled run is in flight is the realistic collision (``PROJECT.md`` §11.6),
because a backfill is what you start *when something looks wrong*.
"""

from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

from finflow.application.ingest_universe import IngestUniverse
from finflow.config import Settings, get_settings
from finflow.entrypoints.cli.locking import ExclusiveLock, LockHeldError
from finflow.entrypoints.cli.wiring import (
    build_clock,
    build_object_store,
    build_ops_store,
    build_sources,
)
from finflow.logging import configure_logging, get_logger
from finflow.registry import RegistryError, load_registry

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finflow-backfill", description=__doc__)
    parser.add_argument(
        "--symbols", nargs="*", help="Limit to these symbols. Default: the whole enabled universe."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Ignore watermarks and re-fetch from backfill_start. Safe: the raw zone is append-only.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use the synthetic source for every vendor. No network.",
    )
    return parser


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    """Run one ingestion pass. Returns a shell exit code."""
    args = build_parser().parse_args(argv)
    settings = settings or get_settings()
    configure_logging(settings)

    try:
        registry = load_registry(settings.registry_dir)
    except RegistryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    lock_path = Path(settings.data_dir) / "pipeline.lock"
    try:
        with ExclusiveLock(lock_path):
            outcome = IngestUniverse(
                registry=registry,
                sources=build_sources(settings, offline=args.offline),
                object_store=build_object_store(settings),
                ops_store=build_ops_store(settings),
                clock=build_clock(),
                deferral=timedelta(hours=settings.rate_limit_deferral_hours),
            ).run(symbols=args.symbols, full=args.full)
    except LockHeldError as exc:
        # Exiting cleanly rather than waiting: a scheduled run is already doing
        # this work, and a queued duplicate helps nobody.
        print(f"another run holds the lock: {exc}", file=sys.stderr)
        return 3

    print(f"run {outcome.run_id}: {outcome.summary()}")
    print(f"snapshot {outcome.snapshot_id}")
    for pair, reason in sorted(outcome.failed.items()):
        print(f"  FAILED  {pair}: {reason}")
    for source in sorted(outcome.deferred_sources):
        print(f"  DEFERRED {source} — remaining symbols resume next run")

    return 1 if outcome.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
