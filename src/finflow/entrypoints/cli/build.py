"""``finflow build`` — load the raw zone into the warehouse and run dbt.

Ingestion is a separate command on purpose: fetching is slow and rate-limited,
building is seconds, and being able to rebuild without re-fetching is the whole
point of an append-only raw zone. The daily run calls both; a developer
debugging a model calls only this one.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from finflow.adapters.warehouse import DuckDBWarehouse, WarehouseLockedError
from finflow.application.build_warehouse import BuildOutcome, BuildWarehouse
from finflow.config import Settings, get_settings
from finflow.entrypoints.cli.locking import ExclusiveLock, LockHeldError
from finflow.entrypoints.cli.wiring import build_clock, build_object_store, build_ops_store
from finflow.logging import configure_logging, get_logger
from finflow.ports.ops_store import PipelineRun
from finflow.registry import RegistryError, load_registry
from finflow.registry.models import Registry

log = get_logger(__name__)

DBT_DIR = Path(__file__).resolve().parents[4] / "dbt"
RUN_RESULTS = DBT_DIR / "target" / "run_results.json"


class DbtFailed(RuntimeError):
    """``dbt build`` returned non-zero.

    Carries the check counts, because the daily run needs them for the digest
    whether dbt failed on a broken model or on a data test — and "seven tests
    failed" is a different message from "the model would not compile".
    """

    def __init__(self, message: str, *, passed: int = 0, failed: int = 0) -> None:
        super().__init__(message)
        self.passed = passed
        self.failed = failed


@dataclass
class BuildResult:
    """What one load-and-transform produced."""

    outcome: BuildOutcome
    checks_passed: int = 0
    checks_failed: int = 0
    failed_checks: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finflow-build", description=__doc__)
    parser.add_argument("--skip-dbt", action="store_true", help="Load bronze only.")
    parser.add_argument(
        "--no-snapshot", action="store_true", help="Do not promote a serving snapshot."
    )
    return parser


def ensure_dbt_packages() -> None:
    """Install dbt packages if they are absent.

    A rebuilt box or a fresh clone has no ``dbt_packages/`` -- it is ignored the
    way ``node_modules`` is, with ``package-lock.yml`` committed instead. The
    daily run must not fail on a missing manual step after a recovery, which is
    the whole point of the box being rebuildable from one script
    (``PROJECT.md`` §11.5).

    Only run when the directory is missing, so the ordinary daily path makes no
    network call to the package hub.
    """
    if (DBT_DIR / "dbt_packages").is_dir():
        return

    log.info("dbt_deps_bootstrapping", reason="dbt_packages/ is absent")
    result = subprocess.run(
        ["dbt", "deps", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise RuntimeError(
            "dbt deps failed. Run `make dbt-deps` once with network access, "
            f"or check dbt/package-lock.yml (exit code {result.returncode})"
        )


def run_dbt(warehouse_path: Path, snapshot_id: str, registry_commit: str | None) -> None:
    """Run ``dbt build`` against the warehouse this process just loaded.

    The snapshot id is passed as a var so every mart row records which data it
    was built from -- a mart that cannot say what it was built from cannot
    support a reproducible backtest.
    """
    ensure_dbt_packages()
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            "--vars",
            f"{{snapshot_id: {snapshot_id}, registry_commit: {registry_commit or 'unknown'}}}",
        ],
        env={
            **_clean_env(),
            "FINFLOW_DUCKDB_PATH": str(warehouse_path),
        },
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # dbt's own output is the diagnostic here; swallowing it would leave the
        # operator with an exit code and nothing to act on.
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")
    log.info("dbt_build_succeeded")


def _clean_env() -> dict[str, str]:
    import os

    return dict(os.environ)


def dbt_check_counts() -> tuple[int, int, tuple[str, ...]]:
    """Read the last dbt run's test results.

    dbt's own artifact rather than a count of our own: the tests are declared in
    ``_marts.yml`` and adding one there must show up in the digest without
    anyone remembering to increment something here.

    A missing or unreadable artifact reports zeros rather than raising. It means
    dbt did not get far enough to write one, which the step status already says.
    """
    if not RUN_RESULTS.is_file():
        return 0, 0, ()
    try:
        payload = json.loads(RUN_RESULTS.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0, 0, ()

    passed = failed = 0
    names: list[str] = []
    for result in payload.get("results", []):
        unique_id = str(result.get("unique_id", ""))
        if not unique_id.startswith("test."):
            continue
        if str(result.get("status")) == "pass":
            passed += 1
        else:
            failed += 1
            names.append(unique_id.split(".")[-2] if "." in unique_id else unique_id)
    return passed, failed, tuple(names)


def load_and_build(
    settings: Settings,
    registry: Registry,
    *,
    snapshot_id: str,
    skip_dbt: bool = False,
    promote_snapshot: bool = True,
) -> BuildResult:
    """Load bronze, run dbt, promote the serving snapshot.

    **The caller must already hold the pipeline lock.** The three phases each
    need the single DuckDB writer and dbt runs in its own process, so the
    connection is handed over between them rather than held; the outer ``flock``
    is what makes that safe. Holding one connection across the dbt call would
    deadlock the run against itself -- which is exactly what it did the first
    time.
    """
    warehouse_path = settings.data_dir / "warehouse.duckdb"
    with DuckDBWarehouse(warehouse_path) as warehouse:
        outcome = BuildWarehouse(
            object_store=build_object_store(settings),
            warehouse=warehouse,
            registry=registry,
        ).run(snapshot_id=snapshot_id)

    if skip_dbt:
        return BuildResult(outcome=outcome)

    try:
        run_dbt(warehouse_path, snapshot_id, registry.commit.sha)
    except RuntimeError as exc:
        passed, failed, _names = dbt_check_counts()
        raise DbtFailed(str(exc), passed=passed, failed=failed) from exc

    passed, failed, names = dbt_check_counts()

    if promote_snapshot:
        with DuckDBWarehouse(warehouse_path) as warehouse:
            warehouse.snapshot_to(settings.data_dir / "serving.duckdb")

    return BuildResult(
        outcome=outcome, checks_passed=passed, checks_failed=failed, failed_checks=names
    )


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    """Load bronze, run dbt, promote a serving snapshot."""
    args = build_parser().parse_args(argv)
    settings = settings or get_settings()
    configure_logging(settings)
    clock = build_clock()

    try:
        registry = load_registry(settings.registry_dir)
    except RegistryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    ops = build_ops_store(settings)
    run = PipelineRun(run_id=uuid.uuid4().hex[:12], started_at=clock.now())
    ops.save_run(run)

    try:
        with ExclusiveLock(settings.data_dir / "pipeline.lock"):
            result = load_and_build(
                settings,
                registry,
                snapshot_id=run.run_id,
                skip_dbt=args.skip_dbt,
                promote_snapshot=not args.no_snapshot,
            )
        outcome = result.outcome
    except LockHeldError as exc:
        print(f"another run holds the lock: {exc}", file=sys.stderr)
        return 3
    except (WarehouseLockedError, RuntimeError) as exc:
        ops.save_run(
            PipelineRun(
                run_id=run.run_id,
                started_at=run.started_at,
                ended_at=clock.now(),
                status="failed",
                error=str(exc),
            )
        )
        print(f"error: {exc}", file=sys.stderr)
        return 1

    ops.save_run(
        PipelineRun(
            run_id=run.run_id,
            started_at=run.started_at,
            ended_at=clock.now(),
            status="succeeded",
            rows_written=outcome.rows,
            snapshot_id=run.run_id,
        )
    )
    print(f"run {run.run_id}: {outcome.summary()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
