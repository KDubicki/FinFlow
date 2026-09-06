"""``finflow daily`` — the run that happens without you.

    commands -> ingest -> build -> dbt -> evaluate -> deliver -> digest

This is the composition root for the whole pipeline: the **only** place that
constructs a vendor client, opens a database or reads a credential
(``PROJECT.md`` §4.1). Everything below it takes ports.

The shape of the thing is a sequence of steps, each of which records its own
status, and none of which is allowed to take the run down without the digest
still going out. That inversion is the milestone: a run that fails silently is
worse than one that fails loudly, and a run that fails loudly but sends no
message is indistinguishable from a machine that is switched off.

Order matters in two places:

- **Commands are drained first**, so a ``/mute`` sent last night applies to
  today's decision rather than tomorrow's.
- **The digest is sent last**, after delivery, so it can report what was
  actually sent rather than what was queued.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import uuid
from argparse import Namespace
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from pathlib import Path

from finflow.adapters.warehouse import DuckDBWarehouse, WarehouseLockedError
from finflow.application.apply_commands import ApplyCommands
from finflow.application.daily_digest import BuildDigest, RunFacts
from finflow.application.deliver_alerts import DeliverAlerts
from finflow.application.evaluate_strategies import (
    EvaluateStrategies,
    EvaluationOutcome,
    FeaturesUnavailable,
)
from finflow.application.ingest_universe import IngestUniverse
from finflow.config import Settings, get_settings
from finflow.domain.messages import FAILED, OK, SKIPPED, Digest, Step, render_digest
from finflow.domain.strategy import DEFAULT_STRATEGIES
from finflow.entrypoints.cli.build import DbtFailed, load_and_build
from finflow.entrypoints.cli.locking import ExclusiveLock, LockHeldError
from finflow.entrypoints.cli.wiring import (
    build_clock,
    build_heartbeat,
    build_inbox,
    build_notifier,
    build_object_store,
    build_ops_store,
    build_sources,
)
from finflow.logging import configure_logging, get_logger
from finflow.ports.clock import Clock
from finflow.ports.notifier import Notifier, NotifierError
from finflow.ports.ops_store import OpsStore, PipelineRun
from finflow.registry import RegistryError, load_registry
from finflow.registry.models import Registry

log = get_logger(__name__)


@dataclass
class RunState:
    """What the run has learned so far.

    Accumulated rather than returned, because every step contributes to the same
    digest and any of them may be the one that fails.
    """

    run_id: str
    steps: list[Step] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    bars: int = 0
    partitions: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    snapshot_id: str | None = None
    evaluation: EvaluationOutcome | None = None
    delivered: int = 0

    def record(self, name: str, status: str = OK, detail: str = "") -> None:
        """Log one step's fate, once."""
        self.steps.append(Step(name=name, status=status, detail=detail))
        if status == FAILED:
            self.failures.append(f"{name}: {detail}")
        log.info("pipeline_step", run_id=self.run_id, step=name, status=status, detail=detail)

    @property
    def ok(self) -> bool:
        """True when nothing has failed."""
        return not self.failures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finflow-daily", description=__doc__)
    parser.add_argument(
        "--offline", action="store_true", help="Use the synthetic source. No network."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages instead of sending them, and do not drain the command inbox.",
    )
    parser.add_argument("--skip-ingest", action="store_true", help="Evaluate on existing raw data.")
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        help="Evaluate as of this date instead of today. For a backfilled decision.",
    )
    return parser


def main(
    argv: list[str] | None = None,
    settings: Settings | None = None,
    clock: Clock | None = None,
) -> int:
    """Run the whole pipeline once. Returns a shell exit code.

    ``settings`` and ``clock`` are injectable for the same reason: the run is
    only reproducible if "now" is an input. A test freezes the clock and gets
    the same decision id every time, which is what makes the whole pipeline
    assertable rather than merely observable.
    """
    args = build_parser().parse_args(argv)
    settings = settings or get_settings()
    configure_logging(settings)

    clock = clock or build_clock()
    state = RunState(run_id=uuid.uuid4().hex[:12])
    heartbeat = build_heartbeat(settings, dry_run=args.dry_run)
    heartbeat.start(state.run_id)

    try:
        registry = load_registry(settings.registry_dir)
    except RegistryError as exc:
        # Nothing downstream can run without a registry, and there is no store
        # to record the attempt in yet, so this is the one early return.
        print(f"error: {exc}", file=sys.stderr)
        heartbeat.failure(state.run_id, str(exc))
        return 2

    ops = build_ops_store(settings)
    run = PipelineRun(run_id=state.run_id, started_at=clock.now())
    ops.save_run(run)

    notifier = build_notifier(settings, dry_run=args.dry_run)

    try:
        with ExclusiveLock(settings.data_dir / "pipeline.lock"):
            _commands(args, settings, state, ops, registry, notifier, clock)
            if not args.skip_ingest:
                _ingest(args, settings, state, ops, registry, clock)
            else:
                state.record("ingest", SKIPPED, "--skip-ingest")
            _build(settings, state, registry)
            _evaluate(settings, state, ops, registry, clock, as_of=args.as_of)
            _deliver(settings, state, ops, notifier, clock, registry)
    except LockHeldError as exc:
        # A scheduled run is already doing this work. Exiting cleanly rather
        # than queueing: a duplicate helps nobody, and the heartbeat is not
        # pinged either way, because this process did not do the run.
        print(f"another run holds the lock: {exc}", file=sys.stderr)
        return 3

    digest_text = _digest(settings, state, ops, registry, clock)
    _send_digest(state, notifier, digest_text)

    ops.save_run(
        PipelineRun(
            run_id=state.run_id,
            started_at=run.started_at,
            ended_at=clock.now(),
            status="succeeded" if state.ok else "failed",
            rows_written=state.bars,
            snapshot_id=state.snapshot_id,
            error="; ".join(state.failures) or None,
        )
    )

    # Exactly one of these, always. There is no partial success in a dead-man's
    # switch: a run that both pinged and failed teaches the watchdog to lie.
    if state.ok:
        heartbeat.success(state.run_id, "; ".join(step.describe() for step in state.steps))
    else:
        heartbeat.failure(state.run_id, "; ".join(state.failures))

    print(digest_text)
    return 0 if state.ok else 1


# ---- the steps -----------------------------------------------------------


def _commands(
    args: Namespace,
    settings: Settings,
    state: RunState,
    ops: OpsStore,
    registry: Registry,
    notifier: Notifier,
    clock: Clock,
) -> None:
    """Apply the controls waiting for this run."""
    try:
        outcome = ApplyCommands(
            ops_store=ops,
            inbox=build_inbox(settings, dry_run=args.dry_run),
            notifier=notifier,
            clock=clock,
            registry=registry,
            known_strategies=tuple(strategy.id for strategy in DEFAULT_STRATEGIES),
        ).run()
    except Exception as exc:
        # A command intake failure must not stop the pipeline: the data work is
        # the point, and an unapplied /mute is visible in the digest.
        state.record("commands", FAILED, str(exc))
        return
    state.record("commands", OK, outcome.summary())
    state.notes.extend(f"Applied: {line}" for line in outcome.applied)
    state.notes.extend(f"Rejected: {line}" for line in outcome.rejected)


def _ingest(
    args: Namespace,
    settings: Settings,
    state: RunState,
    ops: OpsStore,
    registry: Registry,
    clock: Clock,
) -> None:
    """Fetch today's bars."""
    try:
        outcome = IngestUniverse(
            registry=registry,
            sources=build_sources(settings, offline=args.offline),
            object_store=build_object_store(settings),
            ops_store=ops,
            clock=clock,
            deferral=timedelta(hours=settings.rate_limit_deferral_hours),
            request_budget=settings.source_daily_request_budget,
        ).run()
    except Exception as exc:
        state.record("ingest", FAILED, str(exc))
        return

    state.bars = outcome.rows
    state.partitions = len(outcome.written)
    # A partial failure is not a failed step: the failure domain is the
    # instrument (PROJECT.md §4.4), so thirty-nine good symbols still build.
    status = FAILED if outcome.failed and not outcome.written else OK
    state.record("ingest", status, outcome.summary())
    state.notes.extend(f"! {pair}: {reason}" for pair, reason in sorted(outcome.failed.items()))


def _build(settings: Settings, state: RunState, registry: Registry) -> None:
    """Load bronze and run the transforms."""
    try:
        result = load_and_build(settings, registry, snapshot_id=state.run_id)
    except DbtFailed as exc:
        state.checks_passed, state.checks_failed = exc.passed, exc.failed
        state.record("build", FAILED, f"dbt build failed ({exc.failed} checks failed)")
        return
    except (WarehouseLockedError, RuntimeError, OSError) as exc:
        state.record("build", FAILED, str(exc))
        return

    state.snapshot_id = state.run_id
    state.checks_passed, state.checks_failed = result.checks_passed, result.checks_failed
    state.record("build", OK, result.outcome.summary())


def _evaluate(
    settings: Settings,
    state: RunState,
    ops: OpsStore,
    registry: Registry,
    clock: Clock,
    *,
    as_of: date | None,
) -> None:
    """Decide what should be held.

    Skipped outright when the build failed. A decision from a warehouse that
    did not build is precisely the instruction-from-bad-data that the trust
    ladder drops a rung for (``PROJECT.md`` §15), so it is not made at all.
    """
    if any(step.name == "build" and step.status == FAILED for step in state.steps):
        state.record("evaluate", SKIPPED, "the build failed — no instruction is issued")
        return

    warehouse_path = settings.data_dir / "warehouse.duckdb"
    try:
        with DuckDBWarehouse(warehouse_path, read_only=True) as warehouse:
            state.evaluation = EvaluateStrategies(
                warehouse=warehouse,
                ops_store=ops,
                registry=registry,
                clock=clock,
                strategies=DEFAULT_STRATEGIES,
            ).run(run_id=state.run_id, snapshot_id=state.snapshot_id, as_of=as_of)
    except (FeaturesUnavailable, WarehouseLockedError, FileNotFoundError) as exc:
        state.record("evaluate", FAILED, str(exc))
        return
    state.record("evaluate", OK, state.evaluation.summary())


def _venue(registry: Registry) -> str:
    """The calendar most of the registry trades on.

    Used to say how stale a decision is in trading sessions rather than days.
    Right while the universe is US-listed, and M5 — the milestone that widens
    it — owns making freshness per instrument.
    """
    calendars = Counter(i.calendar for i in registry.instruments)
    return calendars.most_common(1)[0][0] if calendars else "XNYS"


def _deliver(
    settings: Settings,
    state: RunState,
    ops: OpsStore,
    notifier: Notifier,
    clock: Clock,
    registry: Registry,
) -> None:
    """Drain the outbox.

    Runs even when evaluation failed: a decision queued yesterday and never
    delivered is exactly what the outbox exists to rescue.
    """
    outcome = DeliverAlerts(
        ops_store=ops,
        notifier=notifier,
        clock=clock,
        lease=timedelta(minutes=settings.alert_lease_minutes),
        calendar=_venue(registry),
    ).run()
    state.delivered = outcome.sent
    state.record("deliver", FAILED if outcome.failed else OK, outcome.summary())
    state.notes.extend(f"! undelivered: {error}" for error in outcome.errors)
    state.notes.extend(f"! abandoned: {item}" for item in outcome.abandoned)


def _digest(
    settings: Settings, state: RunState, ops: OpsStore, registry: Registry, clock: Clock
) -> str:
    """Assemble the message, whatever happened above."""
    facts = RunFacts(
        run_id=state.run_id,
        steps=tuple(state.steps),
        bars_ingested=state.bars,
        partitions_written=state.partitions,
        checks_passed=state.checks_passed,
        checks_failed=state.checks_failed,
        failures=tuple(state.failures),
        snapshot_id=state.snapshot_id,
        notes=tuple(state.notes),
    )
    warehouse_path = settings.data_dir / "warehouse.duckdb"
    warehouse = None
    try:
        if warehouse_path.exists():
            warehouse = DuckDBWarehouse(warehouse_path, read_only=True)
        digest = BuildDigest(
            ops_store=ops,
            registry=registry,
            clock=clock,
            warehouse=warehouse,
            band_pp=settings.rebalance_band_pp,
        ).run(facts, evaluation=state.evaluation, delivered=state.delivered)
    finally:
        if warehouse is not None:
            warehouse.close()

    return render_digest(_with_disk(digest, settings.data_dir))


def _with_disk(digest: Digest, path: Path) -> Digest:
    """Attach disk usage, the likeliest way a small host dies.

    Entirely preventable and therefore embarrassing to be caught by, so the
    number is on every digest rather than on a dashboard nobody opens
    (``PROJECT.md`` §11.6).
    """
    try:
        usage = shutil.disk_usage(path if path.exists() else path.parent)
    except OSError:
        return digest
    return replace(digest, disk_used_pct=100.0 * usage.used / usage.total)


def _send_digest(state: RunState, notifier: Notifier, text: str) -> None:
    """Send the digest, and treat a failure to send as a run failure.

    This is the one message whose absence means something, so if it cannot be
    sent the run has not really succeeded — and the heartbeat must not be
    pinged as though it had.
    """
    try:
        notifier.send(text)
        state.record("digest", OK)
    except NotifierError as exc:
        state.record("digest", FAILED, str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
