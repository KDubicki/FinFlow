"""``finflow backup`` — the nightly copy that leaves the machine.

Two things go to the second device (``PROJECT.md`` §11.3), and they are backed
up differently because they fail differently:

- **The ops store** is small, authoritative and irreplaceable, so it gets a
  consistent ``VACUUM INTO`` snapshot, gzipped, optionally age-encrypted, with
  thirty daily and twelve monthly copies retained.
- **The raw zone** is large and append-only, so it gets a hard-linked
  ``rsync --link-dest`` mirror. It is the one genuinely unrecoverable loss if
  the disk dies, and a copy on the same disk is not a backup.

Run from its own timer rather than from the daily pipeline. A backup that only
happens when the pipeline succeeds is missing on exactly the days the pipeline
broke something.
"""

from __future__ import annotations

import argparse
import sys

from finflow.adapters.ops.backup import (
    EncryptionUnavailable,
    backup,
    encrypt,
    latest_backup,
    mirror_raw_zone,
    prune,
    restore,
)
from finflow.config import Settings, get_settings
from finflow.entrypoints.cli.wiring import build_clock, build_ops_store
from finflow.logging import configure_logging, get_logger

log = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="finflow-backup", description=__doc__)
    parser.add_argument(
        "--restore",
        action="store_true",
        help="Restore the newest backup over the live ops store. Verifies before it lands.",
    )
    parser.add_argument(
        "--skip-raw", action="store_true", help="Back up the ops store only, not the raw zone."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Restore the newest backup to a scratch file and verify it. Changes nothing.",
    )
    return parser


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    """Take, verify or restore the nightly backup."""
    args = build_parser().parse_args(argv)
    settings = settings or get_settings()
    configure_logging(settings)

    if settings.backup_dir is None:
        print(
            "error: FINFLOW_BACKUP_DIR is not set. It must point at a second "
            "physical device — a copy on the same disk is not a backup.",
            file=sys.stderr,
        )
        return 2

    ops_backups = settings.backup_dir / "ops"
    store = build_ops_store(settings)

    if args.check or args.restore:
        archive = latest_backup(ops_backups)
        if archive is None:
            print(f"error: no backup found in {ops_backups}", file=sys.stderr)
            return 1
        destination = store.path if args.restore else settings.data_dir / "ops-restore-check.sqlite"
        try:
            restore(archive, destination)
        except (ValueError, OSError) as exc:
            print(f"error: {archive.name} did not verify: {exc}", file=sys.stderr)
            return 1
        if not args.restore:
            # The check is the point of the exercise, not the file it produced:
            # an untested restore is a hope with a cron schedule.
            destination.unlink(missing_ok=True)
        print(f"{'restored' if args.restore else 'verified'} {archive.name}")
        return 0

    now = build_clock().now()
    archive = backup(store.path, ops_backups, now=now)
    if settings.age_recipient is not None:
        try:
            archive = encrypt(archive, settings.age_recipient)
        except EncryptionUnavailable as exc:
            # The plaintext archive is removed: an unencrypted copy on removable
            # media is exactly what the recipient was configured to prevent.
            archive.unlink(missing_ok=True)
            print(f"error: {exc}", file=sys.stderr)
            return 1

    removed = prune(
        ops_backups,
        keep_daily=settings.backup_keep_daily,
        keep_monthly=settings.backup_keep_monthly,
    )
    print(f"ops store -> {archive.name} ({len(removed)} pruned)")

    if not args.skip_raw:
        mirrored = mirror_raw_zone(settings.raw_dir, settings.backup_dir / "raw", now=now)
        print(f"raw zone  -> {mirrored.name}" if mirrored else "raw zone  -> skipped (no rsync)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
