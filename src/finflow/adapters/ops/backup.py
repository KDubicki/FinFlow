"""Backing up and restoring the operational store.

This is the only state a rebuild cannot recreate (``PROJECT.md`` §4.3), so it is
the only thing here that genuinely needs a backup — and the restore path is
exercised by a test rather than assumed. An untested restore is a hope with a
cron schedule.

``VACUUM INTO`` rather than copying the file: SQLite in WAL mode keeps recent
writes in a side file, so copying the database alone can produce a snapshot
missing the last few transactions, or a torn one if a write lands mid-copy.
``VACUUM INTO`` asks SQLite for a consistent snapshot instead.
"""

from __future__ import annotations

import gzip
import shutil
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

from finflow.adapters.ops.migrations import current_version
from finflow.logging import get_logger

log = get_logger(__name__)


def backup(source: Path, destination_dir: Path, *, now: datetime, compress: bool = True) -> Path:
    """Write a consistent, timestamped snapshot of the ops store.

    ``destination_dir`` is meant to be on a **different physical device** —
    a copy beside the original is not a backup, and on a single machine that is
    the failure that actually happens (``PROJECT.md`` §11.3).
    """
    if not source.exists():
        raise FileNotFoundError(f"no ops store at {source}")
    destination_dir.mkdir(parents=True, exist_ok=True)

    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    suffix = ".sqlite.gz" if compress else ".sqlite"
    destination = destination_dir / f"ops-{stamp}{suffix}"

    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / "snapshot.sqlite"
        conn = sqlite3.connect(source)
        try:
            conn.execute("VACUUM INTO ?", (str(staged),))
        finally:
            conn.close()

        if compress:
            with staged.open("rb") as raw, gzip.open(destination, "wb") as gz:
                shutil.copyfileobj(raw, gz)
        else:
            shutil.copy2(staged, destination)

    log.info("ops_backup_written", destination=str(destination), bytes=destination.stat().st_size)
    return destination


def restore(archive: Path, destination: Path) -> Path:
    """Restore a backup over ``destination``, verifying it before it lands.

    The archive is decompressed and opened *before* anything is overwritten, so
    a corrupt backup fails without having destroyed the database it was meant to
    replace — which would turn a recoverable incident into an unrecoverable one.
    """
    if not archive.exists():
        raise FileNotFoundError(f"no backup at {archive}")

    with tempfile.TemporaryDirectory() as tmp:
        staged = Path(tmp) / "restored.sqlite"
        if archive.suffix == ".gz":
            with gzip.open(archive, "rb") as gz, staged.open("wb") as out:
                shutil.copyfileobj(gz, out)
        else:
            shutil.copy2(archive, staged)

        version = _verify(staged)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(staged, destination)

    log.info(
        "ops_backup_restored", source=str(archive), destination=str(destination), version=version
    )
    return destination


def _verify(path: Path) -> int:
    """Open the candidate and confirm it is a usable ops store."""
    conn = sqlite3.connect(path)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or str(integrity[0]) != "ok":
            raise ValueError(f"backup failed integrity check: {integrity}")
        version = current_version(conn)
        if version == 0:
            raise ValueError("backup has no applied migrations — it is not an ops store")
        # Reading the tables proves the schema is present, not merely the file.
        conn.execute("SELECT count(*) FROM watermarks").fetchone()
        conn.execute("SELECT count(*) FROM pipeline_runs").fetchone()
        return version
    finally:
        conn.close()


def latest_backup(directory: Path) -> Path | None:
    """The most recent backup in ``directory``, by filename.

    Names are timestamped in UTC and sort lexicographically, so "most recent" is
    a sort rather than a stat call.
    """
    if not directory.is_dir():
        return None
    archives = sorted(directory.glob("ops-*.sqlite*"))
    return archives[-1] if archives else None


def prune(directory: Path, *, keep_daily: int = 30) -> list[Path]:
    """Remove all but the most recent ``keep_daily`` backups.

    Deliberately explicit and never called by the pipeline: retention on the
    only irreplaceable state is an operator decision.
    """
    archives = sorted(directory.glob("ops-*.sqlite*"))
    doomed = archives[:-keep_daily] if len(archives) > keep_daily else []
    for path in doomed:
        path.unlink()
    return doomed
