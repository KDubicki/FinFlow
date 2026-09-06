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
import subprocess
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


def prune(directory: Path, *, keep_daily: int = 30, keep_monthly: int = 0) -> list[Path]:
    """Remove backups outside the retention window.

    Two windows, per ``PROJECT.md`` §11.3: the most recent ``keep_daily``
    archives, plus the **first archive of each of the last ``keep_monthly``
    months**. The monthly tier is what survives a corruption that is only
    noticed weeks later — a thirty-day window keeps thirty copies of the same
    bad database.

    ``keep_monthly`` defaults to zero so that calling this with only a daily
    count means exactly what it says. The daily job passes both.
    """
    archives = sorted(directory.glob("ops-*.sqlite*"))
    keep = set(archives[-keep_daily:]) if keep_daily else set()

    if keep_monthly:
        first_of_month: dict[str, Path] = {}
        for archive in archives:
            # ops-20260901T050000Z.sqlite.gz -> 202609
            month = archive.name[4:10]
            first_of_month.setdefault(month, archive)
        for month in sorted(first_of_month)[-keep_monthly:]:
            keep.add(first_of_month[month])

    doomed = [archive for archive in archives if archive not in keep]
    for path in doomed:
        path.unlink()
    return doomed


class EncryptionUnavailable(RuntimeError):
    """``age`` is configured but not installed, or refused the recipient.

    Raised rather than silently writing the backup in the clear. A backup that
    is quietly unencrypted is worse than no backup, because it is trusted.
    """


def encrypt(archive: Path, recipient: str) -> Path:
    """Encrypt one archive to an ``age`` recipient, removing the plaintext.

    ``age`` rather than GPG because the whole configuration is one public key in
    ``.env`` and one private key in a password manager — and a backup scheme
    nobody can remember how to restore from is not a backup scheme.
    """
    destination = archive.with_suffix(archive.suffix + ".age")
    try:
        result = subprocess.run(
            ["age", "-r", recipient, "-o", str(destination), str(archive)],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise EncryptionUnavailable(
            "FINFLOW_AGE_RECIPIENT is set but the `age` binary is not installed"
        ) from exc
    if result.returncode != 0:
        destination.unlink(missing_ok=True)
        raise EncryptionUnavailable(f"age failed: {result.stderr.strip()[:300]}")

    archive.unlink()
    log.info("ops_backup_encrypted", destination=str(destination))
    return destination


def mirror_raw_zone(raw_dir: Path, destination_dir: Path, *, now: datetime) -> Path | None:
    """Hard-linked snapshot of the raw zone on a second device.

    ``rsync --link-dest`` against the previous snapshot, so each night costs
    only the partitions that are new — the raw zone is append-only, so that is
    a handful of files rather than a full copy.

    Returns None when rsync is unavailable, because a missing tool is an
    operator problem to fix rather than a reason to fail the pipeline. The
    digest says it did not happen.
    """
    if not raw_dir.is_dir():
        return None
    destination_dir.mkdir(parents=True, exist_ok=True)
    previous = sorted(p for p in destination_dir.glob("raw-*") if p.is_dir())
    target = destination_dir / f"raw-{now.strftime('%Y%m%dT%H%M%SZ')}"

    command = ["rsync", "-a", "--delete"]
    if previous:
        command.append(f"--link-dest={previous[-1]}")
    command.extend([f"{raw_dir}/", f"{target}/"])

    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
    except FileNotFoundError:
        log.warning("raw_mirror_skipped", reason="rsync is not installed")
        return None
    if result.returncode != 0:
        log.error("raw_mirror_failed", stderr=result.stderr.strip()[:300])
        return None

    log.info(
        "raw_mirror_written",
        destination=str(target),
        linked_from=str(previous[-1]) if previous else None,
    )
    return target
