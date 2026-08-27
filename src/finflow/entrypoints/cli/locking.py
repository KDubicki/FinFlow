"""One writer at a time.

``PROJECT.md`` §11.6: the constraint is enforced rather than documented, because
documentation does not stop a manual backfill starting while the scheduled run
is in flight. A second run exits cleanly; it never half-writes.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import TracebackType


class LockHeldError(RuntimeError):
    """Another process holds the pipeline lock."""


class ExclusiveLock:
    """A non-blocking ``flock`` on a file, released when the process exits.

    ``flock`` rather than a lock file with a PID in it: the kernel releases it
    even if the holder is killed, so a crashed run cannot leave a stale lock
    that blocks every subsequent one until someone notices.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fd: int | None = None

    def __enter__(self) -> ExclusiveLock:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(self._fd)
            self._fd = None
            raise LockHeldError(str(self._path)) from exc
        os.write(self._fd, f"{os.getpid()}\n".encode())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
