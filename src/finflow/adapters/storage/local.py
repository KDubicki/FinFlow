"""An object store backed by the local filesystem.

The default for development and for a single-box deployment. Keys map to paths
directly, so the raw zone is browsable with ``ls`` -- which matters more than it
sounds when diagnosing an ingestion problem at 06:00.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from finflow.contracts.errors import ObjectAlreadyExists, ObjectNotFound


class LocalObjectStore:
    """Write-once blobs under a root directory."""

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        """The directory this store writes under."""
        return self._root

    def _path(self, key: str) -> Path:
        if key.startswith("/") or ".." in Path(key).parts:
            raise ValueError(f"key must be relative and may not traverse upwards: {key!r}")
        return self._root / key

    def put(self, key: str, data: bytes) -> None:
        """Write a new object atomically, refusing to overwrite.

        Written to a temporary file in the destination directory and then
        renamed, so a crash mid-write leaves no half-written partition for the
        warehouse loader to find. ``os.link`` provides the exclusivity: it fails
        if the target exists, which closes the window between checking and
        writing.
        """
        path = self._path(key)
        if path.exists():
            raise ObjectAlreadyExists(key)
        path.parent.mkdir(parents=True, exist_ok=True)

        handle, staged_name = tempfile.mkstemp(dir=path.parent, suffix=".partial")
        staged = Path(staged_name)
        try:
            with os.fdopen(handle, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            try:
                os.link(staged, path)
            except FileExistsError:
                raise ObjectAlreadyExists(key) from None
        finally:
            staged.unlink(missing_ok=True)

    def get(self, key: str) -> bytes:
        """Read an object."""
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError:
            raise ObjectNotFound(key) from None

    def exists(self, key: str) -> bool:
        """True when the key is present."""
        return self._path(key).is_file()

    def list(self, prefix: str = "") -> tuple[str, ...]:
        """Every key under ``prefix``, in lexicographic order."""
        if not self._root.exists():
            return ()
        keys = (
            str(path.relative_to(self._root))
            for path in self._root.rglob("*")
            if path.is_file() and not path.name.endswith(".partial")
        )
        return tuple(sorted(k for k in keys if k.startswith(prefix)))
