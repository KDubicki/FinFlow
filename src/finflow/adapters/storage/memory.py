"""An in-memory object store.

Ships in ``src`` rather than ``tests`` because the demo path and any dry run
need a store that writes nothing, not only the test suite.
"""

from __future__ import annotations

from finflow.contracts.errors import ObjectAlreadyExists, ObjectNotFound


class InMemoryObjectStore:
    """Write-once blobs in a dict. Useful anywhere durability is not wanted."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        """Write a new object, refusing to overwrite."""
        if key in self._objects:
            raise ObjectAlreadyExists(key)
        self._objects[key] = data

    def get(self, key: str) -> bytes:
        """Read an object."""
        try:
            return self._objects[key]
        except KeyError:
            raise ObjectNotFound(key) from None

    def exists(self, key: str) -> bool:
        """True when the key is present."""
        return key in self._objects

    def list(self, prefix: str = "") -> tuple[str, ...]:
        """Every key under ``prefix``, in lexicographic order."""
        return tuple(sorted(k for k in self._objects if k.startswith(prefix)))
