"""The object-store seam.

Write-once, ordered listing, no update in place — and, deliberately, **no
delete method at all**. The raw zone is the one asset that cannot be rebuilt
(``PROJECT.md`` §11.3), so the port refuses to express the operation that would
lose it, mirroring the delete-less credential the real bucket is given.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ObjectStore(Protocol):
    """A flat key-value store of immutable blobs.

    Contract:

    - ``put`` raises ``ObjectAlreadyExists`` rather than overwriting.
    - ``get`` raises ``ObjectNotFound`` for an absent key.
    - ``list`` returns keys under a prefix in lexicographic order, which is
      chronological order given the key layout in ``domain.layout``.
    """

    def put(self, key: str, data: bytes) -> None:
        """Write a new object. Never overwrites."""
        ...

    def get(self, key: str) -> bytes:
        """Read an object."""
        ...

    def exists(self, key: str) -> bool:
        """True when the key is present."""
        ...

    def list(self, prefix: str = "") -> tuple[str, ...]:
        """Every key under ``prefix``, in lexicographic order."""
        ...
