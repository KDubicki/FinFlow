"""Object-store implementations behind the one ``ObjectStore`` port.

The raw zone lives on local disk (``PROJECT.md`` §11.1). The port exists anyway:
it is what keeps the ingestion service ignorant of where bytes go, and it is the
seam an object store would slot into if this ever ran somewhere else.
"""

from __future__ import annotations

from finflow.adapters.storage.local import LocalObjectStore
from finflow.adapters.storage.memory import InMemoryObjectStore

__all__ = ["InMemoryObjectStore", "LocalObjectStore"]
