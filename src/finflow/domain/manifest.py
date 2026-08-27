"""The data manifest, and the snapshot id derived from it.

``snapshot_id`` was ``max(ingested_at)`` in an earlier design, which is wrong:
backfilling GLD's 1990s history today would bump the id and falsely imply every
other instrument had changed. Instead a run records exactly which ingestion
partitions it admitted, per ``(source, symbol)``, and hashes that
(``PROJECT.md`` §6.2).

Reproduction is then exact and per-instrument, and it is precisely what Delta
time travel replaces if the lakehouse extension is ever built.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime

from finflow.domain.layout import RawPartition


@dataclass(frozen=True)
class Manifest:
    """Every raw partition one run admitted, and the id that identifies it."""

    run_id: str
    created_at: datetime
    entries: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    """``"source:symbol"`` to the ordered partition keys admitted for it."""

    registry_commit: str | None = None

    @classmethod
    def of(
        cls,
        run_id: str,
        created_at: datetime,
        partitions: Iterable[RawPartition],
        *,
        registry_commit: str | None = None,
    ) -> Manifest:
        """Build a manifest from the partitions a run wrote."""
        grouped: dict[str, list[str]] = {}
        for partition in partitions:
            grouped.setdefault(f"{partition.source}:{partition.symbol}", []).append(partition.key)
        return cls(
            run_id=run_id,
            created_at=created_at,
            entries={key: tuple(sorted(value)) for key, value in sorted(grouped.items())},
            registry_commit=registry_commit,
        )

    @property
    def snapshot_id(self) -> str:
        """A stable hash of what this run actually read.

        Keyed on the admitted partitions alone — not the run id, not the wall
        clock. Two runs that read the same data therefore share a snapshot id,
        which is what makes "nothing changed today" expressible.
        """
        canonical = json.dumps(
            {key: list(value) for key, value in sorted(self.entries.items())},
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_json(self) -> str:
        """Serialise for the object store."""
        return json.dumps(
            {
                "run_id": self.run_id,
                "created_at": self.created_at.isoformat(),
                "registry_commit": self.registry_commit,
                "snapshot_id": self.snapshot_id,
                "entries": {key: list(value) for key, value in sorted(self.entries.items())},
            },
            indent=2,
            sort_keys=True,
        )

    @property
    def partition_count(self) -> int:
        """How many raw partitions this run admitted."""
        return sum(len(v) for v in self.entries.values())
