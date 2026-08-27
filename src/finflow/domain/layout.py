"""The raw-zone key layout.

One definition, used by the writer now and by the warehouse loader later. Both
sides of a convention drifting apart is a silent class of bug, so the parse and
the render live next to each other and a round-trip test holds them together.

    raw/source=stooq/symbol=GLD/ingested=2026-08-27T05:12:00Z/data.parquet

Partitions are never rewritten. A later fetch of the same symbol is a *new*
partition holding a later opinion, which is what makes vendor restatements
observable rather than silently absorbed (``PROJECT.md`` §6.2).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

RAW_PREFIX = "raw"
MANIFEST_PREFIX = "manifests"
DATA_FILE = "data.parquet"

_KEY = re.compile(
    rf"^{RAW_PREFIX}/source=(?P<source>[^/]+)/symbol=(?P<symbol>[^/]+)"
    rf"/ingested=(?P<ingested>[^/]+)/{DATA_FILE}$"
)


def _stamp(moment: datetime) -> str:
    """Render an instant as a key-safe, lexicographically sortable UTC stamp.

    Colons are legal in S3 keys but awkward in local filesystems and shell
    quoting, so they are dropped. Sorting is what makes "the latest partition"
    a listing operation rather than a scan.
    """
    if moment.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware")
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def _parse_stamp(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=UTC)


@dataclass(frozen=True)
class RawPartition:
    """One ingestion run's output for one ``(source, symbol)`` pair."""

    source: str
    symbol: str
    ingested_at: datetime

    @property
    def key(self) -> str:
        """The object-store key this partition is written to."""
        return raw_key(self.source, self.symbol, self.ingested_at)


def raw_key(source: str, symbol: str, ingested_at: datetime) -> str:
    """Build the key for one raw partition."""
    if "/" in source or "/" in symbol:
        raise ValueError(f"source and symbol may not contain '/': {source!r}, {symbol!r}")
    return (
        f"{RAW_PREFIX}/source={source}/symbol={symbol}/ingested={_stamp(ingested_at)}/{DATA_FILE}"
    )


def raw_prefix(source: str | None = None, symbol: str | None = None) -> str:
    """The listing prefix for a source, or for one symbol within it."""
    if source is None:
        return f"{RAW_PREFIX}/"
    if symbol is None:
        return f"{RAW_PREFIX}/source={source}/"
    return f"{RAW_PREFIX}/source={source}/symbol={symbol}/"


def parse_raw_key(key: str) -> RawPartition:
    """Recover the partition a key describes.

    Raises ``ValueError`` on anything that is not a raw data key, so a stray
    file in the bucket is a loud failure rather than a mystery row.
    """
    match = _KEY.match(key)
    if match is None:
        raise ValueError(f"not a raw partition key: {key!r}")
    try:
        ingested_at = _parse_stamp(match["ingested"])
    except ValueError:
        # A key whose shape is right but whose stamp is not still has to fail
        # with a message that names the key, not one about strptime formats.
        raise ValueError(
            f"not a raw partition key: {key!r} has an unparseable ingested stamp "
            f"{match['ingested']!r}"
        ) from None
    return RawPartition(source=match["source"], symbol=match["symbol"], ingested_at=ingested_at)


def manifest_key(run_id: str) -> str:
    """The key a run's manifest is written to."""
    return f"{MANIFEST_PREFIX}/{run_id}.json"
