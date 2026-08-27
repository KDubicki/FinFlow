"""``SourceClient`` implementations.

Each one fetches and raises. None retries, sleeps beyond its own limiter, knows
where data is stored, or stamps provenance — that is the ingestion service's
job (``PROJECT.md`` §6.7).
"""

from __future__ import annotations

from finflow.adapters.sources.fred import FredClient
from finflow.adapters.sources.stooq import StooqClient
from finflow.adapters.sources.synthetic import SyntheticClient

__all__ = ["FredClient", "StooqClient", "SyntheticClient"]
