"""The instrument registry: an immutable value object and its loader.

Instruments are configuration, not code (``PROJECT.md`` §5). Adding one is an
edit to a single YAML file under ``instruments/``, reviewed as a diff and
validated in CI before it can reach a pipeline run.
"""

from __future__ import annotations

from finflow.registry.errors import RegistryError, RegistryValidationError
from finflow.registry.loader import load_registry
from finflow.registry.models import (
    Costs,
    Instrument,
    MacroSeries,
    Registry,
    RegistryCommit,
    Universe,
    UniverseMember,
)

__all__ = [
    "Costs",
    "Instrument",
    "MacroSeries",
    "Registry",
    "RegistryCommit",
    "RegistryError",
    "RegistryValidationError",
    "Universe",
    "UniverseMember",
    "load_registry",
]
