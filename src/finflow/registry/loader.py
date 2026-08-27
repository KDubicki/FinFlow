"""Building a ``Registry`` from ``instruments/*.yml``.

Every file in the directory is merged into one object and validated as a whole,
because the checks that matter most — a duplicate symbol, a universe naming an
instrument that does not exist — are cross-file by nature.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from finflow.registry.errors import RegistryValidationError
from finflow.registry.git import resolve_commit
from finflow.registry.models import (
    Instrument,
    MacroSeries,
    Registry,
    Universe,
    UniverseMember,
)

DEFAULT_REGISTRY_DIR = Path("instruments")

# Top-level keys a registry file may declare. Anything else is a typo, and
# silently ignoring it is how an instrument goes missing without a message.
_KNOWN_KEYS = frozenset({"instruments", "universes", "series"})


def _read(path: Path) -> dict[str, Any]:
    try:
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RegistryValidationError(f"invalid YAML: {exc}", source=path) from exc
    if content is None:
        return {}
    if not isinstance(content, dict):
        raise RegistryValidationError(
            f"expected a mapping at the top level, found {type(content).__name__}",
            source=path,
        )
    unknown = sorted(set(content) - _KNOWN_KEYS)
    if unknown:
        raise RegistryValidationError(
            f"unknown top-level key(s) {', '.join(unknown)}; "
            f"expected one of {', '.join(sorted(_KNOWN_KEYS))}",
            source=path,
        )
    return content


def _instruments(block: Any, path: Path) -> list[Instrument]:
    if not isinstance(block, list):
        raise RegistryValidationError("'instruments' must be a list", source=path)
    out = []
    for entry in block:
        try:
            out.append(Instrument.model_validate(entry))
        except ValidationError as exc:
            raise RegistryValidationError(_render(exc), source=path) from exc
    return out


def _universes(block: Any, path: Path) -> list[Universe]:
    if not isinstance(block, dict):
        raise RegistryValidationError(
            "'universes' must be a mapping of name to definition", source=path
        )
    out = []
    for name, body in block.items():
        if not isinstance(body, dict):
            raise RegistryValidationError(f"universe {name!r} must be a mapping", source=path)
        try:
            members = [UniverseMember.parse(m) for m in body.get("members", [])]
            out.append(Universe.model_validate({**body, "name": name, "members": members}))
        except (ValidationError, ValueError) as exc:
            problem = _render(exc) if isinstance(exc, ValidationError) else str(exc)
            raise RegistryValidationError(f"universe {name!r}: {problem}", source=path) from exc
    return out


def _macro(block: Any, path: Path) -> list[MacroSeries]:
    if not isinstance(block, list):
        raise RegistryValidationError("'series' must be a list", source=path)
    out = []
    for entry in block:
        try:
            out.append(MacroSeries.model_validate(entry))
        except ValidationError as exc:
            raise RegistryValidationError(_render(exc), source=path) from exc
    return out


def _render(exc: ValidationError) -> str:
    """Flatten a Pydantic error into one line naming the field and the reason."""
    parts = []
    for error in exc.errors():
        location = ".".join(str(p) for p in error["loc"]) or "<root>"
        parts.append(f"{location}: {error['msg']}")
    return "; ".join(parts)


def load_registry(directory: Path | str = DEFAULT_REGISTRY_DIR) -> Registry:
    """Load, merge and validate every ``*.yml`` under ``directory``.

    The result is immutable and is meant to be constructed once at the
    composition root and injected from there (``PROJECT.md`` §5.1).
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise RegistryValidationError(
            f"registry directory does not exist: {directory}", source=directory
        )

    files = sorted(p for p in directory.glob("*.yml") if p.is_file())
    if not files:
        raise RegistryValidationError(f"no *.yml files found in {directory}", source=directory)

    instruments: list[Instrument] = []
    universes: list[Universe] = []
    macro: list[MacroSeries] = []

    for path in files:
        content = _read(path)
        if "instruments" in content:
            instruments += _instruments(content["instruments"], path)
        if "universes" in content:
            universes += _universes(content["universes"], path)
        if "series" in content:
            macro += _macro(content["series"], path)

    try:
        return Registry(
            instruments=tuple(instruments),
            universes=tuple(universes),
            macro=tuple(macro),
            commit=resolve_commit(directory),
        )
    except ValidationError as exc:
        raise RegistryValidationError(_render(exc), source=directory) from exc
