"""Registry failures.

Every message names the file, the offending value and what was expected, because
these errors are read by someone who has just added an instrument and wants to
know which line to fix. The tests assert on the message for that reason.
"""

from __future__ import annotations

from pathlib import Path


class RegistryError(Exception):
    """Base class for anything that goes wrong loading the registry."""


class RegistryValidationError(RegistryError):
    """The registry files parsed but do not describe a consistent registry."""

    def __init__(self, problem: str, *, source: Path | str | None = None) -> None:
        location = f" [{source}]" if source is not None else ""
        super().__init__(f"{problem}{location}")
        self.problem = problem
        self.source = source
