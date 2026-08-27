"""Resolving where the registry came from.

Shelling out to git happens exactly once, at load time, so that nothing
downstream does it in the middle of a computation (``PROJECT.md`` §5.1).
"""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from finflow.registry.models import RegistryCommit

_TIMEOUT_SECONDS = 5


def _git(*args: str, cwd: Path) -> str | None:
    """Run a git command, returning None if git or the repository is absent."""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def resolve_commit(path: Path) -> RegistryCommit:
    """Return the commit that last touched ``path``.

    Degrades rather than fails: a registry loaded from an unpacked tarball, or
    in a container without git, still loads — it is simply not reproducible, and
    ``RegistryCommit.is_reproducible`` says so rather than the caller guessing.
    """
    if not path.exists():
        return RegistryCommit()

    # Run git *inside* the target and use a pathspec relative to it. Passing the
    # caller's path as the pathspec would resolve it against the new cwd, which
    # silently matches nothing and reports an unversioned registry.
    cwd, pathspec = (path, ".") if path.is_dir() else (path.parent, path.name)

    described = _git("log", "-1", "--format=%H%x1f%cI", "--", pathspec, cwd=cwd)
    if not described or "\x1f" not in described:
        return RegistryCommit()

    sha, _, iso = described.partition("\x1f")
    status = _git("status", "--porcelain", "--", pathspec, cwd=cwd)
    try:
        committed_at = datetime.fromisoformat(iso)
    except ValueError:  # pragma: no cover - git always emits valid ISO-8601
        committed_at = None

    return RegistryCommit(sha=sha, committed_at=committed_at, dirty=bool(status))
