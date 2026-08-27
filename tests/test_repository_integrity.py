"""Guards against files that exist locally but not in the repository.

An overly broad `.gitignore` pattern is invisible: everything works on the
machine that wrote the files, `make check` is green, and the breakage only
appears in CI or on a fresh clone. That is exactly what happened -- a bare
`models/` meant for ML artifacts also matched `dbt/models/`, and the entire
warehouse model layer went uncommitted while every local test passed.

These tests ask git what it would actually ship.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _ignored(paths: list[Path]) -> list[str]:
    """Which of these paths git is ignoring."""
    if not paths:
        return []
    result = subprocess.run(
        ["git", "check-ignore", "--stdin"],
        cwd=REPO_ROOT,
        input="\n".join(str(p.relative_to(REPO_ROOT)) for p in paths),
        capture_output=True,
        text=True,
        check=False,
    )
    return [line for line in result.stdout.splitlines() if line]


def _tracked(prefix: str = ".") -> set[str]:
    result = subprocess.run(
        ["git", "ls-files", prefix],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return set(result.stdout.split())


@pytest.mark.parametrize("directory", ["src/finflow", "dbt/models", "dbt/macros", "scripts"])
def test_no_source_file_is_gitignored(directory: str) -> None:
    root = REPO_ROOT / directory
    if not root.is_dir():
        pytest.skip(f"{directory} does not exist yet")

    files = [
        p
        for p in root.rglob("*")
        if p.is_file()
        and p.suffix in {".py", ".sql", ".yml", ".yaml"}
        and "__pycache__" not in p.parts
    ]
    ignored = _ignored(files)
    assert not ignored, (
        f"these files under {directory} are gitignored and would be missing from a "
        f"fresh clone: {', '.join(ignored)}"
    )


def test_every_dbt_model_is_tracked() -> None:
    models = REPO_ROOT / "dbt" / "models"
    if not models.is_dir():
        pytest.skip("no dbt project yet")

    on_disk = {
        str(p.relative_to(REPO_ROOT))
        for p in models.rglob("*")
        if p.is_file() and p.suffix in {".sql", ".yml"}
    }
    missing = on_disk - _tracked("dbt/models")
    assert not missing, f"dbt models exist locally but are untracked: {', '.join(sorted(missing))}"


def test_dbt_packages_are_pinned_by_a_committed_lockfile() -> None:
    # dbt_packages/ is ignored the way node_modules is, so the lockfile is the
    # only thing making an install reproducible -- and CI installs from it.
    if not (REPO_ROOT / "dbt" / "packages.yml").exists():
        pytest.skip("no dbt packages declared")
    assert "dbt/package-lock.yml" in _tracked("dbt"), (
        "dbt/packages.yml declares dependencies but package-lock.yml is not committed, "
        "so installs are not reproducible"
    )


def test_no_env_file_is_tracked() -> None:
    tracked = _tracked()
    leaked = {p for p in tracked if Path(p).name == ".env"}
    assert not leaked, f"a real .env is tracked: {leaked}"
