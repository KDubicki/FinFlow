"""Resolving the registry's git provenance.

``committed_at`` becomes ``dim_instrument``'s SCD2 ``valid_from``
(``PROJECT.md`` §9.2), so these paths decide whether a registry state is
recoverable at all. Degrading quietly is correct; degrading *silently* is not,
which is what ``is_reproducible`` exists to say.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from finflow.registry.git import resolve_commit

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True)


def _repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    _run("git", "init", "-q", cwd=path)
    _run("git", "config", "user.email", "test@example.com", cwd=path)
    _run("git", "config", "user.name", "Test", cwd=path)
    return path


def test_a_tracked_directory_resolves_to_its_last_commit(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "instruments").mkdir()
    (repo / "instruments" / "a.yml").write_text("instruments: []\n", encoding="utf-8")
    _run("git", "add", "-A", cwd=repo)
    _run("git", "commit", "-qm", "seed", cwd=repo)

    commit = resolve_commit(repo / "instruments")

    assert commit.sha is not None
    assert len(commit.sha) == 40
    assert commit.committed_at is not None
    assert commit.committed_at.tzinfo is not None
    assert not commit.dirty
    assert commit.is_reproducible


def test_an_uncommitted_edit_marks_the_registry_dirty(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "instruments").mkdir()
    target = repo / "instruments" / "a.yml"
    target.write_text("instruments: []\n", encoding="utf-8")
    _run("git", "add", "-A", cwd=repo)
    _run("git", "commit", "-qm", "seed", cwd=repo)
    target.write_text("instruments: []\n# edited\n", encoding="utf-8")

    commit = resolve_commit(repo / "instruments")

    assert commit.sha is not None
    assert commit.dirty
    # A dirty registry still loads; it just cannot be recovered from git alone.
    assert not commit.is_reproducible


def test_a_single_file_resolves_by_name(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    target = repo / "a.yml"
    target.write_text("instruments: []\n", encoding="utf-8")
    _run("git", "add", "-A", cwd=repo)
    _run("git", "commit", "-qm", "seed", cwd=repo)

    assert resolve_commit(target).sha is not None


def test_outside_a_repository_it_degrades_rather_than_failing(tmp_path: Path) -> None:
    # An unpacked tarball, or a container built without .git.
    (tmp_path / "instruments").mkdir()
    commit = resolve_commit(tmp_path / "instruments")

    assert commit.sha is None
    assert commit.committed_at is None
    assert not commit.is_reproducible


def test_an_untracked_directory_inside_a_repository_has_no_commit(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "instruments").mkdir()
    (repo / "instruments" / "a.yml").write_text("instruments: []\n", encoding="utf-8")

    assert resolve_commit(repo / "instruments").sha is None


def test_a_missing_path_has_no_commit(tmp_path: Path) -> None:
    assert resolve_commit(tmp_path / "nowhere").sha is None


def test_a_missing_git_binary_degrades(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = _repo(tmp_path)
    (repo / "a.yml").write_text("instruments: []\n", encoding="utf-8")
    _run("git", "add", "-A", cwd=repo)
    _run("git", "commit", "-qm", "seed", cwd=repo)
    monkeypatch.setenv("PATH", "")

    assert resolve_commit(repo).sha is None
