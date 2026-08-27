"""Guards on the guard in ``.importlinter``.

``lint-imports`` enforces the dependency rule, but only over the modules it is
told about. A `forbidden` contract cannot name a package that does not exist
yet, so the list has to be extended by hand as packages appear -- and a rule
that depends on someone remembering is the rule this project exists to avoid.
These tests fail the build instead.
"""

from __future__ import annotations

import configparser
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / ".importlinter"
SRC = REPO_ROOT / "src" / "finflow"

# Packages that talk to the outside world, and so are exempt from both contracts.
OUTER = {"adapters", "entrypoints"}


def _packages() -> set[str]:
    return {p.name for p in SRC.iterdir() if p.is_dir() and (p / "__init__.py").exists()}


@pytest.fixture(scope="module")
def contracts() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(CONFIG, encoding="utf-8")
    return parser


def _modules(parser: configparser.ConfigParser, section: str, key: str) -> set[str]:
    raw = parser[section][key]
    return {line.strip() for line in raw.splitlines() if line.strip()}


def test_every_inner_package_is_covered_by_the_forbidden_contract(
    contracts: configparser.ConfigParser,
) -> None:
    listed = _modules(contracts, "importlinter:contract:core-is-vendor-free", "source_modules")
    expected = {f"finflow.{name}" for name in _packages() - OUTER}
    missing = expected - listed
    assert not missing, (
        f"add {', '.join(sorted(missing))} to source_modules in .importlinter — "
        f"a new inner package is otherwise free to import a vendor client"
    )


def test_every_package_appears_in_the_layers_contract(
    contracts: configparser.ConfigParser,
) -> None:
    # Optional layers are parenthesised; strip the markers before comparing.
    raw = _modules(contracts, "importlinter:contract:layers", "layers")
    listed = {part.strip().strip("()") for line in raw for part in line.split("|")}
    expected = {f"finflow.{name}" for name in _packages()}
    missing = expected - listed
    assert not missing, f"add {', '.join(sorted(missing))} to the layers contract"


def test_the_config_is_pointed_at_this_package(contracts: configparser.ConfigParser) -> None:
    assert contracts["importlinter"]["root_package"] == "finflow"
