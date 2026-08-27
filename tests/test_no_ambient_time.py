"""No layer inward of the adapters may read the clock directly.

Written as an exclusion rather than a list of packages so that it covers
``application`` the day that package appears, without anyone remembering to
extend it. That matters because ``application`` is where ambient time actually
creeps in (``PROJECT.md`` §4.2).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src" / "finflow"

# Packages allowed to read the real clock. Everything else takes a `Clock`.
EXEMPT = {"adapters", "entrypoints"}

# Dotted call targets that read wall-clock time.
FORBIDDEN = {
    "datetime.now",
    "datetime.utcnow",
    "datetime.today",
    "date.today",
    "time.time",
    "time.time_ns",
    "time.monotonic",
}


def _modules() -> list[Path]:
    return sorted(
        path for path in SRC.rglob("*.py") if not (set(path.relative_to(SRC).parts) & EXEMPT)
    )


def _dotted_name(node: ast.AST) -> str:
    """Render ``datetime.datetime.now`` as ``datetime.now`` — the last two parts.

    Two parts is what distinguishes ``datetime.now`` from an unrelated ``now``,
    while staying insensitive to whether the module was imported as
    ``datetime`` or ``dt``.
    """
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts[-2:] if len(parts) > 1 else parts))


def test_at_least_one_module_is_checked() -> None:
    # A guard on the guard: a refactor that moves or renames `src/finflow`
    # would otherwise make this file pass by inspecting nothing.
    assert _modules(), f"no modules found under {SRC}"


@pytest.mark.parametrize("path", _modules(), ids=lambda p: p.name)
def test_module_reads_no_ambient_time(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    offences = [
        f"{path.name}:{node.lineno} calls {_dotted_name(node.func)}()"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _dotted_name(node.func) in FORBIDDEN
    ]
    assert not offences, "Inject a Clock instead of reading the system clock:\n  " + "\n  ".join(
        offences
    )
