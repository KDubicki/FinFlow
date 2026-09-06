"""The dead-man's switch seam.

The one external service the design keeps deliberately (``PROJECT.md`` §11.2): a
monitor running on the box cannot tell you the box is down, which is precisely
the failure it exists to catch.

The contract is what makes it a *dead-man's* switch rather than an alerting
integration: silence is the alarm. A run that dies before it can report failure
looks identical to a box that never woke up, and both must page.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Heartbeat(Protocol):
    """Reports the fate of one pipeline run to an external watchdog.

    Contract:

    - Every run calls exactly one of ``success`` or ``failure``, and there is no
      partial success (daily-operations standard 2).
    - **Every method swallows its own transport errors.** A watchdog that is
      unreachable must not fail a run that otherwise worked — the monitoring
      must never be able to break the thing it monitors.
    """

    def start(self, run_id: str) -> None:
        """Announce that a run has begun, so a hang is distinguishable."""
        ...

    def success(self, run_id: str, detail: str = "") -> None:
        """Report a clean run. This is the ping whose absence raises the alarm."""
        ...

    def failure(self, run_id: str, detail: str = "") -> None:
        """Report a failed run, so the alert arrives before the grace window."""
        ...
