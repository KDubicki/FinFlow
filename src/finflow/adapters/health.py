"""The dead-man's switch.

``PROJECT.md`` §11.2 keeps exactly one external dependency for monitoring, for a
reason that does not go away: a monitor running on the box cannot tell you the
box is down. healthchecks.io expects a ping on a schedule and emails when one
does not arrive, so the alarm is *silence* — which is the only design that
catches a machine that never woke up.

Every method here swallows its own errors. Monitoring that can fail the thing it
monitors is worse than no monitoring, because it fails on exactly the mornings
when the network is already having a bad day.
"""

from __future__ import annotations

import httpx

from finflow.logging import get_logger

log = get_logger(__name__)


class HealthchecksHeartbeat:
    """Pings a healthchecks.io check URL for the fate of each run."""

    def __init__(self, *, client: httpx.Client, url: str, timeout: float = 10.0) -> None:
        self._client = client
        self._url = url.rstrip("/")
        self._timeout = timeout

    def start(self, run_id: str) -> None:
        """Announce the run, so a hang looks different from a missed timer."""
        self._ping("/start", run_id, "")

    def success(self, run_id: str, detail: str = "") -> None:
        """The ping whose absence raises the alarm."""
        self._ping("", run_id, detail)

    def failure(self, run_id: str, detail: str = "") -> None:
        """Report a failure now rather than waiting out the grace window."""
        self._ping("/fail", run_id, detail)

    def _ping(self, suffix: str, run_id: str, detail: str) -> None:
        try:
            self._client.post(
                f"{self._url}{suffix}",
                content=f"run {run_id}\n{detail}".encode(),
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            log.warning("heartbeat_failed", url=self._url + suffix, error=str(exc))


class NullHeartbeat:
    """No watchdog configured.

    Logged at every call rather than silent, because "nothing is watching this"
    is a fact about the deployment that ought to be visible in the run's own
    output — the operator who set it up is not the one reading the logs six
    months later.
    """

    def start(self, run_id: str) -> None:
        """Record that no watchdog was pinged."""
        log.debug("heartbeat_absent", phase="start", run_id=run_id)

    def success(self, run_id: str, detail: str = "") -> None:
        """Record that no watchdog was pinged."""
        log.info("heartbeat_absent", phase="success", run_id=run_id, detail=detail)

    def failure(self, run_id: str, detail: str = "") -> None:
        """Record that no watchdog was pinged."""
        log.warning("heartbeat_absent", phase="failure", run_id=run_id, detail=detail)
