"""Injected clock.

All time-dependent behaviour (timestamp tolerance, retry scheduling,
replay expiry) reads time from a Clock instance instead of the wall
clock, so tests can move time forward instantly and deterministically
without sleeping.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


class Clock:
    """Wall clock used by default in production-style runs."""

    def now(self) -> float:
        return time.time()

    def iso(self) -> str:
        return isoformat(self.now())


class FakeClock(Clock):
    """Deterministic clock for tests and local demos."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def set(self, value: float) -> None:
        self._now = float(value)

    def advance(self, seconds: float) -> float:
        self._now += float(seconds)
        return self._now


def isoformat(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()
