"""Fixture receiver applications.

Delivery targets are local, in-process receivers registered by name.
There is no external network traffic. Each receiver records every raw
delivery it is handed, and separately records *effects*: an effect is
applied at most once per idempotency key, which is how a receiver
dedupes retried deliveries of the same event.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ReceiverResponse:
    status_code: int | None
    latency_ms: int
    error_class: str  # "none" | "http_5xx" | "timeout"
    body: str = ""


@dataclass
class RecordedCall:
    headers: dict[str, str]
    body: bytes
    idempotency_key: str | None


class BaseReceiver:
    name = "base"
    latency_ms = 10

    def __init__(self) -> None:
        self.calls: list[RecordedCall] = []
        self.effects: list[RecordedCall] = []
        self._seen_keys: set[str] = set()

    def handle(self, headers: dict[str, str], body: bytes) -> ReceiverResponse:
        key = headers.get("X-Idempotency-Key")
        call = RecordedCall(headers=dict(headers), body=body, idempotency_key=key)
        self.calls.append(call)
        response = self._respond(call)
        if response.error_class == "none" and 200 <= (response.status_code or 0) < 300:
            # Receiver-side dedupe: one effect per idempotency key, no
            # matter how many times the delivery is retried.
            if key is not None and key not in self._seen_keys:
                self._seen_keys.add(key)
                self.effects.append(call)
            elif key is None:
                self.effects.append(call)
        return response

    def _respond(self, call: RecordedCall) -> ReceiverResponse:  # pragma: no cover
        raise NotImplementedError


class OkReceiver(BaseReceiver):
    name = "ok"
    latency_ms = 12

    def _respond(self, call: RecordedCall) -> ReceiverResponse:
        return ReceiverResponse(200, self.latency_ms, "none", '{"received": true}')


class FlakyReceiver(BaseReceiver):
    """Fails with HTTP 500 for the first ``failures`` calls, then 200."""

    name = "flaky"
    latency_ms = 18

    def __init__(self, failures: int = 2) -> None:
        super().__init__()
        self.failures = failures
        self._attempts = 0

    def _respond(self, call: RecordedCall) -> ReceiverResponse:
        self._attempts += 1
        if self._attempts <= self.failures:
            return ReceiverResponse(500, self.latency_ms, "http_5xx", "temporary failure")
        return ReceiverResponse(200, self.latency_ms, "none", '{"received": true}')


class DownReceiver(BaseReceiver):
    name = "down"
    latency_ms = 9

    def _respond(self, call: RecordedCall) -> ReceiverResponse:
        return ReceiverResponse(500, self.latency_ms, "http_5xx", "service unavailable")


class SlowReceiver(BaseReceiver):
    """Always slower than the transport timeout: a simulated timeout."""

    name = "slow"
    latency_ms = 6000

    def _respond(self, call: RecordedCall) -> ReceiverResponse:
        return ReceiverResponse(None, self.latency_ms, "timeout", "")


def default_receivers() -> dict[str, BaseReceiver]:
    receivers: list[BaseReceiver] = [
        OkReceiver(),
        FlakyReceiver(),
        DownReceiver(),
        SlowReceiver(),
    ]
    return {r.name: r for r in receivers}


@dataclass
class TransportResult:
    status_code: int | None
    latency_ms: int
    error_class: str
    body: str = ""


class FixtureTransport:
    """In-process transport from the delivery engine to a receiver.

    Applies the delivery timeout to the receiver's simulated latency:
    a receiver slower than ``timeout_ms`` is reported as a timeout and
    its response is discarded, exactly as a real HTTP client would.
    """

    def __init__(self, receivers: dict[str, BaseReceiver], timeout_ms: int = 3000) -> None:
        self.receivers = receivers
        self.timeout_ms = timeout_ms

    def send(self, target: str, headers: dict[str, str], body: bytes) -> TransportResult:
        receiver = self.receivers.get(target)
        if receiver is None:
            return TransportResult(None, 0, "unknown_target", f"no receiver named {target!r}")
        response = receiver.handle(headers, body)
        if response.latency_ms > self.timeout_ms:
            return TransportResult(None, response.latency_ms, "timeout", "")
        return TransportResult(
            response.status_code, response.latency_ms, response.error_class, response.body
        )
