"""Delivery engine: fan-out, signed attempts, retry scheduling, DLQ.

Retry policy
------------
Exponential backoff with deterministic, seeded jitter:

    delay(attempt) = base_delay * 2 ** (attempt - 1) + jitter
    jitter         = Random(f"fde-webhook:{delivery_id}:{attempt}").random()

``attempt`` is the attempt that just failed (1-based). Because the
jitter is seeded from the delivery id and attempt number, the whole
schedule for a delivery is pinned and reproducible: the same delivery
always gets the same retry times, on any machine, with no stored RNG
state. After ``max_attempts`` failed attempts a delivery moves to the
dead-letter queue with its full attempt history.
"""

from __future__ import annotations

import json
import random

from .clock import Clock
from .receivers import FixtureTransport
from .signatures import sign
from .store import Store, new_id

BASE_DELAY_SECONDS = 10.0
MAX_ATTEMPTS = 4


def compute_retry_delay(delivery_id: str, attempt: int,
                        base_delay: float = BASE_DELAY_SECONDS) -> float:
    """Pinned retry delay after failed attempt ``attempt`` (1-based)."""
    jitter = random.Random(f"fde-webhook:{delivery_id}:{attempt}").random()
    return base_delay * (2 ** (attempt - 1)) + jitter


class DeliveryEngine:
    def __init__(self, store: Store, transport: FixtureTransport, clock: Clock,
                 base_delay: float = BASE_DELAY_SECONDS,
                 max_attempts: int = MAX_ATTEMPTS) -> None:
        self.store = store
        self.transport = transport
        self.clock = clock
        self.base_delay = base_delay
        self.max_attempts = max_attempts

    # -- fan-out -------------------------------------------------------
    def fan_out(self, source: str, event: dict) -> list[dict]:
        """Create one delivery per matching subscription and attempt
        the first delivery immediately. Returns the delivery records."""
        deliveries = []
        body = json.dumps(event, sort_keys=True, separators=(",", ":"))
        for sub in self.store.matching_subscriptions(event["tenant"], event["type"]):
            delivery_id = new_id("del")
            # Attempt-independent idempotency key: identical on every
            # retry, so a receiver can dedupe retried deliveries.
            idempotency_key = f"{event['id']}:{sub['id']}"
            delivery = self.store.create_delivery(
                id=delivery_id,
                event_id=event["id"],
                event_type=event["type"],
                source=source,
                subscription_id=sub["id"],
                tenant=event["tenant"],
                target=sub["target"],
                idempotency_key=idempotency_key,
                body=body,
                next_attempt_at=self.clock.now(),
                created_at=self.clock.now(),
            )
            deliveries.append(self.attempt(delivery_id))
        return deliveries

    # -- one attempt ---------------------------------------------------
    def attempt(self, delivery_id: str) -> dict:
        delivery = self.store.get_delivery(delivery_id)
        sub = self.store.get_subscription(delivery["tenant"], delivery["subscription_id"])
        attempt_number = delivery["attempt_count"] + 1
        now = self.clock.now()
        body_bytes = delivery["body"].encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": sign(sub["signing_secret"], body_bytes),
            "X-Idempotency-Key": delivery["idempotency_key"],
            "X-Webhook-Event-Id": delivery["event_id"],
            "X-Webhook-Subscription-Id": delivery["subscription_id"],
            "X-Webhook-Attempt": str(attempt_number),
        }
        result = self.transport.send(delivery["target"], headers, body_bytes)
        self.store.record_attempt(
            delivery_id, attempt_number, now, result.status_code,
            result.latency_ms, result.error_class, delivery["idempotency_key"],
        )
        if result.error_class == "none" and result.status_code is not None \
                and 200 <= result.status_code < 300:
            self.store.mark_delivered(delivery_id, attempt_number, now)
        elif attempt_number >= self.max_attempts:
            self.store.move_to_dlq(
                delivery_id, attempt_number,
                reason=f"max attempts reached ({self.max_attempts});"
                       f" last error class: {result.error_class}",
                when=now, error_class=result.error_class,
            )
        else:
            delay = compute_retry_delay(delivery_id, attempt_number, self.base_delay)
            self.store.mark_retry(delivery_id, attempt_number, now + delay,
                                  result.error_class)
        return self.store.get_delivery(delivery_id)

    # -- worker --------------------------------------------------------
    def run_due(self, now: float | None = None) -> list[dict]:
        """Execute every delivery whose scheduled time has arrived.

        Driven entirely by the injected clock (or the explicit ``now``),
        so tests move time forward instead of sleeping.
        """
        when = self.clock.now() if now is None else float(now)
        processed = []
        for delivery in self.store.due_deliveries(when):
            processed.append(self.attempt(delivery["id"]))
        return processed
