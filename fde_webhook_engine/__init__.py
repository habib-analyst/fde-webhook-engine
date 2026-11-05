"""fde-webhook-engine: webhook receiving and delivery engine.

Receives webhooks with HMAC signature verification, timestamp
tolerance and replay protection; delivers them to per-tenant
subscriptions with signed attempts, exponential-backoff retries,
idempotency keys and a dead-letter queue. Delivery targets are local
fixture receivers; there is no external network traffic.
"""

from .clock import Clock, FakeClock
from .engine import DeliveryEngine, compute_retry_delay
from .receivers import default_receivers
from .store import Store

__all__ = [
    "Clock",
    "FakeClock",
    "DeliveryEngine",
    "compute_retry_delay",
    "Store",
    "default_receivers",
]

__version__ = "1.0.0"
