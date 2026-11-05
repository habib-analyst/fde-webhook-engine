"""HMAC-SHA256 signing helpers for inbound and outbound webhooks."""

from __future__ import annotations

import hashlib
import hmac


def sign(secret: str, body: bytes) -> str:
    """Return the hex HMAC-SHA256 of *body* under *secret*."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify(secret: str, body: bytes, presented: str | None) -> bool:
    """Constant-time check of a presented signature.

    Accepts a bare hex digest or the ``sha256=<hex>`` form.
    """
    if not presented:
        return False
    candidate = presented.strip()
    if candidate.startswith("sha256="):
        candidate = candidate[len("sha256=") :]
    expected = sign(secret, body)
    return hmac.compare_digest(expected, candidate)
