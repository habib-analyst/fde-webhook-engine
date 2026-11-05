"""Delivery engine: fan-out, signing, retries, DLQ, dedupe."""

import pytest

from fde_webhook_engine.engine import compute_retry_delay
from fde_webhook_engine.receivers import FixtureTransport, default_receivers
from fde_webhook_engine.signatures import sign


# -- fan-out -------------------------------------------------------------

def test_fanout_matches_type_filter_and_active_only(h):
    match = h.subscribe(event_types=["invoice.paid"], target="ok")
    h.subscribe(event_types=["user.created"], target="ok")
    h.subscribe(event_types=["invoice.paid"], target="ok", active=False)
    resp = h.send_inbound()
    assert resp.status_code == 202
    deliveries = h.deliveries()
    assert len(deliveries) == 1
    assert deliveries[0]["subscription_id"] == match["id"]
    assert len(h.receivers["ok"].calls) == 1


def test_empty_event_types_matches_all(h):
    h.subscribe(event_types=[], target="ok")
    resp = h.send_inbound(event={"id": "evt_any", "type": "anything.at.all",
                                 "tenant": "acme", "payload": {}})
    assert resp.status_code == 202
    assert len(h.deliveries()) == 1


def test_outbound_delivery_is_signed_with_idempotency_key(h):
    sub = h.subscribe(target="ok", secret="fixture-subscription-secret")
    h.send_inbound()
    call = h.receivers["ok"].calls[0]
    assert call.idempotency_key == f"evt_1:{sub['id']}"
    expected = sign("fixture-subscription-secret", call.body)
    assert call.headers["X-Webhook-Signature"] == expected
    assert call.headers["X-Webhook-Event-Id"] == "evt_1"
    assert call.headers["X-Webhook-Attempt"] == "1"


# -- retry schedule --------------------------------------------------------

def test_retry_delay_schedule_is_pinned():
    # Literal values pinned from the seeded formula in engine.py:
    # delay = 10 * 2**(attempt-1) + Random(f"fde-webhook:del_pinned:{attempt}").random()
    assert compute_retry_delay("del_pinned", 1) == pytest.approx(10.463269971589316)
    assert compute_retry_delay("del_pinned", 2) == pytest.approx(20.777198021551005)
    assert compute_retry_delay("del_pinned", 3) == pytest.approx(40.923567660988624)


def test_failed_delivery_schedules_pinned_retry(h):
    h.subscribe(target="down")
    start = h.clock.now()
    h.send_inbound()
    delivery = h.deliveries()[0]
    assert delivery["status"] == "retry_scheduled"
    assert delivery["attempt_count"] == 1
    expected = compute_retry_delay(delivery["id"], 1)
    assert delivery["next_attempt_at"] == pytest.approx(start + expected)
    attempt = delivery["attempts"][0]
    assert attempt["status_code"] == 500
    assert attempt["error_class"] == "http_5xx"
    assert attempt["latency_ms"] == 9


def test_run_due_does_nothing_before_schedule(h):
    h.subscribe(target="down")
    h.send_inbound()
    processed = h.engine.run_due()
    assert processed == []
    h.clock.advance(5)  # still before the ~10.5s first retry
    assert h.engine.run_due() == []
    assert h.deliveries()[0]["attempt_count"] == 1


# -- DLQ -------------------------------------------------------------------

def _drive_to_dlq(h):
    h.subscribe(target="down")
    h.send_inbound()
    for _ in range(3):
        delivery = h.deliveries()[0]
        h.clock.set(delivery["next_attempt_at"])
        processed = h.engine.run_due()
        assert len(processed) == 1
    return h.deliveries()[0]


def test_dlq_after_max_attempts_with_history_intact(h):
    delivery = _drive_to_dlq(h)
    assert delivery["status"] == "dlq"
    assert delivery["attempt_count"] == 4
    assert len(delivery["attempts"]) == 4
    assert [a["attempt_number"] for a in delivery["attempts"]] == [1, 2, 3, 4]
    assert all(a["status_code"] == 500 for a in delivery["attempts"])

    dlq = h.client.get("/tenants/acme/dlq").json()
    assert len(dlq) == 1
    entry = dlq[0]
    assert entry["delivery_id"] == delivery["id"]
    assert "max attempts reached (4)" in entry["reason"]
    assert len(entry["history"]) == 4
    # A dead-lettered delivery is never attempted again.
    h.clock.advance(100000)
    assert h.engine.run_due() == []


# -- flaky / slow receivers --------------------------------------------------

def test_flaky_receiver_succeeds_on_third_attempt(h):
    h.subscribe(target="flaky")
    h.send_inbound()
    delivery = h.deliveries()[0]
    assert delivery["status"] == "retry_scheduled"

    h.clock.set(delivery["next_attempt_at"])
    h.engine.run_due()
    delivery = h.deliveries()[0]
    assert delivery["status"] == "retry_scheduled"
    assert delivery["attempt_count"] == 2

    h.clock.set(delivery["next_attempt_at"])
    h.engine.run_due()
    delivery = h.deliveries()[0]
    assert delivery["status"] == "delivered"
    assert delivery["attempt_count"] == 3
    assert [a["status_code"] for a in delivery["attempts"]] == [500, 500, 200]
    assert len(h.receivers["flaky"].calls) == 3
    assert len(h.receivers["flaky"].effects) == 1


def test_slow_receiver_is_a_timeout_failure(h):
    h.subscribe(target="slow")
    h.send_inbound()
    delivery = h.deliveries()[0]
    assert delivery["status"] == "retry_scheduled"
    attempt = delivery["attempts"][0]
    assert attempt["error_class"] == "timeout"
    assert attempt["status_code"] is None
    assert attempt["latency_ms"] == 6000
    assert h.receivers["slow"].effects == []


# -- receiver-side dedupe ------------------------------------------------------

def test_receiver_dedupes_same_idempotency_key():
    receivers = default_receivers()
    transport = FixtureTransport(receivers)
    headers = {"X-Idempotency-Key": "evt_1:sub_abc"}
    body = b'{"id":"evt_1"}'
    first = transport.send("ok", headers, body)
    second = transport.send("ok", headers, body)
    assert first.status_code == 200
    assert second.status_code == 200
    ok = receivers["ok"]
    assert len(ok.calls) == 2      # both deliveries arrived
    assert len(ok.effects) == 1    # but the effect was applied once


def test_idempotency_key_is_attempt_independent(h):
    h.subscribe(target="flaky")
    h.send_inbound()
    delivery = h.deliveries()[0]
    h.clock.set(delivery["next_attempt_at"])
    h.engine.run_due()
    keys = [c.idempotency_key for c in h.receivers["flaky"].calls]
    assert len(keys) == 2
    assert keys[0] == keys[1]
    attempts = [c.headers["X-Webhook-Attempt"] for c in h.receivers["flaky"].calls]
    assert attempts == ["1", "2"]
