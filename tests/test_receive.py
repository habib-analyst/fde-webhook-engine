"""Receive path: signature verification, timestamp tolerance, replay."""

import json

from fde_webhook_engine.signatures import sign


def test_valid_signature_accepted_and_normalized(h):
    h.subscribe()
    resp = h.send_inbound()
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] is True
    event = data["event"]
    assert event["id"] == "evt_1"
    assert event["type"] == "invoice.paid"
    assert event["tenant"] == "acme"
    assert event["payload"] == {"amount": 4200, "currency": "USD"}
    assert event["received_at"].startswith("2023-11-14T22:13:20")
    assert len(data["deliveries"]) == 1
    assert data["deliveries"][0]["status"] == "delivered"


def test_invalid_signature_rejected(h):
    h.subscribe()
    resp = h.send_inbound(signature_override="0" * 64)
    assert resp.status_code == 401
    assert h.deliveries() == []
    assert h.receivers["ok"].calls == []


def test_wrong_secret_signature_rejected(h):
    resp = h.send_inbound(secret="fixture-wrong-secret")
    assert resp.status_code == 401


def test_tampered_body_rejected(h):
    original = {"id": "evt_9", "type": "invoice.paid", "tenant": "acme",
                "payload": {"amount": 1}}
    body = json.dumps(original).encode()
    signature = sign("fixture-billing-secret", body)
    tampered = json.dumps({**original, "payload": {"amount": 999999}}).encode()
    resp = h.send_inbound(body_override=tampered, signature_override=signature)
    assert resp.status_code == 401
    assert h.deliveries() == []


def test_stale_timestamp_rejected(h):
    resp = h.send_inbound(timestamp_offset=-301)
    assert resp.status_code == 401
    assert "stale" in resp.json()["detail"]


def test_future_timestamp_rejected(h):
    resp = h.send_inbound(timestamp_offset=301)
    assert resp.status_code == 401


def test_timestamp_at_tolerance_edge_accepted(h):
    h.subscribe()
    resp = h.send_inbound(timestamp_offset=-300)
    assert resp.status_code == 202


def test_replay_rejected_with_409(h):
    h.subscribe()
    first = h.send_inbound()
    assert first.status_code == 202
    second = h.send_inbound()
    assert second.status_code == 409
    # The replay never fanned out: one delivery, one receiver effect.
    assert len(h.deliveries()) == 1
    assert len(h.receivers["ok"].calls) == 1
    assert len(h.receivers["ok"].effects) == 1


def test_unknown_source_404(h):
    resp = h.client.post("/inbound/nope", content=b"{}",
                         headers={"X-Signature": "x", "X-Timestamp": "1"})
    assert resp.status_code == 404


def test_missing_event_field_422(h):
    resp = h.send_inbound(event={"id": "evt_x", "type": "invoice.paid"})
    assert resp.status_code == 422
