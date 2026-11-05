"""FastAPI surface: inbound receiver, subscription CRUD, delivery and
DLQ inspection, and a worker trigger.

Inbound verification policy
---------------------------
``POST /inbound/{source}`` requires:

- ``X-Signature``: hex HMAC-SHA256 of the raw request body, keyed with
  the per-source secret.
- ``X-Timestamp``: unix seconds; rejected when more than +-300 seconds
  from the injected clock (stale or future).
- Replay protection: the pair (source, signature) and the pair
  (source, event id) are cached; a repeat inside the tolerance window
  is rejected with HTTP 409. This engine chooses reject-over-accept:
  a replayed webhook is never fanned out twice.
"""

from __future__ import annotations

import json

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from .clock import Clock, isoformat
from .engine import DeliveryEngine
from .receivers import FixtureTransport, default_receivers
from .signatures import verify
from .store import Store

TIMESTAMP_TOLERANCE_SECONDS = 300

# Fixture source secrets. The "fixture-" prefix marks them as synthetic
# demo values, never real credentials.
DEFAULT_SOURCE_SECRETS = {
    "billing": "fixture-billing-secret",
    "github": "fixture-github-secret",
    "stripe": "fixture-stripe-secret",
}


class SubscriptionIn(BaseModel):
    event_types: list[str] = Field(default_factory=list)
    target: str
    active: bool = True
    signing_secret: str = "fixture-subscription-secret"


class SubscriptionPatch(BaseModel):
    event_types: list[str] | None = None
    target: str | None = None
    active: bool | None = None
    signing_secret: str | None = None


def normalize_event(raw: dict, source: str, now: float) -> dict:
    """Schema-light normalization into the internal event shape."""
    payload = raw.get("payload", raw.get("data", {}))
    rest = {k: v for k, v in raw.items()
            if k not in ("id", "type", "tenant", "payload", "data")}
    if isinstance(payload, dict) and rest:
        payload = {**payload, **rest}
    return {
        "id": str(raw["id"]),
        "type": str(raw["type"]),
        "tenant": str(raw["tenant"]),
        "payload": payload,
        "source": source,
        "received_at": isoformat(now),
    }


def create_app(clock: Clock | None = None, store: Store | None = None,
               source_secrets: dict[str, str] | None = None,
               receivers: dict | None = None) -> FastAPI:
    clock = clock or Clock()
    store = store or Store()
    source_secrets = source_secrets or dict(DEFAULT_SOURCE_SECRETS)
    receivers = receivers if receivers is not None else default_receivers()
    transport = FixtureTransport(receivers)
    engine = DeliveryEngine(store, transport, clock)

    app = FastAPI(title="fde-webhook-engine", version="1.0.0")
    app.state.clock = clock
    app.state.store = store
    app.state.engine = engine
    app.state.receivers = receivers

    # -- inbound -------------------------------------------------------
    @app.post("/inbound/{source}", status_code=202)
    async def inbound(source: str, request: Request):
        secret = source_secrets.get(source)
        if secret is None:
            raise HTTPException(404, f"unknown source {source!r}")
        body = await request.body()
        now = clock.now()

        timestamp_raw = request.headers.get("X-Timestamp")
        try:
            timestamp = float(timestamp_raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise HTTPException(400, "missing or invalid X-Timestamp header")
        if abs(now - timestamp) > TIMESTAMP_TOLERANCE_SECONDS:
            raise HTTPException(401, "stale timestamp: outside +-300s tolerance")

        signature = request.headers.get("X-Signature")
        if not verify(secret, body, signature):
            raise HTTPException(401, "invalid X-Signature")

        try:
            raw = json.loads(body or b"{}")
        except json.JSONDecodeError:
            raise HTTPException(400, "body is not valid JSON")
        for field in ("id", "type", "tenant"):
            if field not in raw:
                raise HTTPException(422, f"event is missing required field {field!r}")

        signature_key = f"sig:{source}:{signature}"
        event_key = f"evt:{source}:{raw['id']}"
        store.replay_prune(now - TIMESTAMP_TOLERANCE_SECONDS)
        if store.replay_seen(signature_key) or store.replay_seen(event_key):
            raise HTTPException(409, "replay: this webhook was already accepted")
        store.replay_record(signature_key, now)
        store.replay_record(event_key, now)

        event = normalize_event(raw, source, now)
        store.save_event(source, event, now)
        deliveries = engine.fan_out(source, event)
        return {
            "accepted": True,
            "event": event,
            "deliveries": [
                {"id": d["id"], "subscription_id": d["subscription_id"],
                 "status": d["status"]}
                for d in deliveries
            ],
        }

    # -- subscriptions ---------------------------------------------------
    @app.post("/tenants/{tenant}/subscriptions", status_code=201)
    def create_subscription(tenant: str, body: SubscriptionIn):
        if body.target not in receivers:
            raise HTTPException(422, f"unknown target receiver {body.target!r}")
        return store.create_subscription(
            tenant, body.event_types, body.target, body.signing_secret,
            body.active, clock.now(),
        )

    @app.get("/tenants/{tenant}/subscriptions")
    def list_subscriptions(tenant: str):
        return store.list_subscriptions(tenant)

    @app.get("/tenants/{tenant}/subscriptions/{sub_id}")
    def get_subscription(tenant: str, sub_id: str):
        sub = store.get_subscription(tenant, sub_id)
        if sub is None:
            raise HTTPException(404, "subscription not found")
        return sub

    @app.patch("/tenants/{tenant}/subscriptions/{sub_id}")
    def patch_subscription(tenant: str, sub_id: str, body: SubscriptionPatch):
        fields = {k: v for k, v in body.model_dump().items() if v is not None}
        if "target" in fields and fields["target"] not in receivers:
            raise HTTPException(422, f"unknown target receiver {fields['target']!r}")
        sub = store.update_subscription(tenant, sub_id, fields)
        if sub is None:
            raise HTTPException(404, "subscription not found")
        return sub

    @app.delete("/tenants/{tenant}/subscriptions/{sub_id}", status_code=204)
    def delete_subscription(tenant: str, sub_id: str):
        if not store.delete_subscription(tenant, sub_id):
            raise HTTPException(404, "subscription not found")
        return None

    # -- inspection ------------------------------------------------------
    @app.get("/tenants/{tenant}/deliveries")
    def list_deliveries(tenant: str):
        out = []
        for d in store.list_deliveries(tenant):
            d = dict(d)
            d["attempts"] = store.attempts_for(d["id"])
            out.append(d)
        return out

    @app.get("/tenants/{tenant}/dlq")
    def list_dlq(tenant: str):
        return store.list_dlq(tenant)

    @app.get("/receivers")
    def list_receivers():
        return {
            name: {"calls": len(r.calls), "effects": len(r.effects)}
            for name, r in receivers.items()
        }

    # -- worker ----------------------------------------------------------
    @app.post("/admin/run-due")
    def run_due():
        processed = engine.run_due()
        return {"processed": len(processed),
                "delivery_ids": [d["id"] for d in processed]}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


app = create_app()
