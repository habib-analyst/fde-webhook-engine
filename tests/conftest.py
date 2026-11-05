import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fde_webhook_engine.app import DEFAULT_SOURCE_SECRETS, create_app
from fde_webhook_engine.clock import FakeClock
from fde_webhook_engine.signatures import sign

START = 1_700_000_000.0


class Harness:
    def __init__(self):
        self.clock = FakeClock(START)
        self.app = create_app(clock=self.clock)
        self.client = TestClient(self.app)
        self.store = self.app.state.store
        self.engine = self.app.state.engine
        self.receivers = self.app.state.receivers

    def subscribe(self, tenant="acme", event_types=("invoice.paid",),
                  target="ok", active=True, secret="fixture-subscription-secret"):
        resp = self.client.post(
            f"/tenants/{tenant}/subscriptions",
            json={
                "event_types": list(event_types),
                "target": target,
                "active": active,
                "signing_secret": secret,
            },
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    def send_inbound(self, event=None, source="billing", secret=None,
                     timestamp_offset=0, body_override=None, signature_override=None):
        event = event or {
            "id": "evt_1",
            "type": "invoice.paid",
            "tenant": "acme",
            "payload": {"amount": 4200, "currency": "USD"},
        }
        body = body_override if body_override is not None else json.dumps(event).encode()
        secret = secret or DEFAULT_SOURCE_SECRETS[source]
        signature = signature_override if signature_override is not None else sign(secret, body)
        headers = {
            "X-Signature": signature,
            "X-Timestamp": str(int(self.clock.now() + timestamp_offset)),
            "Content-Type": "application/json",
        }
        return self.client.post(f"/inbound/{source}", content=body, headers=headers)

    def deliveries(self, tenant="acme"):
        resp = self.client.get(f"/tenants/{tenant}/deliveries")
        assert resp.status_code == 200
        return resp.json()


@pytest.fixture()
def h():
    return Harness()
