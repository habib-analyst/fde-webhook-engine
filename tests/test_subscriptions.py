"""Subscription CRUD and cross-tenant isolation."""


def test_subscription_crud_lifecycle(h):
    sub = h.subscribe(event_types=["invoice.paid"], target="ok")
    sub_id = sub["id"]
    assert sub["tenant"] == "acme"
    assert sub["active"] is True

    listed = h.client.get("/tenants/acme/subscriptions").json()
    assert [s["id"] for s in listed] == [sub_id]

    patched = h.client.patch(
        f"/tenants/acme/subscriptions/{sub_id}", json={"active": False}
    )
    assert patched.status_code == 200
    assert patched.json()["active"] is False

    deleted = h.client.delete(f"/tenants/acme/subscriptions/{sub_id}")
    assert deleted.status_code == 204
    assert h.client.get("/tenants/acme/subscriptions").json() == []
    assert h.client.get(f"/tenants/acme/subscriptions/{sub_id}").status_code == 404


def test_unknown_target_rejected(h):
    resp = h.client.post("/tenants/acme/subscriptions",
                         json={"event_types": [], "target": "real-https-url"})
    assert resp.status_code == 422


def test_cross_tenant_isolation(h):
    sub = h.subscribe(tenant="acme")
    sub_id = sub["id"]

    # Another tenant cannot see, read, patch, or delete it.
    assert h.client.get("/tenants/globex/subscriptions").json() == []
    assert h.client.get(f"/tenants/globex/subscriptions/{sub_id}").status_code == 404
    assert h.client.patch(f"/tenants/globex/subscriptions/{sub_id}",
                          json={"active": False}).status_code == 404
    assert h.client.delete(f"/tenants/globex/subscriptions/{sub_id}").status_code == 404
    # The subscription is untouched.
    assert h.client.get(f"/tenants/acme/subscriptions/{sub_id}").json()["active"] is True


def test_cross_tenant_events_do_not_fan_out(h):
    h.subscribe(tenant="acme", target="ok")
    h.subscribe(tenant="globex", target="down")
    resp = h.send_inbound(event={"id": "evt_g", "type": "invoice.paid",
                                 "tenant": "globex", "payload": {}})
    assert resp.status_code == 202
    # Only globex's own subscription received a delivery.
    assert h.deliveries("acme") == []
    globex = h.deliveries("globex")
    assert len(globex) == 1
    assert globex[0]["target"] == "down"
    assert h.receivers["ok"].calls == []
    # DLQ views are tenant-scoped too.
    assert h.client.get("/tenants/acme/dlq").json() == []
