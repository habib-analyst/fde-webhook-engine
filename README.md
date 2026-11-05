# fde-webhook-engine

A webhook integration engine: it **receives** webhooks safely and **delivers** them reliably.

On the receive side it verifies HMAC-SHA256 signatures, enforces a timestamp tolerance window, and blocks replays. On the deliver side it manages per-tenant subscriptions, fans events out to matching subscribers, signs every outbound delivery, retries failures with exponential backoff, and parks exhausted deliveries in a dead-letter queue with their full attempt history.

> **How this differs from a webhook ledger:** a webhook ledger is an append-only audit record of webhooks that arrived — it answers "what did we receive?". This project is the engine around that record: subscription management, signature verification on receive, fan-out delivery with retries and dead-lettering, and delivery receipts. It answers "did every subscriber actually get it, and what happened when they didn't?".

Everything runs locally. Delivery targets are in-process fixture receiver applications, including deliberately faulty ones, so failure behaviour can be tested end to end with no external network and no real endpoints.

## Design

### Receive path — `POST /inbound/{source}`

| Check | Rule |
|---|---|
| Signature | `X-Signature` must be the hex HMAC-SHA256 of the raw request body, keyed with the per-source secret. Compared in constant time. A tampered body fails. |
| Timestamp | `X-Timestamp` (unix seconds) must be within ±300 seconds of the server clock. Stale and future timestamps are rejected. |
| Replay | The (source, signature) and (source, event id) pairs are cached. A repeat inside the tolerance window returns **HTTP 409** and is never fanned out twice. This engine chooses reject-over-accept: a replayed webhook is dropped, not silently re-accepted. |
| Unknown source | HTTP 404. Per-source secrets are configuration, not code. |

Accepted events are normalized, schema-light, into:

```json
{ "id": "...", "type": "invoice.paid", "tenant": "acme",
  "payload": { }, "source": "billing", "received_at": "2023-11-14T22:13:20+00:00" }
```

Only `id`, `type`, and `tenant` are required. Everything else lands in `payload`.

### Subscriptions

Per-tenant CRUD at `/tenants/{tenant}/subscriptions`:

- `event_types` — filter list; an empty list matches every event type.
- `target` — a fixture receiver registered **by name** (`ok`, `flaky`, `down`, `slow`), never a real URL.
- `active` — inactive subscriptions are excluded from fan-out.
- `signing_secret` — per-subscription secret used to sign outbound deliveries.

Tenants are fully isolated: one tenant cannot read, modify, delete, or receive deliveries for another tenant's subscriptions, and cross-tenant access returns 404.

### Delivery engine

On an accepted event, one delivery is created per matching subscription and the first attempt runs immediately.

- Every attempt is signed: `X-Webhook-Signature` is the HMAC-SHA256 of the exact body bytes under the subscription's secret.
- Every delivery carries an **idempotency key**, `X-Idempotency-Key: <event_id>:<subscription_id>`. The key is attempt-independent — identical on every retry — so a receiver can dedupe retried deliveries and apply the effect exactly once.
- A 2xx response marks the delivery `delivered`, with the attempt log as its receipt.
- Every attempt is logged: status code, simulated latency, and error class (`none`, `http_5xx`, `timeout`, `unknown_target`).
- Failures are retried by a worker, `run_due(now)`, which executes every delivery whose scheduled time has arrived. It is also exposed as `POST /admin/run-due`.

### Retry and dead-letter policy

- Max attempts: **4** (1 immediate + 3 retries).
- Backoff: `delay = 10s × 2^(attempt−1) + jitter`, where the jitter is seeded deterministically from the delivery id and attempt number (`Random("fde-webhook:<delivery_id>:<attempt>")`). The schedule for any delivery is pinned and reproducible on any machine, with no stored RNG state.
- Pinned example for delivery `del_pinned`: retry delays of **10.463s, 20.777s, and 40.924s** after attempts 1, 2, and 3.
- After the 4th failed attempt the delivery moves to the **dead-letter queue** (`GET /tenants/{tenant}/dlq`) with the reason and its complete attempt history. Dead-lettered deliveries are never attempted again.

### Fixture receivers

| Receiver | Behaviour | Simulated latency |
|---|---|---|
| `ok` | Always 200 | 12 ms |
| `flaky` | 500 on the first 2 calls, then 200 | 18 ms |
| `down` | Always 500 | 9 ms |
| `slow` | Slower than the 3,000 ms transport timeout, so every attempt is a timeout | 6,000 ms |

Receivers record every call they are handed and, separately, the effects they applied. An effect is applied at most once per idempotency key: hand the `ok` receiver the same delivery twice and it records 2 calls but 1 effect.

## Running it

```bash
pip install -e ".[dev]"
python3 -m uvicorn fde_webhook_engine.app:app --port 8000
```

Send a signed webhook (fixture secret for the `billing` source):

```bash
python3 - <<'PY'
import hashlib, hmac, json, time, urllib.request

body = json.dumps({"id": "evt_1", "type": "invoice.paid",
                   "tenant": "acme", "payload": {"amount": 4200}}).encode()
sig = hmac.new(b"fixture-billing-secret", body, hashlib.sha256).hexdigest()
req = urllib.request.Request(
    "http://localhost:8000/inbound/billing", data=body, method="POST",
    headers={"X-Signature": sig, "X-Timestamp": str(int(time.time())),
             "Content-Type": "application/json"})
print(urllib.request.urlopen(req).read().decode())
PY
```

First create a subscription, or the event fans out to zero deliveries:

```bash
curl -X POST localhost:8000/tenants/acme/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"event_types": ["invoice.paid"], "target": "flaky", "signing_secret": "fixture-subscription-secret"}'
```

## Tests

```bash
python3 -m pytest tests/ -q
# 25 passed
```

The suite runs on an injected clock — no test sleeps. Coverage includes:

- valid / invalid / wrong-secret signatures, and a tampered body;
- stale and future timestamps rejected, the ±300s edge accepted;
- replay rejected with 409 and fanned out exactly once;
- fan-out honouring event-type filters and the active flag;
- retry delays pinned to the literal seeded values above;
- the flaky receiver delivered on its 3rd attempt (500, 500, 200);
- the slow receiver classified as a timeout on every attempt;
- dead-lettering after 4 attempts with all 4 attempt records intact;
- receiver-side dedupe: same idempotency key delivered twice, one effect;
- subscription CRUD and full cross-tenant isolation.

## Measured results

- **25/25 tests pass** in under a second, deterministically.
- Flaky receiver: delivered on attempt **3 of 4**, exactly 1 effect applied.
- Down receiver: dead-lettered after **4 attempts**, history of 4 × HTTP 500 preserved.
- Retry schedule for `del_pinned`: **10.463s / 20.777s / 40.924s**, identical on every run.

## Scope and honesty

- Fixture receivers are in-process stand-ins for subscriber endpoints. There is no real HTTP egress, no real subscriber, and no production traffic — the transport is a local function call that applies a simulated latency and a 3,000 ms timeout.
- All secrets in this repository are fixture values, prefixed `fixture-`, used so signature tests are reproducible. They protect nothing.
- State is SQLite (in-memory by default, a file path for local persistence). There is no queue broker; `run_due` is the whole scheduler.
- Timestamp tolerance, backoff base, and max attempts are constants chosen for legibility, not tuned production values.

## Layout

```
fde_webhook_engine/
  app.py        FastAPI surface: inbound, subscriptions, deliveries, DLQ, worker
  engine.py     Fan-out, signed attempts, pinned backoff schedule, DLQ
  store.py      SQLite state: subscriptions, events, deliveries, attempts, DLQ, replay cache
  receivers.py  Fixture receivers (ok / flaky / down / slow) and the local transport
  signatures.py HMAC-SHA256 sign / verify
  clock.py      Injected clock (wall clock in runs, deterministic clock in tests)
tests/          Receive path, delivery engine, subscriptions and tenant isolation
```
