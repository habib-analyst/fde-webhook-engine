# Launch Note — fde-webhook-engine

Pre-launch checklist, worked item by item. Every item is PASS or N/A
with a reason. Verified locally on 2026-11-05.

| # | Check | Verdict | Reason |
|---|---|---|---|
| 1 | Full test suite green | PASS | `python3 -m pytest tests/ -q` → 25 passed in 0.53s. |
| 2 | Tests deterministic | PASS | Injected FakeClock and seeded jitter; no sleeps, no wall-clock or RNG dependence. |
| 3 | No real credentials anywhere | PASS | Every secret is a synthetic `fixture-*` value; grep finds no real keys, tokens, or passwords. |
| 4 | No external network calls | PASS | Delivery transport is an in-process function call to named fixture receivers; no HTTP client is used for delivery. |
| 5 | $0 cost, free tiers only | PASS | Pure Python stdlib + already-installed FastAPI/pydantic/pytest; no paid service, no card anywhere. |
| 6 | No personal data | PASS | Tenants, events, and payloads in tests are invented fixtures (acme/globex, evt_1); no real user data. |
| 7 | README states scope honestly | PASS | README has a Scope and honesty section: fixture receivers, local transport, SQLite, untuned constants. |
| 8 | No duplication with existing repos | PASS | README opens with the engine-vs-ledger distinction: webhook-ledger is an audit record of received webhooks; this is subscription management plus delivery with retries/DLQ. |
| 9 | Measured results only | PASS | README numbers (25 tests, pinned delays, 3rd-attempt flaky delivery, 4-attempt DLQ) all come from the test suite output and the seeded formula. |
| 10 | Signature security basics | PASS | HMAC-SHA256 over the raw body, constant-time comparison via `hmac.compare_digest`, per-source and per-subscription secrets. |
| 11 | Replay and timestamp abuse covered | PASS | ±300s tolerance and a replay cache returning 409 are implemented and tested, including the tolerance edge. |
| 12 | Tenant isolation tested | PASS | Cross-tenant read/patch/delete return 404 and cross-tenant events never fan out; covered in test_subscriptions.py. |
| 13 | Failure paths tested | PASS | 500s, simulated timeouts, unknown targets, DLQ exhaustion, and inactive subscriptions each have a test. |
| 14 | Idempotency / exactly-once effect tested | PASS | Attempt-independent idempotency key; receiver dedupe test shows 2 calls, 1 effect. |
| 15 | No binary or oversized files | PASS | Source and tests only; the SQLite default is in-memory, so no database file ships. |
| 16 | No internal process language in public files | PASS | README contains no staging, queue, or pipeline language; it reads as a standalone project document. |
| 17 | Identity / naming correct | PASS | Package and project use the single name `fde-webhook-engine`; no other accounts or identities are referenced. |
| 18 | Licence and metadata present | PASS | pyproject.toml declares MIT licence, description, Python >= 3.11, and runtime/dev dependencies. |
| 19 | Runs from a clean checkout | PASS | `pip install -e ".[dev]"` plus the uvicorn command in the README; the app factory needs no environment variables. |
| 20 | Payments / billing surface | N/A | The project moves no money; the word "billing" appears only as a fixture source name for example webhooks. No payment integration exists. |

Result: 19 PASS, 1 N/A with reason, 0 FAIL. Cleared for launch.
