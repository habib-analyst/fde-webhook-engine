"""SQLite-backed state: subscriptions, events, deliveries, attempts,
dead letters, and the inbound replay cache.

A single Store instance owns one SQLite connection. ``path=":memory:"``
gives each test a clean database; a file path persists state across
restarts for local runs.
"""

from __future__ import annotations

import json
import sqlite3
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL,
    event_types TEXT NOT NULL,          -- JSON list; empty list = all types
    target TEXT NOT NULL,               -- fixture receiver name
    active INTEGER NOT NULL DEFAULT 1,
    signing_secret TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id TEXT NOT NULL,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    tenant TEXT NOT NULL,
    payload TEXT NOT NULL,              -- JSON
    received_at REAL NOT NULL,
    PRIMARY KEY (source, id)
);
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    subscription_id TEXT NOT NULL,
    tenant TEXT NOT NULL,
    target TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    body TEXT NOT NULL,                 -- exact bytes (as text) sent to receivers
    status TEXT NOT NULL,               -- scheduled|delivered|retry_scheduled|dlq
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    created_at REAL NOT NULL,
    delivered_at REAL,
    last_error_class TEXT
);
CREATE TABLE IF NOT EXISTS attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    attempted_at REAL NOT NULL,
    status_code INTEGER,
    latency_ms INTEGER NOT NULL,
    error_class TEXT NOT NULL,
    idempotency_key TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dlq (
    delivery_id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL,
    reason TEXT NOT NULL,
    moved_at REAL NOT NULL,
    history TEXT NOT NULL               -- JSON list of attempt rows
);
CREATE TABLE IF NOT EXISTS replay_cache (
    cache_key TEXT PRIMARY KEY,
    seen_at REAL NOT NULL
);
"""


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # -- subscriptions -------------------------------------------------
    def create_subscription(
        self, tenant: str, event_types: list[str], target: str,
        signing_secret: str, active: bool, now: float,
    ) -> dict:
        sub_id = new_id("sub")
        self.conn.execute(
            "INSERT INTO subscriptions VALUES (?,?,?,?,?,?,?)",
            (sub_id, tenant, json.dumps(event_types), target,
             1 if active else 0, signing_secret, now),
        )
        self.conn.commit()
        return self.get_subscription(tenant, sub_id)

    def get_subscription(self, tenant: str, sub_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM subscriptions WHERE tenant=? AND id=?", (tenant, sub_id)
        ).fetchone()
        return _sub_row(row) if row else None

    def list_subscriptions(self, tenant: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM subscriptions WHERE tenant=? ORDER BY created_at, id", (tenant,)
        ).fetchall()
        return [_sub_row(r) for r in rows]

    def update_subscription(self, tenant: str, sub_id: str, fields: dict) -> dict | None:
        current = self.get_subscription(tenant, sub_id)
        if current is None:
            return None
        merged = {**current, **fields}
        self.conn.execute(
            "UPDATE subscriptions SET event_types=?, target=?, active=?, signing_secret=?"
            " WHERE tenant=? AND id=?",
            (json.dumps(merged["event_types"]), merged["target"],
             1 if merged["active"] else 0, merged["signing_secret"], tenant, sub_id),
        )
        self.conn.commit()
        return self.get_subscription(tenant, sub_id)

    def delete_subscription(self, tenant: str, sub_id: str) -> bool:
        cur = self.conn.execute(
            "DELETE FROM subscriptions WHERE tenant=? AND id=?", (tenant, sub_id)
        )
        self.conn.commit()
        return cur.rowcount > 0

    def matching_subscriptions(self, tenant: str, event_type: str) -> list[dict]:
        out = []
        for sub in self.list_subscriptions(tenant):
            if not sub["active"]:
                continue
            types = sub["event_types"]
            if types and event_type not in types:
                continue
            out.append(sub)
        return out

    # -- events --------------------------------------------------------
    def save_event(self, source: str, event: dict, now: float) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?)",
            (source, event["id"], event["type"], event["tenant"],
             json.dumps(event["payload"]), now),
        )
        self.conn.commit()

    # -- replay cache --------------------------------------------------
    def replay_seen(self, cache_key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM replay_cache WHERE cache_key=?", (cache_key,)
        ).fetchone()
        return row is not None

    def replay_record(self, cache_key: str, now: float) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO replay_cache VALUES (?,?)", (cache_key, now)
        )
        self.conn.commit()

    def replay_prune(self, cutoff: float) -> None:
        self.conn.execute("DELETE FROM replay_cache WHERE seen_at < ?", (cutoff,))
        self.conn.commit()

    # -- deliveries ----------------------------------------------------
    def create_delivery(self, **fields) -> dict:
        self.conn.execute(
            "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                fields["id"], fields["event_id"], fields["event_type"], fields["source"],
                fields["subscription_id"], fields["tenant"], fields["target"],
                fields["idempotency_key"], fields["body"], "scheduled", 0,
                fields["next_attempt_at"], fields["created_at"], None, None,
            ),
        )
        self.conn.commit()
        return self.get_delivery(fields["id"])

    def get_delivery(self, delivery_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM deliveries WHERE id=?", (delivery_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_deliveries(self, tenant: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM deliveries WHERE tenant=? ORDER BY created_at, id", (tenant,)
        ).fetchall()
        return [dict(r) for r in rows]

    def due_deliveries(self, now: float) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM deliveries WHERE status IN ('scheduled','retry_scheduled')"
            " AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?"
            " ORDER BY next_attempt_at, id",
            (now,),
        ).fetchall()
        return [dict(r) for r in rows]

    def record_attempt(self, delivery_id: str, attempt_number: int, attempted_at: float,
                       status_code: int | None, latency_ms: int, error_class: str,
                       idempotency_key: str) -> None:
        self.conn.execute(
            "INSERT INTO attempts (delivery_id, attempt_number, attempted_at,"
            " status_code, latency_ms, error_class, idempotency_key)"
            " VALUES (?,?,?,?,?,?,?)",
            (delivery_id, attempt_number, attempted_at, status_code,
             latency_ms, error_class, idempotency_key),
        )
        self.conn.commit()

    def attempts_for(self, delivery_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM attempts WHERE delivery_id=? ORDER BY attempt_number",
            (delivery_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_delivered(self, delivery_id: str, attempt_count: int, when: float) -> None:
        self.conn.execute(
            "UPDATE deliveries SET status='delivered', attempt_count=?,"
            " delivered_at=?, next_attempt_at=NULL, last_error_class='none' WHERE id=?",
            (attempt_count, when, delivery_id),
        )
        self.conn.commit()

    def mark_retry(self, delivery_id: str, attempt_count: int,
                   next_attempt_at: float, error_class: str) -> None:
        self.conn.execute(
            "UPDATE deliveries SET status='retry_scheduled', attempt_count=?,"
            " next_attempt_at=?, last_error_class=? WHERE id=?",
            (attempt_count, next_attempt_at, error_class, delivery_id),
        )
        self.conn.commit()

    def move_to_dlq(self, delivery_id: str, attempt_count: int, reason: str,
                    when: float, error_class: str) -> None:
        history = self.attempts_for(delivery_id)
        tenant = self.get_delivery(delivery_id)["tenant"]
        self.conn.execute(
            "UPDATE deliveries SET status='dlq', attempt_count=?,"
            " next_attempt_at=NULL, last_error_class=? WHERE id=?",
            (attempt_count, error_class, delivery_id),
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO dlq VALUES (?,?,?,?,?)",
            (delivery_id, tenant, reason, when, json.dumps(history)),
        )
        self.conn.commit()

    def list_dlq(self, tenant: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM dlq WHERE tenant=? ORDER BY moved_at", (tenant,)
        ).fetchall()
        out = []
        for r in rows:
            entry = dict(r)
            entry["history"] = json.loads(entry["history"])
            out.append(entry)
        return out


def _sub_row(row: sqlite3.Row) -> dict:
    sub = dict(row)
    sub["event_types"] = json.loads(sub["event_types"])
    sub["active"] = bool(sub["active"])
    return sub
