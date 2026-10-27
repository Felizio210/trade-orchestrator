"""Local Pub/Sub backed by SQLite ({local_dir}/pubsub.sqlite).

Mirrors the Pub/Sub semantics that matter for pipelines:
  * A message is copied to every subscription that exists on the topic at publish time
    (publishing to a topic with no subscriptions drops the message, as on GCP).
  * pull() leases messages for the subscription's ack deadline; anything not acked by then
    is redelivered with delivery_attempt incremented.
  * Every delivery gets a fresh ack_id, so a late ack from an expired lease is ignored.
Because it's SQLite, separate processes (CLI, Airflow tasks) share the same queues.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cloud.base import NotFound, PubSub, ReceivedMessage

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    name TEXT PRIMARY KEY,
    topic TEXT NOT NULL REFERENCES topics(name),
    ack_deadline_s INTEGER NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription TEXT NOT NULL,
    message_id TEXT NOT NULL,
    data BLOB NOT NULL,
    attributes TEXT NOT NULL,
    publish_time REAL NOT NULL,
    lease_until REAL NOT NULL DEFAULT 0,
    ack_id TEXT UNIQUE,
    delivery_attempt INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS deliveries_ready ON deliveries (subscription, lease_until, seq);
"""


class LocalPubSub(PubSub):
    def __init__(self, local_dir: str):
        self.path = Path(local_dir) / "pubsub.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _require(self, conn: sqlite3.Connection, kind: str, name: str) -> sqlite3.Row:
        table = "topics" if kind == "topic" else "subscriptions"
        row = conn.execute(f"SELECT * FROM {table} WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise NotFound(f"{kind.capitalize()} {name!r} does not exist (run setup first)")
        return row

    def ensure_topic(self, topic: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("INSERT OR IGNORE INTO topics VALUES (?, ?)", (topic, time.time()))

    def ensure_subscription(self, subscription: str, topic: str, ack_deadline_s: int = 60) -> None:
        with closing(self._connect()) as conn:
            self._require(conn, "topic", topic)
            conn.execute(
                "INSERT OR IGNORE INTO subscriptions VALUES (?, ?, ?, ?)",
                (subscription, topic, ack_deadline_s, time.time()),
            )

    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        if not isinstance(data, bytes):
            raise TypeError("Pub/Sub message data must be bytes")
        attrs = {k: str(v) for k, v in attributes.items()}
        message_id = uuid.uuid4().hex
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._require(conn, "topic", topic)
                subs = [r[0] for r in conn.execute("SELECT name FROM subscriptions WHERE topic = ?", (topic,))]
                conn.executemany(
                    "INSERT INTO deliveries (subscription, message_id, data, attributes, publish_time) "
                    "VALUES (?, ?, ?, ?, ?)",
                    [(s, message_id, data, json.dumps(attrs), time.time()) for s in subs],
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return message_id

    def pull(self, subscription: str, max_messages: int = 10) -> list[ReceivedMessage]:
        with closing(self._connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")  # lock so two pullers never lease the same message
            try:
                sub = self._require(conn, "subscription", subscription)
                now = time.time()
                rows = conn.execute(
                    "SELECT seq, message_id, data, attributes, publish_time, delivery_attempt FROM deliveries "
                    "WHERE subscription = ? AND lease_until <= ? ORDER BY seq LIMIT ?",
                    (subscription, now, max_messages),
                ).fetchall()
                received = []
                for seq, message_id, data, attrs, publish_time, attempt in rows:
                    ack_id = uuid.uuid4().hex
                    conn.execute(
                        "UPDATE deliveries SET ack_id = ?, lease_until = ?, delivery_attempt = ? WHERE seq = ?",
                        (ack_id, now + sub[2], attempt + 1, seq),
                    )
                    received.append(ReceivedMessage(
                        ack_id=ack_id,
                        message_id=message_id,
                        data=bytes(data),
                        attributes=json.loads(attrs),
                        publish_time=datetime.fromtimestamp(publish_time, timezone.utc),
                        delivery_attempt=attempt + 1,
                    ))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return received

    def _by_ack_id(self, sql: str, subscription: str, ack_ids: list[str]) -> None:
        if not ack_ids:
            return
        with closing(self._connect()) as conn:
            self._require(conn, "subscription", subscription)
            marks = ",".join("?" * len(ack_ids))
            conn.execute(f"{sql} WHERE subscription = ? AND ack_id IN ({marks})", (subscription, *ack_ids))

    def ack(self, subscription: str, ack_ids: list[str]) -> None:
        self._by_ack_id("DELETE FROM deliveries", subscription, ack_ids)

    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        self._by_ack_id("UPDATE deliveries SET lease_until = 0, ack_id = NULL", subscription, ack_ids)

    def list_topics(self) -> list[str]:
        with closing(self._connect()) as conn:
            return [r[0] for r in conn.execute("SELECT name FROM topics ORDER BY name")]

    def list_subscriptions(self) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT s.name, s.topic, s.ack_deadline_s, COUNT(d.seq), "
                "       COALESCE(SUM(d.lease_until > ?), 0) "
                "FROM subscriptions s LEFT JOIN deliveries d ON d.subscription = s.name "
                "GROUP BY s.name ORDER BY s.name",
                (time.time(),),
            ).fetchall()
        return [
            {"name": n, "topic": t, "ack_deadline_s": a, "backlog": b, "leased": leased}
            for n, t, a, b, leased in rows
        ]
