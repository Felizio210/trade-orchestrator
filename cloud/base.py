"""The interface both backends implement. Pipeline code should only use these methods,
so that the same code runs against the local emulation and real GCP.

Conventions shared by both backends:
  * Objects are addressed by gs://bucket/path URIs, locally too.
  * Tables are addressed as "dataset.table" (the GCP project comes from settings).
  * SQL query parameters are written BigQuery-style: @name.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd


class NotFound(LookupError):
    """A bucket, object, topic, subscription, dataset or table doesn't exist."""


def parse_gs_uri(uri: str) -> tuple[str, str]:
    """"gs://bucket/a/b.parquet" -> ("bucket", "a/b.parquet")."""
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got {uri!r}")
    bucket, _, name = uri[5:].partition("/")
    if not bucket:
        raise ValueError(f"No bucket in {uri!r}")
    return bucket, name


_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def split_table_id(table_id: str) -> tuple[str, str]:
    """"market.ticks" -> ("market", "ticks"). Names must be plain identifiers."""
    parts = table_id.split(".")
    if len(parts) != 2 or not all(_IDENT.match(p) for p in parts):
        raise ValueError(f"Table ids must look like dataset.table (letters, digits, _), got {table_id!r}")
    return parts[0], parts[1]


@dataclass(frozen=True)
class Field:
    """A column, using BigQuery type names: STRING, INT64, FLOAT64, BOOL, DATE, TIMESTAMP."""

    name: str
    type: str
    mode: str = "NULLABLE"  # NULLABLE | REQUIRED


@dataclass(frozen=True)
class ReceivedMessage:
    ack_id: str
    message_id: str
    data: bytes
    attributes: dict[str, str] = field(default_factory=dict)
    publish_time: datetime | None = None
    delivery_attempt: int | None = None


class ObjectStorage(ABC):
    """Cloud Storage."""

    @abstractmethod
    def ensure_bucket(self, bucket: str) -> None: ...

    @abstractmethod
    def upload_bytes(self, uri: str, data: bytes, content_type: str | None = None) -> str: ...

    def upload_file(self, uri: str, path: str, content_type: str | None = None) -> str:
        with open(path, "rb") as f:
            return self.upload_bytes(uri, f.read(), content_type)

    @abstractmethod
    def download_bytes(self, uri: str) -> bytes: ...

    @abstractmethod
    def exists(self, uri: str) -> bool: ...

    @abstractmethod
    def list(self, prefix_uri: str) -> list[str]:
        """All object URIs under gs://bucket/prefix, sorted."""

    @abstractmethod
    def delete(self, uri: str) -> None: ...


class PubSub(ABC):
    """Pub/Sub, pull subscriptions only."""

    @abstractmethod
    def ensure_topic(self, topic: str) -> None: ...

    @abstractmethod
    def ensure_subscription(self, subscription: str, topic: str, ack_deadline_s: int = 60) -> None: ...

    @abstractmethod
    def publish(self, topic: str, data: bytes, **attributes: str) -> str:
        """Publish one message; returns its message id."""

    @abstractmethod
    def pull(self, subscription: str, max_messages: int = 10) -> list[ReceivedMessage]:
        """Lease up to max_messages. Unacked messages are redelivered after the ack deadline."""

    @abstractmethod
    def ack(self, subscription: str, ack_ids: list[str]) -> None: ...

    @abstractmethod
    def nack(self, subscription: str, ack_ids: list[str]) -> None:
        """Make messages available for redelivery right away."""

    @abstractmethod
    def list_topics(self) -> list[str]: ...

    @abstractmethod
    def list_subscriptions(self) -> list[dict[str, Any]]:
        """[{"name", "topic", ...}]; the local backend also reports "backlog"."""


class Warehouse(ABC):
    """BigQuery."""

    @abstractmethod
    def ensure_dataset(self, dataset: str) -> None: ...

    @abstractmethod
    def ensure_table(
        self,
        table_id: str,
        schema: list[Field],
        partition_field: str | None = None,
        cluster_fields: list[str] | None = None,
    ) -> None:
        """Create the table if missing; add any new columns if it exists."""

    @abstractmethod
    def replace_from_uri(self, uri: str, table_id: str, match: dict[str, Any]) -> int:
        """Atomically delete rows matching `match` (column = value, ANDed) and load the
        Parquet/CSV file at `uri` in their place. Returns rows loaded. Re-running with the
        same file is idempotent."""

    @abstractmethod
    def replace_from_dataframe(self, df: pd.DataFrame, table_id: str, match: dict[str, Any]) -> int:
        """Same as replace_from_uri, from an in-memory DataFrame."""

    @abstractmethod
    def query(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        """Run SQL with @name parameters. Locally this is SQLite, so stick to portable SQL."""

    @abstractmethod
    def list_datasets(self) -> list[str]: ...

    @abstractmethod
    def list_tables(self, dataset: str) -> list[str]: ...
