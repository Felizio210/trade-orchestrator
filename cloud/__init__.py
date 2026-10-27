"""One interface for Cloud Storage, Pub/Sub and BigQuery, with two backends:

    DLH_ENV=local  ->  folders + SQLite under LOCAL_GCP_DIR (default .local_gcp/)
    DLH_ENV=gcp    ->  real GCP (GCP_PROJECT, Application Default Credentials)

    from cloud import Cloud
    c = Cloud.from_env()
    c.storage.upload_bytes("gs://bucket/a.txt", b"hi")
    c.pubsub.publish("topic", b"{}", key="value")
    c.bq.query("SELECT * FROM market.ticks WHERE symbol = @s", {"s": "EURUSD"})
"""

from __future__ import annotations

from functools import cached_property

from cloud.base import Field, NotFound, ObjectStorage, PubSub, ReceivedMessage, Warehouse, parse_gs_uri
from cloud.config import CloudSettings


class Cloud:
    """Lazily builds the backend for each service, so unused GCP clients are never created."""

    def __init__(self, settings: CloudSettings):
        settings.validate()
        self.settings = settings

    @classmethod
    def from_env(cls) -> "Cloud":
        return cls(CloudSettings.from_env())

    @property
    def is_local(self) -> bool:
        return self.settings.env == "local"

    @cached_property
    def storage(self) -> ObjectStorage:
        if self.is_local:
            from cloud.local.storage import LocalStorage

            return LocalStorage(self.settings.local_dir)
        from cloud.gcp.storage import GcsStorage

        return GcsStorage(self.settings.project, self.settings.location)

    @cached_property
    def pubsub(self) -> PubSub:
        if self.is_local:
            from cloud.local.pubsub import LocalPubSub

            return LocalPubSub(self.settings.local_dir)
        from cloud.gcp.pubsub import GcpPubSub

        return GcpPubSub(self.settings.project)

    @cached_property
    def bq(self) -> Warehouse:
        if self.is_local:
            from cloud.local.bigquery import LocalBigQuery

            return LocalBigQuery(self.settings.local_dir, self.storage)
        from cloud.gcp.bigquery import BigQueryWarehouse

        return BigQueryWarehouse(self.settings.project, self.settings.location)

    def describe(self) -> str:
        if self.is_local:
            return f"local emulation in {self.settings.local_dir}/"
        return f"GCP project {self.settings.project}" + (f" ({self.settings.location})" if self.settings.location else "")


__all__ = [
    "Cloud", "CloudSettings", "Field", "NotFound", "ObjectStorage", "PubSub", "ReceivedMessage",
    "Warehouse", "parse_gs_uri",
]
