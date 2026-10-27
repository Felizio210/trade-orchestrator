"""Cloud Storage via google-cloud-storage."""

from __future__ import annotations

from google.api_core import exceptions
from google.cloud import storage

from cloud.base import NotFound, ObjectStorage, parse_gs_uri


class GcsStorage(ObjectStorage):
    def __init__(self, project: str, location: str | None = None):
        self.client = storage.Client(project=project)
        self.location = location

    def _blob(self, uri: str) -> storage.Blob:
        bucket, name = parse_gs_uri(uri)
        if not name:
            raise ValueError(f"{uri!r} is not an object URI")
        return self.client.bucket(bucket).blob(name)

    def ensure_bucket(self, bucket: str) -> None:
        if self.client.lookup_bucket(bucket) is None:
            try:
                self.client.create_bucket(bucket, location=self.location)
            except exceptions.Conflict as exc:
                raise RuntimeError(
                    f"Bucket name {bucket!r} is taken by another project (bucket names are global). "
                    "Pick a different GCS_BUCKET."
                ) from exc

    def upload_bytes(self, uri: str, data: bytes, content_type: str | None = None) -> str:
        try:
            self._blob(uri).upload_from_string(data, content_type=content_type or "application/octet-stream")
        except exceptions.NotFound as exc:
            raise NotFound(str(exc)) from exc
        return uri

    def download_bytes(self, uri: str) -> bytes:
        try:
            return self._blob(uri).download_as_bytes()
        except exceptions.NotFound as exc:
            raise NotFound(f"No such object: {uri}") from exc

    def exists(self, uri: str) -> bool:
        return self._blob(uri).exists()

    def list(self, prefix_uri: str) -> list[str]:
        bucket, prefix = parse_gs_uri(prefix_uri)
        try:
            return sorted(f"gs://{bucket}/{b.name}" for b in self.client.list_blobs(bucket, prefix=prefix))
        except exceptions.NotFound as exc:
            raise NotFound(f"Bucket gs://{bucket} does not exist") from exc

    def delete(self, uri: str) -> None:
        try:
            self._blob(uri).delete()
        except exceptions.NotFound as exc:
            raise NotFound(f"No such object: {uri}") from exc
