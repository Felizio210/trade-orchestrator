"""Local Cloud Storage: buckets are folders under {local_dir}/gcs/."""

from __future__ import annotations

import os
from pathlib import Path

from cloud.base import NotFound, ObjectStorage, parse_gs_uri

_TMP_SUFFIX = ".uploading"


class LocalStorage(ObjectStorage):
    def __init__(self, local_dir: str):
        self.root = Path(local_dir) / "gcs"

    def _bucket_dir(self, bucket: str, *, must_exist: bool = True) -> Path:
        path = self.root / bucket
        if must_exist and not path.is_dir():
            raise NotFound(f"Bucket gs://{bucket} does not exist (run setup first)")
        return path

    def _object_path(self, uri: str) -> Path:
        bucket, name = parse_gs_uri(uri)
        if not name or name.endswith("/"):
            raise ValueError(f"{uri!r} is not an object URI")
        bucket_dir = self._bucket_dir(bucket)
        path = (bucket_dir / name).resolve()
        if not path.is_relative_to(bucket_dir.resolve()):
            raise ValueError(f"Object name escapes its bucket: {uri!r}")
        return path

    def ensure_bucket(self, bucket: str) -> None:
        self._bucket_dir(bucket, must_exist=False).mkdir(parents=True, exist_ok=True)

    def upload_bytes(self, uri: str, data: bytes, content_type: str | None = None) -> str:
        path = self._object_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + _TMP_SUFFIX)
        tmp.write_bytes(data)
        os.replace(tmp, path)  # readers never see a half-written object, like GCS
        return uri

    def download_bytes(self, uri: str) -> bytes:
        path = self._object_path(uri)
        if not path.is_file():
            raise NotFound(f"No such object: {uri}")
        return path.read_bytes()

    def exists(self, uri: str) -> bool:
        try:
            return self._object_path(uri).is_file()
        except NotFound:
            return False

    def list(self, prefix_uri: str) -> list[str]:
        bucket, prefix = parse_gs_uri(prefix_uri)
        bucket_dir = self._bucket_dir(bucket)
        uris = []
        for path in bucket_dir.rglob("*"):
            if path.is_file() and not path.name.endswith(_TMP_SUFFIX):
                name = path.relative_to(bucket_dir).as_posix()
                if name.startswith(prefix):
                    uris.append(f"gs://{bucket}/{name}")
        return sorted(uris)

    def delete(self, uri: str) -> None:
        path = self._object_path(uri)
        if not path.is_file():
            raise NotFound(f"No such object: {uri}")
        path.unlink()
