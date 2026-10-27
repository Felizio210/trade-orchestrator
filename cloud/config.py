"""Which cloud to talk to: the local emulation or real GCP. Set DLH_ENV=local|gcp."""

from __future__ import annotations

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

ENVS = ("local", "gcp")


@dataclass(frozen=True)
class CloudSettings:
    env: str = "local"
    project: str = "dlh-local"
    # Root of the local emulation: gcs/ (buckets as folders), pubsub.sqlite, bigquery/{dataset}.sqlite
    local_dir: str = ".local_gcp"
    # GCP location for new buckets/datasets, e.g. asia-southeast2 (Jakarta). None = Google's default.
    location: str | None = None

    @classmethod
    def from_env(cls) -> "CloudSettings":
        env = os.environ.get
        settings = cls(
            env=env("DLH_ENV", "local").lower(),
            project=env("GCP_PROJECT") or "dlh-local",
            local_dir=env("LOCAL_GCP_DIR") or ".local_gcp",
            location=env("GCP_LOCATION") or None,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.env not in ENVS:
            raise ValueError(f"DLH_ENV must be one of {ENVS}, got {self.env!r}")
        if self.env == "gcp" and self.project == "dlh-local":
            raise ValueError("DLH_ENV=gcp needs GCP_PROJECT set to your real project id.")
