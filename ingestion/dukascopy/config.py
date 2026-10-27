"""Runtime settings, read from environment variables (optionally a .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # python-dotenv is optional (Airflow injects env itself)
    pass


def _csv(value: str | None) -> list[str]:
    return [v.strip().upper() for v in (value or "").split(",") if v.strip()]


def _timeframes(value: str | None) -> list[str]:
    """Parse "1min, 5min,15min,1h" (order kept, duplicates dropped); empty -> the default four."""
    tfs = list(dict.fromkeys(v.strip() for v in (value or "").split(",") if v.strip()))
    return tfs or ["1min", "5min", "15min", "1h"]


def _point_overrides(value: str | None) -> dict[str, int]:
    """Parse "BTCUSD=10,XAUUSD=1000" into {"BTCUSD": 10, "XAUUSD": 1000}."""
    out: dict[str, int] = {}
    for pair in (value or "").split(","):
        if "=" in pair:
            sym, factor = pair.split("=", 1)
            out[sym.strip().upper()] = int(factor)
    return out


@dataclass(frozen=True)
class Settings:
    # Local staging area: raw .bi5 under {local_data_dir}/raw, CSVs under {local_data_dir}/csv
    local_data_dir: str = "data"

    # Where published data goes. The cloud itself (local emulation vs GCP) is picked by
    # DLH_ENV, see cloud/config.py; these names are the same in both.
    gcs_bucket: str = "dlh-lake"
    gcs_prefix: str = "dukascopy"
    pubsub_topic: str = "dukascopy-files"
    pubsub_subscription: str = "dukascopy-files-bq-loader"
    bq_dataset: str = "market"

    # What to pull
    symbols: list[str] = field(default_factory=lambda: ["EURUSD"])
    datasets: list[str] = field(default_factory=lambda: ["ticks", "bars"])
    # One bars_{tf} dataset/table per timeframe; any pandas offset alias: 1min, 5min, 15min, 1h, ...
    bar_timeframes: list[str] = field(default_factory=lambda: ["1min", "5min", "15min", "1h"])
    bar_price_side: str = "bid"  # bid | ask | mid  (MT5 bars are bid-based)
    # Where bars come from: "ticks" (24 hourly tick files/day) or
    # "candles" (Dukascopy's own BID+ASK 1-minute candle files, 2 requests/day)
    bar_source: str = "ticks"
    point_overrides: dict[str, int] = field(default_factory=dict)

    # What lands in Cloud Storage: per-day csv (or parquet) files that BigQuery loads from.
    # The raw .bi5 files always stay in the local download cache; upload_raw also copies them to GCS.
    lake_format: str = "csv"  # csv | parquet
    upload_raw: bool = False

    # HTTP behaviour
    base_url: str = "https://datafeed.dukascopy.com/datafeed"
    max_workers: int = 2
    max_retries: int = 5
    timeout_s: float = 30.0
    min_interval_s: float = 0.5  # minimum gap between any two requests (all workers)
    rate_limit_backoff_s: float = 30.0  # first wait after a 429 when no Retry-After header
    error_backoff_s: float = 1.0  # first wait after a 5xx/connection error (doubles, max 120s)

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        return cls(
            gcs_bucket=env("GCS_BUCKET") or "dlh-lake",
            gcs_prefix=env("GCS_PREFIX", "dukascopy").strip("/"),
            pubsub_topic=env("PUBSUB_TOPIC") or "dukascopy-files",
            pubsub_subscription=env("PUBSUB_SUBSCRIPTION") or "dukascopy-files-bq-loader",
            bq_dataset=env("BQ_DATASET") or "market",
            local_data_dir=env("LOCAL_DATA_DIR") or "data",
            symbols=_csv(env("DUKASCOPY_SYMBOLS", "EURUSD")),
            datasets=[d.lower() for d in _csv(env("DUKASCOPY_DATASETS", "ticks,bars"))],
            bar_timeframes=_timeframes(env("DUKASCOPY_BAR_TIMEFRAMES") or env("DUKASCOPY_BAR_TIMEFRAME")),
            bar_price_side=env("DUKASCOPY_BAR_PRICE_SIDE", "bid").lower(),
            bar_source=env("DUKASCOPY_BAR_SOURCE", "ticks").lower(),
            point_overrides=_point_overrides(env("DUKASCOPY_POINT_OVERRIDES")),
            lake_format=env("DUKASCOPY_LAKE_FORMAT", "csv").lower(),
            upload_raw=env("DUKASCOPY_UPLOAD_RAW", "false").strip().lower() in ("1", "true", "yes"),
            base_url=env("DUKASCOPY_BASE_URL", "https://datafeed.dukascopy.com/datafeed").rstrip("/"),
            max_workers=int(env("DUKASCOPY_MAX_WORKERS", "2")),
            max_retries=int(env("DUKASCOPY_MAX_RETRIES", "5")),
            timeout_s=float(env("DUKASCOPY_TIMEOUT_S", "30")),
            min_interval_s=float(env("DUKASCOPY_MIN_INTERVAL_S", "0.5")),
            rate_limit_backoff_s=float(env("DUKASCOPY_RATE_LIMIT_BACKOFF_S", "30")),
            error_backoff_s=float(env("DUKASCOPY_ERROR_BACKOFF_S", "1")),
        )

    def validate(self) -> None:
        unknown = set(self.datasets) - {"ticks", "bars"}
        if unknown:
            raise ValueError(f"Unknown datasets {sorted(unknown)}; use ticks and/or bars.")
        if self.bar_price_side not in {"bid", "ask", "mid"}:
            raise ValueError("DUKASCOPY_BAR_PRICE_SIDE must be bid, ask or mid.")
        if self.bar_source not in {"ticks", "candles"}:
            raise ValueError("DUKASCOPY_BAR_SOURCE must be ticks or candles.")
        if self.bar_source == "candles":
            if self.datasets != ["bars"]:
                raise ValueError("DUKASCOPY_BAR_SOURCE=candles only provides bars; set DUKASCOPY_DATASETS=bars.")
            if "1min" not in self.bar_timeframes:
                raise ValueError("DUKASCOPY_BAR_SOURCE=candles builds bars from 1min candles; include 1min.")
        if not self.bar_timeframes:
            raise ValueError("DUKASCOPY_BAR_TIMEFRAMES must list at least one timeframe.")
        if self.lake_format not in {"csv", "parquet"}:
            raise ValueError("DUKASCOPY_LAKE_FORMAT must be csv or parquet.")

    @property
    def raw_dir(self) -> str:
        return f"{self.local_data_dir}/raw"

    @property
    def csv_dir(self) -> str:
        return f"{self.local_data_dir}/csv"
