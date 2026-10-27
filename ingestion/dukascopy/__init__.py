"""Dukascopy historical market data ingestion.

Flow: download (Dukascopy -> local disk) -> publish (GCS + Pub/Sub) -> load (BigQuery).
The cloud side runs against the local emulation or real GCP depending on DLH_ENV.
Public entry points (safe to import from an Airflow DAG):

    from cloud import Cloud
    from ingestion.dukascopy import Settings, download_day, publish_day, load_pending
"""

from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.converter import convert, read_bi5
from ingestion.dukascopy.downloader import download_day, download_range
from ingestion.dukascopy.pipeline import backfill_range, load_pending, publish_day, run_range, setup

__all__ = [
    "Settings", "convert", "read_bi5", "download_day", "download_range",
    "setup", "publish_day", "load_pending", "run_range", "backfill_range",
]
