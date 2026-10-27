"""Warehouse tables for Dukascopy data: schemas and DataFrame shaping.

Both tables are partitioned by `date` (UTC trading day) and clustered by `symbol`, and
each load replaces exactly one (symbol, date), so reloading a day never duplicates rows.
"""

from __future__ import annotations

import re
from datetime import date, datetime

import pandas as pd

from cloud import Field

TICKS_SCHEMA = [
    Field("ts", "TIMESTAMP", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("bid", "FLOAT64", "REQUIRED"),
    Field("ask", "FLOAT64", "REQUIRED"),
    Field("bid_volume", "FLOAT64"),
    Field("ask_volume", "FLOAT64"),
    Field("ingested_at", "TIMESTAMP", "REQUIRED"),
]

BARS_SCHEMA = [
    Field("ts", "TIMESTAMP", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("price_side", "STRING", "REQUIRED"),
    Field("open", "FLOAT64", "REQUIRED"),
    Field("high", "FLOAT64", "REQUIRED"),
    Field("low", "FLOAT64", "REQUIRED"),
    Field("close", "FLOAT64", "REQUIRED"),
    Field("volume", "FLOAT64"),
    Field("ingested_at", "TIMESTAMP", "REQUIRED"),
]

PARTITION_FIELD = "date"
CLUSTER_FIELDS = ["symbol"]


def dataset_name(kind: str, bar_timeframe: str) -> str:
    """"ticks" -> "ticks"; "bars" + "1min" -> "bars_1min"."""
    return "ticks" if kind == "ticks" else f"bars_{bar_timeframe}"


def table_name(dataset: str) -> str:
    """Table names may only contain letters, digits and underscores."""
    return "dukascopy_" + re.sub(r"[^A-Za-z0-9_]", "_", dataset)


def bq_table_id(bq_dataset: str, dataset: str) -> str:
    """Each bar timeframe gets its own BigQuery dataset: ticks -> market.dukascopy_ticks,
    bars_5min -> market_5min.dukascopy_bars."""
    if dataset == "ticks":
        return f"{bq_dataset}.{table_name(dataset)}"
    tf = re.sub(r"[^A-Za-z0-9_]", "_", dataset.removeprefix("bars_"))
    return f"{bq_dataset}_{tf}.dukascopy_bars"


def schema_for(dataset: str) -> list[Field]:
    return TICKS_SCHEMA if dataset == "ticks" else BARS_SCHEMA


def shape(df: pd.DataFrame, dataset: str, symbol: str, day: date, ingested_at: datetime, **extra) -> pd.DataFrame:
    """Add the identifying columns and order columns exactly as the table schema."""
    out = df.assign(date=day, symbol=symbol, ingested_at=pd.Timestamp(ingested_at), **extra)
    return out[[f.name for f in schema_for(dataset)]]
