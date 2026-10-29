"""Read bars from the market warehouse tables the ingestion pipeline loads."""

from __future__ import annotations

from datetime import date

import pandas as pd

from cloud import Cloud
from ingestion.dukascopy.config import Settings
from ingestion.dukascopy.tables import bq_table_id, dataset_name

BAR_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "price_side", "ingested_at"]


def load_bars(cloud: Cloud, market: Settings, symbol: str, timeframe: str, start: date, end: date) -> pd.DataFrame:
    """Bars for one symbol over [start, end] (UTC days, inclusive), oldest first, with a
    0..n-1 index. Filters on `date`, the partition column, so BigQuery only scans those days."""
    if timeframe not in market.bar_timeframes:
        raise ValueError(f"No {timeframe} bars are loaded; DUKASCOPY_BAR_TIMEFRAMES has {market.bar_timeframes}")
    table = bq_table_id(market.bq_dataset, dataset_name("bars", timeframe))
    df = cloud.bq.query(
        f"SELECT {', '.join(BAR_COLUMNS)} FROM `{table}` "
        "WHERE symbol = @symbol AND date BETWEEN @start AND @end ORDER BY ts",
        {"symbol": symbol.upper(), "start": start, "end": end},
    )
    # The local warehouse returns timestamps as text; BigQuery returns datetimes
    for col in ("ts", "ingested_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df.reset_index(drop=True)
