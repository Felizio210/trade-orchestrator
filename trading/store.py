"""Orders and positions as records, and reading/writing them in the trading dataset.

Records hold pandas Timestamps (UTC) and None for missing values, whatever the backend
returned (the local warehouse gives timestamps back as text, BigQuery as datetimes).
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, fields
from datetime import date
from typing import Any

import pandas as pd

from cloud import Cloud
from trading.tables import key_for, schema_for, table_id


@dataclass
class Order:
    order_id: str
    date: date
    strategy: str
    timeframe: str
    symbol: str
    side: str
    broker: str
    status: str
    stop: float
    target: float | None
    risk: float
    signal_ts: pd.Timestamp
    created_at: pd.Timestamp
    updated_at: pd.Timestamp
    qty: float | None = None
    fill_ts: pd.Timestamp | None = None
    fill_price: float | None = None
    commission: float | None = None
    broker_order_id: str | None = None
    reason: str | None = None

    @property
    def direction(self) -> int:
        return 1 if self.side == "long" else -1


@dataclass
class Position:
    position_id: str
    date: date
    strategy: str
    timeframe: str
    symbol: str
    side: str
    status: str
    qty: float
    entry_ts: pd.Timestamp
    entry_price: float
    stop: float
    target: float | None
    risk: float
    entry_commission: float
    updated_at: pd.Timestamp
    exit_ts: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str | None = None
    commission: float | None = None
    pnl: float | None = None
    r_multiple: float | None = None

    @property
    def direction(self) -> int:
        return 1 if self.side == "long" else -1


def clean(value: Any) -> Any:
    """NaN / NaT -> None."""
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def read(cloud: Cloud, dataset: str, table: str, where: str = "", params: dict | None = None) -> pd.DataFrame:
    """Rows of a trading table, with TIMESTAMP columns parsed to UTC and DATE columns to dates."""
    df = cloud.bq.query(f"SELECT * FROM `{table_id(dataset, table)}`" + (f" WHERE {where}" if where else ""),
                        params)
    for f in schema_for(table):
        if f.name not in df.columns:
            continue
        if f.type == "TIMESTAMP":
            df[f.name] = pd.to_datetime(df[f.name], utc=True)
        elif f.type == "DATE":
            df[f.name] = pd.to_datetime(df[f.name]).dt.date
    return df


def records(df: pd.DataFrame, cls: type) -> list:
    names = [f.name for f in fields(cls)]
    return [cls(**{n: clean(row.get(n)) for n in names}) for row in df.to_dict("records")]


def write(cloud: Cloud, dataset: str, table: str, rows: list) -> int:
    """Upsert records (dataclasses or dicts) by the table's key."""
    if not rows:
        return 0
    df = pd.DataFrame([asdict(r) if hasattr(r, "__dataclass_fields__") else r for r in rows])
    return cloud.bq.upsert_from_dataframe(df, table_id(dataset, table), key_for(table))


def insert_new(cloud: Cloud, dataset: str, table: str, row: dict) -> bool:
    """Insert one row unless its key is already there. True if it was inserted."""
    return cloud.bq.insert_if_absent(pd.DataFrame([row]), table_id(dataset, table), key_for(table)) == 1
