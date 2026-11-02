"""Local BigQuery on SQLite: one database file per dataset under {local_dir}/bigquery/.

Each dataset file is ATTACHed under its own name, so `SELECT * FROM market.ticks`
works unchanged in both SQLite and BigQuery. Backticked ids (`project.dataset.table`)
and @name parameters are translated automatically.

Type mapping: TIMESTAMP -> TEXT "YYYY-MM-DD HH:MM:SS.ffffff" (UTC), DATE -> TEXT
"YYYY-MM-DD", INT64/BOOL -> INTEGER, FLOAT64 -> REAL, STRING -> TEXT. Both text formats
sort and compare correctly, so filters like `ts >= '2026-09-03'` behave as expected.
Query results come back as stored (timestamps are strings); parse them if you need to.

This is SQLite, not BigQuery SQL: plain SELECT/WHERE/GROUP BY/JOIN are fine, but
BigQuery-only functions (TIMESTAMP_TRUNC, SAFE_DIVIDE, QUALIFY, ...) won't run locally.
"""

from __future__ import annotations

import io
import json
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from cloud.base import Field, NotFound, ObjectStorage, Warehouse, split_table_id

_SQLITE_TYPES = {
    "STRING": "TEXT", "INT64": "INTEGER", "INTEGER": "INTEGER", "FLOAT64": "REAL", "FLOAT": "REAL",
    "NUMERIC": "REAL", "BOOL": "INTEGER", "BOOLEAN": "INTEGER", "DATE": "TEXT", "TIMESTAMP": "TEXT",
    "DATETIME": "TEXT",
}
_META = "__schema__"


# String literals are matched first so that "@x" or `x` inside quotes is left alone
_SQL_TOKENS = re.compile(r"""'(?:[^']|'')*'|"(?:[^"]|"")*"|`([^`]+)`|(?<![\w@:])@(\w+)""")


def to_sqlite_sql(sql: str) -> str:
    """`proj.dataset.table` / `dataset.table` -> dataset.table, and @param -> :param."""
    def repl(m: re.Match) -> str:
        if m[1] is not None:
            return ".".join(m[1].split(".")[-2:])
        if m[2] is not None:
            return f":{m[2]}"
        return m[0]

    return _SQL_TOKENS.sub(repl, sql)


def _param(value: Any) -> Any:
    if isinstance(value, datetime):
        return pd.Timestamp(value).tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S.%f") \
            if value.tzinfo else value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    return value


def _coerce(df: pd.DataFrame, schema: list[Field]) -> pd.DataFrame:
    """Shape a DataFrame to the table schema and SQLite-friendly values."""
    missing = [f.name for f in schema if f.name not in df.columns and f.mode == "REQUIRED"]
    if missing:
        raise ValueError(f"Missing required columns {missing}")
    out = pd.DataFrame(index=df.index)
    for f in schema:
        col = df[f.name] if f.name in df.columns else pd.Series(None, index=df.index, dtype=object)
        if f.type == "TIMESTAMP":
            col = pd.to_datetime(col, utc=True).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif f.type == "DATE":
            col = pd.to_datetime(col).dt.strftime("%Y-%m-%d")
        elif f.type in ("BOOL", "BOOLEAN"):
            col = col.astype("boolean").astype("Int64")
        out[f.name] = col
    out = out.astype(object).where(out.notna(), None)
    nulls = [f.name for f in schema if f.mode == "REQUIRED" and out[f.name].isna().any()]
    if nulls:
        raise ValueError(f"NULLs in REQUIRED columns {nulls}")
    return out


class LocalBigQuery(Warehouse):
    def __init__(self, local_dir: str, storage: ObjectStorage):
        self.dir = Path(local_dir) / "bigquery"
        self.storage = storage

    def _dataset_file(self, dataset: str) -> Path:
        return self.dir / f"{dataset}.sqlite"

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:", timeout=30, isolation_level=None)
        for f in sorted(self.dir.glob("*.sqlite")):
            conn.execute(f'ATTACH DATABASE ? AS "{f.stem}"', (str(f),))
        return conn

    def _schema(self, conn: sqlite3.Connection, table_id: str) -> list[Field]:
        dataset, table = split_table_id(table_id)
        if dataset not in self.list_datasets():
            raise NotFound(f"Dataset {dataset!r} does not exist (run setup first)")
        row = conn.execute(f'SELECT schema FROM "{dataset}".{_META} WHERE table_name = ?', (table,)).fetchone()
        if row is None:
            raise NotFound(f"Table {table_id!r} does not exist (run setup first)")
        return [Field(**f) for f in json.loads(row[0])]

    def ensure_dataset(self, dataset: str) -> None:
        split_table_id(f"{dataset}.x")  # validates the name
        self.dir.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self._dataset_file(dataset))) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"CREATE TABLE IF NOT EXISTS {_META} (table_name TEXT PRIMARY KEY, schema TEXT NOT NULL)")
            conn.commit()

    def ensure_table(self, table_id, schema, partition_field=None, cluster_fields=None) -> None:
        dataset, table = split_table_id(table_id)
        if dataset not in self.list_datasets():
            raise NotFound(f"Dataset {dataset!r} does not exist (run ensure_dataset first)")
        with closing(self._connect()) as conn:
            existing = {r[1] for r in conn.execute(f'PRAGMA "{dataset}".table_info({table})')}
            if not existing:
                cols = ", ".join(
                    f"{f.name} {_SQLITE_TYPES[f.type.upper()]}" + (" NOT NULL" if f.mode == "REQUIRED" else "")
                    for f in schema
                )
                conn.execute(f'CREATE TABLE "{dataset}".{table} ({cols})')
            else:  # BigQuery allows adding NULLABLE columns; do the same
                for f in schema:
                    if f.name not in existing:
                        conn.execute(f'ALTER TABLE "{dataset}".{table} ADD COLUMN {f.name} {_SQLITE_TYPES[f.type.upper()]}')
            # Partition/cluster columns are what pipelines filter and replace by, so index them
            index_cols = [c for c in [partition_field, *(cluster_fields or [])] if c]
            if index_cols:
                conn.execute(f'CREATE INDEX IF NOT EXISTS "{dataset}".{table}__pc ON {table} ({", ".join(index_cols)})')
            conn.execute(
                f'INSERT OR REPLACE INTO "{dataset}".{_META} VALUES (?, ?)',
                (table, json.dumps([f.__dict__ for f in schema])),
            )

    def replace_from_dataframe(self, df: pd.DataFrame, table_id: str, match: dict[str, Any]) -> int:
        dataset, table = split_table_id(table_id)
        with closing(self._connect()) as conn:
            schema = self._schema(conn, table_id)
            names = {f.name for f in schema}
            unknown = set(match) - names
            if unknown:
                raise ValueError(f"match columns {sorted(unknown)} are not in {table_id}")
            rows = _coerce(df, schema)
            cols = ", ".join(f.name for f in schema)
            marks = ", ".join("?" * len(schema))
            where = " AND ".join(f"{k} = ?" for k in match) or "1 = 0"
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(f'DELETE FROM "{dataset}".{table} WHERE {where}', [_param(v) for v in match.values()])
                conn.executemany(f'INSERT INTO "{dataset}".{table} ({cols}) VALUES ({marks})',
                                 rows.itertuples(index=False, name=None))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    def _keyed_rows(self, conn: sqlite3.Connection, df: pd.DataFrame, table_id: str,
                    key: list[str]) -> tuple[list[Field], pd.DataFrame]:
        schema = self._schema(conn, table_id)
        unknown = set(key) - {f.name for f in schema}
        if not key or unknown:
            raise ValueError(f"key columns {sorted(unknown) or key} are not in {table_id}")
        rows = _coerce(df, schema)
        if rows.duplicated(subset=key).any():
            raise ValueError(f"Duplicate {key} values in the rows for {table_id}")
        return schema, rows

    def upsert_from_dataframe(self, df: pd.DataFrame, table_id: str, key: list[str]) -> int:
        dataset, table = split_table_id(table_id)
        with closing(self._connect()) as conn:
            schema, rows = self._keyed_rows(conn, df, table_id, key)
            cols = ", ".join(f.name for f in schema)
            marks = ", ".join("?" * len(schema))
            where = " AND ".join(f"{k} = ?" for k in key)
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(f'DELETE FROM "{dataset}".{table} WHERE {where}',
                                 rows[key].itertuples(index=False, name=None))
                conn.executemany(f'INSERT INTO "{dataset}".{table} ({cols}) VALUES ({marks})',
                                 rows.itertuples(index=False, name=None))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return len(rows)

    def insert_if_absent(self, df: pd.DataFrame, table_id: str, key: list[str]) -> int:
        dataset, table = split_table_id(table_id)
        with closing(self._connect()) as conn:
            schema, rows = self._keyed_rows(conn, df, table_id, key)
            cols = ", ".join(f.name for f in schema)
            marks = ", ".join("?" * len(schema))
            where = " AND ".join(f"{k} = ?" for k in key)
            sql = (f'INSERT INTO "{dataset}".{table} ({cols}) SELECT {marks} '
                   f'WHERE NOT EXISTS (SELECT 1 FROM "{dataset}".{table} WHERE {where})')
            conn.execute("BEGIN IMMEDIATE")
            try:
                before = conn.total_changes
                conn.executemany(sql, ((*row, *(row[rows.columns.get_loc(k)] for k in key))
                                       for row in rows.itertuples(index=False, name=None)))
                inserted = conn.total_changes - before
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return inserted

    def replace_from_uri(self, uri: str, table_id: str, match: dict[str, Any]) -> int:
        data = self.storage.download_bytes(uri)
        if uri.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(data))
        elif uri.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(data))
        else:
            raise ValueError(f"Only .parquet and .csv loads are supported, got {uri}")
        return self.replace_from_dataframe(df, table_id, match)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        with closing(self._connect()) as conn:
            return pd.read_sql_query(to_sqlite_sql(sql), conn, params={k: _param(v) for k, v in (params or {}).items()})

    def list_datasets(self) -> list[str]:
        return sorted(f.stem for f in self.dir.glob("*.sqlite")) if self.dir.is_dir() else []

    def list_tables(self, dataset: str) -> list[str]:
        if dataset not in self.list_datasets():
            raise NotFound(f"Dataset {dataset!r} does not exist")
        with closing(self._connect()) as conn:
            return [r[0] for r in conn.execute(f'SELECT table_name FROM "{dataset}".{_META} ORDER BY 1')]
