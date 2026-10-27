"""BigQuery via google-cloud-bigquery.

replace_* loads into a short-lived staging table, then swaps rows in a single
multi-statement transaction (DELETE matching rows + INSERT from staging), so readers
never see a half-replaced partition and re-runs are idempotent.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from google.api_core import exceptions
from google.cloud import bigquery

from cloud.base import NotFound, Warehouse, split_table_id

_STAGING_TTL = timedelta(hours=2)


def query_parameter(name: str, value: Any) -> bigquery.ScalarQueryParameter:
    # bool before int: bool is a subclass of int. datetime before date: same reason.
    if isinstance(value, bool):
        type_ = "BOOL"
    elif isinstance(value, int):
        type_ = "INT64"
    elif isinstance(value, float):
        type_ = "FLOAT64"
    elif isinstance(value, datetime):
        type_ = "TIMESTAMP"
    elif isinstance(value, date):
        type_ = "DATE"
    else:
        type_, value = "STRING", str(value)
    return bigquery.ScalarQueryParameter(name, type_, value)


class BigQueryWarehouse(Warehouse):
    def __init__(self, project: str, location: str | None = None):
        self.project = project
        self.location = location
        self.client = bigquery.Client(project=project, location=location)

    def _ref(self, table_id: str) -> str:
        split_table_id(table_id)
        return f"{self.project}.{table_id}"

    def ensure_dataset(self, dataset: str) -> None:
        ds = bigquery.Dataset(f"{self.project}.{dataset}")
        if self.location:
            ds.location = self.location
        self.client.create_dataset(ds, exists_ok=True)

    def ensure_table(self, table_id, schema, partition_field=None, cluster_fields=None) -> None:
        fields = [bigquery.SchemaField(f.name, f.type, mode=f.mode) for f in schema]
        try:
            table = self.client.get_table(self._ref(table_id))
        except exceptions.NotFound:
            table = bigquery.Table(self._ref(table_id), schema=fields)
            if partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY, field=partition_field
                )
            if cluster_fields:
                table.clustering_fields = cluster_fields
            try:
                self.client.create_table(table)
            except exceptions.NotFound as exc:
                raise NotFound(f"Dataset for {table_id!r} does not exist") from exc
            return
        existing = {f.name for f in table.schema}
        new = [f for f in fields if f.name not in existing]
        if new:  # BigQuery allows appending NULLABLE columns
            table.schema = [*table.schema, *new]
            self.client.update_table(table, ["schema"])

    def _staging_ref(self, table_id: str) -> str:
        dataset, table = split_table_id(table_id)
        return f"{self.project}.{dataset}._staging_{table}_{uuid.uuid4().hex[:8]}"

    def _target(self, table_id: str) -> bigquery.Table:
        try:
            return self.client.get_table(self._ref(table_id))
        except exceptions.NotFound as exc:
            raise NotFound(f"Table {table_id!r} does not exist (run setup first)") from exc

    def _swap_in(self, staging: str, target: bigquery.Table, match: dict[str, Any]) -> int:
        try:
            staging_table = self.client.get_table(staging)
            staging_table.expires = datetime.now(timezone.utc) + _STAGING_TTL  # safety net if we crash
            self.client.update_table(staging_table, ["expires"])
            unknown = set(match) - {f.name for f in target.schema}
            if unknown:
                raise ValueError(f"match columns {sorted(unknown)} are not in {target.table_id}")
            cols = ", ".join(f"`{f.name}`" for f in target.schema)
            where = " AND ".join(f"`{k}` = @{k}" for k in match) or "FALSE"
            script = f"""
                BEGIN TRANSACTION;
                DELETE FROM `{target.full_table_id.replace(':', '.')}` WHERE {where};
                INSERT INTO `{target.full_table_id.replace(':', '.')}` ({cols})
                SELECT {cols} FROM `{staging}`;
                COMMIT TRANSACTION;
            """
            config = bigquery.QueryJobConfig(query_parameters=[query_parameter(k, v) for k, v in match.items()])
            self.client.query(script, job_config=config).result()
            return staging_table.num_rows
        finally:
            self.client.delete_table(staging, not_found_ok=True)

    def replace_from_uri(self, uri: str, table_id: str, match: dict[str, Any]) -> int:
        target = self._target(table_id)
        staging = self._staging_ref(table_id)
        if uri.endswith(".parquet"):
            config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
        elif uri.endswith(".csv"):
            config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, schema=target.schema
            )
        else:
            raise ValueError(f"Only .parquet and .csv loads are supported, got {uri}")
        config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        self.client.load_table_from_uri(uri, staging, job_config=config).result()
        return self._swap_in(staging, target, match)

    def replace_from_dataframe(self, df: pd.DataFrame, table_id: str, match: dict[str, Any]) -> int:
        target = self._target(table_id)
        staging = self._staging_ref(table_id)
        config = bigquery.LoadJobConfig(
            schema=target.schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        cols = [f.name for f in target.schema if f.name in df.columns]
        self.client.load_table_from_dataframe(df[cols], staging, job_config=config).result()
        return self._swap_in(staging, target, match)

    def query(self, sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
        config = bigquery.QueryJobConfig(
            query_parameters=[query_parameter(k, v) for k, v in (params or {}).items()]
        )
        return self.client.query(sql, job_config=config).to_dataframe()

    def list_datasets(self) -> list[str]:
        return sorted(d.dataset_id for d in self.client.list_datasets())

    def list_tables(self, dataset: str) -> list[str]:
        try:
            return sorted(t.table_id for t in self.client.list_tables(dataset)
                          if not t.table_id.startswith("_staging_"))
        except exceptions.NotFound as exc:
            raise NotFound(f"Dataset {dataset!r} does not exist") from exc
