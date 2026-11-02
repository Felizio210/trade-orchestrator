"""Runs a trading step each time dukascopy_hourly has loaded new bars: approved strategies'
signals become orders with the broker in TRADING_BROKER (paper by default).
Logic lives in plugins/dlh_tasks.py and trading/executor.py. Run trading_gate (or
`python -m trading setup`) once first so the tables exist.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="trading_live",
    schedule=[Dataset(dlh_tasks.HOURLY_BARS)],
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one writer: insert_if_absent on BigQuery relies on it
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(minutes=20)},
    tags=["trading"],
) as dag:
    PythonOperator(task_id="step", python_callable=dlh_tasks.trading_step)
