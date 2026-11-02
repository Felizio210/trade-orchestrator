"""Approves or rejects strategies after each backtest_daily run, against the TRADING_MIN_* /
TRADING_MAX_* standards. Logic lives in plugins/dlh_tasks.py and trading/gate.py."""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="trading_gate",
    schedule=[Dataset(dlh_tasks.BACKTEST_RESULTS)],
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["trading"],
) as dag:
    setup = PythonOperator(task_id="setup", python_callable=dlh_tasks.trading_setup)
    gate = PythonOperator(task_id="gate", python_callable=dlh_tasks.trading_gate)

    setup >> gate
