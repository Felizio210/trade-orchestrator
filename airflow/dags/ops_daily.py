"""Daily summary at 09:45 UTC, after dukascopy_repair (09:00), backtest_daily and trading_gate:
the health view of `python -m ops status` sent to the alert channels (ops/alerts.py). Fails when
something needs attention, so it also shows red in the UI. Logic lives in plugins/dlh_tasks.py
and ops/health.py."""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="ops_daily",
    schedule="45 9 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # No failure callback: the summary is already the alert
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    tags=["ops"],
) as dag:
    PythonOperator(task_id="summary", python_callable=dlh_tasks.ops_summary)
