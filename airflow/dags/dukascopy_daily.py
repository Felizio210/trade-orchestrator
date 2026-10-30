"""Daily Dukascopy -> GCS -> Pub/Sub -> BigQuery. Logic lives in plugins/dlh_tasks.py."""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

with DAG(
    dag_id="dukascopy_daily",
    schedule="0 2 * * *",  # each run handles its data interval's UTC day (the day before)
    start_date=pendulum.datetime(2024, 10, 16, tz="UTC"),
    catchup=False,  # backfill on purpose with `airflow dags backfill`
    max_active_runs=1,  # one Dukascopy client at a time (rate limits)
    default_args={"retries": 3, "retry_delay": timedelta(minutes=30), "execution_timeout": timedelta(hours=1)},
    tags=["dukascopy"],
) as dag:
    trading_day = ShortCircuitOperator(task_id="trading_day", python_callable=dlh_tasks.is_trading_day)
    download_publish = PythonOperator(task_id="download_publish", python_callable=dlh_tasks.download_publish)
    # Doesn't trigger backtest_daily: dukascopy_repair does, once earlier gaps are filled too
    load = PythonOperator(task_id="load", python_callable=dlh_tasks.load)

    trading_day >> download_publish >> load
