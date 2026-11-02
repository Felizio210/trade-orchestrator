"""Hourly: loads the closed hours of the current UTC day into the bars tables, so trading sees
each new bar within the hour. Logic lives in plugins/dlh_tasks.py.

The rows are provisional: they go straight into BigQuery (no lake, no Pub/Sub), and the
02:00 dukascopy_daily run replaces them with the complete day. Emits the hourly bars
Dataset, which triggers trading_live.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dukascopy_hourly",
    # :10 gives Dukascopy a few minutes to publish the hour that just closed
    schedule="10 * * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,  # each run picks up every closed hour of the day anyway
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(minutes=20)},
    tags=["dukascopy", "trading"],
) as dag:
    hourly = PythonOperator(task_id="hourly", python_callable=dlh_tasks.hourly)
    notify = EmptyOperator(task_id="notify", outlets=[Dataset(dlh_tasks.HOURLY_BARS)])

    hourly >> notify
