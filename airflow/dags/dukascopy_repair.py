"""Daily gap repair: backfills any of the last DUKASCOPY_REPAIR_DAYS days missing from the lake.

dukascopy_daily has catchup=False, so days the scheduler was down are never run, and a run
that fails all its retries stays failed. This catches both. Logic lives in plugins/dlh_tasks.py.
Large historical backfills stay manual: `python -m ingestion.dukascopy backfill`.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

with DAG(
    dag_id="dukascopy_repair",
    # Well after dukascopy_daily (02:00 + up to 4 attempts) so the two don't hit Dukascopy together
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,  # each run already looks back over several days
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(hours=1), "execution_timeout": timedelta(hours=2)},
    tags=["dukascopy"],
) as dag:
    repair = PythonOperator(task_id="repair", python_callable=dlh_tasks.repair)
    repaired = ShortCircuitOperator(task_id="repaired", python_callable=dlh_tasks.repaired)
    # Emits the market bars Dataset only when bars were added, which triggers backtest_daily
    notify = EmptyOperator(task_id="notify", outlets=[Dataset(dlh_tasks.MARKET_BARS)])

    repair >> repaired >> notify
