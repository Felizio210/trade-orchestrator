"""Hourly Dukascopy load: the only scheduled download of market data. Logic lives in
plugins/dlh_tasks.py.

finalize publishes yesterday's complete day to the lake and Pub/Sub (once, at 00:10, from
the tick hours already on disk) and load drains it into BigQuery. intraday loads the closed
hours of today straight into the bars tables as provisional rows, which tomorrow's finalize
replaces. dukascopy_repair (09:00) fills anything finalize missed.

intraday runs whatever finalize did, so trading never waits on yesterday; a failed finalize
or load still marks the run failed. Emits the hourly bars Dataset, which triggers trading_live.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="dukascopy_hourly",
    # :10 gives Dukascopy a few minutes to publish the hour that just closed
    schedule="10 * * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,  # each run picks up every closed hour of the day anyway
    max_active_runs=1,  # one Dukascopy client at a time (rate limits), one writer
    default_args={"retries": 3, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(minutes=20)},
    tags=["dukascopy", "trading"],
) as dag:
    # No retries: they would hold up intraday; the next hourly run is the retry
    finalize = PythonOperator(task_id="finalize", python_callable=dlh_tasks.finalize, retries=0)
    load = PythonOperator(task_id="load", python_callable=dlh_tasks.load)
    # After finalize, so at 00:10 it finds yesterday already in the lake and leaves it alone
    intraday = PythonOperator(task_id="intraday", python_callable=dlh_tasks.intraday,
                              trigger_rule=TriggerRule.ALL_DONE)
    notify = EmptyOperator(task_id="notify", outlets=[Dataset(dlh_tasks.HOURLY_BARS)])

    finalize >> [load, intraday]
    intraday >> notify
