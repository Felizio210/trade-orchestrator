"""Runs a trading step every 5 minutes, a minute after each 5min bar closes (:01, :06, ...):
approved strategies' signals on the bars that just closed become orders with the broker in
TRADING_BROKER (paper by default).

Bars under 1h come from the broker's live feed (dlh-bridge for TRADING_BROKER=dukascopy) on
top of the warehouse, which dukascopy_hourly fills once an hour, so a 5min signal is acted on
right after its bar closes. A timeframe/symbol whose next bar hasn't closed is skipped
without querying anything: 5min is pulled every run, 15min every third (:01, :16, :31, :46),
1h once its hour is in the warehouse. Nothing faster than 5min trades (trading.config
MIN_TIMEFRAME): on 1min bars four signals in five would be stale by the next run.
Logic lives in plugins/dlh_tasks.py and trading/executor.py. Run trading_gate (or
`python -m trading setup`) once first so the tables exist.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="trading_live",
    schedule="1-59/5 * * * *",
    start_date=pendulum.datetime(2026, 10, 30, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # one writer: insert_if_absent on BigQuery relies on it
    # No retries: the next run is the retry, and picks up every bar since the cursor
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=4)},
    tags=["trading"],
) as dag:
    PythonOperator(task_id="step", python_callable=dlh_tasks.trading_step)
