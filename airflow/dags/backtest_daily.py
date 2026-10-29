"""Re-runs the backtests in BACKTEST_DAILY_RUNS each time dukascopy_daily loads new bars.

One task per (strategy, timeframe); results land in the backtest dataset under
run_id daily-{strategy}-{timeframe}-{ds}. Logic lives in plugins/dlh_tasks.py.
"""

from datetime import timedelta

import dlh_tasks
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="backtest_daily",
    schedule=[Dataset(dlh_tasks.MARKET_BARS)],  # after every successful dukascopy_daily load
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(hours=1)},
    tags=["backtest"],
) as dag:
    setup = PythonOperator(task_id="setup", python_callable=dlh_tasks.backtest_setup)
    for strategy, timeframe in dlh_tasks.backtest_jobs():
        setup >> PythonOperator(
            task_id=f"{strategy}_{timeframe}",
            python_callable=dlh_tasks.backtest,
            op_kwargs={"strategy": strategy, "timeframe": timeframe},
        )
