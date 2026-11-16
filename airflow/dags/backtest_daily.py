"""Re-runs the backtests in BACKTEST_DAILY_RUNS after each dukascopy_repair run that leaves no gaps.

One task per (strategy, timeframe); results land in the backtest dataset under
run_id daily-{strategy}-{timeframe}-{ds}. Logic lives in plugins/dlh_tasks.py.
Once every backtest succeeded it emits the backtest results Dataset, which triggers trading_gate.
BACKTEST_WALK_FORWARD=true makes them walk-forward runs (out-of-sample metrics). prune then thins
old equity curves to one row per day, whatever the backtests did.
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
    dag_id="backtest_daily",
    schedule=[Dataset(dlh_tasks.MARKET_BARS)],  # after dukascopy_repair has filled any gaps
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": dlh_tasks.on_failure, "retries": 1, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(hours=1)},
    tags=["backtest"],
) as dag:
    setup = PythonOperator(task_id="setup", python_callable=dlh_tasks.backtest_setup)
    notify = EmptyOperator(task_id="notify", outlets=[Dataset(dlh_tasks.BACKTEST_RESULTS)])
    prune = PythonOperator(task_id="prune", python_callable=dlh_tasks.backtest_prune, trigger_rule=TriggerRule.ALL_DONE)
    for strategy, timeframe in dlh_tasks.backtest_jobs():
        job = PythonOperator(
            task_id=f"{strategy}_{timeframe}",
            python_callable=dlh_tasks.backtest,
            op_kwargs={"strategy": strategy, "timeframe": timeframe},
        )
        setup >> job >> [notify, prune]
