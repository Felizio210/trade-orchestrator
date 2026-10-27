"""Run once by hand: creates the bucket, topic, subscription and BigQuery tables."""

import dlh_tasks
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id="dukascopy_setup", schedule=None, catchup=False, tags=["dukascopy"]) as dag:
    PythonOperator(task_id="setup", python_callable=dlh_tasks.setup)
