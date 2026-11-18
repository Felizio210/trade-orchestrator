#!/usr/bin/env bash
# Starts Airflow (scheduler + web UI at http://localhost:8080). Ctrl+C to stop.
set -euo pipefail
cd "$(dirname "$0")/.."  # the project folder, whatever folder this is started from
source airflow/env.sh
airflow db migrate >/dev/null
# Pools the DAGs use (see airflow/plugins/dlh_tasks.py): one slot each
airflow pools set dukascopy 1 "Dukascopy downloads: one at a time (rate limits)" >/dev/null
airflow pools set trading 1 "trading_live's step: never waits behind other tasks" >/dev/null
echo "executor: $AIRFLOW__CORE__EXECUTOR"
exec airflow standalone
