# source this before any airflow command:  source airflow/env.sh
P="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export DLH_PROJECT_DIR="$P"
export AIRFLOW_HOME="$P/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$P/airflow/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$P/airflow/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=60
export PATH="$P/.venv-airflow/bin:$PATH"
# macOS: forked Airflow workers segfault (SIGSEGV) without these
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export NO_PROXY="*"
set -a; [ -f "$P/.env" ] && . "$P/.env"; set +a
# With a Postgres metadata database (airflow/postgres_setup.sh sets DLH_AIRFLOW_DB_URL in .env), tasks
# run in parallel (LocalExecutor), so a long backtest or a slow download never holds up the
# 5-minute trading step. On the default SQLite database only one task runs at a time.
if [ -n "${DLH_AIRFLOW_DB_URL:-}" ]; then
  export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="$DLH_AIRFLOW_DB_URL"
  export AIRFLOW__CORE__EXECUTOR=LocalExecutor
  export AIRFLOW__CORE__PARALLELISM="${DLH_AIRFLOW_PARALLELISM:-4}"
else
  export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
fi
