# source this before any airflow command:  source airflow/env.sh
P="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
export DLH_PROJECT_DIR="$P"
export AIRFLOW_HOME="$P/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$P/airflow/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$P/airflow/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=60
export PATH="$P/.venv-airflow/bin:$PATH"
# macOS: forked Airflow workers segfault (SIGSEGV) without these
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export NO_PROXY="*"
set -a; [ -f "$P/.env" ] && . "$P/.env"; set +a
