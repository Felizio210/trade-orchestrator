#!/usr/bin/env bash
# Moves Airflow's metadata database to a local Postgres so tasks can run in parallel (LocalExecutor).
# Needs a running Postgres (brew services start postgresql@14). Safe to re-run.
#   airflow/postgres_setup.sh
# Then restart Airflow (airflow/start.sh). Run history stays in the old .airflow/airflow.db (SQLite);
# the new database starts empty. To go back: remove DLH_AIRFLOW_DB_URL from .env.
set -euo pipefail
P="$(cd "$(dirname "$0")/.." && pwd)"
DB=${DLH_AIRFLOW_DB_NAME:-dlh_airflow}
pg_isready -q || { echo "Postgres isn't running: brew services start postgresql@14"; exit 1; }
psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB'" | grep -q 1 || createdb "$DB"
"$P/.venv-airflow/bin/pip" install -q "psycopg2-binary>=2.9"
URL="postgresql+psycopg2://$(whoami)@localhost:5432/$DB"
if grep -q '^DLH_AIRFLOW_DB_URL=' "$P/.env" 2>/dev/null; then
  sed -i '' "s#^DLH_AIRFLOW_DB_URL=.*#DLH_AIRFLOW_DB_URL=$URL#" "$P/.env"
else
  printf '\n# Airflow metadata database (airflow/postgres_setup.sh): enables LocalExecutor\nDLH_AIRFLOW_DB_URL=%s\n' "$URL" >> "$P/.env"
fi
source "$P/airflow/env.sh"
airflow db migrate
echo "Airflow now uses $URL with $AIRFLOW__CORE__EXECUTOR; start it with airflow/start.sh"
