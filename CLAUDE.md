# CLAUDE.md

DLH: a market-data pipeline (Dukascopy → Cloud Storage → Pub/Sub → BigQuery) plus a backtester
that runs trading strategies over the loaded bars. Everything runs either against a **local
emulation** of GCP or **real GCP**, with the same code.

## Environments

- `DLH_ENV=local` (default): GCS, Pub/Sub and BigQuery are emulated on disk under
  `LOCAL_GCP_DIR` (`.local_gcp/`): buckets as folders, `pubsub.sqlite`, one
  `bigquery/<dataset>.sqlite` per dataset.
- `DLH_ENV=gcp`: real GCP; needs `GCP_PROJECT` (and credentials).
- Config comes from `.env` (loaded by python-dotenv). Templates: `.env.local.example`,
  `.env.gcp.example`. Every setting is documented there; keep both templates in sync when
  adding a variable.
- Two virtualenvs: `.venv` (Python 3.14, project + tests) and `.venv-airflow` (Python 3.12,
  Airflow 2.10). Don't mix them.

## Commands

```bash
.venv/bin/python -m pytest                     # all tests (local emulation, no network)
.venv/bin/python -m pytest tests/test_backtest.py -k name

python -m ingestion.dukascopy --help           # download | convert | check | setup | publish | load | run | backfill
python -m backtest --help                      # setup | strategies | run | list
python -m cloud --help                         # status | gcs | pubsub | bq | reset (inspect either backend)

airflow/start.sh                               # Airflow standalone, UI at http://localhost:8080
source airflow/env.sh                          # required before any other `airflow` command
```

## Layout

- `cloud/` — backend abstraction. `cloud/base.py` defines the interfaces (`ObjectStorage`,
  `PubSub`, `Warehouse`); `cloud/local/` and `cloud/gcp/` implement them. Get one with
  `Cloud.from_env()` and use `cloud.storage`, `cloud.pubsub`, `cloud.bq`.
- `ingestion/dukascopy/` — `client.py` (HTTP, rate limiting, typed errors),
  `downloader.py` (raw `.bi5` + `_manifest.json` per day), `converter.py`/`transform.py`
  (ticks → bars), `pipeline.py` (publish to GCS + Pub/Sub, load into BigQuery, backfill),
  `tables.py` (schemas), `instruments.py` (point scales per symbol).
- `backtest/` — `strategies/` (registry + `supply_demand`, `trendline`), `engine.py`
  (simulation), `metrics.py`, `runner.py` (load → signals → simulate → metrics → write),
  `tables.py`. Design notes in `docs/backtest_plan.md`.
- `airflow/dags/` — `dukascopy_daily` (02:00 UTC), `dukascopy_repair` (09:00 UTC gap fill),
  `dukascopy_setup` (manual), `backtest_daily` (triggered by the `dlh://market/bars` Dataset).
  DAG files stay thin: all logic lives in `airflow/plugins/dlh_tasks.py`, which imports
  heavy modules inside functions so the scheduler parses DAGs cheaply.

## Conventions that matter

- **Backend-agnostic code**: pipeline and backtest code only use the `cloud/base.py`
  interface methods, never `google.cloud.*` or `sqlite3` directly. Objects are `gs://` URIs
  (locally too), tables are `dataset.table`, SQL params are BigQuery-style `@name`.
- **SQL must run on both**: the local warehouse is SQLite. Stick to plain
  SELECT/WHERE/GROUP BY/JOIN; BigQuery-only functions (`TIMESTAMP_TRUNC`, `SAFE_DIVIDE`,
  `QUALIFY`, …) fail locally. Locally, TIMESTAMP/DATE come back as strings.
- **Idempotent writes**: loads and backtest results replace by key
  (`replace_from_dataframe(df, table, match={...})` — symbol/day for market data, `run_id`
  for backtests) so retries and re-runs never duplicate rows.
- **Datasets**: ticks in `market.dukascopy_ticks`; bars in one dataset per timeframe
  (`market_1min.dukascopy_bars`, `market_1h.…`); backtest output in `backtest.runs/trades/equity/metrics`.
- **Dukascopy rate limits**: keep `DUKASCOPY_MAX_WORKERS` low; the client backs off on 429.
  Saturdays (UTC) are skipped. Incomplete days are never published.
- **Strategies**: a function `bars -> signals` (`signal` ±1/0, `stop`, `target`); a signal on
  bar t may only use data up to t's close (no lookahead); the engine enters at t+1's open.
- Tests use `tmp_path` and fake Dukascopy clients; they must not hit the network or GCP.
- Never commit `.env`, `data/`, `.local_gcp/`, `.airflow/` or service-account JSON (all gitignored).
- Commits and PRs: no Claude attribution lines (no `Co-Authored-By: Claude`, no "Generated with Claude Code").
