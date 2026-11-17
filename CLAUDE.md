# CLAUDE.md

DLH: a market-data pipeline (Dukascopy → Cloud Storage → Pub/Sub → BigQuery), a backtester
that runs trading strategies over the loaded bars, and a trading layer that trades the strategies
whose backtest passes the standards (paper broker for now). Everything runs either against a **local
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
python -m backtest --help                      # setup | strategies | run [--walk-forward] | list | prune
python -m trading --help                       # setup | gate | step | status | drift | reconcile | kill | resume | broker-check
python -m ops status                           # health: data, lake, Airflow, approvals, broker, quality, drift
python -m ops alert-test                       # send a test alert (ALERT_* in .env)
bridge/run.sh                                  # dlh-bridge (JForex SDK, Java 17 + Maven) for TRADING_BROKER=dukascopy
bridge/install_launchd.sh                      # run dlh-bridge as a launchd service (restarts itself)
python -m cloud --help                         # status | gcs | pubsub | bq | reset (inspect either backend)

airflow/start.sh                               # Airflow standalone, UI at http://localhost:8080 (creates the pools)
airflow/postgres_setup.sh                      # once: Postgres metadata DB -> LocalExecutor (parallel tasks)
source airflow/env.sh                          # required before any other `airflow` command
```

## Layout

- `cloud/` — backend abstraction. `cloud/base.py` defines the interfaces (`ObjectStorage`,
  `PubSub`, `Warehouse`); `cloud/local/` and `cloud/gcp/` implement them. Get one with
  `Cloud.from_env()` and use `cloud.storage`, `cloud.pubsub`, `cloud.bq`.
- `ingestion/dukascopy/` — `client.py` (HTTP, rate limiting, typed errors),
  `downloader.py` (raw `.bi5` + `_manifest.json` per day), `converter.py`/`transform.py`
  (ticks → bars), `pipeline.py` (publish to GCS + Pub/Sub, load into BigQuery, backfill),
  `tables.py` (schemas), `instruments.py` (point scales per symbol), `market_hours.py` (when
  FX/metals trade, in New York time, with the holiday and early-close calendar), `quality.py`
  (checks after each bars load → `{BQ_DATASET}.quality`).
- `backtest/` — `strategies/` (registry + `supply_demand`, `trendline`, `silver_bullet`, `breakout`; each
  has DEFAULTS and a GRID for walk-forward), `engine.py` (simulation), `metrics.py`, `runner.py`
  (load → signals → simulate → metrics → write; walk-forward, stuck-run cleanup, equity pruning),
  `tables.py`. Metrics rows have a `scope`: full | oos (walk-forward) | recent (last
  BACKTEST_RECENT_DAYS). Design notes in `docs/backtest_plan.md`.
- `trading/` — `gate.py` (backtest metrics → approvals; out-of-sample and recent-window checks),
  `executor.py` (one step: broker health → new bars → broker sync → reconcile → signals → risk →
  orders; skips while the broker is down), `brokers/` (`base.py` interface, `dukascopy.py` over
  dlh-bridge, `paper.py` with the engine's fill rules), `risk.py` (incl. per-symbol risk),
  `drift.py` (live trades vs the approved backtest → `trading.drift`), `store.py` (records +
  keyed reads/writes), `tables.py`. Design notes in `docs/trading_plan.md`.
- `ops/` — `alerts.py` (Telegram / webhook / email, throttled; the Airflow failure callback),
  `health.py` (the status view and the daily summary).
- `bridge/` — dlh-bridge: one Java file on the JForex SDK that logs in to the Dukascopy account
  and serves orders/positions/account/ticks/bars on 127.0.0.1 (`TRADING_BRIDGE_URL`). SDK calls must
  run on the strategy thread (`IContext.executeTask`); JForex amounts are millions, the HTTP
  API uses units.
- `airflow/dags/` — `dukascopy_hourly` (hourly at :10, the only scheduled download:
  `finalize` publishes yesterday to the lake once it's complete, `load` drains it into
  BigQuery, `intraday` loads today's closed hours as provisional bars, `freshness` fails when the newest
  bars are more than `DUKASCOPY_MAX_LAG_HOURS` market hours old), `dukascopy_repair`
  (09:00 UTC gap fill over the last days), `dukascopy_setup` (manual), `backtest_daily`
  (triggered by the `dlh://market/bars` Dataset, which only `dukascopy_repair` emits, once the
  repair window has no gaps), `trading_gate` (on `dlh://backtest/metrics` from `backtest_daily`)
  then `drift`, `trading_live` (every 5 minutes; timeframes up to 1h also take the broker's live
  bars from dlh-bridge's `/bars`; own pool, highest priority) and `ops_daily` (09:45 UTC health
  summary to the alert channels). Every DAG alerts on a task's final failure. The DAGs always
  build from ticks; `DUKASCOPY_BAR_SOURCE=candles` only affects the CLI.
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
  for backtests) so retries and re-runs never duplicate rows. Trading state tables use
  `upsert_from_dataframe` / `insert_if_absent` by key, with deterministic ids
  (`{strategy}:{timeframe}:{symbol}:{bar_ts}`): an order is only placed if its signal row was
  actually inserted. On BigQuery that relies on one writer (`max_active_runs=1`).
- **Datasets**: ticks in `market.dukascopy_ticks`; bars in one dataset per timeframe
  (`market_1min.dukascopy_bars`, `market_1h.…`); backtest output in `backtest.runs/trades/equity/metrics`;
  trading state in one dataset per broker, `{TRADING_DATASET}_{TRADING_BROKER}` (`trading_paper`,
  `trading_dukascopy`), so paper and broker trading never share orders, cursors, approvals or the
  kill switch. A step on a broker's dataset that doesn't exist yet creates it and runs the gate for it.
- **Dukascopy rate limits**: keep `DUKASCOPY_MAX_WORKERS` low; the client backs off on 429.
  Saturdays (UTC) are skipped. Incomplete days are never published to the lake; the only
  partial data is `load_intraday`, which writes today's closed bars straight to BigQuery
  until the next day's `finalize` replaces them.
- **Strategies**: a function `bars -> signals` (`signal` ±1/0, `stop`, `target`); a signal on
  bar t may only use data up to t's close (no lookahead); the engine enters at t+1's open.
  The paper broker must keep the engine's fill rules: `test_paper_trades_match_the_backtest_engine`
  checks they produce the same trades.
- Tests use `tmp_path` and fake Dukascopy clients; they must not hit the network or GCP.
  CI (`.github/workflows/tests.yml`) runs them on every push from `requirements.lock`;
  regenerate it with `.venv/bin/pip freeze > requirements.lock` after changing requirements.
- **Market hours** live in `ingestion/dukascopy/market_hours.py` only; anything that asks "should
  this hour have data?" uses it.
- **Column names**: avoid BigQuery reserved words (`window`, `rows`, `range`, ...): the local
  SQLite accepts them, BigQuery doesn't.
- Never commit `.env`, `data/`, `.local_gcp/`, `.airflow/` or service-account JSON (all gitignored).
- Commits and PRs: no Claude attribution lines (no `Co-Authored-By: Claude`, no "Generated with Claude Code").
