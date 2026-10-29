# Backtest: implementation plan

Status: **done**: `python -m backtest --help`, and `airflow/dags/backtest_daily.py` re-runs
`BACKTEST_DAILY_RUNS` after every `dukascopy_daily` load (Airflow Dataset `dlh://market/bars`).
Strategies: `supply_demand`, `trendline`.

## Goal

Run trading strategies over the bars the DLH pipeline already loads, and store the
results so runs can be queried, compared and joined with market data. Same code for
`DLH_ENV=local` (SQLite) and `DLH_ENV=gcp` (BigQuery), through `cloud.bq`.

## Where things live

```
market.dukascopy_ticks          input (optional)
market_1min.dukascopy_bars      input
market_1h.dukascopy_bars        input   (any timeframe in DUKASCOPY_BAR_TIMEFRAMES)
backtest.runs                   output
backtest.trades                 output
backtest.equity                 output
backtest.metrics                output
```

- No separate database: `backtest` is its own dataset in the same warehouse
  (local: `backtest.sqlite` next to the others; GCP: same project and region as `market*`).
- Parity with production is checked in a dev GCP project (`.env.dev` with
  `DLH_ENV=gcp`, `GCP_PROJECT=dlh-dev`), not locally.

## Tables (`backtest/tables.py`)

| Table | Columns | Partition / cluster |
|---|---|---|
| `runs` | run_id, strategy, params_json, symbols, timeframe, start_date, end_date, initial_capital, risk_per_trade, spread_json, commission_rate, git_sha, data_cutoff (max ingested_at read), status, error, created_at, finished_at | -, `strategy` |
| `trades` | run_id, symbol, side, qty, entry_ts, entry_price, exit_ts, exit_price, exit_reason, stop, target, commission, pnl, r_multiple, date (= entry date) | `date`, `run_id, symbol` |
| `equity` | run_id, symbol, ts, date, position, equity, drawdown, side, lots, note (readable labels: LONG/SHORT, size in lots, entry / open / closed / flat) | `date`, `run_id, symbol` |
| `metrics` | run_id, symbol (`ALL` for portfolio), n_trades, total_return, cagr, sharpe, sortino, max_drawdown, win_rate, profit_factor, exposure | -, `run_id` |

Writes use `cloud.bq.replace_from_dataframe(df, table, match={"run_id": run_id})`
(already in the `Warehouse` interface, load job on GCP, so free and idempotent:
re-running a run_id replaces its rows instead of duplicating them).

## Package layout

```
backtest/
  __init__.py
  __main__.py   CLI: python -m backtest setup | run | list
  config.py     BacktestSettings.from_env() (dataset name, defaults for costs)
  tables.py     schemas + table ids for the backtest dataset
  data.py       load_bars(cloud, settings, symbol, timeframe, start, end) -> DataFrame
  strategies/   registry + one module per strategy (supply_demand, trendline)
  engine.py     simulate(bars, signals, costs) -> trades, equity
  metrics.py    compute(trades, equity, timeframe) -> metrics rows
  runner.py     run(): load -> signals -> simulate -> metrics -> write, records status
airflow/dags/backtest_daily.py   runs after each dukascopy_daily load
tests/test_backtest.py           engine/metrics unit tests + local end-to-end run
```

## Strategies

A strategy is a function `bars -> signals`: one row per bar with `signal` (+1 long,
-1 short, 0 none), `stop` and `target` (absolute prices). A signal on bar t only uses
bars up to t's close; the engine enters at t+1's open. One position per symbol at a
time; signals while in a position are ignored. Exits are the stop or the target.

**supply_demand**: fresh-zone retest.
1. ATR(`atr_len`). A *base* is 1..`base_max` candles with body <= `base_body_atr` x ATR;
   an *impulse* is the next candle with body >= `impulse_body_atr` x ATR.
2. Up impulse after a base -> demand zone: proximal = highest base body, distal = lowest
   base low. Down impulse -> supply zone: proximal = lowest base body, distal = highest base high.
3. A zone is live from the bar after its impulse. First touch of the proximal line ->
   long (demand) / short (supply); stop = distal -/+ `stop_buffer_atr` x ATR; target =
   `rr` x risk. The zone is then used up (fresh zones only).
4. A close beyond the distal line, or `zone_max_age` bars, deletes the zone. At most
   `max_zones` live zones per side.

**trendline**: lines through swing points.
1. Swing high/low = highest/lowest of `pivot_len` bars on each side, so it is only known
   `pivot_len` bars later (no lookahead).
2. Up line = last two swing lows, the second higher, at least `min_gap` bars apart.
   Down line = last two swing highs, the second lower. Projected forward bar by bar.
3. `mode=bounce`: a new touch (low within `touch_atr` x ATR of the up line, close above
   it) -> long; mirror for the down line -> short. Stop = line -/+ `stop_atr` x ATR.
   `mode=break`: close crossing below the up line -> short; above the down line -> long.
   Stop = `stop_atr` x ATR from the close. Target = `rr` x risk in both modes.
4. A close through a line ends it; the next swing point builds a new one.

Sizing: risk `risk_per_trade` of current equity per trade (qty = risk / stop distance).
PnL is in the quote currency (USD for EURUSD/XAUUSD; no conversion for JPY crosses yet).

## Engine rules (avoid optimistic results)

1. **No lookahead**: strategy sees bars up to close of bar t; orders fill at open of bar t+1.
2. **Spread**: bars are single-side (`DUKASCOPY_BAR_PRICE_SIDE`, default bid). The engine
   rebuilds bid and ask from a fixed spread in points (`BACKTEST_SPREAD_POINTS`, per-symbol
   `BACKTEST_SPREAD_OVERRIDES`): longs buy the ask and sell the bid, shorts the reverse.
   Stops/targets are checked on the side that would fill them. If a bar hits both, the
   stop wins; a gap through the stop fills at the open.
3. **Commission** per trade, configurable.
4. **Gaps**: positions carry over weekend gaps; equity is marked at the next available bar.
5. **Reproducibility**: every run stores `git_sha` and `data_cutoff`.

## Steps once approved

1. `backtest/tables.py`, `config.py`, `setup` command (creates dataset + tables).
2. `data.py` reading bars via `cloud.bq.query` with a `date` filter (partition pruning on BQ).
3. `strategies/` with `supply_demand` and `trendline`; `engine.py`; `metrics.py`.
4. `runner.py` + CLI `run`, writing all four tables by run_id.
5. Tests: engine on hand-made bars (known PnL), metrics, local end-to-end run.
6. Optional: Airflow DAG `backtest_daily`.

Later, only when needed: DuckDB as local backend (if SQLite gets slow/locks), Cloud Run
Jobs for parameter sweeps, Cloud SQL copy of `runs`/`metrics` for an app.
