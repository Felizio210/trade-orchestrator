# DLH improvement proposals (draft)

16 Nov 2026. A draft for you to react to, not a plan. Each item says what the problem is, what
I would do, and roughly how big it is (S = an hour or two, M = a day, L = several days).
Evidence comes from the code, the Airflow logs and the local warehouse; see
`docs/pipeline_health_report.md` for the defects already fixed.

My recommended order is the first five items. They are the ones that decide whether the
system runs unattended and whether its approvals mean anything.

## Recommended first

### 1. Make failures reach you (S)

The pipeline was down for 11 days and 193 trading runs in a row failed, and the only place
that showed was the Airflow UI. Add an `on_failure_callback` in `default_args` that sends one
message (Telegram, email or Slack) per failed task, and a daily "all green / here is what
failed" summary. Without this, every other safeguard only works when you happen to look.

### 2. Run it somewhere that stays up and can reach Dukascopy (M)

Two causes account for nearly every failure in the logs, and neither is in the code:

- the laptop sleeps (scheduler heartbeat gaps of 15 to 30 minutes, zombie tasks);
- this network blocks or intercepts Dukascopy on and off (DNS failures, SSL interception,
  timeouts; fully blocked today).

The GCP path already exists in the code. The smallest step is one small always-on VM running
Airflow against `DLH_ENV=gcp`. If you want to stay local for now: a VPN for the datafeed plus
`caffeinate` (or a launchd job) to keep the scheduler awake.

### 3. Treat "broker down" as a state, not 193 errors (S to M)

With `TRADING_BROKER=dukascopy` and the bridge stopped, `trading_live` raises on the first
call every five minutes. Proposal: check `/health` first and skip the run with a clear
"broker unreachable" event, alert once when it goes down and once when it returns, and run
dlh-bridge under launchd (or systemd on a VM) so it restarts by itself.

### 4. Judge strategies out of sample (L)

This is the item with the most effect on real money. Today the gate approves on one two-year
backtest that includes the period the parameters were chosen on (the `breakout` defaults say
so in a comment). Two of eight strategy/timeframe pairs pass. Proposals, in order:

- gate on a recent window as well (for example the last 90 days must also be profitable), so
  a strategy that worked in 2025 and stopped is revoked;
- walk-forward: choose parameters on a training window, score on the next, roll forward, and
  gate on the stitched out-of-sample result;
- record which window each metric came from in `backtest.metrics`.

### 5. Compare live results with the backtest (M)

The system promises that an approved strategy trades as backtested, but nothing measures it.
Add a daily job that, per strategy, lines up live fills against the backtest's trades for the
same bars: entry slippage, missed and extra trades, PnL difference. Two known sources of
divergence to quantify:

- **1h signals run late.** 1h bars come only from the warehouse, which fills at about :10 to
  :15, so a 1h signal is ordered around 16 minutes after the bar closed, at the live price,
  while the backtest fills at the next bar's open. The bridge already serves 1h bars; letting
  1h use them like 5min and 15min do (`LIVE_BARS_BELOW` in `trading/executor.py`) removes it.
- **Costs are fixed.** One spread per symbol, no slippage, no overnight swap. Gold spreads
  widen around the daily break and news.

## Data pipeline

### 6. A holiday and early-close calendar (S)

The market-hours rule now handles daylight saving and the three full-day closures. About ten
shortened sessions a year (44 hours over the last two years) still make `intraday` fail for
two to four hours. A small table of early closes, or learning them from last year's bars,
finishes the job.

### 7. Mark empty days in the lake (S)

A day with no data (Christmas) publishes nothing, so `finalize` re-downloads its 24 empty
hours on every hourly run that day, and `repair` does it again for a week. An empty marker
object per symbol/day would make "checked, nothing there" visible and stop the refetching.

### 8. Record where each bar came from (S)

The CLI builds bars from Dukascopy's candle files (`DUKASCOPY_BAR_SOURCE=candles`), the DAGs
from ticks, and both write the same lake path and table. They should agree, but nothing
checks it and nothing records which one produced a day. Add a `source` column (or put it in
the manifest), and a test that both sources give the same bars for a sample day.

### 9. Data quality checks after each load (M)

Cheap checks that would have caught most of what went wrong so far: bars per day against the
expected session length, no gap between consecutive days, finalized day against the
provisional intraday rows it replaces, high >= low and open/close inside the range. Fail the
load task, or write to a `market.quality` table and alert.

### 10. Prune or thin the backtest equity table (S)

`backtest.sqlite` is 413 MB after three days. Every daily run stores a two-year per-bar
equity curve (one row per bar), and yesterday's run is never removed. On BigQuery this is
storage and scan cost; locally it is disk and slow queries. Keep the full curve only for the
newest daily run per strategy, and store older ones resampled to daily, or delete them after
N days.

### 11. The local warehouse stops at ten datasets (S)

`LocalBigQuery` attaches every dataset file to one SQLite connection, and SQLite allows ten.
You have six; each new bar timeframe adds one. Attach only the datasets a query names, or
fail with a clear message.

## Trading and risk

### 12. Reconcile with the broker (M)

The executor only asks the broker about orders it knows. A position opened by hand, or an
order that reached Dukascopy while the step died before recording it, is invisible to the
risk limits. A reconciliation task that lists the account's open positions and compares them
with `trading.positions` would close that. The executor's own docstring names the
"missed, not doubled" case; this is the other half.

### 13. Risk across strategies on the same symbol (M)

Both approved strategies trade XAUUSD on 1h. The limits count positions and total risk, but
two strategies long gold at once are one bet at double size. Options: a per-symbol risk cap,
or net exposure per symbol in `risk.check`.

### 14. Clean up runs stuck in `running` (S)

A killed backtest stays `running` forever (`probe-supply_demand-5min` is one). Mark runs
older than their timeout as `failed` at the start of the next run.

### 15. Secrets (S)

The demo login sits in `.env` in plain text. Fine for a demo account; before a live account,
move it to the macOS Keychain or Secret Manager and have `bridge/run.sh` read it from there.

## Engineering

### 16. Give the trading step its own slot (M)

Airflow runs with `SequentialExecutor` on SQLite: one task at a time across all DAGs. While
`backtest_daily` runs its two-year backtests, or `intraday` waits on retries with a slow
Dukascopy, the five-minute trading step queues behind them. `LocalExecutor` with Postgres
(or a pool that reserves a slot for `trading_live`) fixes it.

### 17. Continuous integration and pinned versions (S)

The tests are good and fast (107 tests, 16 seconds) but only run when someone remembers.
Add a GitHub Actions workflow that runs them on every push. `requirements.txt` uses `>=`
everywhere; pin versions (a lock file) so a pandas release cannot change bar building or
metrics unnoticed.

### 18. A one-page status view (M)

One command or page that answers "is it healthy?": newest bar per timeframe, last successful
run of each DAG, lake days missing, approvals and their age, bridge up or down, kill switch.
`python -m cloud status` and `python -m trading status` each cover part of it.

### 19. Small cleanups (S)

- The `dlh://market/bars/hourly` Dataset is emitted and nothing is scheduled on it; remove it
  or use it.
- The running code is the `backtest` branch, two commits ahead of `main`; merge it so `main`
  is what runs.
- `docs/system_overview.md` is untracked; commit it if it is meant to stay.

## Not proposed

Blocking trading when market data is stale: you decided earlier that stale data should only
fail the hourly freshness task, so it is left out here.
