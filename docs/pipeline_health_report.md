# DLH pipeline health report

16 Nov 2026, branch `backtest` at `fb21430`, local emulation (`DLH_ENV=local`).

## Summary

The code is in better shape than the running system. The test suite passed before I touched
anything (102 tests) and all four CLIs work, but the Airflow logs and the warehouse show the
pipeline has not been running since 5 Nov, and two things stop it from recovering on its own:

- **Dukascopy is not reachable from this network right now.** The connection to
  `datafeed.dukascopy.com` times out while the rest of the internet works. Nothing in the code
  can fix that; it needs another network, a VPN, or the GCP deployment.
- **Nothing is running.** Airflow and dlh-bridge are both stopped. The newest bar in the
  warehouse is 5 Nov 00:55 UTC.

I found five defects in the code and fixed all of them, with tests. Nothing is committed.

## What I checked

| Check | Result |
|---|---|
| Test suite before changes | 102 passed |
| Test suite after changes | 107 passed (5 new) |
| `--help` for `ingestion.dukascopy`, `backtest`, `trading`, `cloud` | all work |
| `cloud status`, `backtest list`, `backtest strategies`, `trading status` on the real local data | all work |
| DAGs parse under the Airflow venv | 6 DAGs, no import errors |
| Airflow task logs, 29 Oct to 5 Nov | 269 failed task attempts, broken down below |
| Read every Python module and `Bridge.java` | 5 defects, listed below |
| `python -m ingestion.dukascopy check` | fails: connection timed out |

Not checked: real GCP (no credentials used), and dlh-bridge end to end (it needs the Dukascopy
login and a reachable Dukascopy).

## Defects fixed

### 1. The market-hours check ignored daylight saving (caused real failures)

`_market_open` in `airflow/plugins/dlh_tasks.py` hard-coded the summer hours in UTC: daily
break at 21:00, weekend from Friday 21:00 to Sunday 22:00. Gold follows New York time, so
since US clocks went back on 1 Nov the break is 22:00 UTC and the Sunday open is 23:00 UTC.

Effect: every day at 23:10 UTC the `intraday` task waited for a 22:00 hour that never has
ticks, used all its retries and failed. The log shows it on 4 Nov:
`2026-11-04 22h not published yet for ['XAUUSD'], will retry` on all four attempts. It would
have repeated every night until March, and `freshness` counted the same phantom hour.

Fix: the hour is converted to New York time and compared there. I also made the Christmas,
New Year's Day and Good Friday sessions count as closed, since on those days the task would
have failed every hour all day.

Verified against the two years of 1h bars in the warehouse (18,001 hours): the old rule
disagreed with the data on 521 hours (352 called open with no bars, 169 called closed that
have bars); the new rule never calls an hour closed that has bars, and the only hours it calls
open without bars are 44 hours of shortened sessions around US holidays (see "Left as it is").

### 2. A long outage left a permanent hole in the data (this is your current situation)

`dukascopy_repair` only looked at the last `DUKASCOPY_REPAIR_DAYS` (7) days. The scheduler has
been down for 11 days, so on restart the window would have been 9 to 15 Nov and 5 to 8 Nov
would never have been fetched. The repair would then have reported "no gaps", triggered
`backtest_daily`, and the backtests would have run over the hole. 5 Nov would have kept its
single provisional hour for good.

Fix: the repair window now stretches back to the newest day already in the lake
(`pipeline.repair_start`), capped by a new `DUKASCOPY_REPAIR_MAX_DAYS` (default 60, added to
both `.env` templates). With your real data it now computes 5 to 15 Nov instead of 9 to 15 Nov.
A new install with an empty lake is unaffected: long backfills stay manual.

### 3. The gate could mix metrics from two different backtests

`latest_metrics` in `trading/gate.py` picked the newest run with pandas `groupby().last()`,
which returns the last non-null value per column, not the last row. If the newest run had an
empty metric (for example `profit_factor` when no trade lost, or `sharpe` on a flat curve),
the value was silently taken from an older run while `run_id` and `params_json` came from the
newest. A strategy could be approved on a number its current backtest did not produce.

Fix: the newest run's whole row is taken.

### 4. A strategy that stopped being backtested kept its approval forever

The gate judged "the newest daily run" of every strategy/timeframe it had ever seen, however
old, and stamped the decision with a fresh `decided_at`. So removing a strategy from
`BACKTEST_DAILY_RUNS` while it was approved would have left it approved indefinitely, and
`TRADING_APPROVAL_MAX_AGE_DAYS` could never catch it, because that check reads `decided_at`.
Your data already has two such leftovers (`trendline:1h` and `silver_bullet:15min`, last run
2 and 3 Nov); both happen to be rejected on their metrics.

Fix: a newest run older than `TRADING_APPROVAL_MAX_AGE_DAYS` is rejected with the reason
`latest backtest is N days old`.

### 5. `finalize` could publish yesterday one hour short, permanently

At 00:10 UTC `finalize` publishes yesterday. Dukascopy publishes each hour a few minutes
after it ends, and an hour that is not out yet downloads as empty, which is not an error. If
23:00 was late, the day went to the lake without it, and because a day in the lake is never
fetched again, it stayed short. `intraday` has a guard for exactly this; `finalize` did not,
and it runs first.

This had not happened yet (2, 3 and 4 Nov all have their 23:00 bar). Fix: for the first three
hourly runs after midnight, an empty 23:00 while the market is open fails the task and the
next run retries; after that the day is accepted as it is.

### Also corrected

The `dukascopy_hourly` docstring said its Dataset triggers `trading_live`. It no longer does:
`trading_live` runs on its own 5-minute cron.

## What the Airflow logs show

Failed task attempts between 29 Oct and 5 Nov:

| Task | Failed attempts | Cause |
|---|---|---|
| `trading_live.step` | 229 | dlh-bridge not running: `Connection refused` on 127.0.0.1:8765. 193 of the 200 logged runs. |
| `dukascopy_hourly.intraday` | 21 | 8 from the daylight-saving bug (fixed); the rest are network errors reaching Dukascopy (DNS, SSL interception, timeouts, connection refused) |
| `dukascopy_daily`, `dukascopy_intraday` | 14 | older DAGs that no longer exist; network errors |
| `dukascopy_repair.repair` | 3 | network errors reaching Dukascopy |
| `dukascopy_hourly.freshness` | 1 | correct: the bars really were 4 hours stale on 5 Nov |
| `trading_gate.setup` | 1 | `No module named 'trading'`, from before the import path fix; not reproducible now |

The scheduler log also has repeated "heartbeat exceeded" and zombie-task errors with gaps of
15 to 30 minutes. That is the laptop sleeping, not a bug.

## Current state, and what you need to do

Nothing below can be done from code, and none of it has been done.

1. **Get Dukascopy reachable.** From this network the connection times out today, and the
   logs show it failing on and off since late October (including SSL interception, which
   points at the ISP). Check with:

```bash
.venv/bin/python -m ingestion.dukascopy check
```

2. **Fill the gap.** Once it is reachable, either start Airflow and let the 09:00 UTC repair
   do it (it now covers 5 to 15 Nov), or run it by hand:

```bash
.venv/bin/python -m ingestion.dukascopy backfill --start 2026-11-05 --end 2026-11-15
```

3. **Start Airflow** (`airflow/start.sh`). All approvals are 11 days old, so nothing will
   trade until `backtest_daily` and `trading_gate` have run again. That is the intended
   behaviour.
4. **Start dlh-bridge** (`bridge/run.sh`) if you want `TRADING_BROKER=dukascopy`, or set
   `TRADING_BROKER=paper`. With the bridge down every `trading_live` run fails.

State of the trading tables: no open positions, no pending orders, kill switch off, the 1h
cursor is at 3 Nov 05:00 UTC. Because the Dukascopy broker only acts on the newest bar,
restarting will not replay old signals.

## Left as it is

These are real but I did not change them; each is covered in `docs/improvement_proposals.md`.

- **Shortened holiday sessions.** On about ten days a year (Thanksgiving, 24 and 31 Dec,
  4 July, US Monday holidays) the market closes two to four hours early. `intraday` will fail
  for those hours and recover by itself. No data is lost.
- **`probe-supply_demand-5min` is stuck in `running`** in `backtest.runs`: a run that was
  killed. It is ignored by the gate (not a `daily-` run).
- **`backtest.sqlite` is 413 MB** after three days of daily runs, almost all of it the
  per-bar equity curves (2.2 million rows). It will keep growing by a run's worth every day.
- **The `dlh://market/bars/hourly` Dataset** is still emitted and nothing listens to it.

## Files changed

```
airflow/plugins/dlh_tasks.py      market hours, finalize guard, repair window
airflow/dags/dukascopy_hourly.py  docstring
ingestion/dukascopy/pipeline.py   repair_start()
trading/gate.py                   newest-run selection, stale-run rejection
.env.local.example, .env.gcp.example   DUKASCOPY_REPAIR_MAX_DAYS
tests/test_pipeline_cloud.py      4 new tests, 1 corrected
tests/test_trading.py             1 new test
```

One existing test was changed: `test_freshness_ignores_closed_market_hours` asserted that the
market reopened at 22:00 UTC on Sunday 1 Nov 2026. That was the bug; your own bars show the
first bar that day at 23:00.
