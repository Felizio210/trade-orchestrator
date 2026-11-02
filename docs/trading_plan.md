# Trading: what was built and how it works

Status: **step 1 done (paper trading)**. `python -m trading --help`; Airflow DAGs
`dukascopy_hourly`, `trading_gate`, `trading_live`. Next: Dukascopy demo account as a
broker (step 2, below).

## Goal

Trade a strategy only after its backtest passes your standards, using exactly the
parameters that were backtested, and keep every decision (approval, signal, order, fill,
exit) on record so a step can be retried or audited. Same code for `DLH_ENV=local` and
`DLH_ENV=gcp`.

## The flow

```
                          (daily)                          (hourly)
dukascopy_repair 09:00 ──> backtest_daily      dukascopy_hourly :10
  last 7 days gap-free       │ backtest.metrics  finalize: yesterday -> lake -> Pub/Sub
                             v                   load:     Pub/Sub -> market_* (final)
                         trading_gate            intraday: today's closed hours
                    metrics vs standards                   -> market_* (provisional)
                    -> trading.approvals                   │
                                                           v
                                                     trading_live
                                               new bars -> signals of approved
                                               strategies -> risk -> orders -> broker
                                                           │
                                       trading.signals / orders / positions / account / events
```

Airflow Datasets chain the DAGs, so each one runs right after what it depends on:

| Dataset | Emitted by | Triggers |
|---|---|---|
| `dlh://market/bars` | `dukascopy_repair` (window gap-free) | `backtest_daily` |
| `dlh://backtest/metrics` (new) | `backtest_daily` (all backtests succeeded) | `trading_gate` |
| `dlh://market/bars/hourly` (new) | `dukascopy_hourly` | `trading_live` |

## Where state lives

Everything is in the warehouse, in a new `trading` dataset next to `market_*` and
`backtest` (locally `.local_gcp/bigquery/trading.sqlite`; on GCP a BigQuery dataset). No
separate database: the volume is a few rows per hour from one writer, which BigQuery
handles fine, and the tables join directly with `backtest.trades` and the bars.

| Table | Key | What it holds |
|---|---|---|
| `approvals` | strategy, timeframe, symbol | approved / rejected, the backtest run and exact `params_json`, the metrics, failed checks in `reasons` |
| `signals` | signal_id | every signal of an approved strategy: ordered, or skipped with the reason |
| `orders` | order_id (= signal_id) | pending → filled / rejected / cancelled; risk, qty, fill price, commission |
| `positions` | position_id (= order_id) | open → closed; entry, stop, target, exit, pnl, R |
| `cursors` | timeframe, symbol | newest bar a step has processed |
| `account` | ts | balance, equity, open positions, open risk, kill switch, per step |
| `controls` | key | `kill_switch`: empty = off, otherwise why it is on |
| `events` | event_id | audit log: approved, revoked, tracking, ordered, filled, rejected, cancelled, closed, kill_switch |

## Mechanisms

### 1. Hourly data (`ingestion/dukascopy/pipeline.py: load_intraday`)

Dukascopy publishes one tick file per hour a few minutes after the hour ends. At :10 each
hour, `dukascopy_hourly`:

1. Takes the day of the hour that just closed (at 00:10 that's still yesterday, so 23h
   isn't missed), and downloads that day's hours **up to now only** (`download_day(...,
   until=)`). Hours already on disk are reused, so in practice it fetches one file per
   symbol.
2. Builds bars from them and keeps only bars that have **fully closed** (a 4h bar still
   forming is left out).
3. Writes them straight to `market_*.dukascopy_bars`, replacing that symbol/day. They skip
   the lake and Pub/Sub: they're **provisional**. A day already in the lake is never touched.
4. If an hour failed, or the hour that just closed is still empty while the market should
   be open, the task fails and Airflow retries 10 minutes later (up to 3 times). Weekends and
   the daily 21:00 UTC rollover are treated as closed.

Before that, the run's `finalize` task publishes **yesterday** to the lake and Pub/Sub if it
isn't there yet, from the tick hours already on disk (only missing hours are fetched), and
`load` drains it into BigQuery, replacing yesterday's provisional rows with the complete day.
That normally happens at 00:10; later runs only check the lake. `finalize` has no retries
(the next hourly run is the retry) and `intraday` runs whatever it did, so a late day never
holds up trading. `dukascopy_repair` at 09:00 fills anything older. There is no separate daily
download: every scheduled DAG builds from ticks, whatever `DUKASCOPY_BAR_SOURCE` says.

Backtests are unaffected: they only read up to yesterday. The "incomplete days are never
published" rule still holds for the lake.

**Delay:** a 1h bar is traded about 10–30 minutes after it closes. For paper trading this
doesn't matter: the paper broker fills at the next bar's open from the data, like the
backtest. For real money it would cost slippage; see step 2.

### 2. The gate (`trading/gate.py`, DAG `trading_gate`)

After each `backtest_daily` run, for every (strategy, timeframe, symbol):

1. Take the newest **done** run whose `run_id` starts with `daily-`. Ad-hoc runs from
   `python -m backtest run` (e.g. hand-tuned parameters) can never approve a strategy.
2. Check its per-symbol metrics against the standards (empty in `.env` = not checked):

   | Setting | Default | Rule |
   |---|---|---|
   | `TRADING_MIN_TRADES` | 50 | n_trades ≥ |
   | `TRADING_MIN_SHARPE` | 1.0 | sharpe ≥ |
   | `TRADING_MIN_PROFIT_FACTOR` | 1.3 | profit_factor ≥ (no losing trade counts as a pass) |
   | `TRADING_MAX_DRAWDOWN` | 0.15 | max_drawdown no worse than -15% |
   | `TRADING_MIN_AVG_R` | 0.1 | avg_r ≥ |
   | `TRADING_MIN_WIN_RATE` | (empty) | win_rate ≥ |
   | `TRADING_MIN_RETURN` | 0 | total_return ≥ |

   A missing metric fails its check. Every failure is written to `reasons`, e.g.
   `sharpe 0.40 < 1.00; max_drawdown -25.0% worse than -15.0%`.
3. Pass → `approved`, storing that run's `params_json`. Trading uses **those** parameters,
   so a later code change to the defaults can't silently trade untested parameters.
4. Approved before and fails now → `revoked` (logged in `events`). It opens nothing new.
   Its open positions run to their stop or target, and its pending orders are cancelled.
5. Safety net: an approval older than `TRADING_APPROVAL_MAX_AGE_DAYS` (3) is ignored, so a
   backtest pipeline that silently stopped doesn't leave strategies trading on old results.

### 3. A trading step (`trading/executor.py`, DAG `trading_live`)

Each run of `trading_live` (and `python -m trading step`) does:

1. **Load state:** fresh approvals, pending orders, open positions, cursors, kill switch.
2. **Cancel** pending orders of strategies that are no longer approved.
3. **For each timeframe/symbol** with an approved strategy or something open: load the
   bars that closed since the cursor, plus `TRADING_WARMUP_DAYS` (60) before them, and run
   each approved strategy over them with its approved parameters. Strategies have no
   lookahead, so the signal on a bar is the one the backtest would have produced there.
4. **Walk the new bars** of all timeframe/symbols in close-time order. On each bar:
   - the broker fills pending orders at this bar's open and applies stops/targets;
   - the daily loss limit is checked;
   - each signal goes through the risk checks; if it passes, it becomes a pending order
     that fills on the next bar's open.
5. **Write** orders, positions, cursors, an account snapshot and events.

The first time a timeframe/symbol is seen, the step only sets its cursor to the newest bar.
Trading starts with the bars that arrive after that, never on history.

### 4. No double orders

- **Deterministic ids:** `signal_id = order_id = position_id =
  "{strategy}:{timeframe}:{symbol}:{bar_ts:%Y%m%dT%H%M}"`, e.g.
  `supply_demand:1h:XAUUSD:20261030T1300`. The same signal always has the same id. A random
  UUID would give a retry a new id, so it would look like a new order.
- **Insert only if absent:** before an order is made, its signal row goes in with
  `Warehouse.insert_if_absent`. That runs as one statement (`INSERT … WHERE NOT EXISTS`
  locally, `MERGE … WHEN NOT MATCHED THEN INSERT` on BigQuery) and returns how many rows
  were inserted. The order is only submitted when that is 1. 0 means an earlier step
  already handled this signal.
- **Order written right after submit.** If a step dies between inserting the signal and
  writing the order, that signal is never ordered: **missed, not doubled**, which is the
  safe direction.
- **Replays are harmless:** a retry that reprocesses bars finds the same ids, already-closed
  positions and already-filled orders, and changes nothing (tested).
- **One writer:** on BigQuery the guarantee assumes a single step at a time.
  `trading_live` has `max_active_runs=1`.
- **Later, with a real broker:** the order_id is also sent as the broker's client order id,
  and brokers reject duplicates. That's a third guard.

### 5. Risk checks (`trading/risk.py`)

The first failing rule is the signal's skip reason (stored in `signals.reason`):

| Rule | Setting |
|---|---|
| kill switch off | `python -m trading kill / resume`, or tripped by the daily loss limit |
| this strategy has no open position or pending order on this timeframe/symbol | one at a time, as in the backtest |
| bar is the newest one (only for real-time brokers; paper replays every bar) | |
| balance > 0 | |
| open positions + pending orders < limit | `TRADING_MAX_OPEN_POSITIONS` = 5 |
| open risk + this trade's risk ≤ limit × balance | `TRADING_MAX_OPEN_RISK` = 5% |

**Daily loss limit:** when the realized loss on a UTC day reaches `TRADING_MAX_DAILY_LOSS`
(3%) of that day's opening balance, the kill switch turns on with the reason. It stays on
until `python -m trading resume`. Open positions keep their stop and target.

**Sizing** is the backtest's: risk = `BACKTEST_RISK_PER_TRADE` × balance, and qty = risk /
distance from fill to stop. Spread and commission also come from `BACKTEST_*`, so paper
trading costs the same as the backtest that approved it.

### 6. The paper broker (`trading/brokers/paper.py`)

It uses the same fill rules as `backtest/engine.py`:

- an order fills at the next bar's open: a long buys the ask, a short sells the bid. If the
  open is already past the stop, the order is rejected;
- stops and targets are checked on the side that would fill them; if a bar hits both, the
  stop wins; a gap fills at the open;
- commission is charged on entry and exit;
- balance = capital + realized PnL − entry commission of open positions (the engine's cash).

`test_paper_trades_match_the_backtest_engine` runs the paper account over the same bars as
the engine, in two steps as hourly runs would, and checks that every trade's entry, exit,
qty and PnL match. If live paper results drift from the backtest, it's the data or the
strategy, not the plumbing.

### 7. Broker interface (`trading/brokers/base.py`)

```python
class Broker(ABC):
    name: str
    realtime: bool                      # True: act only on the newest bar
    def submit(self, order) -> None     # market order with stop + target attached
    def sync(self, timeframe, symbol, bar, orders, positions) -> changed records
    def unrealized(self, position, bar) -> float
```

Stops and targets are attached to the order, so they sit **at the broker**: a crashed or
late step never leaves a position unprotected. A new broker is one class implementing these
methods, selected with `TRADING_BROKER`.

## Changes to existing code

| File | Change |
|---|---|
| `cloud/base.py` | `Warehouse.upsert_from_dataframe(df, table, key)` and `insert_if_absent(df, table, key)` |
| `cloud/local/bigquery.py` | SQLite versions, each in one transaction |
| `cloud/gcp/bigquery.py` | BigQuery versions: staging table, then DELETE+INSERT transaction / MERGE. **Not yet run against real BigQuery** |
| `ingestion/dukascopy/downloader.py` | `download_day(..., until=)`: only hours before `until`; recorded in the manifest |
| `ingestion/dukascopy/pipeline.py` | bar building moved out of `publish_day` into `_day_frames` (same output); new `load_intraday` |
| `airflow/plugins/dlh_tasks.py` | `intraday`, `trading_setup`, `trading_gate`, `trading_step`; new Dataset URIs |
| `airflow/dags/backtest_daily.py` | a `notify` task after all backtests emits `dlh://backtest/metrics` |
| `.env.*.example` | `TRADING_*` settings (same in both) |
| `CLAUDE.md` | trading layer, DAGs, conventions |

New: `trading/` (`config`, `tables`, `store`, `gate`, `risk`, `executor`, `brokers/`,
`__main__`), DAGs `dukascopy_hourly.py`, `trading_gate.py`, `trading_live.py`, and
`tests/test_trading.py` (14 tests). Full suite: 73 passed.

## Running it

```bash
python -m trading setup         # once: trading dataset + tables
python -m trading gate          # after a daily backtest exists
python -m trading step          # first run only starts tracking; later runs trade new bars
python -m trading status
python -m trading kill --reason "news day"   # / resume
```

In Airflow, unpause `dukascopy_hourly`, `trading_gate` and `trading_live`.
`trading_gate` creates the tables on its first run.

## Known limits

- **BigQuery versions** of the two new warehouse methods are written but not yet run
  against a real project. Check them in the dev project before `DLH_ENV=gcp`.
- **Paper sizing** doesn't round to lot steps (the backtest doesn't either). A real broker
  will.
- **Market hours** for the retry rule are a rough FX/metals calendar. A holiday hour retries
  3 times, then the run fails harmlessly; the next hour picks everything up.
- **One account across strategies:** sizing uses the shared balance, so with several
  strategies results differ from per-symbol backtests (each of which starts from its own
  capital). With one strategy on one symbol they match exactly (tested).
- **Late intraday hours:** if hour H arrives after hour H+1 was already traded, the live step
  never sees H (its cursor has passed it). `finalize` still publishes it for backtests.

## Step 2: Dukascopy demo account

Dukascopy has no Python API, so the plan is:

1. A small Java bridge on the official JForex SDK (or the open-source `dukas-proxy`, after
   checking its order support), exposing orders, positions and live prices over
   REST/WebSocket on localhost.
2. `trading/brokers/dukascopy.py` implementing `Broker` over that bridge (`realtime = True`):
   `submit` sends a market order with stop/target and the order_id as client id; `sync`
   reads fills and exits back.
3. Live prices from the same connection, so signals no longer wait for the hourly files.
   That removes the 10–30 minute delay.

Needs from you: the demo login in `.env` (`DUKASCOPY_DEMO_USER`, `DUKASCOPY_DEMO_PASSWORD`;
never committed), and the machine's IP registered on the account so login doesn't ask for a
PIN.
