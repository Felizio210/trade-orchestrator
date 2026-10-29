"""Warehouse tables for backtest results.

Every table carries run_id, and every write replaces exactly one run_id, so re-running
a run never duplicates rows. Results live in their own dataset (default `backtest`),
next to the market datasets so they can be joined with the bars they were run on.
"""

from __future__ import annotations

from cloud import Field

RUNS_SCHEMA = [
    Field("run_id", "STRING", "REQUIRED"),
    Field("strategy", "STRING", "REQUIRED"),
    Field("params_json", "STRING", "REQUIRED"),
    Field("symbols", "STRING", "REQUIRED"),  # comma-separated
    Field("timeframe", "STRING", "REQUIRED"),
    Field("start_date", "DATE", "REQUIRED"),
    Field("end_date", "DATE", "REQUIRED"),
    Field("initial_capital", "FLOAT64", "REQUIRED"),
    Field("risk_per_trade", "FLOAT64", "REQUIRED"),
    Field("spread_json", "STRING", "REQUIRED"),  # {"EURUSD": 10.0, ...} in points
    Field("commission_rate", "FLOAT64", "REQUIRED"),
    Field("git_sha", "STRING"),
    Field("data_cutoff", "TIMESTAMP"),  # latest ingested_at of the bars read
    Field("status", "STRING", "REQUIRED"),  # running | done | failed
    Field("error", "STRING"),
    Field("created_at", "TIMESTAMP", "REQUIRED"),
    Field("finished_at", "TIMESTAMP"),
]

TRADES_SCHEMA = [
    Field("run_id", "STRING", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),  # entry day, the partition
    Field("symbol", "STRING", "REQUIRED"),
    Field("side", "STRING", "REQUIRED"),  # long | short
    Field("qty", "FLOAT64", "REQUIRED"),
    Field("entry_ts", "TIMESTAMP", "REQUIRED"),
    Field("entry_price", "FLOAT64", "REQUIRED"),
    Field("exit_ts", "TIMESTAMP", "REQUIRED"),
    Field("exit_price", "FLOAT64", "REQUIRED"),
    Field("exit_reason", "STRING", "REQUIRED"),  # stop | target | end
    Field("stop", "FLOAT64", "REQUIRED"),
    Field("target", "FLOAT64"),
    Field("commission", "FLOAT64", "REQUIRED"),
    Field("pnl", "FLOAT64", "REQUIRED"),  # after spread and commission
    Field("r_multiple", "FLOAT64", "REQUIRED"),  # pnl / amount risked
]

EQUITY_SCHEMA = [
    Field("run_id", "STRING", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("ts", "TIMESTAMP", "REQUIRED"),
    Field("position", "FLOAT64", "REQUIRED"),  # signed qty
    Field("equity", "FLOAT64", "REQUIRED"),
    Field("drawdown", "FLOAT64", "REQUIRED"),  # equity / running peak - 1
]

METRICS_SCHEMA = [
    Field("run_id", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),  # ALL = every symbol together
    Field("n_trades", "INT64", "REQUIRED"),
    Field("total_return", "FLOAT64"),
    Field("cagr", "FLOAT64"),
    Field("sharpe", "FLOAT64"),
    Field("sortino", "FLOAT64"),
    Field("max_drawdown", "FLOAT64"),
    Field("win_rate", "FLOAT64"),
    Field("profit_factor", "FLOAT64"),
    Field("avg_r", "FLOAT64"),
    Field("exposure", "FLOAT64"),  # fraction of bars with a position open
]

# table -> (schema, partition field, cluster fields)
TABLES = {
    "runs": (RUNS_SCHEMA, None, ["strategy"]),
    "trades": (TRADES_SCHEMA, "date", ["run_id", "symbol"]),
    "equity": (EQUITY_SCHEMA, "date", ["run_id", "symbol"]),
    "metrics": (METRICS_SCHEMA, None, ["run_id"]),
}


def table_id(dataset: str, table: str) -> str:
    return f"{dataset}.{table}"


def schema_for(table: str) -> list[Field]:
    return TABLES[table][0]
