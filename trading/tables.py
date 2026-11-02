"""Warehouse tables for trading state, in their own dataset (default `trading`).

Unlike backtest results, these are small state tables written row by row: each has a
key, and writes either upsert by it or insert only if the key is absent
(Warehouse.upsert_from_dataframe / insert_if_absent). Ids are built from what defines
the record, so a retried step finds the same id instead of making a new one:

    signal_id = order_id = position_id = "{strategy}:{timeframe}:{symbol}:{bar_ts:%Y%m%dT%H%M}"
"""

from __future__ import annotations

from cloud import Field

APPROVALS_SCHEMA = [
    Field("strategy", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("status", "STRING", "REQUIRED"),  # approved | rejected
    Field("run_id", "STRING", "REQUIRED"),  # the backtest run it was judged on
    Field("params_json", "STRING", "REQUIRED"),  # the parameters trading uses, exactly as backtested
    Field("reasons", "STRING"),  # failed checks, "; "-separated
    Field("n_trades", "INT64"),
    Field("total_return", "FLOAT64"),
    Field("sharpe", "FLOAT64"),
    Field("profit_factor", "FLOAT64"),
    Field("max_drawdown", "FLOAT64"),
    Field("win_rate", "FLOAT64"),
    Field("avg_r", "FLOAT64"),
    Field("decided_at", "TIMESTAMP", "REQUIRED"),
]

SIGNALS_SCHEMA = [
    Field("signal_id", "STRING", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),  # bar date
    Field("strategy", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("bar_ts", "TIMESTAMP", "REQUIRED"),  # the bar whose close produced it
    Field("side", "STRING", "REQUIRED"),  # long | short
    Field("stop", "FLOAT64", "REQUIRED"),
    Field("target", "FLOAT64"),
    Field("close", "FLOAT64", "REQUIRED"),
    Field("status", "STRING", "REQUIRED"),  # ordered | skipped
    Field("reason", "STRING"),  # why it was skipped
    Field("created_at", "TIMESTAMP", "REQUIRED"),
]

ORDERS_SCHEMA = [
    Field("order_id", "STRING", "REQUIRED"),  # = signal_id; also the broker's client order id
    Field("date", "DATE", "REQUIRED"),  # signal bar date
    Field("strategy", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("side", "STRING", "REQUIRED"),
    Field("broker", "STRING", "REQUIRED"),
    Field("status", "STRING", "REQUIRED"),  # pending | filled | rejected | cancelled
    Field("stop", "FLOAT64", "REQUIRED"),
    Field("target", "FLOAT64"),
    Field("risk", "FLOAT64", "REQUIRED"),  # amount lost if the stop is hit; qty follows from the fill
    Field("signal_ts", "TIMESTAMP", "REQUIRED"),
    Field("qty", "FLOAT64"),
    Field("fill_ts", "TIMESTAMP"),
    Field("fill_price", "FLOAT64"),
    Field("commission", "FLOAT64"),
    Field("broker_order_id", "STRING"),
    Field("reason", "STRING"),  # why it was rejected or cancelled
    Field("created_at", "TIMESTAMP", "REQUIRED"),
    Field("updated_at", "TIMESTAMP", "REQUIRED"),
]

POSITIONS_SCHEMA = [
    Field("position_id", "STRING", "REQUIRED"),  # = order_id
    Field("date", "DATE", "REQUIRED"),  # entry date
    Field("strategy", "STRING", "REQUIRED"),
    Field("timeframe", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("side", "STRING", "REQUIRED"),
    Field("status", "STRING", "REQUIRED"),  # open | closed
    Field("qty", "FLOAT64", "REQUIRED"),
    Field("entry_ts", "TIMESTAMP", "REQUIRED"),
    Field("entry_price", "FLOAT64", "REQUIRED"),
    Field("stop", "FLOAT64", "REQUIRED"),
    Field("target", "FLOAT64"),
    Field("risk", "FLOAT64", "REQUIRED"),
    Field("entry_commission", "FLOAT64", "REQUIRED"),
    Field("exit_ts", "TIMESTAMP"),
    Field("exit_price", "FLOAT64"),
    Field("exit_reason", "STRING"),  # stop | target
    Field("commission", "FLOAT64"),  # entry + exit
    Field("pnl", "FLOAT64"),  # after spread and commission
    Field("r_multiple", "FLOAT64"),
    Field("updated_at", "TIMESTAMP", "REQUIRED"),
]

CURSORS_SCHEMA = [
    Field("timeframe", "STRING", "REQUIRED"),
    Field("symbol", "STRING", "REQUIRED"),
    Field("last_bar_ts", "TIMESTAMP", "REQUIRED"),  # newest bar a step has processed
    Field("updated_at", "TIMESTAMP", "REQUIRED"),
]

ACCOUNT_SCHEMA = [
    Field("ts", "TIMESTAMP", "REQUIRED"),  # when the step ran
    Field("date", "DATE", "REQUIRED"),
    Field("broker", "STRING", "REQUIRED"),
    Field("balance", "FLOAT64", "REQUIRED"),  # realized
    Field("equity", "FLOAT64", "REQUIRED"),  # balance + open PnL at the last bar seen
    Field("open_positions", "INT64", "REQUIRED"),
    Field("open_risk", "FLOAT64", "REQUIRED"),
    Field("kill_switch", "BOOL", "REQUIRED"),
]

CONTROLS_SCHEMA = [
    Field("key", "STRING", "REQUIRED"),  # kill_switch
    Field("value", "STRING"),  # empty/NULL = off, otherwise why it was turned on
    Field("updated_at", "TIMESTAMP", "REQUIRED"),
]

EVENTS_SCHEMA = [
    Field("event_id", "STRING", "REQUIRED"),
    Field("ts", "TIMESTAMP", "REQUIRED"),
    Field("date", "DATE", "REQUIRED"),
    Field("kind", "STRING", "REQUIRED"),  # approved | rejected | revoked | ordered | filled | closed | ...
    Field("ref", "STRING"),  # the signal/order/position id or strategy it is about
    Field("message", "STRING", "REQUIRED"),
]

# table -> (schema, partition field, cluster fields, key)
TABLES = {
    "approvals": (APPROVALS_SCHEMA, None, ["strategy"], ["strategy", "timeframe", "symbol"]),
    "signals": (SIGNALS_SCHEMA, "date", ["strategy", "symbol"], ["signal_id"]),
    "orders": (ORDERS_SCHEMA, "date", ["status", "symbol"], ["order_id"]),
    "positions": (POSITIONS_SCHEMA, "date", ["status", "symbol"], ["position_id"]),
    "cursors": (CURSORS_SCHEMA, None, None, ["timeframe", "symbol"]),
    "account": (ACCOUNT_SCHEMA, "date", None, ["ts"]),
    "controls": (CONTROLS_SCHEMA, None, None, ["key"]),
    "events": (EVENTS_SCHEMA, "date", ["kind"], ["event_id"]),
}


def table_id(dataset: str, table: str) -> str:
    return f"{dataset}.{table}"


def schema_for(table: str) -> list[Field]:
    return TABLES[table][0]


def key_for(table: str) -> list[str]:
    return TABLES[table][3]
