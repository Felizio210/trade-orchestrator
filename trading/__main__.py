"""CLI. Same commands for DLH_ENV=local and DLH_ENV=gcp.

    python -m trading setup              # trading dataset + tables
    python -m trading gate               # judge the latest daily backtests against the standards
    python -m trading step               # trade every bar that closed since the last step
    python -m trading status             # approvals, account, open positions, kill switch
    python -m trading kill --reason "…"  # stop opening positions (open ones keep their stop/target)
    python -m trading resume             # turn the kill switch off
    python -m trading broker-check       # dlh-bridge: login, account, live price (no orders)
    python -m trading broker-check --order long   # + one minimum-size order, closed again
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone

import pandas as pd

from backtest.config import BacktestSettings
from cloud import Cloud
from ingestion.dukascopy.config import Settings
from trading import store
from trading.brokers.dukascopy import DukascopyBroker
from trading.config import TradingSettings
from trading.executor import KILL_SWITCH, step
from trading.gate import evaluate, setup


def _fmt(value, pct: bool = False) -> str:
    if value is None or value != value:  # None or NaN
        return "-"
    return f"{value * 100:.2f}%" if pct else f"{value:.2f}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m trading")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="Create the trading dataset and tables")
    sub.add_parser("gate", help="Approve or reject strategies from their latest daily backtest")
    sub.add_parser("step", help="Trade every bar that closed since the last step")
    sub.add_parser("status", help="Approvals, account, open positions and the kill switch")
    p_kill = sub.add_parser("kill", help="Stop opening positions")
    p_kill.add_argument("--reason", default="turned on by hand")
    sub.add_parser("resume", help="Turn the kill switch off")
    p_check = sub.add_parser("broker-check", help="Check dlh-bridge: login, account, live price")
    p_check.add_argument("--symbol", default=None, help="default: the first of DUKASCOPY_SYMBOLS")
    p_check.add_argument("--order", choices=("long", "short"),
                         help="also place one minimum-size order with a stop and target 0.5%% away")
    p_check.add_argument("--keep", action="store_true", help="leave that order open instead of closing it")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings, backtest, market = TradingSettings.from_env(), BacktestSettings.from_env(), Settings.from_env()
    cloud = Cloud.from_env()
    ds = settings.dataset
    print(f"Cloud: {cloud.describe()}")

    if args.cmd == "setup":
        for item in setup(settings, cloud):
            print(f"ready  {item}")

    elif args.cmd == "gate":
        rows = evaluate(settings, backtest, cloud)
        if not rows:
            print("(no daily backtest results yet)")
        for r in rows:
            print(f"{r['status']:<9} {r['strategy']}:{r['timeframe']}:{r['symbol']:<8} {r['run_id']}"
                  + (f"\n          {r['reasons']}" if r["reasons"] else ""))

    elif args.cmd == "step":
        result = step(settings, backtest, market, cloud)
        print(" ".join(f"{k}={_fmt(v) if isinstance(v, float) else v}" for k, v in result.items()))

    elif args.cmd == "status":
        approvals = store.read(cloud, ds, "approvals")
        print("\nAPPROVALS")
        if approvals.empty:
            print("(none; run `python -m trading gate`)")
        for r in approvals.sort_values(["status", "strategy"]).itertuples():
            print(f"{r.status:<9} {r.strategy}:{r.timeframe}:{r.symbol:<8} trades={r.n_trades} "
                  f"sharpe={_fmt(r.sharpe)} pf={_fmt(r.profit_factor)} dd={_fmt(r.max_drawdown, True)} "
                  f"avg_r={_fmt(r.avg_r)}" + (f"  ({r.reasons})" if isinstance(r.reasons, str) and r.reasons else ""))
        account = store.read(cloud, ds, "account")
        if not account.empty:
            a = account.sort_values("ts").iloc[-1]
            print(f"\nACCOUNT ({a['broker']}, {a['ts']:%Y-%m-%d %H:%M} UTC)\n"
                  f"balance={_fmt(a['balance'])} equity={_fmt(a['equity'])} open={a['open_positions']} "
                  f"open_risk={_fmt(a['open_risk'])}")
        positions = store.read(cloud, ds, "positions", "status = 'open'")
        print("\nOPEN POSITIONS")
        if positions.empty:
            print("(none)")
        for p in positions.itertuples():
            print(f"{p.position_id:<45} {p.side:<5} qty={p.qty:g} entry={p.entry_price:g} stop={p.stop:g} "
                  f"target={_fmt(p.target)}")
        controls = store.read(cloud, ds, "controls")
        kill = controls.loc[controls["key"] == KILL_SWITCH, "value"].dropna()
        print(f"\nKILL SWITCH: {'ON: ' + kill.iloc[0] if len(kill) and kill.iloc[0] else 'off'}")

    elif args.cmd in ("kill", "resume"):
        value = args.reason if args.cmd == "kill" else None
        store.write(cloud, ds, "controls",
                    [{"key": KILL_SWITCH, "value": value, "updated_at": datetime.now(timezone.utc)}])
        print(f"kill switch {'ON: ' + value if value else 'off'}")

    elif args.cmd == "broker-check":
        _broker_check(DukascopyBroker(settings.bridge_url), args.symbol or market.symbols[0], args.order, args.keep)


def _broker_check(broker: DukascopyBroker, symbol: str, side: str | None, keep: bool) -> None:
    health = broker._call("GET", "/health")
    print(f"bridge {broker.url}: connected={health['connected']} account={health['account_id']}")
    account = broker._call("GET", "/account")
    print(f"balance={account['balance']:.2f} equity={account['equity']:.2f} {account['currency']}")
    tick = broker._call("GET", "/tick", symbol=symbol)
    print(f"{symbol} bid={tick['bid']} ask={tick['ask']} at {pd.Timestamp(tick['time'], unit='ms', tz='UTC')}")
    if not side:
        return
    d = 1 if side == "long" else -1
    price = tick["ask"] if d > 0 else tick["bid"]
    stop, target = round(price * (1 - 0.005 * d), 2), round(price * (1 + 0.005 * d), 2)
    name = f"check_{symbol}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    resp = broker._call("POST", "/orders", {"label": name, "symbol": symbol, "side": side, "qty": 1e-9,
                                            "stop": stop, "target": target, "min_size": True})
    print(f"order {name}: {resp['state']} qty={resp.get('qty')} fill={resp.get('open_price')} "
          f"stop={resp.get('stop')} target={resp.get('target')} {resp.get('reason') or ''}")
    if keep or resp["state"] != "FILLED":
        return
    broker._call("POST", f"/orders/{name}/close", symbol=symbol)
    for _ in range(25):
        time.sleep(0.2)
        resp = broker._call("GET", f"/orders/{name}", symbol=symbol)
        if resp and resp["state"] == "CLOSED":
            break
    print(f"closed: {resp['state']} at {resp.get('close_price')} pnl={resp.get('pnl')} "
          f"commission={resp.get('commission')}")


if __name__ == "__main__":
    main()
