"""CLI. Same commands for DLH_ENV=local and DLH_ENV=gcp.

    python -m backtest setup                     # backtest dataset + tables
    python -m backtest strategies                # strategies and their parameters
    python -m backtest run --strategy supply_demand --symbols EURUSD,XAUUSD --timeframe 1h \\
        --start 2025-01-01 --end 2026-09-30 --param rr=2.5 --param base_max=2
    python -m backtest list                      # latest runs with their results
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from backtest import strategies
from backtest.config import BacktestSettings
from backtest.runner import run, setup
from backtest.tables import table_id
from cloud import Cloud
from ingestion.dukascopy.config import Settings, _csv


def _params(pairs: list[str]) -> dict[str, str]:
    out = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep:
            raise SystemExit(f"--param needs key=value, got {pair!r}")
        out[key.strip()] = value.strip()
    return out


def _fmt(value, pct: bool = False) -> str:
    if value is None or value != value:  # None or NaN
        return "-"
    return f"{value * 100:.2f}%" if pct else f"{value:.2f}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m backtest")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup", help="Create the backtest dataset and tables")
    sub.add_parser("strategies", help="List strategies and their default parameters")

    p_run = sub.add_parser("run", help="Backtest a strategy and store the results")
    p_run.add_argument("--strategy", required=True, choices=sorted(strategies.STRATEGIES))
    p_run.add_argument("--symbols", help="Comma-separated, default DUKASCOPY_SYMBOLS")
    p_run.add_argument("--timeframe", default="1h", help="A loaded bar timeframe, default 1h")
    p_run.add_argument("--start", type=date.fromisoformat, required=True, help="UTC date")
    p_run.add_argument("--end", type=date.fromisoformat, required=True, help="UTC date inclusive")
    p_run.add_argument("--param", action="append", default=[], metavar="KEY=VALUE",
                       help="Override a strategy parameter; repeat for more")
    p_run.add_argument("--run-id", help="Re-run under an existing run_id (replaces its results)")

    p_list = sub.add_parser("list", help="Latest runs with their results")
    p_list.add_argument("--limit", type=int, default=20)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings, market = BacktestSettings.from_env(), Settings.from_env()

    if args.cmd == "strategies":
        for name, module in sorted(strategies.STRATEGIES.items()):
            print(f"{name}\n  " + "\n  ".join(f"{k} = {v}" for k, v in module.DEFAULTS.items()))
        return

    cloud = Cloud.from_env()
    print(f"Cloud: {cloud.describe()}")

    if args.cmd == "setup":
        for item in setup(settings, cloud):
            print(f"ready  {item}")

    elif args.cmd == "run":
        symbols = _csv(args.symbols) if args.symbols else market.symbols
        try:
            result = run(args.strategy, symbols, args.timeframe, args.start, args.end, settings, market, cloud,
                         params=_params(args.param), run_id=args.run_id)
        except ValueError as exc:
            print(f"error: {exc}")
            sys.exit(2)
        print(f"\nrun_id {result['run_id']}")
        print(f"{'SYMBOL':<10} {'TRADES':>6} {'RETURN':>9} {'MAX DD':>9} {'SHARPE':>7} {'WIN':>8} {'PF':>6} {'AVG R':>6}")
        for m in result["metrics"]:
            print(f"{m['symbol']:<10} {m['n_trades']:>6} {_fmt(m['total_return'], True):>9} "
                  f"{_fmt(m['max_drawdown'], True):>9} {_fmt(m['sharpe']):>7} {_fmt(m['win_rate'], True):>8} "
                  f"{_fmt(m['profit_factor']):>6} {_fmt(m['avg_r']):>6}")

    elif args.cmd == "list":
        runs = table_id(settings.dataset, "runs")
        metrics = table_id(settings.dataset, "metrics")
        df = cloud.bq.query(
            f"SELECT r.run_id, r.status, r.timeframe, r.start_date, r.end_date, m.symbol, m.n_trades, "
            f"m.total_return, m.max_drawdown, m.sharpe FROM `{runs}` r "
            f"LEFT JOIN `{metrics}` m ON m.run_id = r.run_id "
            f"WHERE r.run_id IN (SELECT run_id FROM `{runs}` ORDER BY created_at DESC LIMIT @limit) "
            "ORDER BY r.created_at DESC, m.symbol",
            {"limit": args.limit},
        )
        if df.empty:
            print("(no runs yet)")
        for row in df.itertuples():
            print(f"{row.run_id:<40} {row.status:<7} {row.timeframe:<5} {row.start_date}..{row.end_date} "
                  f"{row.symbol or '-':<8} trades={row.n_trades if row.n_trades == row.n_trades else '-'} "
                  f"return={_fmt(row.total_return, True)} dd={_fmt(row.max_drawdown, True)} "
                  f"sharpe={_fmt(row.sharpe)}")


if __name__ == "__main__":
    main()
