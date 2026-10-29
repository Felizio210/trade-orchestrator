"""Performance metrics from a run's trades and equity curve."""

from __future__ import annotations

import numpy as np
import pandas as pd


def drawdown(equity: pd.Series) -> pd.Series:
    return equity / equity.cummax() - 1


def compute(trades: pd.DataFrame, equity: pd.DataFrame, initial_capital: float) -> dict:
    """equity: ts, equity, position (one row per bar). Ratios are annualized with the
    number of bars per year actually seen in the data, so they work for any timeframe."""
    curve = equity["equity"].astype(float)
    final = float(curve.iloc[-1]) if len(curve) else initial_capital
    out = {
        "n_trades": len(trades),
        "total_return": final / initial_capital - 1,
        "cagr": None, "sharpe": None, "sortino": None,
        "max_drawdown": float(drawdown(curve).min()) if len(curve) else 0.0,
        "win_rate": None, "profit_factor": None, "avg_r": None,
        "exposure": float((equity["position"] != 0).mean()) if len(equity) else 0.0,
    }

    if len(curve) > 1:
        ts = pd.to_datetime(equity["ts"], utc=True)
        years = (ts.iloc[-1] - ts.iloc[0]).total_seconds() / (365.25 * 86400)
        if years > 0:
            if final > 0:
                out["cagr"] = (final / initial_capital) ** (1 / years) - 1
            returns = curve.pct_change().dropna()
            per_year = len(returns) / years
            if returns.std() > 0:
                out["sharpe"] = float(returns.mean() / returns.std() * np.sqrt(per_year))
            downside = returns[returns < 0]
            if len(downside) > 1 and downside.std() > 0:
                out["sortino"] = float(returns.mean() / downside.std() * np.sqrt(per_year))

    if len(trades):
        wins, losses = trades.loc[trades["pnl"] > 0, "pnl"], trades.loc[trades["pnl"] < 0, "pnl"]
        out["win_rate"] = len(wins) / len(trades)
        out["profit_factor"] = float(wins.sum() / -losses.sum()) if len(losses) else None
        out["avg_r"] = float(trades["r_multiple"].mean())
    return out


def combine_equity(curves: dict[str, pd.DataFrame], initial_capital: float) -> pd.DataFrame:
    """Sum per-symbol equity curves into one (each symbol trades its own initial_capital).
    A symbol with no bar at some ts keeps its last value."""
    equity = pd.concat({s: e.set_index("ts")["equity"] for s, e in curves.items()}, axis=1).sort_index()
    position = pd.concat({s: e.set_index("ts")["position"] for s, e in curves.items()}, axis=1).sort_index()
    total = equity.ffill().fillna(initial_capital).sum(axis=1)
    in_market = (position.fillna(0) != 0).any(axis=1).astype(float)
    return pd.DataFrame({"ts": total.index, "equity": total.to_numpy(), "position": in_market.to_numpy()})
