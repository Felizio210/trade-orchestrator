"""Dukascopy account (demo or live) through dlh-bridge, the JForex SDK service in bridge/
(start it with bridge/run.sh; it listens on TRADING_BRIDGE_URL).

  * submit: sizes the order from the live price (qty = risk / distance from the bid or ask
    to the stop) and sends a market order with the stop and target attached, labelled with
    the order_id. The bridge rounds the size down to the instrument's step and never sends a
    label twice, so a retried submit can't double an order.
  * sync: reads each pending order and open position back by label (and Dukascopy id): fills,
    rejections, and closes at Dukascopy (by the stop, the target, or by hand). PnL and commission are Dukascopy's, in the
    account currency.
  * balance: the account's balance, so sizing and the risk limits use the real account.

realtime = True: only a signal on the newest bar is acted on (the executor skips the rest).
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd

from trading.brokers.base import Broker
from trading.store import Order, Position

# Orders older than this are not looked for in the account history
HISTORY_DAYS = 30


class BridgeError(RuntimeError):
    pass


def label(order_id: str) -> str:
    """JForex labels: a letter, then letters, digits or _ (supply_demand:1h:XAUUSD:20261103T0100
    -> supply_demand_1h_XAUUSD_20261103T0100)."""
    return re.sub(r"[^A-Za-z0-9_]", "_", order_id)


def exit_reason(pos: Position, price: float) -> str:
    """stop, target, or closed (by hand or by Dukascopy). Decided from our own stop and target:
    Dukascopy's history drops them once the position is closed."""
    d, eps = pos.direction, 1e-9 * max(abs(price), 1.0)
    if (price - pos.stop) * d <= eps:
        return "stop"
    if pos.target is not None and (price - pos.target) * d >= -eps:
        return "target"
    return "closed"


def _ts(ms) -> pd.Timestamp | None:
    return pd.Timestamp(int(ms), unit="ms", tz="UTC") if ms else None


class DukascopyBroker(Broker):
    name = "dukascopy"
    realtime = True

    def __init__(self, url: str, timeout: float = 30.0):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _call(self, method: str, path: str, body: dict | None = None, **query) -> dict | None:
        """JSON from the bridge; None on 404 (no such order)."""
        url = self.url + path + ("?" + urllib.parse.urlencode(query) if query else "")
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404 and path.startswith("/orders/"):
                return None
            detail = e.read().decode(errors="replace")
            raise BridgeError(f"bridge {method} {path}: HTTP {e.code} {detail}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            raise BridgeError(f"bridge not reachable at {self.url} ({e}); start it with bridge/run.sh") from e

    def balance(self) -> float:
        return float(self._call("GET", "/account")["balance"])

    def equity(self) -> float:
        return float(self._call("GET", "/account")["equity"])

    def order(self, order_id: str, symbol: str, since: pd.Timestamp | None = None,
              broker_id: str | None = None) -> dict | None:
        since = since or pd.Timestamp(datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS))
        query = {"symbol": symbol, "since": int((since - timedelta(days=1)).timestamp() * 1000)}
        if broker_id:
            query["id"] = broker_id
        return self._call("GET", f"/orders/{label(order_id)}", **query)

    def submit(self, order: Order) -> None:
        tick = self._call("GET", "/tick", symbol=order.symbol)
        price = float(tick["ask"] if order.direction > 0 else tick["bid"])
        distance = (price - order.stop) * order.direction
        if distance <= 0:
            order.status, order.reason = "rejected", f"price {price:g} already past the stop {order.stop:g}"
            return
        resp = self._call("POST", "/orders", {
            "label": label(order.order_id), "symbol": order.symbol, "side": order.side,
            "qty": order.risk / distance, "stop": order.stop, "target": order.target,
        })
        order.broker_order_id = resp.get("id") or label(order.order_id)
        if resp["state"] in ("REJECTED", "CANCELED"):
            order.status, order.reason = "rejected", resp.get("reason") or "rejected by Dukascopy"
        # Otherwise it stays pending; sync records the fill from Dukascopy's side

    def sync(self, timeframe, symbol, bar, orders, positions):
        now = pd.Timestamp(datetime.now(timezone.utc))
        changed: list[Order | Position] = []
        for order in orders:
            if order.status != "pending" or (order.timeframe, order.symbol) != (timeframe, symbol):
                continue
            remote = self.order(order.order_id, symbol, order.created_at, order.broker_order_id)
            state = remote["state"] if remote else None
            if state in ("CREATED", "OPENED"):
                continue
            order.updated_at = now
            changed.append(order)
            if state not in ("FILLED", "CLOSED"):  # cancelled, rejected, or never reached Dukascopy
                order.status = "rejected"
                order.reason = (remote or {}).get("reason") or f"not filled at Dukascopy ({state or 'not found'})"
                continue
            qty, fill, commission = float(remote["qty"]), float(remote["open_price"]), float(remote["commission"])
            fill_ts = _ts(remote["fill_time"]) or now
            if state == "CLOSED" and not qty:
                qty = float(remote["requested_qty"])
            order.status, order.qty, order.fill_ts, order.fill_price, order.commission = (
                "filled", qty, fill_ts, fill, commission)
            position = Position(
                position_id=order.order_id, date=fill_ts.date(), strategy=order.strategy, timeframe=timeframe,
                symbol=symbol, side=order.side, status="open", qty=qty, entry_ts=fill_ts, entry_price=fill,
                stop=float(remote["stop"]) or order.stop, target=float(remote["target"]) or order.target,
                risk=order.risk, entry_commission=commission, updated_at=now,
            )
            positions.append(position)
            changed.append(position)

        for pos in positions:
            if pos.status != "open" or (pos.timeframe, pos.symbol) != (timeframe, symbol):
                continue
            remote = self.order(pos.position_id, symbol, pos.entry_ts)
            if not remote or remote["state"] != "CLOSED":
                continue
            commission = float(remote["commission"])
            pos.status, pos.exit_ts, pos.exit_price = "closed", _ts(remote["close_time"]) or now, float(remote["close_price"])
            pos.exit_reason = exit_reason(pos, pos.exit_price)
            pos.commission = commission
            pos.pnl = float(remote["pnl"]) - commission
            pos.r_multiple = pos.pnl / pos.risk if pos.risk else None
            pos.updated_at = now
            if not any(r is pos for r in changed):
                changed.append(pos)
        return changed

    def unrealized(self, position, bar):
        remote = self.order(position.position_id, position.symbol, position.entry_ts)
        return float(remote["pnl"]) if remote and remote["state"] == "FILLED" else 0.0
