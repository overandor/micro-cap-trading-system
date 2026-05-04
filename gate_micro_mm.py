#!/usr/bin/env python3
"""
gate_micro_mm.py  ·  v3.0
══════════════════════════════════════════════════════════════════════════════
Gate.io USDT Perpetual Futures  ·  Micro Market Maker
Target: 0.0001–0.10 USDT contracts (SHIB, PEPE, FLOKI, 1000RATS, etc.)
══════════════════════════════════════════════════════════════════════════════

STRATEGY
────────
  • Post-only BID at best_bid   (join inside bid, never cross)
  • Post-only ASK at best_ask   (join inside ask, never cross)
  • TIF = poc  (post-only-cancel: rejected if would immediately fill → zero taker)
  • Replace ONLY when displaced ≥ 1 tick from current best
  • Smallest viable notional per side: MM_NOTIONAL_USDT (default 5.0 USDT)

FILL DETECTION
──────────────
  Private WebSocket → futures.orders + futures.usertrades
  Zero REST polling for fills; instant position updates.

RISK CONTROLS
─────────────
  • Daily loss halt (DAILY_LOSS_LIMIT_USDT, default 3.00)
  • Equity floor circuit-breaker (EQUITY_FLOOR_USDT, default 5.00)
  • Per-symbol inventory cap (MAX_INVENTORY_CONTRACTS, default 5000)
  • Inventory-skew quote sizing (reduce exposure automatically)
  • Emergency bulk cancel on any halt trigger
  • UTC midnight auto-reset of daily P&L

ENVIRONMENT VARIABLES (all optional)
──────────────────────────────────────
  GATE_API_KEY              API key
  GATE_API_SECRET           API secret
  MM_SYMBOLS                Comma-separated override, e.g. SHIB_USDT,PEPE_USDT
  MM_NOTIONAL_USDT          Per-side order size in USDT (default 5.0)
  MM_PRICE_MIN              Min contract price to auto-discover (default 0.000001)
  MM_PRICE_MAX              Max contract price to auto-discover (default 0.10)
  DAILY_LOSS_LIMIT_USDT     Halt threshold (default 3.00)
  EQUITY_FLOOR_USDT         Circuit-breaker floor (default 5.00)
  MAX_INVENTORY_CONTRACTS   Per-symbol cap (default 5000)
  LOG_LEVEL                 DEBUG / INFO / WARNING (default INFO)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
import websockets

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gate_mm")

# ─────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────
REST_BASE   = "https://fx.gate.io"
WS_PUBLIC   = "wss://fx-ws.gateio.ws/v4/ws/usdt"
WS_PRIVATE  = "wss://fx-ws.gateio.ws/v4/ws/usdt"

TAKER_FEE   = Decimal("0.0005")   # 0.05%
MAKER_FEE   = Decimal("-0.00015") # −0.015% rebate

SETTLE      = "usdt"

# ─────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────
@dataclass
class Config:
    api_key:    str
    api_secret: str

    # Discovery
    price_min: Decimal = Decimal(os.getenv("MM_PRICE_MIN", "0.000001"))
    price_max: Decimal = Decimal(os.getenv("MM_PRICE_MAX", "0.10"))

    # Symbols (None → auto-discover)
    symbols: Optional[List[str]] = None

    # Sizing
    notional_usdt: Decimal = Decimal(os.getenv("MM_NOTIONAL_USDT", "5.0"))

    # Quote behavior
    order_ttl_secs:    float = 12.0
    refresh_secs:      float = 1.5    # fallback refresh even if no tick change
    rest_delay_ms:     int   = 150    # throttle between REST calls

    # Risk
    daily_loss_limit:  Decimal = Decimal(os.getenv("DAILY_LOSS_LIMIT_USDT", "3.00"))
    equity_floor:      Decimal = Decimal(os.getenv("EQUITY_FLOOR_USDT", "5.00"))
    max_inventory:     int     = int(os.getenv("MAX_INVENTORY_CONTRACTS", "5000"))

    # Skew: reduce quote size when abs(inventory) > skew_threshold
    skew_threshold:    int = 2000
    skew_reduction:    float = 0.5   # fraction of notional to apply above threshold

    # WS
    ws_reconnect_delay: float = 3.0
    ws_max_retries:     int   = 100


def load_config() -> Config:
    key    = os.getenv("GATE_API_KEY", "")
    secret = os.getenv("GATE_API_SECRET", "")
    cfg    = Config(api_key=key, api_secret=secret)

    sym_env = os.getenv("MM_SYMBOLS", "")
    if sym_env:
        cfg.symbols = [s.strip().upper() for s in sym_env.split(",") if s.strip()]

    return cfg


# ─────────────────────────────────────────────
#  HMAC Auth
# ─────────────────────────────────────────────
def _sign_rest(secret: str, method: str, path: str,
               query: str, body: str, ts: int) -> str:
    hashed_body = hashlib.sha512(body.encode()).hexdigest()
    msg = f"{method}\n{path}\n{query}\n{hashed_body}\n{ts}"
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha512).hexdigest()


def _sign_ws(secret: str, channel: str, ts: int) -> str:
    msg = f"channel={channel}&event=subscribe&time={ts}"
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha512).hexdigest()


def _rest_headers(cfg: Config, method: str, path: str,
                  query: str = "", body: str = "") -> dict:
    ts = int(time.time())
    sig = _sign_rest(cfg.api_secret, method, path, query, body, ts)
    return {
        "KEY":       cfg.api_key,
        "SIGN":      sig,
        "Timestamp": str(ts),
        "Content-Type": "application/json",
        "Accept":    "application/json",
    }


# ─────────────────────────────────────────────
#  REST Client
# ─────────────────────────────────────────────
class GateRest:
    def __init__(self, cfg: Config, session: aiohttp.ClientSession):
        self._cfg     = cfg
        self._session = session
        self._sem     = asyncio.Semaphore(4)

    async def _call(self, method: str, path: str,
                    params: Optional[dict] = None,
                    payload: Optional[dict] = None) -> dict | list:
        query = "&".join(f"{k}={v}" for k, v in (params or {}).items())
        body  = json.dumps(payload) if payload else ""
        url   = f"{REST_BASE}{path}"
        if query:
            url += "?" + query
        hdrs = _rest_headers(self._cfg, method.upper(), path, query, body)

        async with self._sem:
            for attempt in range(5):
                try:
                    async with self._session.request(
                        method, url, headers=hdrs, data=body or None,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        text = await resp.text()
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt * 0.5)
                            continue
                        if resp.status >= 400:
                            log.warning("REST %s %s → %d  %s",
                                        method, path, resp.status, text[:200])
                            return {}
                        return json.loads(text)
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    log.warning("REST attempt %d failed: %s", attempt + 1, exc)
                    await asyncio.sleep(2 ** attempt * 0.3)
            return {}

    # ── Public ──────────────────────────────────
    async def list_contracts(self) -> list:
        return await self._call("GET", f"/api/v4/futures/{SETTLE}/contracts")  # type: ignore

    async def get_orderbook(self, symbol: str, depth: int = 5) -> dict:
        return await self._call("GET", f"/api/v4/futures/{SETTLE}/order_book",
                                params={"contract": symbol, "limit": str(depth), "with_id": "false"})  # type: ignore

    # ── Private ─────────────────────────────────
    async def get_account(self) -> dict:
        return await self._call("GET", f"/api/v4/futures/{SETTLE}/accounts")  # type: ignore

    async def get_positions(self) -> list:
        return await self._call("GET", f"/api/v4/futures/{SETTLE}/positions")  # type: ignore

    async def list_open_orders(self, symbol: str) -> list:
        return await self._call("GET", f"/api/v4/futures/{SETTLE}/orders",
                                params={"contract": symbol, "status": "open"})  # type: ignore

    async def place_order(self, symbol: str, size: int, price: Decimal,
                          tif: str = "poc") -> dict:
        payload = {
            "contract": symbol,
            "size":     size,           # positive=buy, negative=sell
            "price":    str(price),
            "tif":      tif,
            "text":     f"t-{uuid.uuid4().hex[:8]}",
            "is_reduce_only": False,
            "is_close":       False,
        }
        return await self._call("POST", f"/api/v4/futures/{SETTLE}/orders",
                                payload=payload)  # type: ignore

    async def cancel_order(self, order_id: str) -> dict:
        return await self._call("DELETE",
                                f"/api/v4/futures/{SETTLE}/orders/{order_id}")  # type: ignore

    async def cancel_all(self, symbol: str) -> list:
        return await self._call("DELETE", f"/api/v4/futures/{SETTLE}/orders",
                                params={"contract": symbol})  # type: ignore

    async def cancel_all_symbols(self, symbols: List[str]) -> None:
        for sym in symbols:
            try:
                await self.cancel_all(sym)
                await asyncio.sleep(self._cfg.rest_delay_ms / 1000)
            except Exception as exc:
                log.error("cancel_all %s: %s", sym, exc)


# ─────────────────────────────────────────────
#  Order Book (local BBO)
# ─────────────────────────────────────────────
@dataclass
class BookSide:
    """Sorted price levels: bids descending, asks ascending."""
    levels: Dict[Decimal, Decimal] = field(default_factory=dict)

    def update(self, price: Decimal, size: Decimal) -> None:
        if size == 0:
            self.levels.pop(price, None)
        else:
            self.levels[price] = size

    def best(self) -> Optional[Decimal]:
        return max(self.levels) if self.levels else None

    def best_ask(self) -> Optional[Decimal]:
        return min(self.levels) if self.levels else None


class OrderBook:
    def __init__(self, symbol: str):
        self.symbol   = symbol
        self.bids     = BookSide()
        self.asks     = BookSide()
        self.seq: int = 0
        self.valid    = False

    def apply_snapshot(self, data: dict) -> None:
        self.bids.levels.clear()
        self.asks.levels.clear()
        for p, s in data.get("b", []):
            self.bids.update(Decimal(str(p)), Decimal(str(s)))
        for p, s in data.get("a", []):
            self.asks.update(Decimal(str(p)), Decimal(str(s)))
        self.valid = True

    def apply_update(self, data: dict) -> None:
        for p, s in data.get("b", []):
            self.bids.update(Decimal(str(p)), Decimal(str(s)))
        for p, s in data.get("a", []):
            self.asks.update(Decimal(str(p)), Decimal(str(s)))

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids.best()

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks.best_ask()

    @property
    def mid(self) -> Optional[Decimal]:
        bb, ba = self.best_bid, self.best_ask
        if bb and ba:
            return (bb + ba) / 2
        return None


# ─────────────────────────────────────────────
#  Instrument metadata (tick / lot)
# ─────────────────────────────────────────────
@dataclass
class Instrument:
    symbol:    str
    tick:      Decimal   # order_price_round
    lot:       int       # quanto_multiplier (integer contracts)
    mark:      Decimal   # latest mark price
    min_size:  int = 1   # minimum order size in contracts

    def qty_from_notional(self, notional_usdt: Decimal) -> int:
        """Contracts = notional / (tick_value_per_contract × mark_price)."""
        # For USDT-margined perps: 1 contract = quanto_multiplier base units
        # Contract value ≈ mark_price (for quanto = 1)
        if self.mark <= 0:
            return 0
        raw = notional_usdt / self.mark
        return max(self.min_size, int(raw))

    def round_price(self, price: Decimal, side: str) -> Decimal:
        """Round to tick: bids round down, asks round up (conservative)."""
        if side == "buy":
            return (price / self.tick).to_integral_value(ROUND_DOWN) * self.tick
        return (price / self.tick).to_integral_value(ROUND_UP) * self.tick


# ─────────────────────────────────────────────
#  OMS (Order Management)
# ─────────────────────────────────────────────
@dataclass
class LiveOrder:
    order_id:   str
    symbol:     str
    side:       str      # "buy" | "sell"
    price:      Decimal
    size:       int
    placed_at:  float
    status:     str = "open"   # open | filled | cancelled | expired


class OMS:
    def __init__(self):
        self._orders: Dict[str, LiveOrder] = {}
        self._lock = asyncio.Lock()

    async def register(self, order: LiveOrder) -> None:
        async with self._lock:
            self._orders[order.order_id] = order

    async def on_fill(self, order_id: str) -> Optional[LiveOrder]:
        async with self._lock:
            o = self._orders.get(order_id)
            if o:
                o.status = "filled"
            return o

    async def on_cancel(self, order_id: str) -> None:
        async with self._lock:
            o = self._orders.get(order_id)
            if o:
                o.status = "cancelled"

    async def get_open(self, symbol: str) -> Dict[str, LiveOrder]:
        async with self._lock:
            return {
                oid: o for oid, o in self._orders.items()
                if o.symbol == symbol and o.status == "open"
            }

    async def expire_stale(self, ttl: float) -> List[str]:
        """Return IDs of open orders older than ttl seconds."""
        now = time.monotonic()
        async with self._lock:
            return [
                oid for oid, o in self._orders.items()
                if o.status == "open" and (now - o.placed_at) > ttl
            ]

    async def cleanup(self, max_keep: int = 2000) -> None:
        async with self._lock:
            terminal = [oid for oid, o in self._orders.items()
                        if o.status in ("filled", "cancelled", "expired")]
            for oid in terminal[:-max_keep]:
                del self._orders[oid]


# ─────────────────────────────────────────────
#  Position & P&L Tracker
# ─────────────────────────────────────────────
@dataclass
class SymbolPosition:
    symbol:     str
    net:        int     = 0       # net contracts (positive=long, negative=short)
    avg_cost:   Decimal = Decimal("0")
    realized:   Decimal = Decimal("0")
    daily_pnl:  Decimal = Decimal("0")

    def record_fill(self, side: str, qty: int, fill_price: Decimal) -> None:
        signed = qty if side == "buy" else -qty
        if self.net == 0:
            self.avg_cost = fill_price
            self.net      = signed
        elif (self.net > 0 and signed > 0) or (self.net < 0 and signed < 0):
            # Increasing position: VWAP
            total = abs(self.net) + abs(signed)
            self.avg_cost = (
                (abs(self.net) * self.avg_cost + abs(signed) * fill_price) /
                Decimal(total)
            )
            self.net += signed
        else:
            # Reducing / reversing
            closed   = min(abs(self.net), abs(signed))
            pnl      = (fill_price - self.avg_cost) * Decimal(closed) * (
                1 if self.net > 0 else -1
            )
            self.realized  += pnl
            self.daily_pnl += pnl
            remainder = abs(signed) - closed
            if remainder == 0:
                self.net      = self.net + signed
                if self.net == 0:
                    self.avg_cost = Decimal("0")
            else:
                self.net      = signed - (signed // abs(signed)) * closed
                self.avg_cost = fill_price

    def unrealized(self, mark: Decimal) -> Decimal:
        if self.net == 0 or self.avg_cost == 0:
            return Decimal("0")
        return (mark - self.avg_cost) * Decimal(self.net)


class RiskLedger:
    def __init__(self, cfg: Config):
        self._cfg      = cfg
        self._positions: Dict[str, SymbolPosition] = {}
        self._halted   = False
        self._lock     = asyncio.Lock()

    @property
    def halted(self) -> bool:
        return self._halted

    async def halt(self, reason: str) -> None:
        async with self._lock:
            if not self._halted:
                log.critical("HALT: %s", reason)
                self._halted = True

    async def resume(self) -> None:
        async with self._lock:
            self._halted = False
            log.info("Risk reset – resuming")

    async def position(self, symbol: str) -> SymbolPosition:
        async with self._lock:
            if symbol not in self._positions:
                self._positions[symbol] = SymbolPosition(symbol)
            return self._positions[symbol]

    async def record_fill(self, symbol: str, side: str,
                          qty: int, price: Decimal) -> None:
        async with self._lock:
            pos = self._positions.setdefault(symbol, SymbolPosition(symbol))
            pos.record_fill(side, qty, price)

            # Daily loss check
            total_daily = sum(p.daily_pnl for p in self._positions.values())
            if total_daily < -self._cfg.daily_loss_limit:
                self._halted = True
                log.critical("Daily loss limit breached: %.4f USDT", total_daily)

    async def inventory(self, symbol: str) -> int:
        async with self._lock:
            pos = self._positions.get(symbol)
            return pos.net if pos else 0

    async def reset_daily(self) -> None:
        async with self._lock:
            for pos in self._positions.values():
                pos.daily_pnl = Decimal("0")
            self._halted = False
            log.info("Daily P&L reset")

    async def report(self) -> str:
        async with self._lock:
            lines = ["── P&L Report ──────────────────"]
            total_realized = Decimal("0")
            for sym, pos in sorted(self._positions.items()):
                if pos.realized != 0 or pos.net != 0:
                    lines.append(
                        f"  {sym:25s}  net={pos.net:+6d}  "
                        f"realized={float(pos.realized):+.4f}  "
                        f"daily={float(pos.daily_pnl):+.4f}"
                    )
                    total_realized += pos.realized
            lines.append(f"  {'TOTAL':25s}  realized={float(total_realized):+.4f}")
            return "\n".join(lines)


# ─────────────────────────────────────────────
#  Public WebSocket (order books)
# ─────────────────────────────────────────────
class PublicWS:
    """Subscribes to order_book updates for all tracked symbols."""

    def __init__(self, symbols: List[str], books: Dict[str, OrderBook]):
        self._symbols = symbols
        self._books   = books
        self.shutdown = False

    async def run(self) -> None:
        retries = 0
        while not self.shutdown:
            try:
                async with websockets.connect(
                    WS_PUBLIC,
                    ping_interval=20, ping_timeout=20,
                    max_size=2 ** 24, open_timeout=15,
                ) as ws:
                    retries = 0
                    for sym in self._symbols:
                        sub = {
                            "time":    int(time.time()),
                            "channel": "futures.order_book",
                            "event":   "subscribe",
                            "payload": [sym, "20", "0"],
                        }
                        await ws.send(json.dumps(sub))
                        await asyncio.sleep(0.05)
                    log.info("PublicWS subscribed to %d symbols", len(self._symbols))

                    async for raw in ws:
                        if self.shutdown:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        self._dispatch(msg)

            except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                retries += 1
                delay = min(30.0, 3.0 * retries)
                log.warning("PublicWS disconnected (%s), retry %d in %.1fs",
                            exc, retries, delay)
                await asyncio.sleep(delay)

    def _dispatch(self, msg: dict) -> None:
        ch = msg.get("channel", "")
        ev = msg.get("event", "")
        if ch != "futures.order_book":
            return
        result = msg.get("result", {})
        sym    = result.get("contract") or result.get("s", "")
        if sym not in self._books:
            return
        book = self._books[sym]
        if ev in ("all", "snapshot"):
            book.apply_snapshot({
                "b": [[x["p"], x["s"]] for x in result.get("bids", [])],
                "a": [[x["p"], x["s"]] for x in result.get("asks", [])],
            })
        elif ev == "update":
            book.apply_update({
                "b": [[x["p"], x["s"]] for x in result.get("bids", [])],
                "a": [[x["p"], x["s"]] for x in result.get("asks", [])],
            })


# ─────────────────────────────────────────────
#  Private WebSocket (fills / orders)
# ─────────────────────────────────────────────
class PrivateWS:
    """
    Authenticates and subscribes to:
      futures.orders      → order state changes
      futures.usertrades  → fill events
    """

    def __init__(self, cfg: Config, oms: OMS, risk: RiskLedger):
        self._cfg     = cfg
        self._oms     = oms
        self._risk    = risk
        self.shutdown = False
        self._fill_callbacks: List = []

    def on_fill(self, cb) -> None:
        self._fill_callbacks.append(cb)

    async def run(self) -> None:
        retries = 0
        while not self.shutdown:
            try:
                async with websockets.connect(
                    WS_PRIVATE,
                    ping_interval=20, ping_timeout=20,
                    max_size=2 ** 23, open_timeout=15,
                ) as ws:
                    retries = 0
                    await self._auth(ws)
                    await self._subscribe(ws, "futures.orders")
                    await self._subscribe(ws, "futures.usertrades")
                    log.info("PrivateWS authenticated")

                    async for raw in ws:
                        if self.shutdown:
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self._dispatch(msg)

            except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                retries += 1
                delay = min(60.0, 3.0 * retries)
                log.warning("PrivateWS disconnected (%s), retry %d in %.1fs",
                            exc, retries, delay)
                await asyncio.sleep(delay)

    async def _auth(self, ws) -> None:
        ts  = int(time.time())
        sig = _sign_ws(self._cfg.api_secret, "futures.login", ts)
        msg = {
            "time":    ts,
            "channel": "futures.login",
            "event":   "api",
            "payload": {
                "api_key":   self._cfg.api_key,
                "signature": sig,
                "timestamp": str(ts),
            },
        }
        await ws.send(json.dumps(msg))
        await asyncio.sleep(0.5)

    async def _subscribe(self, ws, channel: str) -> None:
        ts  = int(time.time())
        sig = _sign_ws(self._cfg.api_secret, channel, ts)
        msg = {
            "time":    ts,
            "channel": channel,
            "event":   "subscribe",
            "auth": {
                "method":    "api_key",
                "KEY":       self._cfg.api_key,
                "SIGN":      sig,
                "Timestamp": str(ts),
            },
            "payload": ["!all"],
        }
        await ws.send(json.dumps(msg))

    async def _dispatch(self, msg: dict) -> None:
        ch     = msg.get("channel", "")
        result = msg.get("result", {})
        if not result:
            return

        if ch == "futures.usertrades":
            # Single trade or list
            trades = result if isinstance(result, list) else [result]
            for t in trades:
                await self._handle_trade(t)

        elif ch == "futures.orders":
            orders = result if isinstance(result, list) else [result]
            for o in orders:
                await self._handle_order(o)

    async def _handle_trade(self, t: dict) -> None:
        oid       = str(t.get("order_id", ""))
        sym       = t.get("contract", "")
        side      = "buy" if t.get("size", 0) > 0 else "sell"
        qty       = abs(int(t.get("size", 0)))
        price     = Decimal(str(t.get("price", "0")))
        fee       = Decimal(str(t.get("fee", "0")))

        log.info("FILL  %s  %s  qty=%d  px=%s  fee=%s",
                 sym, side.upper(), qty, price, fee)

        await self._oms.on_fill(oid)
        await self._risk.record_fill(sym, side, qty, price)

        for cb in self._fill_callbacks:
            try:
                await cb(sym, side, qty, price)
            except Exception as exc:
                log.error("fill_callback error: %s", exc)

    async def _handle_order(self, o: dict) -> None:
        oid    = str(o.get("id", ""))
        status = o.get("status", "")
        if status in ("cancelled", "closed"):
            await self._oms.on_cancel(oid)


# ─────────────────────────────────────────────
#  Quote Engine (per symbol)
# ─────────────────────────────────────────────
class QuoteEngine:
    """
    Maintains exactly 1 bid and 1 ask per symbol.
    Replaces ONLY when best price has moved ≥ 1 tick from current quote.
    Uses post-only TIF=poc to guarantee maker fills.
    """

    def __init__(self, symbol: str, instrument: Instrument,
                 cfg: Config, rest: GateRest, oms: OMS, risk: RiskLedger,
                 book: OrderBook):
        self.symbol     = symbol
        self._inst      = instrument
        self._cfg       = cfg
        self._rest      = rest
        self._oms       = oms
        self._risk      = risk
        self._book      = book

        self._bid_id:   Optional[str]     = None
        self._ask_id:   Optional[str]     = None
        self._bid_px:   Optional[Decimal] = None
        self._ask_px:   Optional[Decimal] = None
        self._last_refresh: float         = 0.0

    async def refresh(self) -> None:
        """Called on each market tick / timer. Places or replaces quotes."""
        if self._risk.halted:
            await self._cancel_both()
            return

        bb = self._book.best_bid
        ba = self._book.best_ask
        if not bb or not ba:
            return  # no book yet

        tick      = self._inst.tick
        inv       = await self._risk.inventory(self.symbol)
        notional  = self._effective_notional(inv)
        qty       = self._inst.qty_from_notional(notional)
        if qty <= 0:
            return

        # Target prices: join inside market (never cross)
        target_bid = self._inst.round_price(bb, "buy")
        target_ask = self._inst.round_price(ba, "sell")

        if target_bid >= target_ask:
            return  # degenerate book

        now = time.monotonic()
        need_bid = self._needs_replace(self._bid_px, target_bid, tick)
        need_ask = self._needs_replace(self._ask_px, target_ask, tick)
        forced   = (now - self._last_refresh) >= self._cfg.refresh_secs

        if need_bid or forced:
            await self._replace_side("buy", target_bid, qty)
        if need_ask or forced:
            await self._replace_side("sell", target_ask, qty)

        self._last_refresh = now

    async def cancel_all(self) -> None:
        await self._cancel_both()

    # ── internals ───────────────────────────────
    def _needs_replace(self, current: Optional[Decimal],
                       target: Decimal, tick: Decimal) -> bool:
        if current is None:
            return True
        return abs(target - current) >= tick

    def _effective_notional(self, inventory: int) -> Decimal:
        thresh = self._cfg.skew_threshold
        if abs(inventory) > thresh:
            return self._cfg.notional_usdt * Decimal(str(self._cfg.skew_reduction))
        return self._cfg.notional_usdt

    async def _replace_side(self, side: str, price: Decimal, qty: int) -> None:
        # Cancel existing
        existing_id = self._bid_id if side == "buy" else self._ask_id
        if existing_id:
            try:
                await self._rest.cancel_order(existing_id)
            except Exception:
                pass
            await self._oms.on_cancel(existing_id)
            if side == "buy":
                self._bid_id = None
                self._bid_px = None
            else:
                self._ask_id = None
                self._ask_px = None
            await asyncio.sleep(self._cfg.rest_delay_ms / 1000)

        if self._risk.halted:
            return

        # Place new
        signed_qty = qty if side == "buy" else -qty
        try:
            resp = await self._rest.place_order(
                self.symbol, signed_qty, price, tif="poc"
            )
        except Exception as exc:
            log.error("place_order %s %s: %s", self.symbol, side, exc)
            return

        if not resp or not resp.get("id"):
            # poc rejection (would have crossed) — acceptable, skip
            return

        oid = str(resp["id"])
        order = LiveOrder(
            order_id  = oid,
            symbol    = self.symbol,
            side      = side,
            price     = price,
            size      = qty,
            placed_at = time.monotonic(),
        )
        await self._oms.register(order)

        if side == "buy":
            self._bid_id = oid
            self._bid_px = price
        else:
            self._ask_id = oid
            self._ask_px = price

        log.debug("QUOTE  %s  %s  qty=%d  px=%s  id=%s",
                  self.symbol, side.upper(), qty, price, oid)

    async def _cancel_both(self) -> None:
        for oid in [self._bid_id, self._ask_id]:
            if oid:
                try:
                    await self._rest.cancel_order(oid)
                except Exception:
                    pass
        self._bid_id = self._ask_id = None
        self._bid_px = self._ask_px = None

    async def on_fill(self, symbol: str, side: str,
                      qty: int, price: Decimal) -> None:
        """Called by PrivateWS fill handler. Clears filled side so next tick replaces."""
        if symbol != self.symbol:
            return
        if side == "buy":
            self._bid_id = None
            self._bid_px = None
        else:
            self._ask_id = None
            self._ask_px = None


# ─────────────────────────────────────────────
#  TTL Reaper
# ─────────────────────────────────────────────
class TTLReaper:
    """Cancels orders older than TTL and clears their quote-engine slot."""

    def __init__(self, cfg: Config, rest: GateRest, oms: OMS,
                 engines: Dict[str, QuoteEngine]):
        self._cfg     = cfg
        self._rest    = rest
        self._oms     = oms
        self._engines = engines

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self._cfg.order_ttl_secs / 2)
            stale = await self._oms.expire_stale(self._cfg.order_ttl_secs)
            for oid in stale:
                try:
                    await self._rest.cancel_order(oid)
                except Exception:
                    pass
                await self._oms.on_cancel(oid)
            if stale:
                log.debug("TTL reaped %d orders", len(stale))
            await self._oms.cleanup()


# ─────────────────────────────────────────────
#  Market Ticker Loop
# ─────────────────────────────────────────────
class TickerLoop:
    """Drives QuoteEngine.refresh() on every book update per symbol."""

    def __init__(self, symbol: str, book: OrderBook, engine: QuoteEngine):
        self._symbol = symbol
        self._book   = book
        self._engine = engine
        self._prev_bid: Optional[Decimal] = None
        self._prev_ask: Optional[Decimal] = None

    async def run(self) -> None:
        while True:
            await asyncio.sleep(0.1)
            if not self._book.valid:
                continue
            bb = self._book.best_bid
            ba = self._book.best_ask
            if bb != self._prev_bid or ba != self._prev_ask:
                self._prev_bid = bb
                self._prev_ask = ba
                try:
                    await self._engine.refresh()
                except Exception as exc:
                    log.error("QuoteEngine.refresh %s: %s", self._symbol, exc)


# ─────────────────────────────────────────────
#  Reconciler (startup)
# ─────────────────────────────────────────────
async def reconcile(rest: GateRest, risk: RiskLedger, symbols: List[str]) -> None:
    """Sync live exchange positions into risk ledger at boot."""
    log.info("Reconciling positions from exchange...")
    positions = await rest.get_positions()
    if not positions:
        return
    for pos in positions:
        sym = pos.get("contract", "")
        if sym not in symbols:
            continue
        size  = int(pos.get("size", 0))
        entry = Decimal(str(pos.get("entry_price", "0")))
        p     = await risk.position(sym)
        p.net       = size
        p.avg_cost  = entry
    log.info("Reconciliation complete")


# ─────────────────────────────────────────────
#  Daily Reset Task
# ─────────────────────────────────────────────
async def daily_reset_loop(risk: RiskLedger) -> None:
    """Fires at UTC midnight to reset daily P&L and remove halt."""
    while True:
        now   = time.time()
        secs  = 86400 - (now % 86400)
        await asyncio.sleep(secs + 5)
        await risk.reset_daily()


# ─────────────────────────────────────────────
#  Heartbeat / Status
# ─────────────────────────────────────────────
async def heartbeat_loop(risk: RiskLedger, rest: GateRest,
                         books: Dict[str, OrderBook],
                         engines: Dict[str, QuoteEngine],
                         symbols: List[str]) -> None:
    while True:
        await asyncio.sleep(30)
        acct = await rest.get_account()
        avail = acct.get("available", "?")
        valid = sum(1 for b in books.values() if b.valid)
        log.info("HB  avail=%s USDT  books_valid=%d/%d  halted=%s",
                 avail, valid, len(symbols), risk.halted)
        if log.isEnabledFor(logging.INFO):
            log.info(await risk.report())


# ─────────────────────────────────────────────
#  Symbol Discovery
# ─────────────────────────────────────────────
async def discover_symbols(rest: GateRest, cfg: Config) -> Tuple[
    List[str], Dict[str, Instrument]
]:
    """Discover micro-priced perpetuals from exchange contract list."""
    contracts = await rest.list_contracts()
    if not contracts:
        raise RuntimeError("Failed to fetch contract list from Gate.io")

    instruments: Dict[str, Instrument] = {}
    for c in contracts:
        sym   = c.get("name", "")
        mark  = Decimal(str(c.get("mark_price", "0") or "0"))
        tick  = Decimal(str(c.get("order_price_round", "0.0001") or "0.0001"))
        lot   = int(c.get("quanto_multiplier", 1) or 1)
        in_tr = c.get("in_delisting", False)
        trade_status = c.get("trade_status", "")

        if in_tr or trade_status != "tradable":
            continue
        if mark <= 0:
            continue
        if cfg.price_min <= mark <= cfg.price_max:
            instruments[sym] = Instrument(
                symbol=sym, tick=tick, lot=lot, mark=mark
            )

    symbols = sorted(instruments.keys())
    log.info("Discovered %d micro contracts", len(symbols))
    if log.isEnabledFor(logging.DEBUG):
        for s in symbols:
            inst = instruments[s]
            log.debug("  %s  mark=%s  tick=%s", s, inst.mark, inst.tick)
    return symbols, instruments


# ─────────────────────────────────────────────
#  Main Orchestrator
# ─────────────────────────────────────────────
class MicroMM:
    def __init__(self, cfg: Config):
        self._cfg   = cfg
        self._tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        # Graceful shutdown on SIGINT / SIGTERM
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        async with aiohttp.ClientSession() as session:
            rest = GateRest(self._cfg, session)

            # ── discover symbols ──────────────────
            if self._cfg.symbols:
                # Manual override: fetch tick/lot for those symbols only
                all_contracts = await rest.list_contracts()
                instruments: Dict[str, Instrument] = {}
                lookup = {c["name"]: c for c in all_contracts if "name" in c}
                for sym in self._cfg.symbols:
                    c = lookup.get(sym)
                    if not c:
                        log.warning("Symbol %s not found on exchange, skipping", sym)
                        continue
                    mark = Decimal(str(c.get("mark_price", "0") or "0"))
                    tick = Decimal(str(c.get("order_price_round", "0.0001") or "0.0001"))
                    lot  = int(c.get("quanto_multiplier", 1) or 1)
                    instruments[sym] = Instrument(symbol=sym, tick=tick,
                                                   lot=lot, mark=mark)
                symbols = [s for s in self._cfg.symbols if s in instruments]
            else:
                symbols, instruments = await discover_symbols(rest, self._cfg)

            if not symbols:
                log.error("No symbols available – exiting")
                return

            log.info("Trading %d symbols: %s", len(symbols),
                     ", ".join(symbols[:10]) + ("…" if len(symbols) > 10 else ""))

            # ── shared state ─────────────────────
            books   = {s: OrderBook(s)       for s in symbols}
            oms     = OMS()
            risk    = RiskLedger(self._cfg)
            engines: Dict[str, QuoteEngine] = {}

            for sym in symbols:
                engines[sym] = QuoteEngine(
                    symbol     = sym,
                    instrument = instruments[sym],
                    cfg        = self._cfg,
                    rest       = rest,
                    oms        = oms,
                    risk       = risk,
                    book       = books[sym],
                )

            # ── cancel existing open orders ───────
            log.info("Cancelling any existing open orders...")
            await rest.cancel_all_symbols(symbols)
            await asyncio.sleep(1.0)

            # ── reconcile positions ───────────────
            await reconcile(rest, risk, symbols)

            # ── private WS ───────────────────────
            private_ws = PrivateWS(self._cfg, oms, risk)
            for eng in engines.values():
                private_ws.on_fill(eng.on_fill)

            # ── public WS ────────────────────────
            public_ws = PublicWS(symbols, books)

            # ── TTL reaper ────────────────────────
            reaper = TTLReaper(self._cfg, rest, oms, engines)

            # ── launch tasks ─────────────────────
            tasks = [
                asyncio.create_task(public_ws.run(),  name="public_ws"),
                asyncio.create_task(private_ws.run(), name="private_ws"),
                asyncio.create_task(reaper.run(),     name="ttl_reaper"),
                asyncio.create_task(
                    daily_reset_loop(risk),           name="daily_reset"),
                asyncio.create_task(
                    heartbeat_loop(risk, rest, books, engines, symbols),
                    name="heartbeat"),
            ]

            # One ticker-loop task per symbol
            for sym in symbols:
                tasks.append(asyncio.create_task(
                    TickerLoop(sym, books[sym], engines[sym]).run(),
                    name=f"ticker_{sym}",
                ))

            self._tasks = tasks
            log.info("All tasks launched. Running…")

            # ── wait for shutdown signal ──────────
            await stop_event.wait()
            log.info("Shutdown signal received, cleaning up…")

        # ── teardown ──────────────────────────────
        public_ws.shutdown  = True
        private_ws.shutdown = True

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Final bulk cancel
        log.info("Emergency cancel on all symbols…")
        async with aiohttp.ClientSession() as session:
            rest2 = GateRest(self._cfg, session)
            await rest2.cancel_all_symbols(symbols)
            log.info(await risk.report())

        log.info("Shutdown complete.")


# ─────────────────────────────────────────────
#  Entrypoint
# ─────────────────────────────────────────────
async def _main() -> None:
    cfg = load_config()
    if not cfg.api_key or not cfg.api_secret:
        log.warning("GATE_API_KEY / GATE_API_SECRET not set – running without auth")
    mm = MicroMM(cfg)
    await mm.start()


if __name__ == "__main__":
    asyncio.run(_main())
