"""
main.py — Ultra-Fast Async Trading Bot
Angel One SmartAPI | BankNifty/FinNifty Weekly Options
Railway.app-ready | Telegram Command & Control
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

# ── Third-party (lazy imports where heavy) ────────────────────────────────────
import httpx
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from strategies import (
    AlphaStrategy,
    IndicatorEngine,
    SignalResult,
    Tick,
    TrailingState,
    market_health_index,
)

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

IST = ZoneInfo("Asia/Kolkata")

# ──────────────────────────────────────────────────────────────────────────────
# Config from environment variables
# ──────────────────────────────────────────────────────────────────────────────

class Cfg:
    # Angel One
    API_KEY          = os.getenv("API_KEY", "")
    CLIENT_ID        = os.getenv("CLIENT_ID", "")
    PASSWORD         = os.getenv("PASSWORD", "")
    TOTP_SECRET      = os.getenv("TOTP_SECRET", "")

    # Telegram
    TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

    # Risk
    CAPITAL          = float(os.getenv("CAPITAL", "20000"))
    MAX_DAILY_LOSS   = float(os.getenv("MAX_DAILY_LOSS", "1000"))
    DAILY_TARGET     = float(os.getenv("DAILY_TARGET", "1500"))
    LOT_SIZE         = int(os.getenv("LOT_SIZE", "1"))          # number of lots

    # Instruments
    UNDERLYING       = os.getenv("UNDERLYING", "BANKNIFTY")      # BANKNIFTY | FINNIFTY
    TRADE_MODE       = os.getenv("TRADE_MODE", "paper").lower()  # paper | live

    # State file for Railway restart recovery
    STATE_FILE       = Path(os.getenv("STATE_FILE", "db.json"))

    # Market hours (IST HHMM)
    MARKET_START     = int(os.getenv("MARKET_START", "915"))
    MARKET_END       = int(os.getenv("MARKET_END", "1525"))
    TRADE_START      = int(os.getenv("TRADE_START", "930"))
    TRADE_END        = int(os.getenv("TRADE_END", "1430"))


def _hhmm() -> int:
    now = datetime.now(IST)
    return now.hour * 100 + now.minute


def _is_trading_hours() -> bool:
    t = _hhmm()
    return Cfg.TRADE_START <= t <= Cfg.TRADE_END


def _now_label() -> str:
    return datetime.now(IST).strftime("%H:%M:%S")


# ──────────────────────────────────────────────────────────────────────────────
# Persistent State (db.json)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ActiveTrade:
    order_id: str
    symbol: str
    token: str
    option_type: str          # "CE" or "PE"
    lot_size: int
    entry_price: float
    sl_price: float
    entry_time: str           # ISO string
    mode: str                 # "paper" | "live"

    def trail(self) -> TrailingState:
        return TrailingState(
            entry_price=self.entry_price,
            current_sl=self.sl_price,
            high_water=self.entry_price,
            lot_size=self.lot_size * _lot_qty(self.symbol),
        )


def _lot_qty(symbol: str) -> int:
    """Official lot sizes for index options (validate annually)."""
    sizes = {"BANKNIFTY": 15, "FINNIFTY": 40, "NIFTY": 75}
    for k, v in sizes.items():
        if k in symbol.upper():
            return v
    return 25


class StateStore:
    """Thread-safe persistence to db.json for Railway restart recovery."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self.daily_pnl: float = 0.0
        self.trade_count: int = 0
        self.active_trade: Optional[ActiveTrade] = None
        self.day: str = ""
        self._load()

    def _load(self) -> None:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                if data.get("day") == today:
                    self.daily_pnl = float(data.get("daily_pnl", 0.0))
                    self.trade_count = int(data.get("trade_count", 0))
                    raw_trade = data.get("active_trade")
                    if raw_trade:
                        self.active_trade = ActiveTrade(**raw_trade)
                    self.day = today
                    logger.info("State restored from %s — PnL=%.0f trades=%d",
                                self._path, self.daily_pnl, self.trade_count)
                    return
            except Exception as exc:
                logger.warning("State load failed: %s", exc)
        self.day = today

    async def save(self) -> None:
        async with self._lock:
            data: Dict[str, Any] = {
                "day": self.day,
                "daily_pnl": self.daily_pnl,
                "trade_count": self.trade_count,
                "active_trade": asdict(self.active_trade) if self.active_trade else None,
                "saved_at": datetime.now(IST).isoformat(),
            }
            try:
                self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            except Exception as exc:
                logger.error("State save failed: %s", exc)

    def day_blocked(self) -> bool:
        """True if daily loss limit or target has been hit."""
        return (
            self.daily_pnl <= -Cfg.MAX_DAILY_LOSS
            or self.daily_pnl >= Cfg.DAILY_TARGET
        )

    def record_closed_trade(self, pnl: float) -> None:
        self.daily_pnl = round(self.daily_pnl + pnl, 2)
        self.trade_count += 1
        self.active_trade = None


# ──────────────────────────────────────────────────────────────────────────────
# AngelOne client wrapper
# ──────────────────────────────────────────────────────────────────────────────

class AngelClient:
    """Manages SmartConnect session with TOTP re-login."""

    def __init__(self) -> None:
        self._client: Optional[SmartConnect] = None
        self.auth_token: str = ""
        self.feed_token: str = ""
        self._login_time: float = 0.0

    def login(self) -> bool:
        try:
            totp = pyotp.TOTP(Cfg.TOTP_SECRET).now()
            self._client = SmartConnect(api_key=Cfg.API_KEY)
            session = self._client.generateSession(Cfg.CLIENT_ID, Cfg.PASSWORD, totp)
            if not (session and session.get("status")):
                raise RuntimeError(f"Session failed: {session}")
            self.auth_token = session["data"]["jwtToken"]
            self.feed_token = self._client.getfeedToken()
            self._login_time = time.time()
            logger.info("✅ Angel One login successful")
            return True
        except Exception as exc:
            logger.error("❌ Login failed: %s", exc)
            return False

    def ensure_session(self) -> None:
        """Re-login if session is older than 6 hours."""
        if time.time() - self._login_time > 21600:
            logger.info("Session refresh triggered")
            self.login()

    def place_order(self, params: Dict[str, Any]) -> str:
        """Returns order_id or raises."""
        self.ensure_session()
        if not self._client:
            raise RuntimeError("Client not initialized")
        resp = self._client.placeOrder(params)
        order_id = str(resp["data"]["orderid"])
        return order_id

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        self.ensure_session()
        if not self._client:
            return 0.0
        resp = self._client.ltpData(exchange, symbol, token)
        return float(resp["data"]["ltp"])

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> None:
        try:
            if self._client:
                self._client.cancelOrder(order_id=order_id, variety=variety)
        except Exception as exc:
            logger.warning("Cancel order %s failed: %s", order_id, exc)

    def get_funds(self) -> float:
        self.ensure_session()
        if not self._client:
            return 0.0
        try:
            data = self._client.rmsLimit().get("data", {})
            return float(data.get("availablecash", 0))
        except Exception:
            return 0.0

    @property
    def raw(self) -> Optional[SmartConnect]:
        return self._client


# ──────────────────────────────────────────────────────────────────────────────
# Instrument resolver
# ──────────────────────────────────────────────────────────────────────────────

SCRIP_MASTER_URL = (
    "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
)

STRIKE_STEP = {"BANKNIFTY": 100, "FINNIFTY": 50, "NIFTY": 50}
SPOT_TOKEN   = {"BANKNIFTY": "26009", "FINNIFTY": "26037", "NIFTY": "26000"}


async def fetch_scrip_master() -> List[Dict[str, Any]]:
    cache = Path("scrip_master_cache.json")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(SCRIP_MASTER_URL)
            r.raise_for_status()
            rows = r.json()
            cache.write_text(json.dumps(rows), encoding="utf-8")
            return rows
    except Exception as exc:
        logger.warning("Scrip master download failed (%s), using cache", exc)
        if cache.exists():
            return json.loads(cache.read_text(encoding="utf-8"))
        raise


def find_atm_option(rows: List[Dict], underlying: str, spot_price: float,
                    option_type: str, trade_date: str) -> Dict[str, Any]:
    """Return the nearest ATM weekly option contract dict."""
    step = STRIKE_STEP.get(underlying, 100)
    atm = round(spot_price / step) * step
    from datetime import date as _date, datetime as _dt
    today = _dt.strptime(trade_date, "%Y-%m-%d").date()

    candidates = []
    for row in rows:
        if row.get("exch_seg") != "NFO":
            continue
        if row.get("instrumenttype") != "OPTIDX":
            continue
        if row.get("name") != underlying:
            continue
        sym = str(row.get("symbol", ""))
        if not sym.endswith(option_type):
            continue
        raw_expiry = str(row.get("expiry", "")).strip().upper()
        try:
            expiry = _dt.strptime(raw_expiry, "%d%b%Y").date()
        except ValueError:
            continue
        if expiry < today:
            continue
        raw_strike = float(row.get("strike", 0)) / 100.0
        candidates.append((expiry, abs(raw_strike - atm), row))

    if not candidates:
        raise RuntimeError(f"No {underlying} {option_type} contracts found")
    candidates.sort(key=lambda x: (x[0], x[1]))
    return candidates[0][2]


# ──────────────────────────────────────────────────────────────────────────────
# Order helpers
# ──────────────────────────────────────────────────────────────────────────────

TICK_SIZE = 0.05


def _round_tick(price: float) -> float:
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def _sl_price(entry: float, option_type: str, pct: float = 0.30) -> float:
    """Initial SL = 30% below entry for long option."""
    return _round_tick(entry * (1.0 - pct))


async def place_limit_order_async(
    angel: AngelClient,
    symbol: str,
    token: str,
    qty: int,
    action: str,        # "BUY" | "SELL"
    ltp: float,
    buffer: float = 2.0,
) -> str:
    """Non-blocking limit order with a 2-pt buffer for immediate fill."""
    price = _round_tick(ltp + buffer if action == "BUY" else ltp - buffer)
    params = {
        "variety": "NORMAL",
        "tradingsymbol": symbol,
        "symboltoken": token,
        "transactiontype": action,
        "exchange": "NFO",
        "ordertype": "LIMIT",
        "producttype": "INTRADAY",
        "duration": "IOC",
        "quantity": str(qty),
        "price": str(price),
    }
    if Cfg.TRADE_MODE == "paper":
        simulated_id = f"PAPER_{int(time.time() * 1000)}"
        logger.info("[PAPER] %s %s qty=%d @%.2f (limit)", action, symbol, qty, price)
        return simulated_id

    loop = asyncio.get_running_loop()
    order_id = await loop.run_in_executor(None, lambda: angel.place_order(params))
    return order_id


# ──────────────────────────────────────────────────────────────────────────────
# Telegram notification
# ──────────────────────────────────────────────────────────────────────────────

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self._bot: Optional[Bot] = Bot(token=token) if token else None
        self._chat_id = chat_id

    async def send(self, text: str, reply_markup=None) -> None:
        if not self._bot or not self._chat_id:
            logger.info("[TG] %s", text)
            return
        try:
            await self._bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
        except Exception as exc:
            logger.error("Telegram send failed: %s", exc)

    async def alert_error(self, component: str, error: str) -> None:
        await self.send(f"🚨 *ERROR* — `{component}`\n`{error}`")


# ──────────────────────────────────────────────────────────────────────────────
# Core Trading Engine
# ──────────────────────────────────────────────────────────────────────────────

class TradingEngine:
    """
    Central async engine. Responsibilities:
      - Manage WebSocket tick feed (with exponential backoff reconnect)
      - Run Alpha strategy on each tick/candle
      - Fire orders within 100ms of signal
      - Manage trailing SL
      - Daily P&L kill-switch
      - State persistence
    """

    WS_RETRY_BASE = 2.0       # seconds base for exponential backoff
    WS_RETRY_MAX  = 60.0      # maximum backoff cap (seconds)

    def __init__(self) -> None:
        self.angel      = AngelClient()
        self.tg         = TelegramNotifier(Cfg.TELEGRAM_TOKEN, Cfg.TELEGRAM_CHAT_ID)
        self.state      = StateStore(Cfg.STATE_FILE)
        self.strategy   = AlphaStrategy()
        self.indicators: Dict[str, IndicatorEngine] = {}   # keyed by token
        self._scrip_rows: List[Dict] = []
        self._spot_ltp: Dict[str, float] = {}              # underlying -> ltp
        self._option_ltp: Dict[str, float] = {}            # token -> ltp
        self._ws_retry_count = 0
        self._running = False
        self._trade_armed = False      # user confirmed real/paper trade
        self._trade_mode_confirmed = Cfg.TRADE_MODE   # set by /starttrade command
        self._ws: Optional[SmartWebSocketV2] = None
        self._subscribed_tokens: set[str] = set()
        self._trailing: Optional[TrailingState] = None
        self._vix: float = 15.0
        self._ad_ratio: float = 1.0
        self._last_pct_notify: Dict[str, float] = {}  # token -> last notified price
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ── Startup ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._running = True

        logger.info("🚀 Trading Engine starting — mode=%s underlying=%s",
                    Cfg.TRADE_MODE, Cfg.UNDERLYING)

        # Angel One login (skip in paper-no-creds mode)
        if Cfg.API_KEY:
            ok = await asyncio.get_event_loop().run_in_executor(None, self.angel.login)
            if not ok and Cfg.TRADE_MODE == "live":
                raise RuntimeError("Angel One login failed for LIVE mode")
        else:
            logger.warning("No API_KEY — running in paper-only simulation mode")

        # Load instrument master
        try:
            self._scrip_rows = await fetch_scrip_master()
            logger.info("Scrip master loaded (%d instruments)", len(self._scrip_rows))
        except Exception as exc:
            await self.tg.alert_error("SCRIP_MASTER", str(exc))
            logger.error("Scrip master failed: %s", exc)

        # Notify Telegram
        await self.tg.send(
            f"🤖 *Ultra-Fast Algo Bot Started*\n"
            f"Mode: `{Cfg.TRADE_MODE.upper()}`\n"
            f"Underlying: `{Cfg.UNDERLYING}`\n"
            f"Capital: ₹{Cfg.CAPITAL:,.0f} | MaxLoss: ₹{Cfg.MAX_DAILY_LOSS:,.0f}\n"
            f"Target: ₹{Cfg.DAILY_TARGET:,.0f} | Lot: {Cfg.LOT_SIZE}\n"
            f"Time: `{_now_label()}`"
        )

        # Resume active trade if restarted
        if self.state.active_trade:
            self._trailing = self.state.active_trade.trail()
            await self.tg.send(
                f"♻️ *Resumed active trade from previous session*\n"
                f"Symbol: `{self.state.active_trade.symbol}`\n"
                f"Entry: ₹{self.state.active_trade.entry_price:.2f} "
                f"SL: ₹{self.state.active_trade.sl_price:.2f}"
            )

        # Run WebSocket feed loop
        asyncio.create_task(self._ws_loop())
        # Run background maintenance loop
        asyncio.create_task(self._maintenance_loop())

    # ── WebSocket feed with exponential backoff ───────────────────────────────

    async def _ws_loop(self) -> None:
        while self._running:
            try:
                await self._connect_ws()
                self._ws_retry_count = 0
            except Exception as exc:
                self._ws_retry_count += 1
                delay = min(
                    self.WS_RETRY_MAX,
                    self.WS_RETRY_BASE * (2 ** (self._ws_retry_count - 1)),
                )
                logger.warning("WebSocket error (attempt %d): %s — retrying in %.1fs",
                               self._ws_retry_count, exc, delay)
                await self.tg.send(
                    f"⚠️ WS reconnecting in {delay:.0f}s (attempt {self._ws_retry_count})"
                )
                await asyncio.sleep(delay)
                # Refresh session before reconnect
                if Cfg.API_KEY:
                    await self._loop.run_in_executor(None, self.angel.login)  # type: ignore[union-attr]

    async def _connect_ws(self) -> None:
        if not self.angel.auth_token:
            await asyncio.sleep(5)
            return

        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING)
        tokens_to_sub = [{"exchangeType": 1, "tokens": [spot_token]}] if spot_token else []

        loop = asyncio.get_running_loop()

        def on_open(wsapp):
            logger.info("WebSocket connected ✅")
            if tokens_to_sub:
                wsapp.subscribe("bot1", 3, tokens_to_sub)   # mode 3 = QUOTE

        def on_data(_wsapp, message):
            loop.call_soon_threadsafe(
                asyncio.create_task, self._on_tick_raw(message)
            )

        def on_error(_wsapp, error):
            logger.error("WS error: %s", error)

        def on_close(_wsapp, code, msg):
            logger.warning("WS closed: %s %s", code, msg)

        self._ws = SmartWebSocketV2(
            self.angel.auth_token,
            Cfg.API_KEY,
            Cfg.CLIENT_ID,
            self.angel.feed_token,
        )
        self._ws.on_open = on_open
        self._ws.on_data = on_data
        self._ws.on_error = on_error
        self._ws.on_close = on_close

        await loop.run_in_executor(None, self._ws.connect)

    # ── Tick processing ───────────────────────────────────────────────────────

    async def _on_tick_raw(self, message: Any) -> None:
        """Deserialize WebSocket tick and route to strategy."""
        t_start = time.monotonic()
        try:
            if isinstance(message, (bytes, bytearray)):
                import struct
                # SmartWebSocketV2 sends binary-encoded ticks
                data = self._parse_binary_tick(message)
            elif isinstance(message, dict):
                data = message
            else:
                return

            token = str(data.get("token", ""))
            ltp   = float(data.get("last_traded_price", data.get("ltp", 0)) or 0) / 100.0
            vol   = int(data.get("volume_trade_for_the_day", data.get("volume", 0)) or 0)
            ts_ms = int(data.get("exchange_timestamp", time.time() * 1000) or time.time() * 1000)

            if ltp <= 0:
                return

            tick = Tick(token=token, ltp=ltp, volume=vol, timestamp_ms=ts_ms)
            await self._process_tick(tick, t_start)

        except Exception as exc:
            logger.debug("Tick parse error: %s", exc)

    def _parse_binary_tick(self, data: bytes) -> Dict[str, Any]:
        """
        Parse Angel One binary WebSocket message.
        See Angel One SmartAPI WebSocket documentation for format.
        """
        try:
            # Mode 3 (QUOTE): subscription_mode(1) + exchange_type(1) + token(25) +
            #   seq_no(8) + exchange_ts(8) + ltp(8) + ... (all ints, big-endian)
            if len(data) < 51:
                return {}
            token_raw = data[2:27].decode("utf-8").strip("\x00")
            ltp_raw   = int.from_bytes(data[43:51], "big")   # int64 paise
            vol = 0
            if len(data) >= 67:
                vol = int.from_bytes(data[59:67], "big")
            ts_ms = int(time.time() * 1000)
            return {"token": token_raw, "last_traded_price": ltp_raw, "volume": vol,
                    "exchange_timestamp": ts_ms}
        except Exception:
            return {}

    async def _process_tick(self, tick: Tick, t_start: float) -> None:
        """Strategy evaluation & order firing within 100ms."""
        token = tick.token
        ltp   = tick.ltp

        # Update spot price map (underlying index)
        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
        if token == spot_token:
            self._spot_ltp[Cfg.UNDERLYING] = ltp

        # Update option LTP if we have a trade
        if self.state.active_trade and token == self.state.active_trade.token:
            self._option_ltp[token] = ltp
            # 1% price move notification
            await self._maybe_notify_price_move(token, ltp)
            # Trailing SL management
            await self._manage_trailing(ltp)

        # Strategy evaluation only on underlying ticks
        if token != spot_token:
            return

        # Skip if not in trading hours or daily block
        if not _is_trading_hours() or self.state.day_blocked():
            return

        # Skip if already in a trade
        if self.state.active_trade:
            return

        # Strategy must be armed by user
        if not self._trade_armed:
            return

        # Build / update indicator
        if token not in self.indicators:
            self.indicators[token] = IndicatorEngine()
        ind = self.indicators[token]
        completed_candle = ind.on_tick(tick)

        # Evaluate only when a new 5-min candle closes (or on every tick if no candle yet)
        current_vol = float(tick.volume)
        result: SignalResult = self.strategy.evaluate(ltp, ind, current_vol)

        if result.signal in ("CALL", "PUT"):
            elapsed_ms = (time.monotonic() - t_start) * 1000
            logger.info(
                "🎯 Signal %s | RSI=%.1f | ST=%s | Spike=%s | eval=%.1fms",
                result.signal, result.rsi, result.supertrend_dir,
                result.volume_spike, elapsed_ms,
            )
            await self._fire_order(result, ltp)

    # ── Order execution ───────────────────────────────────────────────────────

    async def _fire_order(self, signal: SignalResult, spot_ltp: float) -> None:
        """Find ATM option and place BUY order within 100ms of signal."""
        t0 = time.monotonic()
        try:
            option_type = "CE" if signal.signal == "CALL" else "PE"
            today = datetime.now(IST).strftime("%Y-%m-%d")

            # Find ATM contract
            row = find_atm_option(
                self._scrip_rows, Cfg.UNDERLYING, spot_ltp, option_type, today,
            )
            symbol = str(row["symbol"])
            token  = str(row["token"])
            raw_lot_size = int(float(row.get("lotsize") or _lot_qty(Cfg.UNDERLYING)))
            qty = Cfg.LOT_SIZE * raw_lot_size

            # Get option LTP
            if Cfg.TRADE_MODE == "live" and Cfg.API_KEY:
                loop = asyncio.get_running_loop()
                opt_ltp = await loop.run_in_executor(
                    None, lambda: self.angel.get_ltp("NFO", symbol, token)
                )
            else:
                opt_ltp = spot_ltp * 0.01   # paper simulation ~1% of index

            if opt_ltp <= 0:
                logger.warning("Option LTP unavailable for %s", symbol)
                return

            order_id = await place_limit_order_async(
                self.angel, symbol, token, qty, "BUY", opt_ltp
            )

            sl = _sl_price(opt_ltp, option_type)
            trade = ActiveTrade(
                order_id=order_id,
                symbol=symbol,
                token=token,
                option_type=option_type,
                lot_size=Cfg.LOT_SIZE,
                entry_price=opt_ltp,
                sl_price=sl,
                entry_time=datetime.now(IST).isoformat(),
                mode=self._trade_mode_confirmed,
            )
            self.state.active_trade = trade
            self._trailing = TrailingState(
                entry_price=opt_ltp,
                current_sl=sl,
                high_water=opt_ltp,
                lot_size=qty,
            )
            await self.state.save()

            elapsed_ms = (time.monotonic() - t0) * 1000
            logger.info("✅ Order fired in %.1fms", elapsed_ms)

            await self.tg.send(
                f"✅ *ORDER PLACED* — `{signal.signal}`\n"
                f"Symbol: `{symbol}`\n"
                f"Qty: {qty} | Entry: ₹{opt_ltp:.2f} | SL: ₹{sl:.2f}\n"
                f"Reason: {signal.reason}\n"
                f"⚡ Fired in *{elapsed_ms:.1f}ms*"
            )

        except Exception as exc:
            logger.error("Order fire failed: %s", exc)
            await self.tg.alert_error("ORDER_FIRE", str(exc))

    # ── Trailing SL management ────────────────────────────────────────────────

    async def _manage_trailing(self, ltp: float) -> None:
        if not self._trailing or not self.state.active_trade:
            return
        trade = self.state.active_trade
        changed, new_sl = self._trailing.update(ltp)

        if changed:
            trade.sl_price = new_sl
            await self.state.save()
            pnl = self._trailing.unrealised_pnl(ltp)
            be_tag = " 🔒 Breakeven" if self._trailing.is_breakeven else ""
            await self.tg.send(
                f"📈 *SL TRAILED*{be_tag}\n"
                f"`{trade.symbol}` LTP=₹{ltp:.2f}\n"
                f"New SL: ₹{new_sl:.2f} | Unrealised P&L: ₹{pnl:.0f}"
            )

        # Hard SL hit
        if ltp <= trade.sl_price:
            await self._exit_trade(ltp, reason="STOP_LOSS_HIT")

    async def _exit_trade(self, ltp: float, reason: str = "SIGNAL_EXIT") -> None:
        if not self.state.active_trade:
            return
        trade = self.state.active_trade
        qty = trade.lot_size * _lot_qty(trade.symbol)

        try:
            eid = await place_limit_order_async(
                self.angel, trade.symbol, trade.token, qty, "SELL", ltp
            )
        except Exception as exc:
            logger.error("Exit order failed: %s", exc)
            await self.tg.alert_error("EXIT_ORDER", str(exc))
            return

        pnl = round((ltp - trade.entry_price) * qty, 2)
        self.state.record_closed_trade(pnl)
        self._trailing = None
        await self.state.save()

        emoji = "🟢" if pnl >= 0 else "🔴"
        await self.tg.send(
            f"{emoji} *TRADE CLOSED* — {reason}\n"
            f"`{trade.symbol}` Exit: ₹{ltp:.2f}\n"
            f"Entry: ₹{trade.entry_price:.2f} | P&L: ₹{pnl:+.0f}\n"
            f"Daily P&L: ₹{self.state.daily_pnl:+.0f}"
        )

        # Kill switch
        if self.state.daily_pnl <= -Cfg.MAX_DAILY_LOSS:
            await self.tg.send(
                "🛑 *DAILY LOSS LIMIT REACHED* — Bot stopping for the day.\n"
                f"Total Loss: ₹{abs(self.state.daily_pnl):.0f}"
            )
            logger.critical("Daily loss limit hit. Bot shutting down for today.")
            self._trade_armed = False

    # ── 1% price move notifications ───────────────────────────────────────────

    async def _maybe_notify_price_move(self, token: str, ltp: float) -> None:
        last = self._last_pct_notify.get(token, ltp)
        if last <= 0:
            self._last_pct_notify[token] = ltp
            return
        pct = abs((ltp - last) / last) * 100
        if pct >= 1.0:
            self._last_pct_notify[token] = ltp
            direction = "📈" if ltp > last else "📉"
            await self.tg.send(
                f"{direction} *1% Price Move* — `{token}`\n"
                f"₹{last:.2f} → ₹{ltp:.2f} ({pct:+.1f}%)"
            )

    # ── Background maintenance ────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        """Periodic session refresh, EOD square-off, state save."""
        while self._running:
            try:
                hhmm = _hhmm()

                # EOD square off at 14:25
                if hhmm == 1425 and self.state.active_trade:
                    loop = asyncio.get_running_loop()
                    ltp = 0.0
                    if Cfg.API_KEY:
                        t = self.state.active_trade
                        ltp = await loop.run_in_executor(
                            None, lambda: self.angel.get_ltp("NFO", t.symbol, t.token)
                        )
                    await self._exit_trade(ltp or self.state.active_trade.entry_price,
                                           reason="EOD_SQUARE_OFF")

                # Periodic state persistence
                await self.state.save()

            except Exception as exc:
                logger.error("Maintenance loop error: %s", exc)
                await self.tg.alert_error("MAINTENANCE", str(exc))

            await asyncio.sleep(30)

    # ── Telegram command handlers ─────────────────────────────────────────────

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        loop = asyncio.get_running_loop()
        balance = 0.0
        if Cfg.API_KEY:
            balance = await loop.run_in_executor(None, self.angel.get_funds)
        mhi = market_health_index(self._vix, self._ad_ratio)
        trade_info = "None"
        if self.state.active_trade:
            t = self.state.active_trade
            opt_ltp = self._option_ltp.get(t.token, t.entry_price)
            unreal = round((opt_ltp - t.entry_price) * t.lot_size * _lot_qty(t.symbol), 2)
            trade_info = (
                f"`{t.symbol}` Entry=₹{t.entry_price:.2f} "
                f"SL=₹{t.sl_price:.2f} Unreal=₹{unreal:+.0f}"
            )
        msg = (
            f"📊 *Bot Status* — `{_now_label()}`\n"
            f"Balance: ₹{balance:,.0f}\n"
            f"Today P&L: ₹{self.state.daily_pnl:+.0f}\n"
            f"Trades today: {self.state.trade_count}\n"
            f"Active Trade: {trade_info}\n"
            f"Armed: {'✅' if self._trade_armed else '❌'}\n"
            f"Market Health: {mhi['label']} (score={mhi['score']})\n"
            f"VIX: {self._vix:.1f} | A-D: {self._ad_ratio:.2f}"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def cmd_check_strategy(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        spot = self._spot_ltp.get(Cfg.UNDERLYING, 0)
        token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
        ind = self.indicators.get(token)
        if not ind or ind.num_candles < 5 or spot <= 0:
            await update.message.reply_text(
                "⏳ *Warming up* — need at least 5 candles of data. Wait a few minutes.",
                parse_mode="Markdown",
            )
            return
        result = self.strategy.evaluate(spot, ind, float(ind.candles[-1].volume if ind.candles else 0))
        if result.signal in ("CALL", "PUT"):
            label = "✅ *Strong Signal Found!*"
        else:
            label = "❌ *Market Unstable — Avoid*"
        msg = (
            f"{label}\n"
            f"Signal: `{result.signal}` | Strength: {result.strength}%\n"
            f"Supertrend: `{result.supertrend_dir}` | VWAP: {'above' if result.above_vwap else 'below'}\n"
            f"RSI: {result.rsi:.1f} | VolSpike: {'✅' if result.volume_spike else '❌'}\n"
            f"Reason: _{result.reason}_"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")

    async def cmd_start_trade(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        keyboard = [
            [
                InlineKeyboardButton("📄 Paper Trade", callback_data="confirm_paper"),
                InlineKeyboardButton("💰 Real Trade", callback_data="confirm_live"),
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_trade")],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "⚠️ *Choose trading mode*\n\n"
            "Paper = simulated, no real money\n"
            "Real = live orders on Angel One",
            parse_mode="Markdown",
            reply_markup=markup,
        )

    async def callback_handler(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "confirm_paper":
            self._trade_mode_confirmed = "paper"
            self._trade_armed = True
            await query.edit_message_text(
                "✅ *Paper trading armed.* Bot will simulate entries.",
                parse_mode="Markdown",
            )
        elif data == "confirm_live":
            if not Cfg.API_KEY:
                await query.edit_message_text(
                    "❌ Cannot start LIVE trade — API_KEY not configured.",
                    parse_mode="Markdown",
                )
                return
            self._trade_mode_confirmed = "live"
            self._trade_armed = True
            await query.edit_message_text(
                "🚨 *LIVE trading armed.* Real orders will be placed on Angel One.",
                parse_mode="Markdown",
            )
        elif data == "cancel_trade":
            self._trade_armed = False
            await query.edit_message_text("❌ Trade start cancelled.")

    async def cmd_stop(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        self._trade_armed = False
        await update.message.reply_text(
            "🛑 *Bot disarmed.* No new trades will be placed.\n"
            "Active trade (if any) continues to trail SL.",
            parse_mode="Markdown",
        )

    async def cmd_exit_now(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.state.active_trade:
            await update.message.reply_text("No active trade to exit.")
            return
        t = self.state.active_trade
        loop = asyncio.get_running_loop()
        ltp = self._option_ltp.get(t.token, t.entry_price)
        if Cfg.API_KEY and ltp == t.entry_price:
            ltp = await loop.run_in_executor(
                None, lambda: self.angel.get_ltp("NFO", t.symbol, t.token)
            )
        await self._exit_trade(ltp, reason="MANUAL_EXIT")
        await update.message.reply_text("✅ Manual exit initiated.")

    async def cmd_setvix(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        args = ctx.args or []
        if not args:
            await update.message.reply_text("Usage: /setvix <value>  e.g. /setvix 14.5")
            return
        try:
            self._vix = float(args[0])
            await update.message.reply_text(f"VIX updated to {self._vix:.2f}")
        except ValueError:
            await update.message.reply_text("Invalid VIX value")

    async def cmd_setad(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        args = ctx.args or []
        if not args:
            await update.message.reply_text("Usage: /setad <ratio>  e.g. /setad 1.8")
            return
        try:
            self._ad_ratio = float(args[0])
            await update.message.reply_text(f"A-D ratio updated to {self._ad_ratio:.2f}")
        except ValueError:
            await update.message.reply_text("Invalid A-D ratio")

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        help_text = (
            "🤖 *Ultra-Fast Algo Bot Commands*\n\n"
            "/status — Balance, P&L, Market Health Index\n"
            "/checkstrategy — Analyze current setup\n"
            "/starttrade — Choose Paper or Real mode\n"
            "/stop — Disarm bot (no new trades)\n"
            "/exitnow — Force exit active trade\n"
            "/setvix <val> — Update VIX manually\n"
            "/setad <val> — Update Advance-Decline ratio\n"
            "/help — Show this message"
        )
        await update.message.reply_text(help_text, parse_mode="Markdown")


# ──────────────────────────────────────────────────────────────────────────────
# Telegram Application builder
# ──────────────────────────────────────────────────────────────────────────────

def build_telegram_app(engine: TradingEngine) -> Optional[Application]:
    if not Cfg.TELEGRAM_TOKEN:
        logger.warning("No TELEGRAM_TOKEN — Telegram commands disabled")
        return None

    app = Application.builder().token(Cfg.TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("status",        engine.cmd_status))
    app.add_handler(CommandHandler("checkstrategy", engine.cmd_check_strategy))
    app.add_handler(CommandHandler("starttrade",    engine.cmd_start_trade))
    app.add_handler(CommandHandler("stop",          engine.cmd_stop))
    app.add_handler(CommandHandler("exitnow",       engine.cmd_exit_now))
    app.add_handler(CommandHandler("setvix",        engine.cmd_setvix))
    app.add_handler(CommandHandler("setad",         engine.cmd_setad))
    app.add_handler(CommandHandler("help",          engine.cmd_help))
    app.add_handler(CallbackQueryHandler(engine.callback_handler))
    return app


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

async def _async_main() -> None:
    engine = TradingEngine()
    await engine.start()

    tg_app = build_telegram_app(engine)
    if tg_app:
        await tg_app.initialize()
        await tg_app.start()
        await tg_app.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram polling started")

    # Keep alive
    try:
        while True:
            await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        engine._running = False
        if tg_app:
            await tg_app.updater.stop()
            await tg_app.stop()
            await tg_app.shutdown()
        await engine.state.save()
        logger.info("Bot shut down gracefully.")


def main() -> None:
    # Graceful shutdown on SIGTERM (Railway sends this)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown():
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass   # Windows

    try:
        loop.run_until_complete(_async_main())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
