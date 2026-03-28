"""
Ultra-Fast Algo Bot — Single-file deployment for Railway.app
Angel One SmartAPI | BankNifty/FinNifty Weekly Options
Hybrid Supertrend(3,10) + VWAP + RSI(9) + Volume Spike Strategy
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGIES MODULE (inlined — no external import needed)
# ══════════════════════════════════════════════════════════════════════════════

import collections
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

logger = logging.getLogger("bot")


@dataclass
class Candle:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Tick:
    token: str
    ltp: float
    volume: int
    timestamp_ms: int


@dataclass
class SignalResult:
    signal: str           # "CALL" | "PUT" | "NONE"
    strength: int         # 0–100
    reason: str
    supertrend_dir: str   # "UP" | "DOWN"
    above_vwap: bool
    rsi: float
    volume_spike: bool
    generated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class IndicatorEngine:
    """
    Stateful 5-min candle builder + Supertrend(3,10) + VWAP + RSI(9).
    Pure Python — no Pandas/numpy — for predictable sub-ms latency.
    """

    CANDLE_SECONDS        = 300
    RSI_PERIOD            = 9
    SUPERTREND_PERIOD     = 10
    SUPERTREND_MULTIPLIER = 3.0
    VOLUME_SPIKE_LOOKBACK = 5
    VOLUME_SPIKE_FACTOR   = 2.0

    def __init__(self) -> None:
        self._candles: Deque[Candle] = collections.deque(maxlen=100)
        self._cur_open = 0.0
        self._cur_high = 0.0
        self._cur_low  = float("inf")
        self._cur_close = 0.0
        self._cur_volume = 0
        self._cur_candle_start = 0.0

        # VWAP
        self._vwap_cum_pv   = 0.0
        self._vwap_cum_v    = 0.0
        self._vwap_reset_ts = 0.0

        # RSI (Wilder smoothing)
        self._rsi_gains: Deque[float] = collections.deque(maxlen=self.RSI_PERIOD)
        self._rsi_losses: Deque[float] = collections.deque(maxlen=self.RSI_PERIOD)
        self._rsi_avg_gain: Optional[float] = None
        self._rsi_avg_loss: Optional[float] = None
        self._rsi_prev_close: Optional[float] = None

        # Supertrend
        self._st_upper:    Optional[float] = None
        self._st_lower:    Optional[float] = None
        self._st_direction = "UP"
        self._atr_values: Deque[float] = collections.deque(maxlen=self.SUPERTREND_PERIOD)
        self._atr_smooth:  Optional[float] = None

    # ── VWAP ─────────────────────────────────────────────────────────────────

    def _maybe_reset_vwap(self, ts_sec: float) -> None:
        import datetime
        from zoneinfo import ZoneInfo
        dt = datetime.datetime.fromtimestamp(ts_sec, tz=ZoneInfo("Asia/Kolkata"))
        session_start = dt.replace(hour=9, minute=15, second=0, microsecond=0)
        reset_epoch = session_start.timestamp()
        if reset_epoch > self._vwap_reset_ts:
            self._vwap_cum_pv   = 0.0
            self._vwap_cum_v    = 0.0
            self._vwap_reset_ts = reset_epoch

    @property
    def vwap(self) -> float:
        return self._vwap_cum_pv / self._vwap_cum_v if self._vwap_cum_v > 0 else 0.0

    # ── RSI ───────────────────────────────────────────────────────────────────

    def _update_rsi(self, close: float) -> None:
        if self._rsi_prev_close is None:
            self._rsi_prev_close = close
            return
        change = close - self._rsi_prev_close
        self._rsi_prev_close = close
        gain = max(0.0, change)
        loss = max(0.0, -change)
        self._rsi_gains.append(gain)
        self._rsi_losses.append(loss)
        n = self.RSI_PERIOD
        if len(self._rsi_gains) < n:
            return
        if self._rsi_avg_gain is None:
            self._rsi_avg_gain = sum(self._rsi_gains) / n
            self._rsi_avg_loss = sum(self._rsi_losses) / n
        else:
            self._rsi_avg_gain = (self._rsi_avg_gain * (n - 1) + gain) / n
            self._rsi_avg_loss = (self._rsi_avg_loss * (n - 1) + loss) / n  # type: ignore[operator]

    @property
    def rsi(self) -> float:
        if self._rsi_avg_gain is None or self._rsi_avg_loss is None:
            return 50.0
        if self._rsi_avg_loss == 0:
            return 100.0
        rs = self._rsi_avg_gain / self._rsi_avg_loss
        return round(100.0 - (100.0 / (1.0 + rs)), 2)

    # ── Supertrend ────────────────────────────────────────────────────────────

    def _true_range(self, candle: Candle, prev_close: float) -> float:
        return max(
            candle.high - candle.low,
            abs(candle.high - prev_close),
            abs(candle.low  - prev_close),
        )

    def _update_supertrend(self, candle: Candle) -> None:
        if len(self._candles) < 2:
            return
        prev = self._candles[-2]
        tr   = self._true_range(candle, prev.close)
        n    = self.SUPERTREND_PERIOD
        if self._atr_smooth is None:
            self._atr_values.append(tr)
            if len(self._atr_values) >= n:
                self._atr_smooth = sum(self._atr_values) / n
        else:
            self._atr_smooth = (self._atr_smooth * (n - 1) + tr) / n

        if self._atr_smooth is None:
            return

        mid         = (candle.high + candle.low) / 2.0
        m           = self.SUPERTREND_MULTIPLIER
        basic_upper = mid + m * self._atr_smooth
        basic_lower = mid - m * self._atr_smooth

        prev_upper  = self._st_upper if self._st_upper is not None else basic_upper
        prev_lower  = self._st_lower if self._st_lower is not None else basic_lower

        final_upper = basic_upper if (basic_upper < prev_upper or prev.close > prev_upper) else prev_upper
        final_lower = basic_lower if (basic_lower > prev_lower or prev.close < prev_lower) else prev_lower

        if self._st_direction == "UP":
            if candle.close < final_lower:
                self._st_direction = "DOWN"
        else:
            if candle.close > final_upper:
                self._st_direction = "UP"

        self._st_upper = final_upper
        self._st_lower = final_lower

    # ── Candle builder ────────────────────────────────────────────────────────

    def _candle_start(self, ts_sec: float) -> float:
        return ts_sec - (ts_sec % self.CANDLE_SECONDS)

    def _flush_candle(self) -> Optional[Candle]:
        if self._cur_volume == 0 or self._cur_open == 0.0:
            return None
        c = Candle(
            timestamp=self._cur_candle_start,
            open=self._cur_open, high=self._cur_high,
            low=self._cur_low,   close=self._cur_close,
            volume=self._cur_volume,
        )
        self._candles.append(c)
        self._update_rsi(c.close)
        self._update_supertrend(c)
        self._cur_open   = 0.0
        self._cur_high   = 0.0
        self._cur_low    = float("inf")
        self._cur_close  = 0.0
        self._cur_volume = 0
        return c

    def on_tick(self, tick: Tick) -> Optional[Candle]:
        ts_sec = tick.timestamp_ms / 1000.0
        self._maybe_reset_vwap(ts_sec)
        self._vwap_cum_pv += tick.ltp * max(0, tick.volume)
        self._vwap_cum_v  += max(0, tick.volume)

        bucket = self._candle_start(ts_sec)
        if self._cur_candle_start == 0.0:
            self._cur_candle_start = bucket

        completed: Optional[Candle] = None
        if bucket != self._cur_candle_start:
            completed = self._flush_candle()
            self._cur_candle_start = bucket

        price = tick.ltp
        if self._cur_open == 0.0:
            self._cur_open = price
        self._cur_high   = max(self._cur_high, price)
        self._cur_low    = min(self._cur_low,  price)
        self._cur_close  = price
        self._cur_volume += max(0, tick.volume)
        return completed

    # ── Volume spike ──────────────────────────────────────────────────────────

    def volume_spike(self, current_volume: float) -> bool:
        if len(self._candles) < self.VOLUME_SPIKE_LOOKBACK:
            return False
        recent = list(self._candles)[-self.VOLUME_SPIKE_LOOKBACK:]
        avg = sum(c.volume for c in recent) / len(recent)
        return avg > 0 and current_volume >= avg * self.VOLUME_SPIKE_FACTOR

    @property
    def candles(self) -> List[Candle]:
        return list(self._candles)

    @property
    def supertrend_direction(self) -> str:
        return self._st_direction

    @property
    def num_candles(self) -> int:
        return len(self._candles)


class AlphaStrategy:
    """
    Entry rules:
      CALL: ST=UP, price>VWAP, RSI≥60, volume spike
      PUT:  ST=DOWN, price<VWAP, RSI≤40, volume spike
    """

    RSI_CALL_MIN = 60.0
    RSI_PUT_MAX  = 40.0

    def __init__(self) -> None:
        self._last_signal_time  = 0.0
        self._signal_cooldown_s = 60.0

    def evaluate(self, ltp: float, indicator: IndicatorEngine,
                 current_volume: float) -> SignalResult:
        t0         = time.time()
        vwap       = indicator.vwap
        rsi        = indicator.rsi
        st_dir     = indicator.supertrend_direction
        above_vwap = ltp > vwap if vwap > 0 else False
        vol_spike  = indicator.volume_spike(current_volume)

        def _no_signal(reason: str) -> SignalResult:
            return SignalResult(signal="NONE", strength=0, reason=reason,
                                supertrend_dir=st_dir, above_vwap=above_vwap,
                                rsi=rsi, volume_spike=vol_spike)

        if (t0 - self._last_signal_time) < self._signal_cooldown_s:
            return _no_signal("Cooldown active")

        warmup = IndicatorEngine.SUPERTREND_PERIOD + 2   # = 12 candles
        if indicator.num_candles < warmup:
            return _no_signal(f"Warming up ({indicator.num_candles}/{warmup} candles)")

        # ── CALL ─────────────────────────────────────────────────────────────
        if st_dir == "UP" and above_vwap and rsi >= self.RSI_CALL_MIN:
            if vol_spike:
                self._last_signal_time = t0
                return SignalResult(
                    signal="CALL", strength=min(100, 80 + int((rsi - 60) * 2)),
                    reason=f"ST=UP VWAP✓ RSI={rsi:.1f} VolSpike✓ LTP={ltp:.1f}",
                    supertrend_dir=st_dir, above_vwap=True, rsi=rsi, volume_spike=True,
                )
            return _no_signal(f"ST=UP VWAP✓ RSI={rsi:.1f} — awaiting vol spike")

        # ── PUT ──────────────────────────────────────────────────────────────
        if st_dir == "DOWN" and not above_vwap and rsi <= self.RSI_PUT_MAX:
            if vol_spike:
                self._last_signal_time = t0
                return SignalResult(
                    signal="PUT", strength=min(100, 80 + int((40 - rsi) * 2)),
                    reason=f"ST=DOWN VWAP✗ RSI={rsi:.1f} VolSpike✓ LTP={ltp:.1f}",
                    supertrend_dir=st_dir, above_vwap=False, rsi=rsi, volume_spike=True,
                )
            return _no_signal(f"ST=DOWN VWAP✗ RSI={rsi:.1f} — awaiting vol spike")

        return _no_signal(f"No confluence — ST={st_dir} RSI={rsi:.1f} VWAP={'above' if above_vwap else 'below'}")


@dataclass
class TrailingState:
    entry_price:      float
    current_sl:       float
    high_water:       float
    lot_size:         int
    is_breakeven:     bool  = False
    trail_step_price: float = 100.0

    BREAKEVEN_TRIGGER_PNL = 300.0
    TRAIL_TRIGGER_PNL     = 700.0

    def unrealised_pnl(self, ltp: float) -> float:
        return (ltp - self.entry_price) * self.lot_size

    def update(self, ltp: float) -> Tuple[bool, float]:
        pnl     = self.unrealised_pnl(ltp)
        new_sl  = self.current_sl
        changed = False

        if pnl >= self.BREAKEVEN_TRIGGER_PNL and not self.is_breakeven:
            new_sl = max(self.current_sl, self.entry_price)
            self.is_breakeven = True
            changed = True

        if pnl >= self.TRAIL_TRIGGER_PNL and ltp > self.high_water:
            increments = math.floor((ltp - self.high_water) / self.trail_step_price)
            if increments > 0:
                candidate = new_sl + increments * self.trail_step_price
                if candidate > new_sl:
                    new_sl = candidate
                    self.high_water += increments * self.trail_step_price
                    changed = True

        if changed:
            self.current_sl = round(new_sl, 2)
        return changed, self.current_sl


def market_health_index(vix: float, advance_decline: float) -> Dict:
    score = 50.0
    # VIX component
    if vix < 12:   score += 25
    elif vix < 15: score += 15
    elif vix < 18: score += 5
    elif vix < 22: score -= 10
    else:          score -= 25
    # A/D ratio component
    if advance_decline >= 2.0:   score += 25
    elif advance_decline >= 1.5: score += 15
    elif advance_decline >= 1.0: score += 5
    elif advance_decline >= 0.5: score -= 10
    else:                        score -= 25
    score = max(0.0, min(100.0, score))
    label = "🟢 Healthy" if score >= 70 else ("🟡 Neutral" if score >= 45 else "🔴 Volatile")
    return {"score": round(score, 1), "label": label, "vix": vix, "ad_ratio": advance_decline}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ══════════════════════════════════════════════════════════════════════════════

import asyncio
import json
import os
import signal
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

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
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ── Static IP Proxy Setup (Railway → AngelOne API) ────────────────────────────
# Railway-ல் HTTP_PROXY மற்றும் HTTPS_PROXY variable செட் செய்யப்பட்டால்
# அனைத்து outgoing requests-உம் proxy வழியாக செல்லும் (Static IP கிடைக்கும்)
_proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
if _proxy_url:
    os.environ.setdefault("HTTP_PROXY",  _proxy_url)
    os.environ.setdefault("HTTPS_PROXY", _proxy_url)
    os.environ["ALL_PROXY"] = _proxy_url
    logging.getLogger("bot").info("✅ Proxy configured: %s", _proxy_url.split("@")[-1])
else:
    logging.getLogger("bot").warning(
        "⚠️  HTTP_PROXY / HTTPS_PROXY not set — Railway-ல் proxy variable add செய்யுங்கள்!"
    )
# ─────────────────────────────────────────────────────────────────────────────

IST = ZoneInfo("Asia/Kolkata")


def _get_env(*names: str, default: str = "") -> str:
    """Try multiple env var names in order — whichever is set first wins."""
    for name in names:
        val = os.getenv(name, "").strip()
        if val:
            return val
    return default


# ── Config ────────────────────────────────────────────────────────────────────

class Cfg:
    # Angel One — accepts both short and long env var names
    API_KEY     = _get_env("API_KEY",     "ANGEL_API_KEY")
    CLIENT_ID   = _get_env("CLIENT_ID",   "ANGEL_CLIENT_ID")
    PASSWORD    = _get_env("PASSWORD",    "ANGEL_PASSWORD",   "ANGEL_PIN")
    TOTP_SECRET = _get_env("TOTP_SECRET", "ANGEL_TOTP_SECRET","TOTP_STR")

    # Telegram — accepts any common naming convention
    TELEGRAM_TOKEN   = _get_env("TELEGRAM_TOKEN", "TELEGRAM_BOT_TOKEN",
                                "BOT_TOKEN",       "TG_TOKEN")
    TELEGRAM_CHAT_ID = _get_env("TELEGRAM_CHAT_ID", "TG_CHAT_ID",
                                "CHAT_ID")

    CAPITAL        = float(_get_env("CAPITAL",        default="20000"))
    MAX_DAILY_LOSS = float(_get_env("MAX_DAILY_LOSS", default="1000"))
    DAILY_TARGET   = float(_get_env("DAILY_TARGET",   default="1500"))
    LOT_SIZE       = int(_get_env("LOT_SIZE",         default="1"))

    UNDERLYING  = _get_env("UNDERLYING",  default="BANKNIFTY")
    TRADE_MODE  = _get_env("TRADE_MODE",  default="paper").lower()
    STATE_FILE  = Path(_get_env("STATE_FILE", default="db.json"))

    TRADE_START = int(_get_env("TRADE_START", default="930"))
    TRADE_END   = int(_get_env("TRADE_END",   default="1430"))


def _hhmm() -> int:
    n = datetime.now(IST)
    return n.hour * 100 + n.minute


def _now_label() -> str:
    return datetime.now(IST).strftime("%H:%M:%S")


def _is_trading_hours() -> bool:
    t = _hhmm()
    return Cfg.TRADE_START <= t <= Cfg.TRADE_END


SCRIP_MASTER_URL = (
    "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
)
STRIKE_STEP = {"BANKNIFTY": 100, "FINNIFTY": 50, "NIFTY": 50}
SPOT_TOKEN  = {"BANKNIFTY": "26009", "FINNIFTY": "26037", "NIFTY": "26000"}
TICK_SIZE   = 0.05


def _round_tick(price: float) -> float:
    return round(round(price / TICK_SIZE) * TICK_SIZE, 2)


def _sl_price(entry: float, pct: float = 0.30) -> float:
    return _round_tick(entry * (1.0 - pct))


def _lot_qty(symbol: str) -> int:
    sizes = {"BANKNIFTY": 15, "FINNIFTY": 40, "NIFTY": 75}
    for k, v in sizes.items():
        if k in symbol.upper():
            return v
    return 25


# ── Persistent state ──────────────────────────────────────────────────────────

@dataclass
class ActiveTrade:
    order_id:    str
    symbol:      str
    token:       str
    option_type: str
    lot_size:    int
    entry_price: float
    sl_price:    float
    entry_time:  str
    mode:        str


class StateStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self.daily_pnl   = 0.0
        self.trade_count = 0
        self.active_trade: Optional[ActiveTrade] = None
        self.day = ""
        self._load()

    def _load(self) -> None:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                if data.get("day") == today:
                    self.daily_pnl   = float(data.get("daily_pnl", 0.0))
                    self.trade_count = int(data.get("trade_count", 0))
                    raw = data.get("active_trade")
                    if raw:
                        self.active_trade = ActiveTrade(**raw)
                    self.day = today
                    logger.info("State restored — PnL=%.0f trades=%d", self.daily_pnl, self.trade_count)
                    return
            except Exception as exc:
                logger.warning("State load failed: %s", exc)
        self.day = today

    async def save(self) -> None:
        async with self._lock:
            data = {
                "day":          self.day,
                "daily_pnl":    self.daily_pnl,
                "trade_count":  self.trade_count,
                "active_trade": asdict(self.active_trade) if self.active_trade else None,
                "saved_at":     datetime.now(IST).isoformat(),
            }
            try:
                self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            except Exception as exc:
                logger.error("State save failed: %s", exc)

    def day_blocked(self) -> bool:
        return self.daily_pnl <= -Cfg.MAX_DAILY_LOSS or self.daily_pnl >= Cfg.DAILY_TARGET

    def record_closed_trade(self, pnl: float) -> None:
        self.daily_pnl    = round(self.daily_pnl + pnl, 2)
        self.trade_count += 1
        self.active_trade = None


# ── Angel One client ──────────────────────────────────────────────────────────

class AngelClient:
    def __init__(self) -> None:
        self._client: Optional[SmartConnect] = None
        self.auth_token  = ""
        self.feed_token  = ""
        self._login_time = 0.0

    def login(self) -> bool:
        try:
            totp           = pyotp.TOTP(Cfg.TOTP_SECRET).now()
            self._client   = SmartConnect(api_key=Cfg.API_KEY)
            session        = self._client.generateSession(Cfg.CLIENT_ID, Cfg.PASSWORD, totp)
            if not (session and session.get("status")):
                raise RuntimeError(f"Session failed: {session}")
            self.auth_token  = session["data"]["jwtToken"]
            self.feed_token  = self._client.getfeedToken()
            self._login_time = time.time()
            logger.info("✅ Angel One login successful")
            return True
        except Exception as exc:
            logger.error("❌ Login failed: %s", exc)
            return False

    def ensure_session(self) -> None:
        if time.time() - self._login_time > 21600:
            self.login()

    def place_order(self, params: Dict[str, Any]) -> str:
        self.ensure_session()
        if not self._client:
            raise RuntimeError("Client not initialized")
        resp     = self._client.placeOrder(params)
        order_id = str(resp["data"]["orderid"])
        return order_id

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        self.ensure_session()
        if not self._client:
            return 0.0
        resp = self._client.ltpData(exchange, symbol, token)
        return float(resp["data"]["ltp"])

    def get_funds(self) -> float:
        self.ensure_session()
        if not self._client:
            return 0.0
        try:
            data = self._client.rmsLimit().get("data", {})
            return float(data.get("availablecash", 0))
        except Exception:
            return 0.0


# ── Scrip master helpers ──────────────────────────────────────────────────────

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
        logger.warning("Scrip master download failed (%s), trying cache", exc)
        if cache.exists():
            return json.loads(cache.read_text(encoding="utf-8"))
        raise


def find_atm_option(rows: List[Dict], underlying: str, spot: float,
                    option_type: str, trade_date: str) -> Dict[str, Any]:
    from datetime import date as _date, datetime as _dt
    step  = STRIKE_STEP.get(underlying, 100)
    atm   = round(spot / step) * step
    today = _dt.strptime(trade_date, "%Y-%m-%d").date()
    cands = []
    for row in rows:
        if row.get("exch_seg") != "NFO": continue
        if row.get("instrumenttype") != "OPTIDX": continue
        if row.get("name") != underlying: continue
        if not str(row.get("symbol", "")).endswith(option_type): continue
        try:
            expiry = _dt.strptime(str(row.get("expiry", "")).strip().upper(), "%d%b%Y").date()
        except ValueError:
            continue
        if expiry < today: continue
        strike = float(row.get("strike", 0)) / 100.0
        cands.append((expiry, abs(strike - atm), row))
    if not cands:
        raise RuntimeError(f"No {underlying} {option_type} contracts found")
    cands.sort(key=lambda x: (x[0], x[1]))
    return cands[0][2]


# ── Order helper ──────────────────────────────────────────────────────────────

async def place_limit_order(angel: AngelClient, symbol: str, token: str,
                             qty: int, action: str, ltp: float,
                             buffer: float = 2.0) -> str:
    price  = _round_tick(ltp + buffer if action == "BUY" else ltp - buffer)
    params = {
        "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
        "transactiontype": action, "exchange": "NFO", "ordertype": "LIMIT",
        "producttype": "INTRADAY", "duration": "IOC",
        "quantity": str(qty), "price": str(price),
    }
    if Cfg.TRADE_MODE == "paper":
        sim_id = f"PAPER_{int(time.time() * 1000)}"
        logger.info("[PAPER] %s %s qty=%d @%.2f", action, symbol, qty, price)
        return sim_id
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: angel.place_order(params))


# ── Telegram notifier ─────────────────────────────────────────────────────────

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self._bot     = Bot(token=token) if token else None
        self._chat_id = chat_id

    async def send(self, text: str, reply_markup=None) -> None:
        if not self._bot or not self._chat_id:
            logger.info("[TG] %s", text[:200])
            return
        try:
            await self._bot.send_message(
                chat_id=self._chat_id, text=text,
                parse_mode="Markdown", reply_markup=reply_markup,
            )
        except Exception as exc:
            logger.error("Telegram send failed: %s", exc)

    async def alert_error(self, component: str, error: str) -> None:
        await self.send(f"🚨 *ERROR* — `{component}`\n`{error}`")


# ══════════════════════════════════════════════════════════════════════════════
# Trading Engine
# ══════════════════════════════════════════════════════════════════════════════

class TradingEngine:
    WS_RETRY_BASE = 2.0
    WS_RETRY_MAX  = 60.0

    def __init__(self) -> None:
        self.angel      = AngelClient()
        self.tg         = TelegramNotifier(Cfg.TELEGRAM_TOKEN, Cfg.TELEGRAM_CHAT_ID)
        self.state      = StateStore(Cfg.STATE_FILE)
        self.strategy   = AlphaStrategy()
        self.indicators: Dict[str, IndicatorEngine] = {}
        self._scrip_rows: List[Dict] = []
        self._spot_ltp: Dict[str, float] = {}
        self._option_ltp: Dict[str, float] = {}
        self._ws_retry_count = 0
        self._running        = False
        self._trade_armed    = False
        self._trade_mode_confirmed = Cfg.TRADE_MODE
        self._ws: Optional[SmartWebSocketV2] = None
        self._trailing: Optional[TrailingState] = None
        self._vix     = 15.0
        self._ad_ratio = 1.0
        self._last_pct_notify: Dict[str, float] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Simulation state
        self._sim_price  = 55000.0    # BankNifty base price
        self._sim_volume = 0
        self._sim_candle_count = 0

    @property
    def _demo_mode(self) -> bool:
        """True when running paper mode without Angel One credentials."""
        return not bool(Cfg.API_KEY)

    # ── Startup ───────────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._loop    = asyncio.get_running_loop()
        self._running = True
        logger.info("🚀 Starting — mode=%s underlying=%s", Cfg.TRADE_MODE, Cfg.UNDERLYING)

        if Cfg.API_KEY:
            ok = await self._loop.run_in_executor(None, self.angel.login)
            if not ok and Cfg.TRADE_MODE == "live":
                raise RuntimeError("Angel One login failed")
        else:
            logger.warning("No API_KEY — paper-only simulation mode")

        try:
            self._scrip_rows = await fetch_scrip_master()
            logger.info("Scrip master: %d instruments", len(self._scrip_rows))
        except Exception as exc:
            logger.error("Scrip master failed: %s", exc)
            await self.tg.alert_error("SCRIP_MASTER", str(exc))

        await self.tg.send(
            f"🤖 *Ultra-Fast Algo Bot Started*\n"
            f"Mode: `{Cfg.TRADE_MODE.upper()}`  Underlying: `{Cfg.UNDERLYING}`\n"
            f"Capital: ₹{Cfg.CAPITAL:,.0f}  MaxLoss: ₹{Cfg.MAX_DAILY_LOSS:,.0f}\n"
            f"Target: ₹{Cfg.DAILY_TARGET:,.0f}  Lot: {Cfg.LOT_SIZE}\n"
            f"Time: `{_now_label()}`\n\n"
            f"Send /help for commands."
        )

        if self.state.active_trade:
            t = self.state.active_trade
            self._trailing = TrailingState(
                entry_price=t.entry_price, current_sl=t.sl_price,
                high_water=t.entry_price, lot_size=t.lot_size * _lot_qty(t.symbol),
            )
            await self.tg.send(
                f"♻️ *Resumed trade from saved state*\n"
                f"`{t.symbol}` Entry=₹{t.entry_price:.2f} SL=₹{t.sl_price:.2f}"
            )

        asyncio.create_task(self._maintenance_loop())

        # Start simulation loop when no API key (demo mode)
        if self._demo_mode:
            asyncio.create_task(self._simulation_loop())
            await self.tg.send(
                "🎮 *Demo Simulation Mode Active*\n"
                "API Key இல்லாமல் Bot-ஐ test செய்யலாம்!\n\n"
                "• Realistic BankNifty price ticks generate ஆகும்\n"
                "• 6-8 நிமிடத்தில் signal வரும்\n"
                "• /starttrade → Paper Trade → signal காத்திருங்கள்\n\n"
                "_Real trading-க்கு Angel One API Key தேவை_"
            )

    # ── Demo Simulation Loop ──────────────────────────────────────────────────

    async def _simulation_loop(self) -> None:
        """
        Generates realistic BankNifty tick data when running without API credentials.
        Each tick fires every 3 seconds. Candles close every 30 seconds (fast-mode).
        A signal should appear within 6-8 minutes.
        """
        import random
        rng         = random.Random(42)
        spot_token  = SPOT_TOKEN.get(Cfg.UNDERLYING, "26009")
        tick_count  = 0
        # Candle period: 30 real seconds = simulated 5-min bar (for fast demo)
        CANDLE_TICKS = 10   # 10 ticks × 3s = 30s per candle
        base_vol     = 50000

        logger.info("🎮 Simulation loop started — tick every 3s, candle every 30s")

        # Force-set the candle second so IndicatorEngine builds candles on our schedule
        candle_ts = time.time()

        while self._running:
            await asyncio.sleep(3)
            if not self._trade_armed and not self.state.active_trade:
                # Bot not armed yet — still generate ticks for indicator warm-up
                pass

            tick_count += 1

            # ── Price simulation (random walk with trend bias) ─────────────────
            # After 8 candles, add a bullish trend to likely trigger CALL signal
            candle_num = tick_count // CANDLE_TICKS
            if candle_num < 4:
                drift = rng.uniform(-0.0015, 0.0015)   # flat / choppy
            elif candle_num < 8:
                drift = rng.uniform(-0.0005, 0.002)    # mild uptrend
            else:
                drift = rng.uniform(0.001, 0.003)      # strong trend → RSI rises

            self._sim_price = round(self._sim_price * (1 + drift), 2)
            self._sim_price = max(40000.0, min(70000.0, self._sim_price))

            # ── Volume simulation (spike every ~10 candles) ────────────────────
            self._sim_volume += rng.randint(800, 2000)
            # Inject a volume spike when we're in strong trend phase
            if candle_num >= 10 and (tick_count % CANDLE_TICKS) == 5:
                self._sim_volume += base_vol * 3   # 3× average spike

            # ── Build the tick using fake 5-min candle boundaries ─────────────
            # Map real elapsed seconds → fake 5-min epoch so IndicatorEngine
            # sees proper candle boundaries
            fake_ts_sec = candle_ts + (tick_count * 300 / CANDLE_TICKS)
            ts_ms       = int(fake_ts_sec * 1000)

            tick = Tick(
                token=spot_token,
                ltp=self._sim_price,
                volume=self._sim_volume,
                timestamp_ms=ts_ms,
            )

            t_start = time.monotonic()
            await self._process_tick_demo(tick, t_start)

            # ── Status update every 12 ticks (one simulated candle) ────────────
            if tick_count % CANDLE_TICKS == 0:
                self._sim_candle_count += 1
                ind = self.indicators.get(spot_token)
                rsi_val = ind.rsi if ind else 50.0
                logger.info(
                    "📊 Demo candle #%d | Price=%.0f RSI=%.1f ST=%s",
                    self._sim_candle_count,
                    self._sim_price,
                    rsi_val,
                    ind.supertrend_direction if ind else "?",
                )

    async def _process_tick_demo(self, tick: Tick, t_start: float) -> None:
        """Like _process_tick but bypasses market-hours check for simulation."""
        token      = tick.token
        ltp        = tick.ltp
        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")

        self._spot_ltp[Cfg.UNDERLYING] = ltp

        # Track option price for active demo trade
        if self.state.active_trade and token == self.state.active_trade.token:
            self._option_ltp[token] = ltp
            await self._manage_trailing(ltp)

        if self.state.day_blocked():
            return
        if self.state.active_trade or not self._trade_armed:
            return    # Armed but already in trade, or not armed

        if token not in self.indicators:
            self.indicators[token] = IndicatorEngine()
        ind = self.indicators[token]
        ind.on_tick(tick)

        result = self.strategy.evaluate(ltp, ind, float(tick.volume))
        if result.signal in ("CALL", "PUT"):
            elapsed = (time.monotonic() - t_start) * 1000
            logger.info("🎯 DEMO %s | RSI=%.1f | ST=%s | eval=%.1fms",
                        result.signal, result.rsi, result.supertrend_dir, elapsed)
            await self._fire_order(result, ltp)

    # ── WebSocket with exponential backoff ────────────────────────────────────

    async def _ws_loop(self) -> None:
        while self._running:
            try:
                await self._connect_ws()
                self._ws_retry_count = 0
            except Exception as exc:
                self._ws_retry_count += 1
                delay = min(self.WS_RETRY_MAX,
                            self.WS_RETRY_BASE * (2 ** (self._ws_retry_count - 1)))
                logger.warning("WS error (attempt %d): %s — retry in %.0fs",
                               self._ws_retry_count, exc, delay)
                await self.tg.send(
                    f"⚠️ WS reconnecting in {delay:.0f}s (attempt {self._ws_retry_count})"
                )
                await asyncio.sleep(delay)
                if Cfg.API_KEY and self._loop:
                    await self._loop.run_in_executor(None, self.angel.login)

    async def _connect_ws(self) -> None:
        if not self.angel.auth_token:
            await asyncio.sleep(5)
            return

        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING)
        sub_payload = [{"exchangeType": 1, "tokens": [spot_token]}] if spot_token else []
        loop = asyncio.get_running_loop()

        def on_open(wsapp):
            logger.info("WebSocket connected ✅")
            if sub_payload:
                wsapp.subscribe("bot1", 3, sub_payload)

        def on_data(_wsapp, message):
            loop.call_soon_threadsafe(
                asyncio.create_task, self._on_tick_raw(message)
            )

        def on_error(_wsapp, error):
            logger.error("WS error: %s", error)

        def on_close(_wsapp, code=None, msg=None):
            logger.warning("WS closed: %s %s", code, msg)

        self._ws = SmartWebSocketV2(
            self.angel.auth_token, Cfg.API_KEY,
            Cfg.CLIENT_ID,         self.angel.feed_token,
        )
        self._ws.on_open  = on_open
        self._ws.on_data  = on_data
        self._ws.on_error = on_error
        self._ws.on_close = on_close

        await loop.run_in_executor(None, self._ws.connect)

    # ── Tick processing ───────────────────────────────────────────────────────

    async def _on_tick_raw(self, message: Any) -> None:
        t_start = time.monotonic()
        try:
            data = self._parse_tick(message)
            if not data:
                return
            token  = str(data.get("token", ""))
            ltp    = float(data.get("ltp", 0))
            vol    = int(data.get("volume", 0))
            ts_ms  = int(data.get("ts_ms", time.time() * 1000))
            if ltp <= 0:
                return
            tick = Tick(token=token, ltp=ltp, volume=vol, timestamp_ms=ts_ms)
            await self._process_tick(tick, t_start)
        except Exception as exc:
            logger.debug("Tick error: %s", exc)

    def _parse_tick(self, message: Any) -> Optional[Dict[str, Any]]:
        if isinstance(message, dict):
            ltp = float(message.get("last_traded_price", message.get("ltp", 0)) or 0)
            if ltp > 100:        # Angel sends paise for binary ticks
                ltp /= 100.0
            return {
                "token":  str(message.get("token", "")),
                "ltp":    ltp,
                "volume": int(message.get("volume_trade_for_the_day",
                                          message.get("volume", 0)) or 0),
                "ts_ms":  int(message.get("exchange_timestamp", time.time() * 1000) or
                              time.time() * 1000),
            }
        if isinstance(message, (bytes, bytearray)) and len(message) >= 51:
            try:
                token_raw = message[2:27].decode("utf-8").strip("\x00")
                ltp_raw   = int.from_bytes(message[43:51], "big") / 100.0
                vol       = int.from_bytes(message[59:67], "big") if len(message) >= 67 else 0
                return {"token": token_raw, "ltp": ltp_raw, "volume": vol,
                        "ts_ms": int(time.time() * 1000)}
            except Exception:
                pass
        return None

    async def _process_tick(self, tick: Tick, t_start: float) -> None:
        token = tick.token
        ltp   = tick.ltp
        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")

        if token == spot_token:
            self._spot_ltp[Cfg.UNDERLYING] = ltp

        if self.state.active_trade and token == self.state.active_trade.token:
            self._option_ltp[token] = ltp
            await self._maybe_notify_price_move(token, ltp)
            await self._manage_trailing(ltp)

        if token != spot_token:
            return
        # Skip market hours check in demo mode (simulation bypasses via _process_tick_demo)
        if not self._demo_mode and (not _is_trading_hours() or self.state.day_blocked()):
            return
        if self.state.day_blocked():
            return
        if self.state.active_trade or not self._trade_armed:
            return

        if token not in self.indicators:
            self.indicators[token] = IndicatorEngine()
        ind = self.indicators[token]
        ind.on_tick(tick)

        result = self.strategy.evaluate(ltp, ind, float(tick.volume))
        if result.signal in ("CALL", "PUT"):
            elapsed = (time.monotonic() - t_start) * 1000
            logger.info("🎯 %s | RSI=%.1f | ST=%s | eval=%.1fms",
                        result.signal, result.rsi, result.supertrend_dir, elapsed)
            await self._fire_order(result, ltp)

    # ── Order execution ───────────────────────────────────────────────────────

    async def _fire_order(self, signal: SignalResult, spot_ltp: float) -> None:
        t0 = time.monotonic()
        try:
            option_type = "CE" if signal.signal == "CALL" else "PE"
            today       = datetime.now(IST).strftime("%Y-%m-%d")
            row         = find_atm_option(self._scrip_rows, Cfg.UNDERLYING,
                                          spot_ltp, option_type, today)
            symbol      = str(row["symbol"])
            token       = str(row["token"])
            raw_lot_sz  = int(float(row.get("lotsize") or _lot_qty(Cfg.UNDERLYING)))
            qty         = Cfg.LOT_SIZE * raw_lot_sz

            if Cfg.TRADE_MODE == "live" and Cfg.API_KEY:
                opt_ltp = await self._loop.run_in_executor(  # type: ignore[union-attr]
                    None, lambda: self.angel.get_ltp("NFO", symbol, token)
                )
            else:
                opt_ltp = spot_ltp * 0.01    # paper sim ~1% of index

            if opt_ltp <= 0:
                logger.warning("Option LTP 0 for %s — skipping", symbol)
                return

            order_id = await place_limit_order(self.angel, symbol, token, qty, "BUY", opt_ltp)
            sl       = _sl_price(opt_ltp)

            trade = ActiveTrade(
                order_id=order_id, symbol=symbol, token=token,
                option_type=option_type, lot_size=Cfg.LOT_SIZE,
                entry_price=opt_ltp, sl_price=sl,
                entry_time=datetime.now(IST).isoformat(),
                mode=self._trade_mode_confirmed,
            )
            self.state.active_trade = trade
            self._trailing = TrailingState(
                entry_price=opt_ltp, current_sl=sl,
                high_water=opt_ltp, lot_size=qty,
            )
            await self.state.save()

            elapsed = (time.monotonic() - t0) * 1000
            logger.info("✅ Order in %.1fms", elapsed)
            await self.tg.send(
                f"✅ *ORDER PLACED* — `{signal.signal}`\n"
                f"Symbol: `{symbol}`\n"
                f"Qty: {qty} | Entry: ₹{opt_ltp:.2f} | SL: ₹{sl:.2f}\n"
                f"Reason: _{signal.reason}_\n"
                f"⚡ *{elapsed:.0f}ms*"
            )
        except Exception as exc:
            logger.error("Order failed: %s", exc)
            await self.tg.alert_error("ORDER_FIRE", str(exc))

    # ── Trailing SL ───────────────────────────────────────────────────────────

    async def _manage_trailing(self, ltp: float) -> None:
        if not self._trailing or not self.state.active_trade:
            return
        trade = self.state.active_trade
        changed, new_sl = self._trailing.update(ltp)
        if changed:
            trade.sl_price = new_sl
            await self.state.save()
            pnl = self._trailing.unrealised_pnl(ltp)
            be  = " 🔒 Breakeven" if self._trailing.is_breakeven else ""
            await self.tg.send(
                f"📈 *SL TRAILED*{be}\n"
                f"`{trade.symbol}` LTP=₹{ltp:.2f}\n"
                f"New SL: ₹{new_sl:.2f}  Unrealised P&L: ₹{pnl:.0f}"
            )
        if ltp <= trade.sl_price:
            await self._exit_trade(ltp, "STOP_LOSS_HIT")

    async def _exit_trade(self, ltp: float, reason: str = "SIGNAL_EXIT") -> None:
        if not self.state.active_trade:
            return
        trade = self.state.active_trade
        qty   = trade.lot_size * _lot_qty(trade.symbol)
        try:
            await place_limit_order(self.angel, trade.symbol, trade.token, qty, "SELL", ltp)
        except Exception as exc:
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
            f"P&L: ₹{pnl:+.0f}  Daily: ₹{self.state.daily_pnl:+.0f}"
        )
        if self.state.daily_pnl <= -Cfg.MAX_DAILY_LOSS:
            self._trade_armed = False
            await self.tg.send(
                "🛑 *DAILY LOSS LIMIT HIT* — Bot disarmed for today.\n"
                f"Total Loss: ₹{abs(self.state.daily_pnl):.0f}"
            )

    async def _maybe_notify_price_move(self, token: str, ltp: float) -> None:
        last = self._last_pct_notify.get(token, ltp)
        if last > 0 and abs((ltp - last) / last) * 100 >= 1.0:
            self._last_pct_notify[token] = ltp
            d = "📈" if ltp > last else "📉"
            await self.tg.send(
                f"{d} *1% Move* `{token}` ₹{last:.2f}→₹{ltp:.2f}"
            )

    # ── Maintenance loop ──────────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        while self._running:
            try:
                if _hhmm() == 1425 and self.state.active_trade:
                    t   = self.state.active_trade
                    ltp = t.entry_price
                    if Cfg.API_KEY and self._loop:
                        ltp = await self._loop.run_in_executor(
                            None, lambda: self.angel.get_ltp("NFO", t.symbol, t.token)
                        )
                    await self._exit_trade(ltp or t.entry_price, "EOD_SQUARE_OFF")
                await self.state.save()
            except Exception as exc:
                logger.error("Maintenance error: %s", exc)
            await asyncio.sleep(30)

    # ── Telegram commands ─────────────────────────────────────────────────────

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        bal = 0.0
        if Cfg.API_KEY and self._loop:
            bal = await self._loop.run_in_executor(None, self.angel.get_funds)
        mhi = market_health_index(self._vix, self._ad_ratio)
        t = self.state.active_trade
        trade_info = "None"
        if t:
            opt_ltp = self._option_ltp.get(t.token, t.entry_price)
            unreal  = round((opt_ltp - t.entry_price) * t.lot_size * _lot_qty(t.symbol), 2)
            trade_info = f"`{t.symbol}` Entry=₹{t.entry_price:.2f} SL=₹{t.sl_price:.2f} Unreal=₹{unreal:+.0f}"
        await update.message.reply_text(
            f"📊 *Bot Status* — `{_now_label()}`\n"
            f"Balance: ₹{bal:,.0f}\n"
            f"Today P&L: ₹{self.state.daily_pnl:+.0f}\n"
            f"Trades today: {self.state.trade_count}\n"
            f"Active Trade: {trade_info}\n"
            f"Armed: {'✅' if self._trade_armed else '❌'}\n"
            f"Market Health: {mhi['label']} (score={mhi['score']})\n"
            f"VIX: {self._vix:.1f}  A-D: {self._ad_ratio:.2f}",
            parse_mode="Markdown",
        )

    async def cmd_checkstrategy(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        spot  = self._spot_ltp.get(Cfg.UNDERLYING, 0)
        token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
        ind   = self.indicators.get(token)
        if not ind or ind.num_candles < 5 or spot <= 0:
            await update.message.reply_text(
                "⏳ *Warming up* — need 5+ candles. Try again in a few minutes.",
                parse_mode="Markdown",
            )
            return
        vol    = float(ind.candles[-1].volume) if ind.candles else 0.0
        result = self.strategy.evaluate(spot, ind, vol)
        label  = "✅ *Strong Signal Found!*" if result.signal != "NONE" else "❌ *Market Unstable — Avoid*"
        await update.message.reply_text(
            f"{label}\n"
            f"Signal: `{result.signal}` | Strength: {result.strength}%\n"
            f"ST: `{result.supertrend_dir}` | VWAP: {'above' if result.above_vwap else 'below'}\n"
            f"RSI: {result.rsi:.1f} | VolSpike: {'✅' if result.volume_spike else '❌'}\n"
            f"_{result.reason}_",
            parse_mode="Markdown",
        )

    async def cmd_starttrade(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        kbd = [
            [InlineKeyboardButton("📄 Paper Trade", callback_data="confirm_paper"),
             InlineKeyboardButton("💰 Real Trade",  callback_data="confirm_live")],
            [InlineKeyboardButton("❌ Cancel",       callback_data="cancel_trade")],
        ]
        await update.message.reply_text(
            "⚠️ *Choose trading mode:*\n\nPaper = simulated  |  Real = live Angel One orders",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kbd),
        )

    async def callback_handler(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        q = update.callback_query
        await q.answer()
        if q.data == "confirm_paper":
            self._trade_mode_confirmed = "paper"
            self._trade_armed = True
            await q.edit_message_text("✅ *Paper trading armed.*", parse_mode="Markdown")
        elif q.data == "confirm_live":
            if not Cfg.API_KEY:
                await q.edit_message_text("❌ API_KEY not configured.", parse_mode="Markdown")
                return
            self._trade_mode_confirmed = "live"
            self._trade_armed = True
            await q.edit_message_text("🚨 *LIVE trading armed.* Real orders will be placed.",
                                       parse_mode="Markdown")
        elif q.data == "cancel_trade":
            self._trade_armed = False
            await q.edit_message_text("❌ Cancelled.")

    async def cmd_stop(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        self._trade_armed = False
        await update.message.reply_text("🛑 *Bot disarmed.* No new trades.", parse_mode="Markdown")

    async def cmd_exitnow(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self.state.active_trade:
            await update.message.reply_text("No active trade.")
            return
        t   = self.state.active_trade
        ltp = self._option_ltp.get(t.token, t.entry_price)
        await self._exit_trade(ltp, "MANUAL_EXIT")
        await update.message.reply_text("✅ Manual exit initiated.")

    async def cmd_setvix(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        args = ctx.args or []
        if not args:
            await update.message.reply_text("Usage: /setvix 14.5")
            return
        try:
            self._vix = float(args[0])
            await update.message.reply_text(f"VIX set to {self._vix:.2f}")
        except ValueError:
            await update.message.reply_text("Invalid value.")

    async def cmd_setad(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        args = ctx.args or []
        if not args:
            await update.message.reply_text("Usage: /setad 1.8")
            return
        try:
            self._ad_ratio = float(args[0])
            await update.message.reply_text(f"A-D ratio set to {self._ad_ratio:.2f}")
        except ValueError:
            await update.message.reply_text("Invalid value.")

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        """First command — verifies bot is alive and receiving messages."""
        user = update.effective_user
        name = user.first_name if user else "Trader"
        await update.message.reply_text(
            f"👋 *வணக்கம் {name}!*\n\n"
            f"🤖 Ultra-Fast Algo Bot இயங்குகிறது!\n\n"
            f"Mode: `{Cfg.TRADE_MODE.upper()}`\n"
            f"Underlying: `{Cfg.UNDERLYING}`\n"
            f"Capital: ₹{Cfg.CAPITAL:,.0f}\n\n"
            f"Commands பார்க்க /help அனுப்பவும்\n"
            f"Trade தொடங்க /starttrade அனுப்பவும்",
            parse_mode="Markdown",
        )

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "🤖 *Ultra-Fast Algo Bot Commands*\n\n"
            "/start — Bot live என்று confirm செய்\n"
            "/status — Balance, P&L, Market Health\n"
            "/checkstrategy — Current setup analyze\n"
            "/starttrade — Paper அல்லது Real trade தொடங்கு\n"
            "/stop — Bot disarm (new trades இல்லை)\n"
            "/exitnow — Active trade force exit\n"
            "/setvix <val> — VIX manually set\n"
            "/setad <val> — Advance-Decline ratio set\n"
            "/help — இந்த list காட்டு",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def build_app(engine: TradingEngine) -> Optional[Application]:
    """Build Telegram Application with all command handlers registered."""
    token = Cfg.TELEGRAM_TOKEN
    if not token:
        logger.warning(
            "❌ No Telegram token found! Checked env vars: "
            "TELEGRAM_TOKEN, TELEGRAM_BOT_TOKEN, BOT_TOKEN, TG_TOKEN"
        )
        return None

    logger.info("✅ Telegram token found (len=%d) — building app...", len(token))
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start",         engine.cmd_start))
    app.add_handler(CommandHandler("status",        engine.cmd_status))
    app.add_handler(CommandHandler("checkstrategy", engine.cmd_checkstrategy))
    app.add_handler(CommandHandler("starttrade",    engine.cmd_starttrade))
    app.add_handler(CommandHandler("stop",          engine.cmd_stop))
    app.add_handler(CommandHandler("exitnow",       engine.cmd_exitnow))
    app.add_handler(CommandHandler("setvix",        engine.cmd_setvix))
    app.add_handler(CommandHandler("setad",         engine.cmd_setad))
    app.add_handler(CommandHandler("help",          engine.cmd_help))
    app.add_handler(CallbackQueryHandler(engine.callback_handler))
    return app


async def _async_main() -> None:
    # ── 1. Print startup env info ─────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ULTRA-FAST ALGO BOT STARTING")
    logger.info("TRADE_MODE  : %s", Cfg.TRADE_MODE)
    logger.info("UNDERLYING  : %s", Cfg.UNDERLYING)
    logger.info("API_KEY set : %s", bool(Cfg.API_KEY))
    logger.info("TG token set: %s (len=%d)", bool(Cfg.TELEGRAM_TOKEN), len(Cfg.TELEGRAM_TOKEN))
    logger.info("TG chat_id  : %s", Cfg.TELEGRAM_CHAT_ID or "NOT SET")
    logger.info("=" * 60)

    # ── 2. Build Telegram app FIRST ───────────────────────────────────────────
    engine = TradingEngine()
    tg_app = build_app(engine)

    if tg_app:
        logger.info("Initialising Telegram application...")
        await tg_app.initialize()
        await tg_app.start()
        await tg_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
        logger.info("✅ Telegram polling ACTIVE — send /start to your bot now")
    else:
        logger.warning("⚠️  Telegram disabled — bot will run silently")

    # ── 3. Start trading engine ───────────────────────────────────────────────
    await engine.start()

    # ── 4. Keep alive ─────────────────────────────────────────────────────────
    try:
        while True:
            await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        engine._running = False
        if tg_app:
            logger.info("Stopping Telegram...")
            await tg_app.updater.stop()
            await tg_app.stop()
            await tg_app.shutdown()
        await engine.state.save()
        logger.info("Bot shut down cleanly.")


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown():
        logger.info("Shutdown signal received")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    try:
        loop.run_until_complete(_async_main())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
