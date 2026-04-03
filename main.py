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
import os
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Deque, Dict, List, Optional, Tuple

os.environ.setdefault('TZ', 'Asia/Kolkata')

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
    adx:            float = 0.0
    ema_bullish:    bool  = False
    ema_fast_slope: float = 0.0   # Added for Slope/Regime filter
    candle_pattern: str   = "NONE"
    entry_atr:      float = 0.0   # ATR at time of signal (for dynamic SL)
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
    VOLUME_SPIKE_LOOKBACK = 15   # 75-min baseline
    VOLUME_SPIKE_FACTOR   = 2.0  # (Down from 2.5) catch more breakouts
    EMA_FAST              = 9
    EMA_SLOW              = 21
    ADX_PERIOD            = 14

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

        # EMA
        self._ema_fast:      Optional[float] = None
        self._ema_fast_prev: Optional[float] = None
        self._ema_slow:      Optional[float] = None

        # ADX (Wilder smoothing)
        self._adx_s_plus_dm:  Optional[float] = None
        self._adx_s_minus_dm: Optional[float] = None
        self._adx_s_tr:       Optional[float] = None
        self._adx_dx: Deque[float] = collections.deque(maxlen=self.ADX_PERIOD)
        self._adx_value: Optional[float] = None

        # Candle pattern
        self._last_pattern: str = "NONE"

        # ── Layer 1: Kalman Filter ────────────────────────────────────────────
        self._kalman_x: float = 0.0      # estimated price
        self._kalman_P: float = 1.0      # error covariance
        self._kalman_Q: float = 0.001    # process noise
        self._kalman_R: float = 0.01     # measurement noise

        # ── Layer 4: M15 Multi-Timeframe Builder ─────────────────────────────
        self._m15cs: int = 900           # 15-min in seconds
        self._m15_open: float = 0.0
        self._m15_high: float = 0.0
        self._m15_low: float  = float("inf")
        self._m15_close: float = 0.0
        self._m15_cstart: float = 0.0
        self._m15_candles: Deque[Candle] = collections.deque(maxlen=30)
        self._m15_atr_v: Deque[float]   = collections.deque(maxlen=self.SUPERTREND_PERIOD)
        self._m15_atr_s: Optional[float] = None
        self._m15_st_up: Optional[float] = None
        self._m15_st_lo: Optional[float] = None
        self._m15_st_dir: str = "UP"

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
        self._update_ema(c.close)
        self._update_supertrend(c)
        self._update_adx(c)
        self._detect_candle_pattern()
        self._cur_open   = 0.0
        self._cur_high   = 0.0
        self._cur_low    = float("inf")
        self._cur_close  = 0.0
        self._cur_volume = 0
        return c

    def on_tick(self, tick: Tick) -> Optional[Candle]:
        ts_sec = tick.timestamp_ms / 1000.0
        price  = tick.ltp
        vol    = max(0, tick.volume)
        self._maybe_reset_vwap(ts_sec)
        self._vwap_cum_pv += price * vol
        self._vwap_cum_v  += vol

        # Layer 1: Kalman smoothing
        self._kalman_update(price)

        # 5-min candle builder
        bucket = self._candle_start(ts_sec)
        if self._cur_candle_start == 0.0:
            self._cur_candle_start = bucket
        completed: Optional[Candle] = None
        if bucket != self._cur_candle_start:
            completed = self._flush_candle()
            self._cur_candle_start = bucket
        if self._cur_open == 0.0:
            self._cur_open = price
        self._cur_high   = max(self._cur_high, price)
        self._cur_low    = min(self._cur_low,  price)
        self._cur_close  = price
        self._cur_volume += vol

        # Layer 4: M15 candle builder
        m15b = ts_sec - (ts_sec % self._m15cs)
        if self._m15_cstart == 0.0:
            self._m15_cstart = m15b
        if m15b != self._m15_cstart:
            self._m15_flush()
            self._m15_cstart = m15b
        if self._m15_open == 0.0:
            self._m15_open = price
        self._m15_high  = max(self._m15_high, price)
        self._m15_low   = min(self._m15_low,  price)
        self._m15_close = price

        return completed

    # ── Volume spike ──────────────────────────────────────────────────────────

    @property
    def current_volume(self) -> int:
        return self._cur_volume

    def volume_spike(self) -> bool:
        if len(self._candles) < self.VOLUME_SPIKE_LOOKBACK:
            return False
        recent = list(self._candles)[-self.VOLUME_SPIKE_LOOKBACK:]
        avg = sum(c.volume for c in recent) / len(recent)
        return avg > 0 and self._cur_volume >= avg * self.VOLUME_SPIKE_FACTOR

    @property
    def candles(self) -> List[Candle]:
        return list(self._candles)

    @property
    def supertrend_direction(self) -> str:
        return self._st_direction

    @property
    def num_candles(self) -> int:
        return len(self._candles)

    # ── EMA ───────────────────────────────────────────────────────────────────

    def _update_ema(self, close: float) -> None:
        kf = 2.0 / (self.EMA_FAST + 1)
        ks = 2.0 / (self.EMA_SLOW + 1)
        if self._ema_fast is None:
            self._ema_fast_prev = close
            self._ema_fast      = close
            self._ema_slow      = close
        else:
            self._ema_fast_prev = self._ema_fast
            self._ema_fast      = close * kf + self._ema_fast * (1.0 - kf)
            self._ema_slow      = close * ks + self._ema_slow * (1.0 - ks)

    @property
    def ema_fast(self) -> float:
        return round(self._ema_fast or 0.0, 2)

    @property
    def ema_slow(self) -> float:
        return round(self._ema_slow or 0.0, 2)

    @property
    def ema_fast_slope(self) -> float:
        if not self._ema_fast or not self._ema_fast_prev:
            return 0.0
        return round(self._ema_fast - self._ema_fast_prev, 2)

    @property
    def ema_bullish(self) -> bool:
        """True when EMA9 > EMA21 (uptrend)."""
        return (self._ema_fast or 0.0) > (self._ema_slow or 0.0)

    # ── ADX ───────────────────────────────────────────────────────────────────

    def _update_adx(self, candle: Candle) -> None:
        if len(self._candles) < 2:
            return
        prev     = self._candles[-2]
        tr       = self._true_range(candle, prev.close)
        plus_dm  = max(0.0, candle.high - prev.high)
        minus_dm = max(0.0, prev.low - candle.low)
        if plus_dm > minus_dm:
            minus_dm = 0.0
        elif minus_dm > plus_dm:
            plus_dm = 0.0
        else:
            plus_dm = 0.0; minus_dm = 0.0
        n = self.ADX_PERIOD
        if self._adx_s_tr is None:
            self._adx_s_plus_dm, self._adx_s_minus_dm, self._adx_s_tr = plus_dm, minus_dm, tr
        else:
            self._adx_s_plus_dm  = self._adx_s_plus_dm  - self._adx_s_plus_dm  / n + plus_dm   # type: ignore
            self._adx_s_minus_dm = self._adx_s_minus_dm - self._adx_s_minus_dm / n + minus_dm  # type: ignore
            self._adx_s_tr       = self._adx_s_tr       - self._adx_s_tr       / n + tr        # type: ignore
        if self._adx_s_tr and self._adx_s_tr > 0:
            pdi = 100.0 * self._adx_s_plus_dm  / self._adx_s_tr  # type: ignore
            mdi = 100.0 * self._adx_s_minus_dm / self._adx_s_tr  # type: ignore
            di_sum = pdi + mdi
            if di_sum > 0:
                dx = 100.0 * abs(pdi - mdi) / di_sum
                self._adx_dx.append(dx)
                if len(self._adx_dx) >= n:
                    self._adx_value = (sum(self._adx_dx) / n) if self._adx_value is None \
                        else (self._adx_value * (n - 1) + dx) / n

    @property
    def adx(self) -> float:
        return round(self._adx_value or 0.0, 2)

    @property
    def current_atr(self) -> float:
        """ATR reused from Supertrend calculation."""
        return round(self._atr_smooth or 0.0, 2)

    # ── Candle Pattern ────────────────────────────────────────────────────────

    def _detect_candle_pattern(self) -> None:
        if len(self._candles) < 2:
            self._last_pattern = "NONE"; return
        curr = self._candles[-1]
        prev = self._candles[-2]
        body       = abs(curr.close - curr.open)
        rng        = max(curr.high - curr.low, 0.01)
        upper_wick = curr.high - max(curr.open, curr.close)
        lower_wick = min(curr.open, curr.close) - curr.low
        if body < rng * 0.08:
            self._last_pattern = "DOJI"; return
        if prev.close < prev.open and curr.close > curr.open \
                and curr.open <= prev.close and curr.close >= prev.open:
            self._last_pattern = "BULLISH_ENGULFING"; return
        if prev.close > prev.open and curr.close < curr.open \
                and curr.open >= prev.close and curr.close <= prev.open:
            self._last_pattern = "BEARISH_ENGULFING"; return
        if curr.close >= curr.open and lower_wick >= body * 2.0 and upper_wick <= body * 0.5:
            self._last_pattern = "HAMMER"; return
        if curr.close <= curr.open and upper_wick >= body * 2.0 and lower_wick <= body * 0.5:
            self._last_pattern = "SHOOTING_STAR"; return
        self._last_pattern = "BULLISH" if curr.close > curr.open else "BEARISH"

    @property
    def last_candle_pattern(self) -> str:
        return self._last_pattern

    # ── Layer 1: Kalman Filter ────────────────────────────────────────────────

    def _kalman_update(self, price: float) -> None:
        if self._kalman_x == 0.0:
            self._kalman_x = price
            return
        P_pred = self._kalman_P + self._kalman_Q
        K = P_pred / (P_pred + self._kalman_R)
        self._kalman_x += K * (price - self._kalman_x)
        self._kalman_P  = (1.0 - K) * P_pred

    @property
    def kalman_price(self) -> float:
        """Kalman-smoothed price (noise-reduced)."""
        return round(self._kalman_x, 2)

    # ── Layer 4: M15 Supertrend ───────────────────────────────────────────────

    def _m15_flush(self) -> None:
        if self._m15_open == 0.0:
            return
        c = Candle(
            timestamp=self._m15_cstart,
            open=self._m15_open, high=self._m15_high,
            low=self._m15_low,   close=self._m15_close,
            volume=0,
        )
        self._m15_candles.append(c)
        self._m15_update_supertrend(c)
        self._m15_open  = 0.0
        self._m15_high  = 0.0
        self._m15_low   = float("inf")
        self._m15_close = 0.0

    def _m15_update_supertrend(self, candle: Candle) -> None:
        if len(self._m15_candles) < 2:
            return
        prev = self._m15_candles[-2]
        tr   = self._true_range(candle, prev.close)
        n    = self.SUPERTREND_PERIOD
        if self._m15_atr_s is None:
            self._m15_atr_v.append(tr)
            if len(self._m15_atr_v) >= n:
                self._m15_atr_s = sum(self._m15_atr_v) / n
        else:
            self._m15_atr_s = (self._m15_atr_s * (n - 1) + tr) / n
        if self._m15_atr_s is None:
            return
        mid   = (candle.high + candle.low) / 2.0
        m     = self.SUPERTREND_MULTIPLIER
        bu    = mid + m * self._m15_atr_s
        bl    = mid - m * self._m15_atr_s
        pu    = self._m15_st_up if self._m15_st_up is not None else bu
        pl    = self._m15_st_lo if self._m15_st_lo is not None else bl
        fu    = bu if (bu < pu or prev.close > pu) else pu
        fl    = bl if (bl > pl or prev.close < pl) else pl
        if self._m15_st_dir == "UP":
            if candle.close < fl:
                self._m15_st_dir = "DOWN"
        else:
            if candle.close > fu:
                self._m15_st_dir = "UP"
        self._m15_st_up = fu
        self._m15_st_lo = fl

    @property
    def m15_supertrend_direction(self) -> str:
        return self._m15_st_dir

    @property
    def m15_num_candles(self) -> int:
        return len(self._m15_candles)

    # ── Layer 6: Range Ratio ─────────────────────────────────────────────────

    @property
    def range_ratio(self) -> float:
        """Current incomplete candle range vs avg of last 5 completed candles."""
        if len(self._candles) < 5:
            return 1.0
        recent  = list(self._candles)[-5:]
        avg_rng = sum(max(c.high - c.low, 0.01) for c in recent) / len(recent)
        cur_rng = max(self._cur_high - self._cur_low, 0.0) if self._cur_high > 0 else 0.0
        return round(cur_rng / avg_rng, 2) if avg_rng > 0 else 1.0


class AlphaStrategy:
    """
    Enhanced v2.0 — 7-condition confluence:
      CALL: ST=UP, EMA9>EMA21, price>VWAP, RSI≥55, ADX≥20, VolSpike, non-bearish candle, prime time
      PUT:  ST=DOWN, EMA9<EMA21, price<VWAP, RSI≤45, ADX≥20, VolSpike, non-bullish candle, prime time
    """

    RSI_CALL_MIN        = 55.0   # relaxed from 60
    RSI_PUT_MAX         = 45.0   # relaxed from 40
    ADX_MIN             = 20.0   # skip sideways markets
    PRIME_WINDOWS       = [(945, 1115), (1230, 1400)]  # IST HHMM
    CALL_BLOCK_PATTERNS = {"BEARISH_ENGULFING", "SHOOTING_STAR", "DOJI"}
    PUT_BLOCK_PATTERNS  = {"BULLISH_ENGULFING", "HAMMER", "DOJI"}
    CALL_BOOST_PATTERNS = {"BULLISH_ENGULFING", "HAMMER"}
    PUT_BOOST_PATTERNS  = {"BEARISH_ENGULFING", "SHOOTING_STAR"}

    def __init__(self, bypass_time_filter: bool = False) -> None:
        self._last_signal_time   = 0.0
        self._signal_cooldown_s  = 180.0  # Increased cooldown to 3 mins
        self._bypass_time_filter = bypass_time_filter  # True in demo/paper mode

    def _in_prime_window(self) -> bool:
        if self._bypass_time_filter:
            return True
        t = _hhmm()
        return any(s <= t <= e for s, e in self.PRIME_WINDOWS)

    def evaluate(self, ltp: float, indicator: IndicatorEngine) -> SignalResult:
        t0         = time.time()
        
        # Layer 1: Kalman filtered price for all signal logic
        price = indicator.kalman_price if indicator.kalman_price > 0 else ltp

        vwap       = indicator.vwap
        rsi        = indicator.rsi
        st_dir     = indicator.supertrend_direction
        above_vwap = price > vwap if vwap > 0 else False
        vol_spike  = indicator.volume_spike()
        adx        = indicator.adx
        ema_bull   = indicator.ema_bullish
        ema_slope  = indicator.ema_fast_slope
        pattern    = indicator.last_candle_pattern
        atr        = indicator.current_atr

        def _no(reason: str) -> SignalResult:
            return SignalResult(signal="NONE", strength=0, reason=reason,
                                supertrend_dir=st_dir, above_vwap=above_vwap,
                                rsi=rsi, volume_spike=vol_spike,
                                adx=adx, ema_bullish=ema_bull, 
                                ema_fast_slope=ema_slope,
                                candle_pattern=pattern)

        if (t0 - self._last_signal_time) < self._signal_cooldown_s:
            return _no("Cooldown active")

        warmup = max(IndicatorEngine.ADX_PERIOD + 2, IndicatorEngine.SUPERTREND_PERIOD + 2)  # 16
        if indicator.num_candles < warmup:
            return _no(f"Warming up ({indicator.num_candles}/{warmup} candles)")

        if not self._in_prime_window():
            return _no(f"Outside prime window (IST {_hhmm()}) — wait 9:45 or 12:30")

        if adx > 0 and adx < self.ADX_MIN:
            return _no(f"Market sideways — ADX={adx:.1f}<{self.ADX_MIN}")
        # Layer 3: ADX tier boost (>25=+5, >30=+15 to signal strength)
        adx_boost = 15 if adx >= 30 else (5 if adx >= 25 else 0)

        # Layer 4: M15 Multi-Timeframe alignment (skip until M15 warmed up)
        m15_dir = indicator.m15_supertrend_direction
        if indicator.m15_num_candles >= IndicatorEngine.SUPERTREND_PERIOD + 2:
            if st_dir != m15_dir:
                return _no(f"MTF mismatch: M5={st_dir} vs M15={m15_dir} — no alignment")

        # Layer 6: Dynamic pullback threshold = max(0.3%, ATR×0.1/price)
        atr = indicator.current_atr
        pullback_th = max(0.003, (atr * 0.1 / price) if price > 0 else 0.003)

        # ── CALL ─────────────────────────────────────────────────────────────
        if st_dir == "UP" and ema_bull and above_vwap and rsi >= self.RSI_CALL_MIN:
            slope_threshold = 0.0005 * price
            if ema_slope < slope_threshold:
                return _no(f"ST=UP ✓ — flat EMA slope ({ema_slope:.2f}<{slope_threshold:.2f}) whipsaw risk")
            if not vol_spike:
                return _no(f"ST=UP EMA✓ VWAP✓ RSI={rsi:.1f} ADX={adx:.1f} — waiting vol spike")
            if pattern in self.CALL_BLOCK_PATTERNS:
                return _no(f"ST=UP — blocked by {pattern}")
            # Layer 6a: Pullback guard — not chasing overextended move
            if indicator.ema_fast > 0:
                overshoot = (price - indicator.ema_fast) / indicator.ema_fast
                if overshoot > pullback_th * 2:
                    return _no(f"CALL overextended {overshoot:.2%} above EMA9 >limit {pullback_th*2:.2%}")
            # Layer 6b: Range momentum — candle must show energy
            if indicator.range_ratio < 1.3 and indicator.num_candles >= 10:
                return _no(f"Weak range={indicator.range_ratio:.2f}x <1.3x avg")
            self._last_signal_time = t0
            strength = min(100, 65 + int((rsi - 55) * 2.5)
                          + (15 if pattern in self.CALL_BOOST_PATTERNS else 0)
                          + adx_boost)
            return SignalResult(
                signal="CALL", strength=strength,
                reason=f"ST=UP EMA✓ VWAP✓ RSI={rsi:.1f} ADX={adx:.1f} M15={m15_dir} Ptn={pattern} LTP={ltp:.1f}",
                supertrend_dir=st_dir, above_vwap=True, rsi=rsi, volume_spike=True,
                adx=adx, ema_bullish=True, ema_fast_slope=ema_slope, candle_pattern=pattern,
                entry_atr=indicator.current_atr,
            )

        # ── PUT ──────────────────────────────────────────────────────────────
        if st_dir == "DOWN" and not ema_bull and not above_vwap and rsi <= self.RSI_PUT_MAX:
            slope_threshold = 0.0005 * price
            if ema_slope > -slope_threshold:
                return _no(f"ST=DOWN ✓ — flat EMA slope ({ema_slope:.2f}>{-slope_threshold:.2f}) whipsaw risk")
            if not vol_spike:
                return _no(f"ST=DOWN EMA✗ VWAP✗ RSI={rsi:.1f} ADX={adx:.1f} — waiting vol spike")
            if pattern in self.PUT_BLOCK_PATTERNS:
                return _no(f"ST=DOWN — blocked by {pattern}")
            # Layer 6a: Pullback guard
            if indicator.ema_fast > 0:
                overshoot = (indicator.ema_fast - price) / indicator.ema_fast
                if overshoot > pullback_th * 2:
                    return _no(f"PUT overextended {overshoot:.2%} below EMA9 >limit {pullback_th*2:.2%}")
            # Layer 6b: Range momentum
            if indicator.range_ratio < 1.3 and indicator.num_candles >= 10:
                return _no(f"Weak range={indicator.range_ratio:.2f}x <1.3x avg")
            self._last_signal_time = t0
            strength = min(100, 65 + int((45 - rsi) * 2.5)
                          + (15 if pattern in self.PUT_BOOST_PATTERNS else 0)
                          + adx_boost)
            return SignalResult(
                signal="PUT", strength=strength,
                reason=f"ST=DOWN EMA✗ VWAP✗ RSI={rsi:.1f} ADX={adx:.1f} M15={m15_dir} Ptn={pattern} LTP={ltp:.1f}",
                supertrend_dir=st_dir, above_vwap=False, rsi=rsi, volume_spike=True,
                adx=adx, ema_bullish=False, ema_fast_slope=ema_slope, candle_pattern=pattern,
                entry_atr=indicator.current_atr,
            )

        return _no(f"No confluence — ST={st_dir} M15={m15_dir} EMA={'bull' if ema_bull else 'bear'} "
                   f"RSI={rsi:.1f} ADX={adx:.1f} VWAP={'above' if above_vwap else 'below'}")


@dataclass
class TrailingState:
    entry_price:      float
    current_sl:       float
    high_water:       float
    lot_size:         int
    is_breakeven:     bool  = False
    trail_step_price: float = 50.0  # Dynamic per trade, default 50

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

# --- MONKEY PATCH: Fix websocket-client vs smartapi parameter mismatch ---
# Newer websocket-client versions pass 4 arguments (self, ws, close_status_code, close_msg)
# But smartapi-python 1.5.5 expects exactly 2 arguments (self, ws).
_orig_on_close = getattr(SmartWebSocketV2, "_on_close", None)
if _orig_on_close:
    def _patched_on_close(self, ws, *args, **kwargs):
        return _orig_on_close(self, ws)
    SmartWebSocketV2._on_close = _patched_on_close
# -------------------------------------------------------------------------

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

# Silence noisy HTTP request logs from Telegram polling
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ── Static IP Proxy Setup (Railway → AngelOne API) ────────────────────────────
# Railway-ல் HTTP_PROXY மற்றும் HTTPS_PROXY variable செட் செய்யப்பட்டால்
# அனைத்து outgoing requests-உம் proxy வழியாக செல்லும் (Static IP கிடைக்கும்)
_proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
if _proxy_url:
    os.environ.setdefault("HTTP_PROXY",  _proxy_url)
    os.environ.setdefault("HTTPS_PROXY", _proxy_url)
    os.environ["ALL_PROXY"] = _proxy_url
    
    if "@" in _proxy_url:
        parts = _proxy_url.split("@")
        proxy_host = parts[-1] if len(parts) > 1 else _proxy_url
    else:
        proxy_host = _proxy_url
    logging.getLogger("bot").info("✅ Proxy configured: %s", proxy_host)
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
    BOT_VERSION = "v1.5.0-Institutional"
    
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
    RISK_PER_TRADE_PCT = float(_get_env("RISK_PER_TRADE_PCT", default="1.5"))

    # ── Advanced Trading Settings ───────────────────────────────────────────
    ORDER_STATUS_TIMEOUT = int(_get_env("ORDER_STATUS_TIMEOUT", default="8"))
    ORDER_RETRIES        = int(_get_env("ORDER_RETRIES",        default="8"))

    # ── Capital Protection Settings ─────────────────────────────────────────
    MAX_DAILY_TRADES    = int(_get_env("MAX_DAILY_TRADES",    default="2"))    # max trades/day
    MIN_SIGNAL_STRENGTH = int(_get_env("MIN_SIGNAL_STRENGTH", default="75"))   # min strength to trade
    CONSEC_LOSS_STOP    = int(_get_env("CONSEC_LOSS_STOP",    default="2"))    # stop after N consecutive losses
    PROFIT_LOCK_AT      = float(_get_env("PROFIT_LOCK_AT",    default="500"))  # lock profits (tighten SL) above this
    SLIPPAGE_BUFFER     = float(_get_env("SLIPPAGE_BUFFER",   default="3.0"))  # padding for limit order execution

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
    if datetime.now(IST).weekday() >= 5:
        return False
    t = _hhmm()
    return Cfg.TRADE_START <= t <= Cfg.TRADE_END


SCRIP_MASTER_URL = (
    "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
)
STRIKE_STEP  = {"BANKNIFTY": 100, "FINNIFTY": 50, "NIFTY": 50}
SPOT_TOKEN   = {"BANKNIFTY": "26009", "FINNIFTY": "26037", "NIFTY": "26000"}
# Layer 5: Bank leader tokens (NSE) for correlation filter
BANK_LEADERS = {"HDFCBANK": "1333", "ICICIBANK": "4963"}
TICK_SIZE    = 0.05


def _round_tick(price: float) -> float:
    decimal_price = Decimal(str(price))
    tick = Decimal(str(TICK_SIZE))
    rounded = (decimal_price / tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick
    return float(rounded)


def _sl_price(entry: float, pct: float = 0.30) -> float:
    return _round_tick(entry * (1.0 - pct))


def _sl_price_dynamic(entry: float, atr: float = 0.0, fallback_pct: float = 0.30) -> float:
    """ATR-based SL: 1.5×ATR below entry. Clamped 8%–35% from entry."""
    if atr > 0:
        proposed = entry - (1.5 * atr)
        proposed = max(proposed, entry * 0.65)  # cap at 35% loss
        proposed = min(proposed, entry * 0.92)  # min 8% buffer
    else:
        proposed = entry * (1.0 - fallback_pct)
    return _round_tick(proposed)


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
    high_water:  float = 0.0
    is_breakeven: bool = False


class StateStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self.daily_pnl       = 0.0
        self.trade_count     = 0
        self.consec_losses   = 0   # consecutive losing trades today
        self.active_trade: Optional[ActiveTrade] = None
        self.day = ""
        self._load()

    def _load(self) -> None:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                if data.get("day") == today:
                    self.daily_pnl     = float(data.get("daily_pnl", 0.0))
                    self.trade_count   = int(data.get("trade_count", 0))
                    self.consec_losses = int(data.get("consec_losses", 0))
                    raw = data.get("active_trade")
                    if raw:
                        self.active_trade = ActiveTrade(**raw)
                    self.day = today
                    logger.info("State restored — PnL=%.0f trades=%d consec_loss=%d",
                                self.daily_pnl, self.trade_count, self.consec_losses)
                    return
            except Exception as exc:
                logger.warning("State load failed: %s", exc)
        self.day = today

    async def check_day_rollover(self) -> None:
        async with self._lock:
            today = datetime.now(IST).strftime("%Y-%m-%d")
            if self.day and self.day != today:
                self.day = today
                self.daily_pnl = 0.0
                self.trade_count = 0
                self.consec_losses = 0
                logger.info("🌅 Day rollover! Resetting daily metrics for %s", today)

    async def save(self) -> None:
        async with self._lock:
            data = {
                "day":          self.day,
                "daily_pnl":    self.daily_pnl,
                "trade_count":  self.trade_count,
                "consec_losses":self.consec_losses,
                "active_trade": asdict(self.active_trade) if self.active_trade else None,
                "saved_at":     datetime.now(IST).isoformat(),
            }
            try:
                # 🛡️ Atomic Save (Prevents corruption on crash)
                tmp_file = self._path.with_suffix(".tmp")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: tmp_file.write_text(json.dumps(data, indent=2), encoding="utf-8"))
                await loop.run_in_executor(None, tmp_file.replace, self._path)
            except Exception as exc:
                logger.error("State save failed: %s", exc)

    def day_blocked(self) -> bool:
        return self.daily_pnl <= -Cfg.MAX_DAILY_LOSS or self.daily_pnl >= Cfg.DAILY_TARGET

    def _write_journal(journal_path: Path, jdata: dict) -> None:
        with journal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(jdata) + "\n")

    async def record_closed_trade(self, pnl: float) -> None:
        async with self._lock:
            # 📓 Trade Journal Auto-Writer
            if self.active_trade:
                try:
                    journal_path = self._path.parent / "trade_journal.jsonl"
                    jdata = asdict(self.active_trade)
                    jdata["exit_time"]    = datetime.now(IST).isoformat()
                    jdata["realised_pnl"] = pnl
                    jdata["trade_id"]     = self.trade_count + 1
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, lambda: _write_journal(journal_path, jdata))
                except Exception:
                    pass
            
            self.daily_pnl = round(self.daily_pnl + pnl, 2)
            self.trade_count += 1
            if pnl < 0:
                self.consec_losses += 1
            else:
                self.consec_losses = 0  # reset on win
                
            self.active_trade = None


# ── Angel One client ──────────────────────────────────────────────────────────

class AngelClient:
    def __init__(self) -> None:
        self._client: Optional[SmartConnect] = None
        self.auth_token  = ""
        self.feed_token  = ""
        self._login_time = 0.0
        self._session_lock = asyncio.Lock()  # ✅ Fix #1: Async-safe session lock

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

    async def ensure_session(self) -> None:
        # ✅ Fix #1: Only one coroutine can refresh the session at a time
        if time.time() - self._login_time > 21600:
            async with self._session_lock:
                # Double-check after acquiring lock (another coroutine may have refreshed)
                if time.time() - self._login_time > 21600:
                    logger.info("🔄 Session expired — re-logging in...")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, self.login)

    async def place_order(self, params: Dict[str, Any]) -> str:
        await self.ensure_session()
        if not self._client:
            raise RuntimeError("Client not initialized")
        
        for attempt in range(3):
            try:
                resp = self._client.placeOrder(params)
                if not resp or not resp.get("status"):
                    # Capture Angel One explicit rejection
                    raise RuntimeError(f"Order rejected internally: {resp}")
                if not resp.get("data") or "orderid" not in resp["data"]:
                    raise RuntimeError(f"Invalid API response (no orderid): {resp}")
                return str(resp["data"]["orderid"])
            except Exception as exc:
                if attempt == 2:
                    raise RuntimeError(f"Failed after 3 retries: {exc}")
                logger.warning("placeOrder attempt %d failed: %s. Retrying in 1s...", attempt + 1, exc)
                time.sleep(1.0)

    async def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        await self.ensure_session()
        if not self._client:
            return 0.0
        resp = self._client.ltpData(exchange, symbol, token)
        return float(resp["data"]["ltp"])

    async def get_funds(self) -> float:
        await self.ensure_session()
        if not self._client:
            return 0.0
        try:
            data = self._client.rmsLimit().get("data", {})
            return float(data.get("availablecash", 0))
        except Exception:
            return 0.0

    async def get_order_status(self, order_id: str) -> Optional[Dict[str, Any]]:
        await self.ensure_session()
        if not self._client:
            return None
        try:
            resp = self._client.orderBook()
            data = resp.get("data") if resp else None
            if data and isinstance(data, list):
                for order in data:
                    if str(order.get("orderid", "")) == str(order_id):
                        return order
        except Exception as exc:
            logger.warning("OrderBook fetch failed: %s", exc)
        return None


# ── Scrip master helpers ──────────────────────────────────────────────────────

async def fetch_scrip_master() -> List[Dict[str, Any]]:
    cache = Path("scrip_master_cache.json")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(SCRIP_MASTER_URL)
            r.raise_for_status()
            rows = r.json()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: cache.write_text(json.dumps(rows), encoding="utf-8"))
            return rows
    except Exception as exc:
        logger.warning("Scrip master download failed (%s), trying cache", exc)
        if cache.exists():
            loop = asyncio.get_running_loop()
            text = await loop.run_in_executor(None, lambda: cache.read_text(encoding="utf-8"))
            return json.loads(text)
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
                             buffer: float = -1.0, trade_mode: str = "paper") -> Tuple[str, int, float]:
    if buffer < 0:
        buffer = Cfg.SLIPPAGE_BUFFER
    price  = _round_tick(ltp + buffer if action == "BUY" else ltp - buffer)
    params = {
        "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
        "transactiontype": action, "exchange": "NFO", "ordertype": "LIMIT",
        "producttype": "INTRADAY", "duration": "IOC",
        "quantity": str(qty), "price": str(price),
    }
    if trade_mode == "paper":
        sim_id = f"PAPER_{int(time.time() * 1000)}"
        logger.info("[PAPER] %s %s qty=%d @%.2f", action, symbol, qty, price)
        return sim_id, qty, price
    
    loop = asyncio.get_running_loop()
    try:
        order_id = await angel.place_order(params)
        
        # ── Poll OrderBook to ensure fill ──
        final_filled = 0
        final_price  = 0.0
        for _ in range(Cfg.ORDER_STATUS_TIMEOUT):
            await asyncio.sleep(1.0)
            status_data = await angel.get_order_status(order_id)
            if status_data:
                status = str(status_data.get("status", "")).lower()
                final_filled = int(status_data.get("filledshares", 0) or 0)
                final_price = float(status_data.get("averageprice", 0) or 0.0)

                if "rejected" in status or "cancelled" in status:
                    if final_filled == 0:
                        raise RuntimeError(f"Order {order_id} {status}: {status_data.get('text', '')}")
                    break
                if "completed" in status or final_filled >= qty:
                    if final_price > 0.0:
                        break
        
        if final_filled > 0 and final_price <= 0.0:
            final_price = ltp
        
        if final_filled == 0:
            raise RuntimeError(f"Order {order_id} failed to fill within IOC timeout.")
            
        return order_id, final_filled, final_price
    except Exception as exc:
        raise RuntimeError(f"Angel One API Order Error: {exc}")


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

class MetricsCollector:
    def __init__(self) -> None:
        self.trade_times: List[float] = []
        self.wins = 0
        self.losses = 0

    def record_trade(self, entry_time_str: str, pnl: float) -> None:
        try:
            entry_time = datetime.fromisoformat(entry_time_str)
            execution_time = (datetime.now(IST) - entry_time).total_seconds()
            self.trade_times.append(execution_time)
        except Exception:
            pass
        if pnl > 0: self.wins += 1
        elif pnl < 0: self.losses += 1

    def get_win_rate(self) -> float:
        total = self.wins + self.losses
        return round((self.wins / total * 100), 2) if total > 0 else 0.0

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, timeout_sec: int = 60) -> None:
        self.failures = 0
        self.last_failure = 0.0
        self.state = "CLOSED"
        self.failure_threshold = failure_threshold
        self.timeout = timeout_sec

    async def execute(self, coro_func: Any, *args: Any, **kwargs: Any) -> Any:
        if self.state == "OPEN":
            if time.time() - self.last_failure > self.timeout:
                self.state = "HALF_OPEN"
                logger.warning("🛡️ Circuit Breaker HALF-OPEN — Attempting recovery order.")
            else:
                raise RuntimeError("Circuit breaker OPEN: Angel API repeatedly failing.")
        try:
            result = await coro_func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failures = 0
                logger.info("🛡️ Circuit Breaker CLOSED — Connectivity restored.")
            return result
        except Exception as e:
            self.failures += 1
            self.last_failure = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "OPEN"
                logger.error("🛡️ CIRCUIT BREAKER TRIPPED! Angel API banned or failing!")
            raise e


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
        self.strategy   = AlphaStrategy(bypass_time_filter=not bool(Cfg.API_KEY))
        self.metrics    = MetricsCollector()
        self.order_circuit = CircuitBreaker()
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
        self._prev_vol: Dict[str, int] = {}  # Delta volume map
        # Layer 5: Bank leader correlation tracking (HDFC + ICICI)
        self._bank_ltps:      Dict[str, float] = {}   # current price
        self._bank_prev_ltps: Dict[str, float] = {}   # price from last maintenance tick
        # Simulation state
        self._sim_price  = 55000.0    # BankNifty base price
        self._sim_volume = 0
        self._sim_candle_count = 0
        self._exit_lock      = asyncio.Lock()   # 🛡️ Prevents duplicate exit orders
        self._ws_connected   = False

    @property
    def _demo_mode(self) -> bool:
        """True when running paper mode without Angel One credentials."""
        return not bool(Cfg.API_KEY)

    async def _preload_historical_data(self) -> None:
        if not self.angel.auth_token:
            return
            
        spot_token = SPOT_TOKEN.get(Cfg.UNDERLYING)
        if not spot_token:
            return
            
        logger.info("⏳ Fetching historical data to warm up indicators...")
        from datetime import timedelta
        dt_now = datetime.now(IST)
        
        # Fetch ~4 days back, but ensure we don't land on a weekend
        dt_from = dt_now - timedelta(days=4)
        while dt_from.weekday() > 4:  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
            dt_from -= timedelta(days=1)
            
        # Angel One Historical API requires '999' prefix for indices (26009 -> 99926009)
        hist_token = f"999{spot_token}" if spot_token in ["26000", "26009", "26037"] else spot_token
            
        params = {
            "exchange": "NSE",
            "symboltoken": hist_token,
            "interval": "FIVE_MINUTE",
            "fromdate": dt_from.strftime("%Y-%m-%d 09:15"),
            "todate": dt_now.strftime("%Y-%m-%d %H:%M")
        }
        
        try:
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(None, self.angel._client.getCandleData, params)
            if not res or not res.get("status"):
                logger.error("❌ Failed to fetch historical data (status False): %s", res)
                return
                
            data = res.get("data")
            if not data:
                logger.warning("⚠️ No historical data. Res: %s | Params: %s", res, params)
                return
                
            ind = IndicatorEngine()
            # data item: [timestamp, open, high, low, close, volume]
            for row in data:
                try:
                    c_dt = datetime.fromisoformat(row[0])
                    ts_ms = int(c_dt.timestamp() * 1000)
                    op, hi, lo, cl, vol = row[1], row[2], row[3], row[4], row[5]
                    
                    vol_q = max(0, int(vol // 4))
                    # Forge ticks to simulate candle building
                    for t_ms, p in [
                        (ts_ms + 1000, op),
                        (ts_ms + 60000, hi),
                        (ts_ms + 120000, lo),
                        (ts_ms + 299000, cl)
                    ]:
                        ind.on_tick(Tick(spot_token, p, vol_q, t_ms))
                except Exception as inner:
                    logger.debug("Failed to parse historical candle info: %s", inner)
                    
            self.indicators[spot_token] = ind
            logger.info("✅ Historical Warmup Complete! %d candles loaded.", len(data))
            
            await self.tg.send(f"✅ *Indicators Warmed Up*\n"
                               f"Loaded {len(data)} past candles.\n"
                               f"Ready to trade immediately!")
        except Exception as exc:
            logger.error("❌ Exception in preload_historical_data: %s", exc)

    async def start(self) -> None:
        self._loop    = asyncio.get_running_loop()
        self._running = True
        logger.info("🚀 Starting — mode=%s underlying=%s", Cfg.TRADE_MODE, Cfg.UNDERLYING)

        if Cfg.API_KEY:
            ok = await self._loop.run_in_executor(None, self.angel.login)
            if not ok and Cfg.TRADE_MODE == "live":
                raise RuntimeError("Angel One login failed")
            if ok:
                await self._preload_historical_data()
        else:
            logger.warning("No API_KEY — paper-only simulation mode")

        try:
            self._scrip_rows = await fetch_scrip_master()
            logger.info("Scrip master: %d instruments", len(self._scrip_rows))
        except Exception as exc:
            logger.error("Scrip master failed: %s", exc)
            await self.tg.alert_error("SCRIP_MASTER", str(exc))

        await self.tg.send(
            f"🤖 *Ultra-Fast Algo Bot Started (`{Cfg.BOT_VERSION}`)*\n"
            f"Mode: `{self._trade_mode_confirmed.upper()}`  Underlying: `{Cfg.UNDERLYING}`\n"
            f"Capital: ₹{Cfg.CAPITAL:,.0f}  MaxLoss: ₹{Cfg.MAX_DAILY_LOSS:,.0f}\n"
            f"Target: ₹{Cfg.DAILY_TARGET:,.0f}  Lot: {Cfg.LOT_SIZE}\n"
            f"Time: `{_now_label()}`\n\n"
            f"Send /help for commands."
        )

        if self.state.active_trade:
            t = self.state.active_trade
            self._trailing = TrailingState(
                entry_price=t.entry_price, current_sl=t.sl_price,
                high_water=getattr(t, 'high_water', 0.0) or t.entry_price,
                lot_size=t.lot_size,
            )
            if getattr(t, 'is_breakeven', False):
                self._trailing.is_breakeven = True
            await self.tg.send(
                f"♻️ *Resumed trade from saved state*\n"
                f"`{t.symbol}` Entry=₹{t.entry_price:.2f} SL=₹{t.sl_price:.2f}\n"
                f"Qty=*{t.lot_size}* | High Water=₹{self._trailing.high_water:.2f}"
            )

        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._auto_heal_loop())   # ← self-healing background task

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
        else:
            # ── Real mode: start WebSocket for live tick data ──────────────
            asyncio.create_task(self._ws_loop())
            logger.info("📡 WebSocket loop started — connecting to Angel One feed...")
            await self.tg.send(
                f"📡 *Live Data Feed Connecting...*\n"
                f"Angel One WebSocket → BankNifty tick stream\n"
                f"Mode: `{Cfg.TRADE_MODE.upper()}`\n"
                f"Send /starttrade when ready to arm trading!"
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

            # Simulate realistic bid-ask spread bounce & occasional micro-gaps
            bounce = rng.uniform(-0.0003, 0.0003) if (tick_count % 3 != 0) else 0.0
            gap    = rng.uniform(-0.002, 0.002) if (tick_count % 25 == 0) else 0.0

            self._sim_price = round(self._sim_price * (1 + drift + bounce + gap), 2)
            self._sim_price = max(40000.0, min(70000.0, self._sim_price))

            # ── Volume simulation (spike every ~10 candles) ────────────────────
            delta_vol = rng.randint(800, 2000)
            # Inject a volume spike when we're in strong trend phase
            if candle_num >= 10 and (tick_count % CANDLE_TICKS) == 5:
                delta_vol += base_vol * 3   # 3× average spike

            # ── Build the tick using fake 5-min candle boundaries ─────────────
            # Map real elapsed seconds → fake 5-min epoch so IndicatorEngine
            # sees proper candle boundaries
            fake_ts_sec = candle_ts + (tick_count * 300 / CANDLE_TICKS)
            ts_ms       = int(fake_ts_sec * 1000)

            tick = Tick(
                token=spot_token,
                ltp=self._sim_price,
                volume=delta_vol,
                timestamp_ms=ts_ms,
            )

            t_start = time.monotonic()
            await self._process_tick_demo(tick, t_start)

            # 🎟️ Emulate Option Tick dynamically if active
            if self.state.active_trade:
                opt = self.state.active_trade
                opt_tick = Tick(token=opt.token, ltp=round(self._sim_price * 0.01 + rng.uniform(-2, 2), 2),
                                volume=10, timestamp_ms=ts_ms)
                await self._process_tick_demo(opt_tick, t_start)

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

        if token == spot_token:
            self._spot_ltp[Cfg.UNDERLYING] = ltp

        # Track option price for active demo trade
        if self.state.active_trade and token == self.state.active_trade.token:
            self._option_ltp[token] = ltp
            await self._manage_trailing(ltp)

        if token != spot_token:
            return
            
        await self.state.check_day_rollover()

        if self.state.day_blocked():
            return

        if token not in self.indicators:
            self.indicators[token] = IndicatorEngine()
        ind = self.indicators[token]
        ind.on_tick(tick)

        if self.state.active_trade:
            await self._check_smart_exit(ltp, ind)
            return

        if not self._trade_armed:
            return

        result = self.strategy.evaluate(ltp, ind)
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

        spot_token  = SPOT_TOKEN.get(Cfg.UNDERLYING)
        sub_payload = []
        if spot_token:
            sub_payload.append({"exchangeType": 1, "tokens": [spot_token]})
        if self.state.active_trade:
            sub_payload.append({"exchangeType": 2, "tokens": [self.state.active_trade.token]})

        loop        = asyncio.get_running_loop()

        # ── Callback definitions ───────────────────────────────────────────────

        def on_open(wsapp):
            self._ws_connected = True
            logger.info("WebSocket connected ✅ — subscribing to %s", spot_token)
            if sub_payload and self._ws:
                try:
                    self._ws.subscribe("bot1", 3, sub_payload)
                    logger.info("📡 Subscribed to BankNifty feed (token=%s)", spot_token)
                except Exception as sub_err:
                    logger.error("Subscribe failed: %s", sub_err)

        def on_data(_wsapp, message):
            asyncio.run_coroutine_threadsafe(
                self._on_tick_raw(message), loop
            )

        def on_error(_wsapp, error):
            self._ws_connected = False
            logger.error("WS error: %s", error)

        # BUG FIX: websocket-client 1.8 passes (wsapp, code, msg) → 3 args.
        # Use *args to accept any number of arguments safely.
        def on_close(*args):
            self._ws_connected = False
            code = args[1] if len(args) > 1 else None
            msg  = args[2] if len(args) > 2 else None
            logger.warning("WS closed: code=%s msg=%s", code, msg)

        # ── Build & connect ───────────────────────────────────────────────────
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
            raw_vol= int(data.get("volume", 0))
            ts_ms  = int(data.get("ts_ms", time.time() * 1000))
            if ltp <= 0:
                return
            
            # 🛡️ Delta Volume tracking (Angel V2 sends cumulative)
            prev = self._prev_vol.get(token, raw_vol)
            delta_vol = max(0, raw_vol - prev)
            self._prev_vol[token] = raw_vol

            tick = Tick(token=token, ltp=ltp, volume=delta_vol, timestamp_ms=ts_ms)
            await self._process_tick(tick, t_start)
        except Exception as exc:
            logger.debug("Tick error: %s", exc)

    def _parse_tick(self, message: Any) -> Optional[Dict[str, Any]]:
        if isinstance(message, dict):
            # Angel One Websocket V2 Dict payloads provide `last_traded_price` strictly in paise.
            ltp = float(message.get("last_traded_price", message.get("ltp", 0)) or 0)
            ltp /= 100.0  # Force conversion to Rupees
            return {
                "token":  str(message.get("token", "")),
                "ltp":    ltp,
                "volume": int(message.get("volume_trade_for_the_day",
                                          message.get("volume", 0)) or 0),
                "ts_ms":  int(message.get("exchange_timestamp", time.time() * 1000) or
                              time.time() * 1000),
            }
        if isinstance(message, (bytes, bytearray)) and len(message) >= 55:
            try:
                token_raw = message[2:27].decode("utf-8").strip("\x00")
                ltp_raw   = int.from_bytes(message[39:47], "big") / 100.0
                vol       = int.from_bytes(message[47:55], "big")
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

        # Layer 5: Track bank leader prices for correlation filter
        if token in BANK_LEADERS.values():
            self._bank_ltps[token] = ltp

        if self.state.active_trade and token == self.state.active_trade.token:
            self._option_ltp[token] = ltp
            await self._maybe_notify_price_move(token, ltp)
            await self._manage_trailing(ltp)

        if token != spot_token:
            return
        
        await self.state.check_day_rollover()
        
        # Skip market hours check in demo mode (simulation bypasses via _process_tick_demo)
        if not self._demo_mode and (not _is_trading_hours() or self.state.day_blocked()):
            return
        if self.state.day_blocked():
            return

        # Continuous Background Execution (Crucial for Smart Exits)
        if token not in self.indicators:
            self.indicators[token] = IndicatorEngine()
        ind = self.indicators[token]
        ind.on_tick(tick)

        if self.state.active_trade:
            await self._check_smart_exit(ltp, ind)
            return
            
        if not self._trade_armed:
            return

        result = self.strategy.evaluate(ltp, ind)
        if result.signal in ("CALL", "PUT"):
            elapsed = (time.monotonic() - t_start) * 1000
            logger.info("🎯 %s | RSI=%.1f | ST=%s | eval=%.1fms",
                        result.signal, result.rsi, result.supertrend_dir, elapsed)
            await self._fire_order(result, ltp)

    # ── Order execution ───────────────────────────────────────────────────────

    async def _fire_order(self, signal: SignalResult, spot_ltp: float) -> None:
        # ── Capital Protection Gates ──────────────────────────────────────────
        # Gate 1: Max trades per day
        if self.state.trade_count >= Cfg.MAX_DAILY_TRADES:
            logger.info("🛑 Max daily trades (%d) reached — no new trade", Cfg.MAX_DAILY_TRADES)
            return
        # Gate 2: Minimum signal strength
        if signal.strength < Cfg.MIN_SIGNAL_STRENGTH:
            logger.info("🛑 Signal strength %d%% < minimum %d%% — skipping",
                        signal.strength, Cfg.MIN_SIGNAL_STRENGTH)
            return
        # Gate 3: Consecutive loss stop
        if self.state.consec_losses >= Cfg.CONSEC_LOSS_STOP:
            logger.info("🛑 %d consecutive losses — trading stopped for today",
                        self.state.consec_losses)
            return
        # Gate 4 (Layer 5): Bank leader correlation — at least 1 must confirm
        if self._bank_ltps and self._bank_prev_ltps:
            confirms = sum(
                1 for tok, ltp in self._bank_ltps.items()
                if (signal.signal == "CALL" and ltp > self._bank_prev_ltps.get(tok, ltp))
                or (signal.signal == "PUT"  and ltp < self._bank_prev_ltps.get(tok, ltp))
            )
            if confirms == 0:
                logger.info("🛑 Layer5 Correlation: Neither HDFC nor ICICI confirms %s — skip",
                            signal.signal)
                return
            logger.info("📊 Layer5 Correlation: %d/2 bank leaders confirm %s", confirms, signal.signal)

        t0 = time.monotonic()
        try:
            option_type = "CE" if signal.signal == "CALL" else "PE"
            today       = datetime.now(IST).strftime("%Y-%m-%d")
            row         = find_atm_option(self._scrip_rows, Cfg.UNDERLYING,
                                          spot_ltp, option_type, today)
            symbol      = str(row["symbol"])
            token       = str(row["token"])
            raw_lot_sz  = int(float(row.get("lotsize") or _lot_qty(Cfg.UNDERLYING)))

            if self._trade_mode_confirmed == "live" and Cfg.API_KEY:
                opt_ltp = await self.angel.get_ltp("NFO", symbol, token)
            else:
                opt_ltp = spot_ltp * 0.01    # paper sim ~1% of index

            if opt_ltp <= 0:
                logger.warning("Option LTP 0 for %s — skipping", symbol)
                return

            # ── Dynamic Risk-Based Position Sizing ──
            sl_est = _sl_price_dynamic(opt_ltp, atr=signal.entry_atr)
            risk_per_share = max((opt_ltp - sl_est), 0.10)
            
            # Max risk per trade: defaults to 1.5% of absolute capital
            max_risk_trade = Cfg.CAPITAL * (Cfg.RISK_PER_TRADE_PCT / 100.0) 
            
            lots_by_risk = max(1, int(max_risk_trade / (risk_per_share * raw_lot_sz)))
            lots_by_capital = max(1, int(Cfg.CAPITAL / (opt_ltp * raw_lot_sz)))
            safe_lots = min(lots_by_risk, lots_by_capital, Cfg.LOT_SIZE)
            
            qty = safe_lots * raw_lot_sz
            logger.info("📐 Sizing: Lots=%d (Risk Bound) Qty=%d", safe_lots, qty)

            order_id, filled_qty, avg_price = await self.order_circuit.execute(
                place_limit_order, self.angel, symbol, token, qty, "BUY", opt_ltp, trade_mode=self._trade_mode_confirmed
            )
            sl       = _sl_price_dynamic(avg_price, atr=signal.entry_atr)

            trade = ActiveTrade(
                order_id=order_id, symbol=symbol, token=token,
                option_type=option_type, lot_size=filled_qty,
                entry_price=avg_price, sl_price=sl,
                entry_time=datetime.now(IST).isoformat(),
                mode=self._trade_mode_confirmed,
                high_water=avg_price,
                is_breakeven=False
            )
            self.state.active_trade = trade
            self._trailing = TrailingState(
                entry_price=avg_price, current_sl=sl,
                high_water=avg_price, lot_size=filled_qty,
                trail_step_price=max(50.0, signal.entry_atr * 0.5)
            )
            await self.state.save()

            # 📡 Subscribe Option Token
            if self._ws:
                try:
                    spot_t = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
                    sub_p = []
                    if spot_t: sub_p.append({"exchangeType": 1, "tokens": [spot_t]})
                    sub_p.append({"exchangeType": 2, "tokens": [token]})
                    self._ws.subscribe("bot_opt", 3, sub_p)
                    logger.info("📡 Live WS Subscribed to Option Token: %s", token)
                except Exception as wse:
                    logger.warning("Failed to subscribe option token %s: %s", token, wse)

            elapsed = (time.monotonic() - t0) * 1000
            logger.info("✅ Order in %.1fms", elapsed)
            await self.tg.send(
                f"🤝 *TRADE EXECUTED* — `{signal.signal}` (Strength: {signal.strength}%)\n"
                f"Symbol: `{symbol}`\n"
                f"Qty: {filled_qty} | Avg Entry: ₹{avg_price:.2f} | SL: ₹{sl:.2f}\n"
                f"ADX: {signal.adx:.1f} | EMA: {'🟢' if signal.ema_bullish else '🔴'} | Pattern: `{signal.candle_pattern}`\n"
                f"_{signal.reason}_\n"
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

        # Layer 8: Tier-Based Profit Lock
        # Tier 1 (₹500+): Lock 50% of floating gain
        # Tier 2 (₹1000+): Lock 70% of floating gain
        pnl_unrealised = self._trailing.unrealised_pnl(ltp)
        lock_sl        = 0.0
        lock_tier      = ""
        if pnl_unrealised >= 1000:
            gain_per_unit = pnl_unrealised * 0.70 / trade.lot_size
            lock_sl  = _round_tick(trade.entry_price + gain_per_unit)
            lock_tier = "70%"
        elif pnl_unrealised >= 500:
            gain_per_unit = pnl_unrealised * 0.50 / trade.lot_size
            lock_sl  = _round_tick(trade.entry_price + gain_per_unit)
            lock_tier = "50%"
        if lock_sl > self._trailing.current_sl:
            self._trailing.current_sl = lock_sl
            trade.sl_price            = lock_sl
            changed                   = True
            logger.info("🔒 Tier Profit Lock %s: SL=₹%.2f unrealised=₹%.0f",
                        lock_tier, lock_sl, pnl_unrealised)
            await self.tg.send(
                f"🔒 *Profit Lock {lock_tier}*\n"
                f"Unrealised P&L: ₹{pnl_unrealised:+.0f}\n"
                f"SL locked at ₹{lock_sl:.2f}"
            )

        if changed:
            trade.sl_price = self._trailing.current_sl
            trade.high_water = self._trailing.high_water
            trade.is_breakeven = self._trailing.is_breakeven
            await self.state.save()
            pnl = self._trailing.unrealised_pnl(ltp)
            be  = " 🔒 Breakeven" if self._trailing.is_breakeven else ""
            await self.tg.send(
                f"📈 *SL TRAILED*{be}\n"
                f"`{trade.symbol}` LTP=₹{ltp:.2f}\n"
                f"New SL: ₹{self._trailing.current_sl:.2f}  Unrealised P&L: ₹{pnl:.0f}"
            )
        if ltp <= trade.sl_price:
            await self._exit_trade(ltp, "STOP_LOSS_HIT")

    async def _exit_trade(self, ltp: float, reason: str = "SIGNAL_EXIT") -> None:
        if self._exit_lock.locked():
            logger.debug("Exit already in progress (%s) — skipping duplicate", reason)
            return
        if not self.state.active_trade:
            return
        async with self._exit_lock:
            try:
                trade = self.state.active_trade
                rem_qty = trade.lot_size
                total_sold = 0
                weighted_prc = 0.0

                for attempt in range(3):
                    try:
                        _oid, sold_qty, avg_prc = await self.order_circuit.execute(
                            place_limit_order, self.angel, trade.symbol, trade.token, rem_qty, "SELL", ltp, trade_mode=trade.mode
                        )
                        if sold_qty > 0:
                            weighted_prc += (sold_qty * avg_prc)
                            total_sold += sold_qty
                            rem_qty -= sold_qty

                        if rem_qty <= 0:
                            break
                    except Exception as exc:
                        if attempt == 2 and total_sold == 0:
                            await self.tg.alert_error("EXIT_ORDER", str(exc))
                            return
                        await asyncio.sleep(1.0)

                if total_sold == 0:
                    return

                final_exit_price = round(weighted_prc / total_sold, 2)

                # === 📉 Book-keeping Block (sold qty பொறுத்து P&L) ===
                pnl = round((final_exit_price - trade.entry_price) * total_sold, 2)
                self.metrics.record_trade(trade.entry_time, pnl)

                if rem_qty > 0:
                    # Partial fill — update lot_size & notify; keep active_trade alive
                    trade.lot_size = rem_qty
                    if self._trailing:
                        self._trailing.lot_size = rem_qty
                    self.state.daily_pnl = round(self.state.daily_pnl + pnl, 2)
                    await self.state.save()
                    logger.warning("⚠️ Partial exit: sold=%d stranded=%d @₹%.2f",
                                   total_sold, rem_qty, final_exit_price)
                    await self.tg.send(
                        f"⚠️ *PARTIAL FILL EXIT* — {reason}\n"
                        f"`{trade.symbol}` Exit Avg: ₹{final_exit_price:.2f}\n"
                        f"Sold: {total_sold} | Stranded: {rem_qty}\n"
                        f"P&L (partial): ₹{pnl:+.0f}  Daily: ₹{self.state.daily_pnl:+.0f}\n"
                        f"⚠️ *{rem_qty} qty இன்னும் open* — /exitnow மீண்டும் இயக்கவும்!"
                    )
                else:
                    # Complete exit — clear active_trade fully & unsubscribe WS
                    if self._ws:
                        try:
                            self._ws.unsubscribe("bot_opt", 3, [{"exchangeType": 2, "tokens": [trade.token]}])
                            spot_t = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
                            sub_p = []
                            if spot_t: sub_p.append({"exchangeType": 1, "tokens": [spot_t]})
                            self._ws.subscribe("bot_spot", 3, sub_p)
                            logger.info("📡 Live WS Reverted to Spot only.")
                        except Exception:
                            pass

                    await self.state.record_closed_trade(pnl)
                    self._trailing = None
                    self._option_ltp.pop(trade.token, None)
                    self._last_pct_notify.pop(trade.token, None)
                    await self.state.save()

                    emoji = "🟢" if pnl >= 0 else "🔴"
                    await self.tg.send(
                        f"{emoji} *TRADE CLOSED* — {reason}\n"
                        f"`{trade.symbol}` Exit: ₹{final_exit_price:.2f}\n"
                        f"P&L: ₹{pnl:+.0f}  Daily: ₹{self.state.daily_pnl:+.0f}\n"
                        f"Win Rate: {self.metrics.get_win_rate():.1f}%"
                    )
                    if self.state.daily_pnl <= -Cfg.MAX_DAILY_LOSS:
                        self._trade_armed = False
                        await self.tg.send(
                            "🛑 *DAILY LOSS LIMIT HIT* — Bot disarmed for today.\n"
                            f"Total Loss: ₹{abs(self.state.daily_pnl):.0f}"
                        )
                    elif self.state.daily_pnl >= Cfg.DAILY_TARGET:
                        self._trade_armed = False
                        await self.tg.send(
                            "🎯 *DAILY TARGET HIT* — Bot disarmed. நல்ல வேலை!\n"
                            f"Total Profit: ₹{self.state.daily_pnl:.0f}"
                        )
            except Exception as exc:
                logger.error("_exit_trade unhandled error: %s", exc)

    async def _check_smart_exit(self, spot_ltp: float, ind: IndicatorEngine) -> None:
        """Hybrid Early Exit: Reversal Pattern + Price breaking EMA9."""
        trade = self.state.active_trade
        if not trade or not self._trailing: return
        
        # Only deploy Smart Exits to protect large floating profits (reduce whip-outs)
        act_ltp = self._option_ltp.get(trade.token, trade.entry_price)
        pnl_unrealised = self._trailing.unrealised_pnl(act_ltp)
        if pnl_unrealised < 250:
            return

        pattern = ind.last_candle_pattern
        if trade.option_type == "CE":
            if pattern in self.strategy.CALL_BLOCK_PATTERNS and spot_ltp < ind.ema_fast:
                logger.info("🧠 Smart Exit Triggered: CE reversal %s + EMA break", pattern)
                await self.tg.send(f"🧠 *Smart Exit triggered!*\nPattern: `{pattern}`\nSecured P&L: ₹{pnl_unrealised:.0f}")
                await self._exit_trade(act_ltp, "SMART_EXIT")
        else:
            if pattern in self.strategy.PUT_BLOCK_PATTERNS and spot_ltp > ind.ema_fast:
                logger.info("🧠 Smart Exit Triggered: PE reversal %s + EMA break", pattern)
                await self.tg.send(f"🧠 *Smart Exit triggered!*\nPattern: `{pattern}`\nSecured P&L: ₹{pnl_unrealised:.0f}")
                await self._exit_trade(act_ltp, "SMART_EXIT")

    async def _maybe_notify_price_move(self, token: str, ltp: float) -> None:
        last = self._last_pct_notify.get(token, ltp)
        # ✅ Fix #5: Options-க்கு 2% threshold — 1% = அதிக noise notifications
        if last > 0 and abs((ltp - last) / last) * 100 >= 2.0:
            self._last_pct_notify[token] = ltp
            pct = round((ltp - last) / last * 100, 1)
            d = "📈" if ltp > last else "📉"
            await self.tg.send(
                f"{d} *{abs(pct):.1f}% Move* `{token}` ₹{last:.2f}→₹{ltp:.2f}"
            )

    # ── Maintenance loop ──────────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        _eod_squared_off_today = ""   # ✅ Fix #2: track date to avoid duplicate EOD exits
        while self._running:
            try:
                await self.state.check_day_rollover()
                today = datetime.now(IST).strftime("%Y-%m-%d")

                # ✅ Fix #2: Use >= range (not ==) so 30s sleep never misses 14:25
                # ✅ Fix #3: Warn clearly if live LTP unavailable instead of silently using entry_price
                t_now = _hhmm()
                if 1425 <= t_now <= 1428 and self.state.active_trade and _eod_squared_off_today != today:
                    _eod_squared_off_today = today   # mark done for this calendar day
                    t   = self.state.active_trade
                    ltp = 0.0
                    if Cfg.API_KEY and self._loop:
                        try:
                            ltp = await self.angel.get_ltp("NFO", t.symbol, t.token)
                        except Exception as ltp_err:
                            logger.warning("⚠️ EOD LTP fetch failed (%s) — using last known option LTP", ltp_err)
                    # Fallback priority: live LTP → last WS tick → entry price (last resort)
                    if ltp <= 0:
                        ltp = self._option_ltp.get(t.token, 0.0)
                    if ltp <= 0:
                        ltp = t.entry_price
                        logger.warning("⚠️ EOD: No live LTP available — exiting at entry price ₹%.2f (may be inaccurate)", ltp)
                    await self._exit_trade(ltp, "EOD_SQUARE_OFF")

                # Layer 9: Smart Time Exit — only for dead/weak trades (>20 min, PnL < ₹150)
                if self.state.active_trade and not (1425 <= t_now <= 1428):
                    t = self.state.active_trade
                    try:
                        entry_dt  = datetime.fromisoformat(t.entry_time)
                        elapsed   = (datetime.now(IST) - entry_dt).total_seconds() / 60.0
                        if elapsed >= 20:
                            opt_ltp  = self._option_ltp.get(t.token, t.entry_price)
                            trade_pnl = (opt_ltp - t.entry_price) * t.lot_size
                            if trade_pnl < 150:
                                logger.info("⏱ Layer9 Time Exit: %.1f min PnL=₹%.0f — dead trade",
                                            elapsed, trade_pnl)
                                await self.tg.send(
                                    f"⏱ *Time Exit (Layer 9)*\n"
                                    f"`{t.symbol}` — {elapsed:.0f} min elapsed\n"
                                    f"P&L: ₹{trade_pnl:+.0f} < ₹150 threshold\n"
                                    f"Dead trade exiting..."
                                )
                                await self._exit_trade(opt_ltp, f"TIME_EXIT_{elapsed:.0f}min")
                    except Exception as te:
                        logger.debug("Time exit check failed: %s", te)

                # Layer 5: Update bank prev prices for correlation direction
                self._bank_prev_ltps.update(self._bank_ltps)

                await self.state.save()
            except Exception as exc:
                logger.error("Maintenance error: %s", exc)
            await asyncio.sleep(30)

    # ── Self-Healing System ───────────────────────────────────────────────────

    async def _health_check_and_heal(self) -> dict:
        """
        6-point health diagnostic + auto-fix.
        Returns a dict with check name → (status_emoji, description, fixed).
        """
        results: dict = {}

        # ── 1. Proxy check ────────────────────────────────────────────────────
        proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")
        if proxy:
            # Ensure ALL_PROXY is also set (may have been cleared)
            if not os.environ.get("ALL_PROXY"):
                os.environ["ALL_PROXY"] = proxy
                results["Proxy"] = ("🔧", f"ALL_PROXY re-set → {proxy.split('@')[-1]}", True)
            else:
                results["Proxy"] = ("✅", f"Static IP active: {proxy.split('@')[-1]}", False)
        else:
            results["Proxy"] = ("⚠️", "HTTP_PROXY not set in Railway vars!", False)

        # ── 2. Telegram polling check ─────────────────────────────────────────
        try:
            me = await self.tg._bot.get_me() if self.tg._bot else None
            if me:
                results["Telegram"] = ("✅", f"Polling OK — @{me.username}", False)
            else:
                results["Telegram"] = ("❌", "Bot object missing", False)
        except Exception as tg_err:
            # Try to recover: delete webhook
            try:
                if self.tg._bot:
                    await self.tg._bot.delete_webhook(drop_pending_updates=True)
                results["Telegram"] = ("🔧", f"Webhook cleared (was: {tg_err})", True)
            except Exception:
                results["Telegram"] = ("❌", str(tg_err)[:80], False)

        # ── 3. AngelOne login / session check ─────────────────────────────────
        if not Cfg.API_KEY:
            results["AngelOne"] = ("ℹ️", "API_KEY not set — Demo mode", False)
        else:
            session_age_h = (time.time() - self.angel._login_time) / 3600
            if session_age_h > 5.5 or not self.angel.auth_token:
                # Re-login
                try:
                    ok = await asyncio.get_running_loop().run_in_executor(
                        None, self.angel.login
                    )
                    if ok:
                        results["AngelOne"] = ("🔧", f"Session refreshed (was {session_age_h:.1f}h old)", True)
                    else:
                        results["AngelOne"] = ("❌", "Re-login failed — check API_KEY/TOTP", False)
                except Exception as ae:
                    results["AngelOne"] = ("❌", str(ae)[:80], False)
            else:
                results["AngelOne"] = ("✅", f"Session valid ({session_age_h:.1f}h old)", False)

        # ── 4. Scrip Master check ─────────────────────────────────────────────
        if len(self._scrip_rows) > 10000:
            results["ScripMaster"] = ("✅", f"{len(self._scrip_rows):,} instruments loaded", False)
        else:
            try:
                self._scrip_rows = await fetch_scrip_master()
                results["ScripMaster"] = ("🔧", f"Reloaded: {len(self._scrip_rows):,} instruments", True)
            except Exception as se:
                results["ScripMaster"] = ("❌", f"Reload failed: {se}", False)

        # ── 5. WebSocket / tick feed check ────────────────────────────────────
        if self._demo_mode:
            results["TickFeed"] = ("ℹ️", "Demo simulation active (no WS needed)", False)
        elif self._ws is not None and self._ws_connected:
            results["TickFeed"] = ("✅", "WebSocket connected", False)
        else:
            results["TickFeed"] = ("⚠️", "WS not connected — will auto-reconnect", False)

        # ── 6. State integrity check ───────────────────────────────────────────
        try:
            await self.state.save()
            results["State"] = ("✅", f"P&L=₹{self.state.daily_pnl:+.0f}  Trades={self.state.trade_count}", False)
        except Exception as ste:
            results["State"] = ("❌", f"Save failed: {ste}", False)

        return results

    async def cmd_fix(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        """Telegram /fix — Full self-diagnostic + auto-heal."""
        await update.message.reply_text(
            "🔍 *Self-Diagnostic Running...*\n"
            "6 components சோதிக்கிறோம், சற்று காத்திருங்கள்...",
            parse_mode="Markdown",
        )

        try:
            results = await self._health_check_and_heal()
        except Exception as e:
            await update.message.reply_text(f"❌ Diagnostic failed: {e}")
            return

        fixed_count = sum(1 for (_, _, fixed) in results.values() if fixed)
        errors      = sum(1 for (emoji, _, _) in results.values() if emoji == "❌")

        lines = ["🛠️ *Self-Heal Report*\n"]
        for name, (emoji, desc, fixed) in results.items():
            tag = " _(fixed)_" if fixed else ""
            lines.append(f"{emoji} *{name}*: {desc}{tag}")

        if fixed_count > 0:
            lines.append(f"\n✅ *{fixed_count} issue(s) auto-fixed!*")
        if errors > 0:
            lines.append(f"\n⚠️ *{errors} issue(s) need manual attention.*")
        if fixed_count == 0 and errors == 0:
            lines.append("\n🟢 *All systems healthy — no action needed!*")

        lines.append(f"\n_Checked at {_now_label()}_")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

    async def _auto_heal_loop(self) -> None:
        """Background loop: silently self-heals every 5 minutes."""
        await asyncio.sleep(300)  # first run after 5 min
        while self._running:
            try:
                results = await self._health_check_and_heal()
                fixed = [(n, d) for n, (e, d, f) in results.items() if f]
                errors = [(n, d) for n, (e, d, f) in results.items() if e == "❌"]
                if fixed:
                    msg = "🔧 *Auto-Heal:* " + ", ".join(f"{n} fixed" for n, _ in fixed)
                    await self.tg.send(msg)
                    logger.info("Auto-heal fixed: %s", [n for n, _ in fixed])
                if errors:
                    msg = "⚠️ *Auto-Heal Alert:* " + ", ".join(f"{n}: {d[:40]}" for n, d in errors)
                    await self.tg.send(msg)
            except Exception as exc:
                logger.warning("Auto-heal loop error: %s", exc)
            await asyncio.sleep(300)  # every 5 minutes

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
            unreal  = round((opt_ltp - t.entry_price) * t.lot_size, 2)
            trade_info = f"`{t.symbol}` Entry=₹{t.entry_price:.2f} SL=₹{t.sl_price:.2f} Unreal=₹{unreal:+.0f}"
        
        mode_str = self._trade_mode_confirmed.upper() if self._trade_armed else "UNARMED"
        
        await update.message.reply_text(
            f"📊 *Bot Status* — `{_now_label()}`\n"
            f"Balance: ₹{bal:,.0f}\n"
            f"Today P&L: ₹{self.state.daily_pnl:+.0f}\n"
            f"Trades today: {self.state.trade_count}\n"
            f"Win Rate: {self.metrics.get_win_rate()}%\n"
            f"Active Trade: {trade_info}\n"
            f"Armed: {'✅' if self._trade_armed else '❌'} ({mode_str})\n"
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
        result = self.strategy.evaluate(spot, ind)
        label  = "✅ *Strong Signal Found!*" if result.signal != "NONE" else "❌ *No Signal Yet*"
        await update.message.reply_text(
            f"{label}\n"
            f"Signal: `{result.signal}` | Strength: {result.strength}%\n"
            f"ST: `{result.supertrend_dir}` | VWAP: {'above' if result.above_vwap else 'below'}\n"
            f"RSI: {result.rsi:.1f} | ADX: {result.adx:.1f} | VolSpike: {'✅' if result.volume_spike else '❌'}\n"
            f"EMA Trend: {'🟢 Bullish' if result.ema_bullish else '🔴 Bearish'} | Pattern: `{result.candle_pattern}`\n"
            f"EMA9: {ind.ema_fast:.1f} | EMA21: {ind.ema_slow:.1f} | ATR: {ind.current_atr:.1f}\n"
            f"_{result.reason}_",
            parse_mode="Markdown",
        )

    async def _get_armed_status_reason(self) -> str:
        spot = self._spot_ltp.get(Cfg.UNDERLYING, 0)
        token = SPOT_TOKEN.get(Cfg.UNDERLYING, "")
        ind = self.indicators.get(token)
        if not ind or spot <= 0 or ind.num_candles < 5:
            return "\n⏳ *Warming Up:* போதிய மார்க்கெட் டேட்டா இன்னும் கிடைக்கவில்லை. டேட்டா சேர்ந்ததும் நான் தேடத் தொடங்குவேன்..."
        
        result = self.strategy.evaluate(spot, ind)
        if result.signal == "NONE":
            return f"\n🔍 *Current Block:* {result.reason}\n\n🦅 கண்கொத்திப் பாம்பாக மார்க்கெட்டை கவனித்து வருகிறேன்! சாதகமான சூழல் வந்தவுடன் உடனடியாக Trade எடுக்கப்படும்."
        else:
            return f"\n🔥 *{result.signal} Signal Active!* (Strength: {result.strength}%)\nஉடனடியாக Trade எடுக்கப்படுகிறது!"

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
            reason_msg = await self._get_armed_status_reason()
            await q.edit_message_text(f"✅ *Paper trading armed.*\n{reason_msg}", parse_mode="Markdown")
        elif q.data == "confirm_live":
            if not Cfg.API_KEY:
                await q.edit_message_text("❌ API_KEY not configured.", parse_mode="Markdown")
                return
            self._trade_mode_confirmed = "live"
            self._trade_armed = True
            reason_msg = await self._get_armed_status_reason()
            await q.edit_message_text(f"🚨 *LIVE trading armed.*\n{reason_msg}", parse_mode="Markdown")
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
            f"🤖 Ultra-Fast Algo Bot (`{Cfg.BOT_VERSION}`) இயங்குகிறது!\n\n"
            f"Mode: `{self._trade_mode_confirmed.upper()}`\n"
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
            "/fix — 🛠️ Self-diagnostic + auto-heal (6 checks)\n"
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
    if len(token) < 10 or ":" not in token:
        raise ValueError("Invalid Telegram token format")

    logger.info("✅ Telegram token found (len=%d) — building app...", len(token))
    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start",         engine.cmd_start))
    app.add_handler(CommandHandler("status",        engine.cmd_status))
    app.add_handler(CommandHandler("checkstrategy", engine.cmd_checkstrategy))
    app.add_handler(CommandHandler("starttrade",    engine.cmd_starttrade))
    app.add_handler(CommandHandler("stop",          engine.cmd_stop))
    app.add_handler(CommandHandler("exitnow",       engine.cmd_exitnow))
    app.add_handler(CommandHandler("fix",           engine.cmd_fix))
    app.add_handler(CommandHandler("setvix",        engine.cmd_setvix))
    app.add_handler(CommandHandler("setad",         engine.cmd_setad))
    app.add_handler(CommandHandler("help",          engine.cmd_help))
    app.add_handler(CallbackQueryHandler(engine.callback_handler))
    return app


async def _async_main() -> None:
    # === EMERGENCY DEBUG BLOCK ===
    import sys
    import os
    logger.info("=" * 70)
    logger.info("🚨 EMERGENCY DEBUG: Bot starting at %s", datetime.now(IST))
    logger.info(f"🚨 Python: {sys.version}")
    logger.info(f"🚨 Platform: {sys.platform}")
    logger.info(f"🚨 CWD: {os.getcwd()}")
    logger.info(f"🚨 Files: {os.listdir('.')[:10]}")
    logger.info(f"🚨 Env keys: {[k for k in os.environ.keys() if not k.startswith('_')]}")
    logger.info(f"🚨 TELEGRAM_TOKEN exists: {bool(Cfg.TELEGRAM_TOKEN)}")
    logger.info(f"🚨 TELEGRAM_CHAT_ID: {Cfg.TELEGRAM_CHAT_ID}")
    logger.info("=" * 70)
    # === END DEBUG ===

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

        # Railway redeploy-ல் பழைய container சில seconds வரை run ஆகும்.
        # deleteWebhook + சிறு delay → Conflict error தவிர்க்கும்.
        try:
            await tg_app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("🔄 Webhook deleted — waiting 3s for old instance to exit...")
        except Exception as _e:
            logger.warning("delete_webhook failed (harmless): %s", _e)
        await asyncio.sleep(3)  # old container-க்கு exit time கொடு

        await tg_app.start()
        await tg_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            error_callback=lambda exc: logger.warning("Polling error (auto-retry): %s", exc),
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
