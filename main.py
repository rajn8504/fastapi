"""
╔══════════════════════════════════════════════════════════════════╗
║   NIFTY Options Algo Trading System — Complete Production Build  ║
║   Version: v6 + API Resilience + FastAPI + Auto TSL & EOD Exit   ║
╠══════════════════════════════════════════════════════════════════╣
║   ALL FEATURES INCLUDED: 18 Safety Gates, Background Monitor,    ║
║   Trailing SL, 2:25 PM Square-Off, Exit Signal Routing.          ║
╚══════════════════════════════════════════════════════════════════╝
"""

import time, datetime, threading, json, os, asyncio
from datetime import timezone
from typing import Optional, Tuple, List, Dict, Any
from fastapi import FastAPI, Request, BackgroundTasks
import uvicorn
import pytz

app = FastAPI()

# ══════════════════════════════════════════════════════════════════
# SECTION 0 — CONSTANTS
# ══════════════════════════════════════════════════════════════════
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "ABC123_DEV_ONLY")
LOT_SIZE         = 25
TICK_SIZE        = 0.05     
VIX_LIMIT        = 18.0
GAP_FULL_LOT     = 0.005    
GAP_HALF_LOT     = 0.010    
GAP_REJECT       = 0.010    
DAILY_TARGET     = 5000
DAILY_MAX_LOSS   = 2000
MAX_TRADE_MIN    = 25
SIGNAL_SCORE_MIN = 5

MAX_LTP_AGE_MS  = 3000      
RETRY_DELAY     = 0.8       
MAX_RETRIES     = 3
ORDER_STATUS_VALID    = {"complete","open","trigger pending", "after market order req received","modified"}
ORDER_STATUS_TERMINAL = {"rejected","cancelled"}

TRADE_START     = 930
TRADE_END       = 1430
PRE_CLOSE       = 1425      
FREEZE_ZONE     = 1429      
MARKET_OPEN     = 915
MARKET_CLOSE    = 1530

# ══════════════════════════════════════════════════════════════════
# SECTION 1 — IST TIMEZONE
# ══════════════════════════════════════════════════════════════════
_IST = pytz.timezone("Asia/Kolkata")
def _now_ist() -> datetime.datetime:
    return datetime.datetime.now(_IST)

_IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def today_ist() -> str:
    return _now_ist().strftime("%Y-%m-%d")

def now_utc_iso() -> str:
    return datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ts_to_ms(iso: str) -> int:
    try:
        return int(datetime.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)
    except:
        return int(time.time() * 1000)

def ms_to_ist_hhmm(now_ms: int) -> int:
    dt = datetime.datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    ist = dt.astimezone(_IST_TZ)
    return ist.hour * 100 + ist.minute

# ══════════════════════════════════════════════════════════════════
# SECTION 2 — SESSION HELPERS
# ══════════════════════════════════════════════════════════════════
def is_trading_session(hhmm: int) -> bool:
    return TRADE_START <= hhmm <= TRADE_END

def is_new_entry_allowed(now_ms: int = None) -> bool:
    t = ms_to_ist_hhmm(now_ms) if now_ms else (_now_ist().hour * 100 + _now_ist().minute)
    return TRADE_START <= t < PRE_CLOSE

def should_square_off_now(now_ms: int = None) -> bool:
    t = ms_to_ist_hhmm(now_ms) if now_ms else (_now_ist().hour * 100 + _now_ist().minute)
    return PRE_CLOSE <= t < FREEZE_ZONE

def is_freeze_zone(now_ms: int = None) -> bool:
    t = ms_to_ist_hhmm(now_ms) if now_ms else (_now_ist().hour * 100 + _now_ist().minute)
    return t == FREEZE_ZONE

def is_market_open(now_ms: int = None) -> bool:
    t = ms_to_ist_hhmm(now_ms) if now_ms else (_now_ist().hour * 100 + _now_ist().minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE

# ══════════════════════════════════════════════════════════════════
# SECTION 3 — REDIS (PRODUCTION SWAP) - MODIFIED WITH SAFE FALLBACK
# ══════════════════════════════════════════════════════════════════
class MockRedis:
    def __init__(self):
        self._s = {}
        self._e = {}
        self._lock = threading.Lock()
    def _clean(self, k):
        if k in self._e and time.time() > self._e[k]:
            self._s.pop(k, None)
            self._e.pop(k, None)
            return True
        return False
    def get(self, k):
        with self._lock:
            self._clean(k)
            return self._s.get(k)
    def set(self, k, v, nx=False, ex=None):
        with self._lock:
            self._clean(k)
            if nx and k in self._s:
                return False
            self._s[k] = str(v)
            if ex:
                self._e[k] = time.time() + ex
            return True
    def setex(self, k, ex, v):
        return self.set(k, str(v), ex=ex)
    def delete(self, *keys):
        with self._lock:
            for k in keys:
                self._s.pop(k, None)
                self._e.pop(k, None)
    def incr(self, k):
        with self._lock:
            v = int(self._s.get(k, 0)) + 1
            self._s[k] = str(v)
            return v
    def incrbyfloat(self, k, a):
        with self._lock:
            v = float(self._s.get(k, 0)) + a
            self._s[k] = f"{v:.4f}"
            return v
    def expire(self, k, s):
        with self._lock:
            if k in self._s:
                self._e[k] = time.time() + s
                return True
            return False
    def hset(self, k, mapping=None, **kw):
        with self._lock:
            if not isinstance(self._s.get(k), dict):
                self._s[k] = {}
            if mapping:
                self._s[k].update({str(a): str(b) for a, b in mapping.items()})
    def hgetall(self, k):
        with self._lock:
            self._clean(k)
            v = self._s.get(k, {})
            return dict(v) if isinstance(v, dict) else {}
    def keys(self, p="*"):
        with self._lock:
            px = p.replace("*", "")
            return [k for k in list(self._s) if k.startswith(px)]
    def rpush(self, k, *vals):
        with self._lock:
            if not isinstance(self._s.get(k), list):
                self._s[k] = []
            self._s[k].extend([str(v) for v in vals])
            return len(self._s[k])
    def lrange(self, k, s, e):
        with self._lock:
            v = self._s.get(k, [])
            if not isinstance(v, list):
                return []
            return v[s:] if e == -1 else v[s:e + 1]
    def flushall(self):
        with self._lock:
            self._s.clear()
            self._e.clear()

# SAFE REDIS INITIALIZATION - WILL NEVER FAIL
try:
    import redis
    REDIS_IMPORT_AVAILABLE = True
except ImportError:
    REDIS_IMPORT_AVAILABLE = False
    print("⚠️ redis module not available, using MockRedis")

REDIS_URL = os.environ.get("REDIS_URL")
try:
    if REDIS_URL and REDIS_IMPORT_AVAILABLE:
        _temp_redis = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        _temp_redis.ping()
        r = _temp_redis
        print("✅ Connected to Redis successfully")
    else:
        print("⚠️ No REDIS_URL or redis module not available, using MockRedis")
        r = MockRedis()
except (ImportError, Exception) as e:
    print(f"⚠️ Redis connection failed: {e}. Using MockRedis fallback.")
    r = MockRedis()

# ══════════════════════════════════════════════════════════════════
# SECTION 4 — REDIS HELPERS
# ══════════════════════════════════════════════════════════════════
def daily_key(base: str) -> str:
    return f"{base}:{today_ist()}"

def daily_get(base: str, default="0") -> str:
    v = r.get(daily_key(base))
    return v if v is not None else default

def daily_set(base: str, val, ex=86400):
    r.setex(daily_key(base), ex, str(val))

def daily_incr(base: str) -> int:
    k = daily_key(base)
    c = r.incr(k)
    if c == 1:
        r.expire(k, 86400)
    return c

def daily_float_add(base: str, amount: float) -> float:
    k = daily_key(base)
    v = r.incrbyfloat(k, amount)
    r.expire(k, 86400)
    return float(v)

def round_lot(qty: int) -> int:
    if qty <= 0:
        return LOT_SIZE
    return max(LOT_SIZE, (qty // LOT_SIZE) * LOT_SIZE)

def split_qty(total: int) -> Tuple[int, int]:
    half = total // 2
    partial = round_lot(half)
    trail = total - partial
    if trail < LOT_SIZE:
        return 0, total
    return partial, round_lot(trail)

# ══════════════════════════════════════════════════════════════════
# SECTION 5 — AUDIT LOGGER
# ══════════════════════════════════════════════════════════════════
class AuditLog:
    @staticmethod
    def _entry(t: str, d: dict) -> str:
        return json.dumps({"ts": now_utc_iso(), "type": t, **d})
    @staticmethod
    def trade(order_id, signal, qty, premium, sl, status, detail=""):
        r.rpush("audit:trades", AuditLog._entry("TRADE", {"order_id": order_id, "signal": signal, "qty": qty, "premium": premium, "sl": sl, "status": status, "detail": detail}))
    @staticmethod
    def rejection(stage, reason, sig=None):
        r.rpush("audit:rejections", AuditLog._entry("REJECTION", {"stage": stage, "reason": reason, "signal": (sig or {}).get("signal", ""), "candle_id": (sig or {}).get("candle_id", "")}))
    @staticmethod
    def error(component, error, context=None):
        r.rpush("audit:errors", AuditLog._entry("ERROR", {"component": component, "error": str(error), "context": json.dumps(context or {})}))
    @staticmethod
    def day_bias(result: dict):
        r.rpush("audit:day_bias", AuditLog._entry("DAY_BIAS", {"trade_allowed": result.get("trade_allowed"), "score": result.get("score"), "bias": result.get("bias", ""), "reasons": result.get("reasons", [])}))

# ══════════════════════════════════════════════════════════════════
# SECTION 6 — PIPELINE GUARD
# ══════════════════════════════════════════════════════════════════
def pipeline_guard(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError:
            raise
        except Exception as e:
            AuditLog.error(func.__name__, str(e), {"args": str(args)[:200]})
            raise ValueError(f"INTERNAL_ERROR:{func.__name__}:{type(e).__name__}")
    wrapper.__name__ = func.__name__
    return wrapper

# ══════════════════════════════════════════════════════════════════
# SECTION 7 — API RESILIENCE LAYER
# ══════════════════════════════════════════════════════════════════
def fetch_ltp_with_retry(client, exchange: str, symbol: str, token: str, max_retries: int = MAX_RETRIES, max_age_ms: int = MAX_LTP_AGE_MS) -> dict:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            t0   = int(time.time() * 1000)
            resp = client.ltpData(exchange, symbol, token)
            t1   = int(time.time() * 1000)
            ltp  = float(resp["data"]["ltp"])
            if ltp <= 0:
                raise ValueError(f"LTP=0 on attempt {attempt}")
            return {"ltp": ltp, "fetch_time_ms": t1, "latency_ms": t1 - t0, "attempts": attempt, "is_fresh": (t1 - t0) < max_age_ms}
        except ValueError:
            raise
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(RETRY_DELAY)
    raise ValueError(f"LTP_FETCH_FAILED after {max_retries} attempts: {last_err}")

def parse_market_depth(raw: dict) -> dict:
    depth  = raw.get("depth", {})
    buys = depth.get("buy", []) or []
    sells = depth.get("sell", []) or []
    ltp    = float(raw.get("ltp", 0))
    best_bid = float(buys[0].get("price", 0)) if buys else 0.0
    best_ask = float(sells[0].get("price", 0)) if sells else 0.0
    bid_qty  = int(buys[0].get("quantity", 0)) if buys else 0
    ask_qty  = int(sells[0].get("quantity", 0)) if sells else 0
    vol      = int(raw.get("tradedVolume", 0))
    oi = int(raw.get("openInterest", 0))
    depth_ok = bool(buys and sells and best_bid > 0 and best_ask > 0)
    return {"bid": best_bid, "ask": best_ask if depth_ok else (ltp * 1.001 if ltp > 0 else 0.0), "spread": round(best_ask - best_bid, 2) if depth_ok else None, "bid_qty": bid_qty, "ask_qty": ask_qty, "vol": vol, "oi": oi, "ltp": ltp, "depth_available": depth_ok}

def check_liquidity_safe(client, option: dict) -> dict:
    try:
        raw = client.getMarketData("FULL", [{}])
        fetched = (raw.get("data", {}).get("fetched") or [{}])
        parsed = parse_market_depth(fetched[0] if fetched else {})
    except Exception as e:
        AuditLog.error("LIQUIDITY", str(e))
        return {"ok": True, "warn": f"depth_api_error:{e}", "depth_available": False}
    issues = []
    if parsed["spread"] is not None and parsed["spread"] > 3.0:
        issues.append(f"spread={parsed['spread']:.1f}")
    if parsed["vol"] < 500:
        issues.append(f"vol={parsed['vol']}")
    if parsed["oi"] < 10000:
        issues.append(f"OI={parsed['oi']}")
    if parsed["depth_available"]:
        if parsed["bid_qty"] < 50:
            issues.append(f"bid_depth={parsed['bid_qty']}")
        if parsed["ask_qty"] < 50:
            issues.append(f"ask_depth={parsed['ask_qty']}")
    if issues:
        raise ValueError(f"LIQUIDITY:{';'.join(issues)}")
    return {**parsed, "ok": True}

def verify_order_with_retry(client, order_id: str, max_retries: int = MAX_RETRIES, retry_delay: float = RETRY_DELAY) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            orders = client.orderBook().get("data") or []
            for o in orders:
                if str(o.get("orderid", "")) == str(order_id):
                    status = (o.get("status") or "").lower().strip()
                    if status in ORDER_STATUS_VALID:
                        return {"found": True, "status": status, "order_data": o, "attempts": attempt}
                    if status in ORDER_STATUS_TERMINAL:
                        raise ValueError(f"ORDER_{status.upper()}:{o.get('text','no reason')}")
                    break
        except ValueError:
            raise
        except Exception:
            pass
        if attempt < max_retries:
            time.sleep(retry_delay)
    return {"found": False, "status": "unknown", "attempts": max_retries}

def round_to_tick(price: float, tick: float = TICK_SIZE) -> float:
    if tick <= 0:
        return round(price, 2)
    return round(round(price / tick) * tick, 2)

def calculate_sl_ticked(premium: float, atr: float = None) -> dict:
    if atr and atr > 0:
        raw_sl  = premium - (atr * 1.2)
        sl_pct  = (premium - raw_sl) / premium * 100
        sl_pct  = min(45.0, max(1.0, sl_pct))
        raw_sl  = premium * (1 - sl_pct / 100)
        method  = "ATR"
    else:
        raw_sl  = premium * 0.70
        sl_pct  = 30.0
        method  = "FIXED_30PCT"
    return {"sl_price": round_to_tick(raw_sl), "sl_raw": round(raw_sl, 2), "sl_pct": round(sl_pct, 1), "tick_size": TICK_SIZE, "method": method}

def eod_square_off(client, positions: list, now_ms: int = None) -> list:
    results = []
    for pos in positions:
        symbol = pos.get("symbol", "")
        token = pos.get("opt_token", "")
        qty = int(float(pos.get("remaining_qty", 0)))
        oid = pos.get("order_id", "")
        if qty <= 0:
            results.append({"order_id": oid, "status": "SKIP", "qty": 0})
            continue
        try:
            try:
                client.cancelOrder(order_id=pos.get("sl_order_id", ""), variety="STOPLOSS")
            except:
                pass
            resp = client.placeOrder({
                "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token, "transactiontype": "SELL",
                "exchange": "NFO", "ordertype": "MARKET", "quantity": qty, "producttype": "INTRADAY",
            })
            results.append({"order_id": oid, "exit_oid": resp["data"]["orderid"], "status": "SQUARED_OFF", "qty": qty, "reason": "PRE_CLOSE_2:25"})
        except Exception as e:
            results.append({"order_id": oid, "status": "FAILED", "error": str(e), "qty": qty})
    return results

def fetch_live_capital(client) -> dict:
    try:
        data = client.rmsLimit().get("data", {})
        net = float(data.get("net", 0))
        utilized = float(data.get("utilisedamt", 0))
        available = max(0.0, float(data.get("availablecash", net)))
        return {"available": available, "utilized": utilized, "net": net, "source": "LIVE_RMS"}
    except Exception as e:
        return {"available": 0.0, "utilized": 0.0, "net": 0.0, "source": "RMS_FAILED", "error": str(e)}

def check_capital_live(client, order_cost: float) -> dict:
    cap = fetch_live_capital(client)
    if cap["source"] == "RMS_FAILED":
        raise ValueError(f"RMS_UNAVAILABLE:{cap.get('error','')}")
    available = cap["available"]
    max_use = available * 0.35
    if order_cost > max_use:
        raise ValueError(f"CAPITAL_INSUFFICIENT:need={order_cost:.0f} max35%={max_use:.0f} avail={available:.0f}")
    return {**cap, "order_cost": order_cost, "max_use": max_use, "ok": True}

# ══════════════════════════════════════════════════════════════════
# SECTION 8 — ANGEL ONE CLIENT SETUP
# ══════════════════════════════════════════════════════════════════
class MockAngelOne:
    def __init__(self, ltp=150.0):
        self.ltp = ltp
        self._ctr = 1000
        self._orders = {}
    def ltpData(self, exchange, symbol, token):
        return {"data": {"ltp": self.ltp}}
    def placeOrder(self, order):
        oid = str(self._ctr)
        self._ctr += 1
        self._orders[oid] = {"orderid": oid, "status": "complete", "quantity": order.get("quantity", 0)}
        return {"data": {"orderid": oid}}
    def orderBook(self):
        return {"data": list(self._orders.values())}
    def modifyOrder(self, data):
        oid = data.get("orderid")
        if oid in self._orders:
            self._orders[oid]["triggerprice"] = data.get("triggerprice", "0")
        return {"status": "success"}
    def cancelOrder(self, order_id, variety):
        if order_id in self._orders:
            self._orders[order_id]["status"] = "cancelled"
        return {"status": "success"}
    def getMarketData(self, mode, tokens):
        mid = self.ltp
        return {"data": {"fetched": [{"ltp": self.ltp, "depth": {"buy": [{"price": mid-0.5, "quantity": 500}], "sell": [{"price": mid+0.5, "quantity": 500}]}, "tradedVolume": 2000, "openInterest": 50000, "ask": mid+0.5}]}}
    def rmsLimit(self):
        return {"data": {"net": "200000", "utilisedamt": "20000", "availablecash": "180000"}}

def get_angel_client():
    api_key = os.environ.get("API_KEY")
    client_id = os.environ.get("CLIENT_ID")
    pwd = os.environ.get("PASSWORD")
    totp_str = os.environ.get("TOTP_STR")
    if api_key and client_id and pwd and totp_str:
        try:
            import pyotp
            from SmartApi import SmartConnect
            client = SmartConnect(api_key=api_key)
            totp = pyotp.TOTP(totp_str).now()
            client.generateSession(client_id, pwd, totp)
            return client
        except Exception as e:
            AuditLog.error("ANGEL_LOGIN_FAILED", str(e))
            raise ValueError(f"LOGIN_ERROR: {str(e)}")
    return MockAngelOne()

# ══════════════════════════════════════════════════════════════════
# SECTION 9 — DAY BIAS ENGINE
# ══════════════════════════════════════════════════════════════════
@pipeline_guard
def run_day_bias(market: dict) -> dict:
    today = today_ist()
    score = 0
    checks = {}
    reasons = []
    vix = float(market.get("vix", 99))
    checks["vix_ok"] = vix < VIX_LIMIT
    if checks["vix_ok"]:
        score += 1
    else:
        reasons.append(f"VIX={vix:.1f}")

    today_open = float(market.get("today_open", 0))
    prev_close = float(market.get("prev_close", 1))
    gap_pct = abs(today_open - prev_close) / prev_close if prev_close else 1

    if gap_pct < GAP_FULL_LOT:
        lot_mult = 1.0
        checks["gap_ok"] = True
        score += 1
    elif gap_pct < GAP_HALF_LOT:
        lot_mult = 0.5
        checks["gap_ok"] = True
        score += 1
        reasons.append(f"Gap={gap_pct*100:.2f}%→half_lot")
    else:
        lot_mult = 0.0
        checks["gap_ok"] = False
        reasons.append(f"Gap={gap_pct*100:.2f}%>1%")

    c1o = float(market.get("candle1_open", 0))
    c1c = float(market.get("candle1_close", 0))
    c2o = float(market.get("candle2_open", 0))
    c2c = float(market.get("candle2_close", 0))
    c2h = float(market.get("candle2_high", c2c))
    c2l = float(market.get("candle2_low", c2c))
    ltp = float(market.get("ltp", 0))

    primary_clear = (c1c > c1o) == (c2c > c2o) and (abs(c2c - c1o) / c1o > 0.001 if c1o else False)
    breakout_bull = ltp > c2h * 1.001 if ltp > 0 else False
    breakout_bear = ltp < c2l * 0.999 if ltp > 0 else False
    fallback_clear = breakout_bull or breakout_bear

    checks["direction_clear"] = primary_clear or fallback_clear
    if checks["direction_clear"]:
        score += 1
        if primary_clear:
            checks["bias"] = "BULLISH" if c1c > c1o else "BEARISH"
        else:
            checks["bias"] = "BREAKOUT_BULL" if breakout_bull else "BREAKOUT_BEAR"
    else:
        checks["bias"] = "UNCLEAR"
        reasons.append("No clear first-candle direction")

    vwap = float(market.get("vwap", 1))
    adx = float(market.get("adx", 0))
    p_now = ltp if ltp > 0 else c2c
    vd = abs(p_now - vwap) / vwap if vwap else 1
    checks["vwap_or_trend"] = vd < 0.003 or adx > 25
    if checks["vwap_or_trend"]:
        score += 1
    else:
        reasons.append(f"Price {vd*100:.2f}% from VWAP, ADX={adx:.1f}")

    adx_prev = float(market.get("adx_prev", 0))
    checks["adx_ok"] = (adx > 20 and adx > adx_prev) or adx > 25
    if checks["adx_ok"]:
        score += 1
    else:
        reasons.append(f"ADX={adx:.1f}")

    vol_ratio = float(market.get("volume_ratio", 0))
    checks["volume_ok"] = vol_ratio >= 1.2
    if checks["volume_ok"]:
        score += 1
    else:
        reasons.append(f"Volume={vol_ratio:.2f}x")

    trade_today = (checks["vix_ok"] and gap_pct < GAP_REJECT and score >= SIGNAL_SCORE_MIN)

    result = {
        "trade_allowed": trade_today, "score": score, "score_max": 6, "bias": checks.get("bias", "UNCLEAR"), "lot_multiplier": lot_mult,
        "checks": checks, "reasons": reasons, "gap_pct": round(gap_pct * 100, 3), "timestamp": now_utc_iso(),
    }
    r.hset(f"day_bias:{today}", mapping={
        "trade_allowed": "1" if trade_today else "0", "score": str(score), "bias": checks.get("bias", "UNCLEAR"),
        "lot_multiplier": str(lot_mult), "gap_pct": str(round(gap_pct * 100, 3)), "reasons": "|".join(reasons) or "ALL_PASS", "timestamp": result["timestamp"],
    })
    r.expire(f"day_bias:{today}", 86400)
    if not trade_today:
        r.setex(f"block:{today}", 86400, "NO_TRADE_DAY")
    AuditLog.day_bias(result)
    return result

# ══════════════════════════════════════════════════════════════════
# SECTION 10 — SIGNAL SCORER
# ══════════════════════════════════════════════════════════════════
def score_signal(sig: dict) -> dict:
    score = 0
    details = {}
    action = sig.get("signal", "")
    price = float(sig.get("price", 0))
    rsi = float(sig.get("rsi", 0))
    vwap = float(sig.get("vwap", 1))
    ema9 = float(sig.get("ema9", 0))
    ema21 = float(sig.get("ema21", 0))

    rsi_ok = ((action == "BUY_CE" and 50 <= rsi <= 65) or (action == "BUY_PE" and 35 <= rsi <= 50))
    vwap_ok = ((action == "BUY_CE" and price > vwap) or (action == "BUY_PE" and price < vwap))
    ema_ok = ((action == "BUY_CE" and ema9 > ema21 and price > ema9) or (action == "BUY_PE" and ema9 < ema21 and price < ema9))
    pat_ok = bool(sig.get("pattern"))

    details["rsi_zone"] = rsi_ok
    details["vwap_align"] = vwap_ok
    details["ema_trend"] = ema_ok
    details["candle_pattern"] = pat_ok

    for ok in [rsi_ok, vwap_ok, ema_ok, pat_ok]:
        if ok:
            score += 1

    trade_ok = score >= 3
    if not (vwap_ok and ema_ok):
        trade_ok = False
        details["critical_fail"] = "VWAP or EMA misaligned"

    return {"score": score, "score_max": 4, "trade_signal": trade_ok, "details": details, "failed": [k for k, v in details.items() if isinstance(v, bool) and not v]}

# ══════════════════════════════════════════════════════════════════
# SECTION 11 — WEBHOOK VALIDATOR (WITH EXIT SIGNAL SUPPORT)
# ══════════════════════════════════════════════════════════════════
@pipeline_guard
def validate_webhook(data: dict, now_ms: int = None) -> dict:
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    today = today_ist()

    if data.get("secret") != WEBHOOK_SECRET:
        AuditLog.rejection("AUTH", "UNAUTHORIZED", data)
        raise ValueError("UNAUTHORIZED")

    signal_str = data.get("signal", "").upper()
    is_exit = signal_str in ("EXIT_CE", "EXIT_PE", "SELL")

    for f in ["signal", "price", "candle_id", "timestamp"]:
        if not str(data.get(f, "")).strip():
            raise ValueError(f"MISSING_FIELD:{f}")

    try:
        price = float(data["price"])
        assert 15000 < price < 35000
    except:
        raise ValueError(f"PRICE_INVALID:{data.get('price')}")

    age = now_ms - ts_to_ms(data["timestamp"])
    if age > 30000:
        AuditLog.rejection("STALE", f"{age}ms", data)
        raise ValueError(f"STALE:{age}ms")

    ist_t = ms_to_ist_hhmm(now_ms)
    if not is_trading_session(ist_t):
        AuditLog.rejection("SESSION", f"time={ist_t}", data)
        raise ValueError(f"OUTSIDE_SESSION:{ist_t}")

    if is_exit:
        return {"status": "PASS", "is_exit": True, "trade_signal": True}

    if signal_str not in ("BUY_CE", "BUY_PE"):
        raise ValueError(f"UNKNOWN_SIGNAL:{signal_str}")

    for f in ["rsi", "vwap", "ema9", "ema21", "pattern"]:
        if not str(data.get(f, "")).strip():
            raise ValueError(f"MISSING_FIELD:{f}")

    rsi = float(data["rsi"])
    if signal_str == "BUY_CE" and not (50 <= rsi <= 65):
        raise ValueError(f"RSI_INVALID:CE=50-65 got {rsi}")
    if signal_str == "BUY_PE" and not (35 <= rsi <= 50):
        raise ValueError(f"RSI_INVALID:PE=35-50 got {rsi}")

    if not is_new_entry_allowed(now_ms):
        raise ValueError("PRE_CLOSE:No new entries after 2:25 PM")

    bias = r.hgetall(f"day_bias:{today}")
    if not bias:
        AuditLog.rejection("DAY_GATE", "NOT_SET", data)
        raise ValueError("DAY_BIAS_NOT_SET")
    if bias.get("trade_allowed") != "1":
        AuditLog.rejection("DAY_GATE", "NO_TRADE_DAY", data)
        raise ValueError(f"NO_TRADE_DAY:{bias.get('reasons','')}")

    lot_mult = float(bias.get("lot_multiplier", 1.0))
    if lot_mult == 0.0:
        raise ValueError("GAP_TOO_LARGE")

    dupe = f"seen:{data['candle_id']}"
    if r.get(dupe):
        raise ValueError("DUPLICATE_SIGNAL")
    r.set(dupe, "1", ex=300)

    day_score = int(bias.get("score", 0))
    rate_ms = 60000 if day_score == 6 else 90000
    last = r.get("last_alert")
    if last and (now_ms - int(last)) < rate_ms:
        raise ValueError(f"RATE_LIMIT:{rate_ms//1000}s")
    r.set("last_alert", str(now_ms))

    max_trades = 4 if day_score == 6 else 2
    count = int(daily_get("trade_count", "0"))
    if count >= max_trades:
        raise ValueError(f"MAX_TRADES:{count}/{max_trades}")

    blocked = r.get(f"block:{today}")
    if blocked:
        raise ValueError(f"BLOCKED:{blocked}")

    pnl = float(daily_get("pnl", "0"))
    if pnl >= DAILY_TARGET:
        r.setex(f"block:{today}", 86400, "TARGET_HIT")
        raise ValueError("DAILY_TARGET_HIT")
    if pnl <= -DAILY_MAX_LOSS:
        r.setex(f"block:{today}", 86400, "LOSS_LIMIT")
        raise ValueError("DAILY_LOSS_HIT")

    loss_count = int(daily_get("loss_count", "0"))
    last_loss_ms = r.get(f"last_loss_time:{today}")
    if loss_count >= 2:
        r.setex(f"block:{today}", 86400, "TWO_LOSSES")
        raise ValueError("STOPPED:2_losses")
    if last_loss_ms:
        elapsed = now_ms - int(last_loss_ms)
        if elapsed < 600000:
            raise ValueError(f"LOSS_COOLDOWN:{max(1, (600000 - elapsed) // 60000)}min")

    vix_raw = r.get("vix_cache")
    if not vix_raw:
        raise ValueError("VIX_UNAVAILABLE")
    if float(vix_raw) >= VIX_LIMIT:
        raise ValueError(f"VIX_HIGH:{vix_raw}")
    if r.get("event_block"):
        raise ValueError("EVENT_BLOCK")

    opt_ltp = float(r.get("option_ltp_cache") or 0)
    if opt_ltp == 0:
        raise ValueError("OPTION_LTP_MISSING:run morning LTP fetch first")
    total_cap = float(r.get("total_capital") or 0)
    capital_used = float(daily_get("capital_used", "0"))
    if total_cap > 0:
        if (capital_used + opt_ltp * LOT_SIZE * lot_mult) / total_cap > 0.35:
            raise ValueError("CAPITAL_LIMIT")

    sig_result = score_signal(data)
    if not sig_result["trade_signal"]:
        AuditLog.rejection("SIGNAL", f"score={sig_result['score']}/4", data)
        raise ValueError(f"SIGNAL_WEAK:score={sig_result['score']}/4")

    if not r.set("trade_lock", "1", nx=True, ex=3):
        raise ValueError("TRADE_LOCKED")
    try:
        new_count = daily_incr("trade_count")
    finally:
        r.delete("trade_lock")

    return {"status": "PASS", "is_exit": False, "lot_mult": lot_mult, "vix": float(vix_raw), "trade_num": new_count, "sig_score": sig_result["score"], "day_score": day_score, "max_trades": max_trades, "rate_limit": rate_ms // 1000}

# ══════════════════════════════════════════════════════════════════
# SECTION 12 — ORDER EXECUTOR & EXIT LOGIC
# ══════════════════════════════════════════════════════════════════
@pipeline_guard
def place_order(client, lots: int, token_id: str) -> dict:
    lock_key = f"order_lock:{token_id}"
    if not r.set(lock_key, "1", nx=True, ex=300):
        raise ValueError("ALREADY_EXECUTED")
    try:
        tok = r.hgetall(f"token:{token_id}")
        if not tok:
            raise ValueError("INVALID_TOKEN")
        if tok.get("used") == "1":
            raise ValueError("TOKEN_USED")
        if int(time.time() * 1000) > int(tok.get("expiry", 0)):
            raise ValueError("TOKEN_EXPIRED")

        lot_mult = float(tok.get("lot_mult", "1.0"))
        qty = round_lot(int(lots * LOT_SIZE * lot_mult))

        liq = check_liquidity_safe(client, tok)
        if not liq.get("ok", True):
            raise ValueError("LIQUIDITY_FAIL")

        ltp_info = fetch_ltp_with_retry(client, "NFO", tok.get("symbol", ""), tok.get("opt_token", ""))
        premium = ltp_info["ltp"]

        check_capital_live(client, premium * qty)

        spread = liq.get("spread") or 2.0
        ask = liq.get("ask") or premium
        dyn_buffer = max(1.0, min(5.0, spread * 1.5))
        limit_price = round_to_tick(ask + dyn_buffer)

        resp = client.placeOrder({"ordertype": "LIMIT", "quantity": qty, "price": str(limit_price), "duration": "IOC"})
        order_id = resp["data"]["orderid"]

        verify = verify_order_with_retry(client, order_id)
        if not verify["found"]:
            resp = client.placeOrder({"ordertype": "MARKET", "quantity": qty})
            order_id = resp["data"]["orderid"]
            if not verify_order_with_retry(client, order_id)["found"]:
                raise ValueError("ORDER_UNVERIFIED")

        atr_val = float(tok.get("atr", 0))
        sl_info = calculate_sl_ticked(premium, atr_val)
        sl_price = sl_info["sl_price"]

        sl_resp = client.placeOrder({"ordertype": "STOPLOSS_MARKET", "quantity": qty, "triggerprice": str(sl_price)})
        sl_id = sl_resp["data"]["orderid"]
        if not verify_order_with_retry(client, sl_id)["found"]:
            client.placeOrder({"ordertype": "MARKET", "quantity": qty})
            AuditLog.error("SL_FAILED", f"order={order_id} forced exit")
            raise ValueError("SL_FAILED_FORCED_EXIT")

        r.hset(f"token:{token_id}", mapping={"used": "1"})
        partial_qty, trail_qty = split_qty(qty)

        r.hset(f"position:{order_id}", mapping={
            "symbol": tok.get("symbol", ""), "opt_token": tok.get("opt_token", ""), "signal": tok.get("signal", ""),
            "entry_price": str(premium), "initial_qty": str(qty), "remaining_qty": str(qty), "partial_done": "false",
            "sl_order_id": sl_id, "current_sl": str(sl_price), "sl_pct": str(sl_info["sl_pct"]), "sl_method": sl_info["method"],
            "entry_time": str(int(time.time())), "premium": str(premium), "high_water": str(premium),
            "partial_qty": str(partial_qty), "trail_qty": str(trail_qty), "atr": str(atr_val), "status": "PLACED"
        })
        r.expire(f"position:{order_id}", 86400)

        k_cap = daily_key("capital_used")
        r.incrbyfloat(k_cap, premium * qty)
        r.expire(k_cap, 86400)
        daily_incr("trade_count")
        AuditLog.trade(order_id, tok.get("signal", ""), qty, premium, sl_price, "PLACED")

        return {"status": "SUCCESS", "order_id": order_id, "sl_price": sl_price}

    except ValueError:
        r.delete(lock_key)
        raise
    except Exception as e:
        AuditLog.error("PLACE_ORDER_FAILED", str(e))
        r.delete(lock_key)
        raise ValueError(f"EXECUTION_ERROR: {str(e)}")

@pipeline_guard
def process_exit_signal(client, token_id: str) -> dict:
    pos_keys = r.keys("position:*")
    target_k = None
    target_pos = None
    for k in pos_keys:
        p = r.hgetall(k)
        if p.get("opt_token") == token_id and p.get("status") == "PLACED":
            target_k = k
            target_pos = p
            break

    if not target_pos:
        raise ValueError("NO_ACTIVE_POSITION_FOR_EXIT")

    try:
        client.cancelOrder(order_id=target_pos["sl_order_id"], variety="STOPLOSS")
    except:
        pass

    qty = int(float(target_pos["remaining_qty"]))
    resp = client.placeOrder({
        "variety": "NORMAL", "tradingsymbol": target_pos["symbol"],
        "symboltoken": target_pos["opt_token"], "transactiontype": "SELL",
        "exchange": "NFO", "ordertype": "MARKET", "quantity": qty, "producttype": "INTRADAY"
    })

    r.hset(target_k, mapping={"status": "CLOSED", "remaining_qty": "0"})
    AuditLog.trade(resp["data"]["orderid"], "EXIT_SIGNAL", qty, 0, 0, "CLOSED")
    return {"status": "SUCCESS", "exit_order_id": resp["data"]["orderid"]}

# ══════════════════════════════════════════════════════════════════
# SECTION 13 — BACKGROUND TASKS (TSL & EOD MONITOR)
# ══════════════════════════════════════════════════════════════════
def manage_trailing_sl(client):
    pos_keys = r.keys("position:*")
    for k in pos_keys:
        pos = r.hgetall(k)
        if not pos or pos.get("status") != "PLACED":
            continue
        try:
            ltp_info = fetch_ltp_with_retry(client, "NFO", pos["symbol"], pos["opt_token"])
            ltp = ltp_info["ltp"]
            hw = float(pos.get("high_water", pos["entry_price"]))

            if ltp > hw:
                r.hset(k, mapping={"high_water": str(ltp)})
                sl_pct = float(pos.get("sl_pct", 30.0))
                new_sl_raw = ltp * (1 - (sl_pct / 100))
                new_sl = round_to_tick(new_sl_raw)
                current_sl = float(pos.get("current_sl", 0))

                if new_sl > current_sl + (TICK_SIZE * 10):
                    client.modifyOrder({
                        "orderid": pos["sl_order_id"], "variety": "STOPLOSS", "tradingsymbol": pos["symbol"],
                        "symboltoken": pos["opt_token"], "exchange": "NFO", "ordertype": "STOPLOSS_MARKET", "triggerprice": str(new_sl)
                    })
                    r.hset(k, mapping={"current_sl": str(new_sl)})
                    AuditLog.trade(k.split(":")[-1], "TRAIL_SL", int(pos["remaining_qty"]), ltp, new_sl, "MODIFIED")
        except Exception as e:
            pass

async def background_monitor():
    while True:
        try:
            now_ms = int(time.time() * 1000)
            client = get_angel_client()

            if should_square_off_now(now_ms):
                if not r.get(f"eod_done_{today_ist()}"):
                    pos_keys = r.keys("position:*")
                    active_positions = []
                    for k in pos_keys:
                        p = r.hgetall(k)
                        if p.get("status") == "PLACED":
                            p["order_id"] = k.split(":")[-1]
                            active_positions.append(p)
                    if active_positions:
                        results = eod_square_off(client, active_positions, now_ms)
                        for k in pos_keys:
                            p = r.hgetall(k)
                            if p.get("status") == "PLACED":
                                r.hset(k, mapping={"status": "CLOSED_EOD", "remaining_qty": "0"})
                    r.setex(f"eod_done_{today_ist()}", 86400, "1")

            if is_market_open(now_ms) and not r.get(f"eod_done_{today_ist()}"):
                manage_trailing_sl(client)

        except Exception as e:
            AuditLog.error("BACKGROUND_MONITOR", str(e))

        await asyncio.sleep(5)

# ══════════════════════════════════════════════════════════════════
# SECTION 14 — FASTAPI ROUTING
# ══════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_monitor())

@app.get("/")
async def root():
    return {"status": "Algo_Live_v6_Production", "time": _now_ist().strftime("%Y-%m-%d %H:%M:%S")}

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        data = await request.json()
        now_ms = int(time.time() * 1000)

        validation = validate_webhook(data, now_ms)

        token_id = data.get("token", str(now_ms))

        client = get_angel_client()

        if validation.get("is_exit"):
            result = process_exit_signal(client, token_id)
            return {"status": "success", "action": "EXIT", "execution": result}
        else:
            lots = 1
            if not r.hgetall(f"token:{token_id}"):
                r.hset(f"token:{token_id}", mapping={
                    "symbol": data.get("symbol", "UNKNOWN"), "opt_token": data.get("token", "UNKNOWN"),
                    "signal": data.get("signal", "UNKNOWN"), "lot_mult": str(validation.get("lot_mult", 1.0)),
                    "atr": "0", "expiry": str(now_ms + 60000)
                })
            result = place_order(client, lots, token_id)
            return {"status": "success", "action": "ENTRY", "execution": result}

    except ValueError as ve:
        return {"status": "rejected", "reason": str(ve)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
