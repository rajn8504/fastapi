import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from telegram import Bot
from telegram.ext import Application, CommandHandler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("AutoTrader")

IST = ZoneInfo("Asia/Kolkata")
EXCHANGE_TYPES = {"NSE": 1, "NFO": 2, "BSE": 3, "BFO": 4, "MCX": 5}


def now_ist() -> datetime:
    return datetime.now(IST)


def load_json_env(name: str, default: Any) -> Any:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{name} must contain valid JSON: {exc}") from exc


@dataclass(frozen=True)
class UnderlyingDefinition:
    key: str
    option_name: str
    strategy_name: str
    strike_step: int
    default_lot_size: int
    spot_exchange: str
    search_terms: Tuple[str, ...]


UNDERLYINGS: Dict[str, UnderlyingDefinition] = {
    "BANKNIFTY": UnderlyingDefinition(
        key="BANKNIFTY",
        option_name="BANKNIFTY",
        strategy_name="BANKNIFTY_PUT",
        strike_step=100,
        default_lot_size=15,
        spot_exchange="NSE",
        search_terms=("NIFTY BANK", "BANKNIFTY"),
    ),
    "NIFTY": UnderlyingDefinition(
        key="NIFTY",
        option_name="NIFTY",
        strategy_name="NIFTY_PUT",
        strike_step=50,
        default_lot_size=25,
        spot_exchange="NSE",
        search_terms=("NIFTY 50", "NIFTY"),
    ),
    "FINNIFTY": UnderlyingDefinition(
        key="FINNIFTY",
        option_name="FINNIFTY",
        strategy_name="FINNIFTY_PUT",
        strike_step=50,
        default_lot_size=40,
        spot_exchange="NSE",
        search_terms=("NIFTY FIN SERVICE", "FINNIFTY"),
    ),
}


@dataclass(frozen=True)
class Instrument:
    exchange: str
    tradingsymbol: str
    token: str
    name: str

    @property
    def exchange_type(self) -> int:
        exchange_type = EXCHANGE_TYPES.get(self.exchange)
        if exchange_type is None:
            raise RuntimeError(f"Unsupported exchange for websocket subscription: {self.exchange}")
        return exchange_type


@dataclass(frozen=True)
class OptionContract(Instrument):
    strike: float
    expiry: date
    option_type: str
    lot_size: int
    tick_size: float


@dataclass
class OrderResult:
    order_id: Optional[str]
    status: str
    filled_qty: int = 0
    avg_price: float = 0.0
    raw: Optional[Dict[str, Any]] = None


@dataclass
class Trade:
    strategy: str
    contract: OptionContract
    order_id: str
    qty: int
    entry_price: float
    sl_price: float
    target_price: float
    confidence: int
    entry_time: datetime
    side: str = "SELL"


class Config:
    API_KEY = os.getenv("ANGEL_API_KEY", "")
    CLIENT_ID = os.getenv("ANGEL_CLIENT_ID", "")
    PASSWORD = os.getenv("ANGEL_PASSWORD", "")
    TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")

    TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

    CAPITAL = float(os.getenv("CAPITAL", "20000"))
    MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "1000"))
    MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "3"))
    TRADE_COOLDOWN_MINUTES = int(os.getenv("TRADE_COOLDOWN", "30"))
    TRADE_MODE = os.getenv("TRADE_MODE", "paper").strip().lower()

    TRAIL_TRIGGER_PROFIT = float(os.getenv("TRAIL_TRIGGER", "200"))
    TRAIL_LOCK_PERCENT = float(os.getenv("TRAIL_LOCK", "0.5"))

    STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.02"))
    TARGET_PCT = float(os.getenv("TARGET_PCT", "0.04"))
    LOT_MULTIPLIER = max(1, int(os.getenv("LOT_MULTIPLIER", "1")))
    ENTRY_ORDER_TYPE = os.getenv("ENTRY_ORDER_TYPE", "MARKET").strip().upper()

    MARKET_START = int(os.getenv("MARKET_START", "915"))
    MARKET_END = int(os.getenv("MARKET_END", "1525"))

    SCRIP_MASTER_URL = os.getenv(
        "ANGEL_SCRIP_MASTER_URL",
        "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json",
    )
    SCRIP_MASTER_CACHE = Path(os.getenv("ANGEL_SCRIP_MASTER_CACHE", "instrument_master_cache.json"))
    REFERENCE_PRICE_CACHE = Path(os.getenv("REFERENCE_PRICE_CACHE", "reference_prices.json"))

    INDEX_OVERRIDES = load_json_env("ANGEL_INDEX_MAP_JSON", {})

    @classmethod
    def validate(cls) -> None:
        required = {
            "ANGEL_API_KEY": cls.API_KEY,
            "ANGEL_CLIENT_ID": cls.CLIENT_ID,
            "ANGEL_PASSWORD": cls.PASSWORD,
            "ANGEL_TOTP_SECRET": cls.TOTP_SECRET,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        if cls.TRADE_MODE not in {"paper", "live"}:
            raise RuntimeError("TRADE_MODE must be 'paper' or 'live'")
        if cls.ENTRY_ORDER_TYPE not in {"MARKET", "LIMIT"}:
            raise RuntimeError("ENTRY_ORDER_TYPE must be MARKET or LIMIT")


config = Config()


class ReferencePriceStore:
    def __init__(self, path: Path):
        self.path = path
        self._data = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not load reference price cache: %s", exc)
            return {}

    def _save(self) -> None:
        self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")

    def get(self, key: str) -> float:
        today = now_ist().date().isoformat()
        node = self._data.get(key)
        if not node or node.get("as_of") != today:
            return 0.0
        return float(node.get("close", 0.0))

    def update(self, key: str, close_price: float) -> None:
        self._data[key] = {"as_of": now_ist().date().isoformat(), "close": round(close_price, 2)}
        self._save()


class InstrumentMaster:
    def __init__(self, url: str, cache_path: Path):
        self.url = url
        self.cache_path = cache_path
        self._rows: List[Dict[str, Any]] = []

    def load(self) -> None:
        rows: Optional[List[Dict[str, Any]]] = None
        try:
            response = httpx.get(self.url, timeout=20.0)
            response.raise_for_status()
            rows = response.json()
            self.cache_path.write_text(json.dumps(rows), encoding="utf-8")
            logger.info("Loaded %s instruments from Angel scrip master", len(rows))
        except Exception as exc:
            logger.warning("Could not download scrip master, trying cache: %s", exc)

        if rows is None and self.cache_path.exists():
            rows = json.loads(self.cache_path.read_text(encoding="utf-8"))
            logger.info("Loaded %s instruments from local cache", len(rows))

        if not rows:
            raise RuntimeError("Unable to load Angel One scrip master data")

        self._rows = rows

    @staticmethod
    def _parse_expiry(raw: str) -> Optional[date]:
        raw = (raw or "").strip().upper()
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%d%b%Y").date()
        except ValueError:
            return None

    @staticmethod
    def _parse_strike(raw: str) -> float:
        if not raw:
            return 0.0
        try:
            return float(raw) / 100.0
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _parse_tick_size(raw: str) -> float:
        if not raw:
            return 0.05
        try:
            value = float(raw)
            return value / 100.0 if value >= 1 else value
        except (TypeError, ValueError):
            return 0.05

    def find_option_contract(
        self,
        underlying: UnderlyingDefinition,
        option_type: str,
        spot_price: float,
        trade_day: date,
    ) -> OptionContract:
        option_type = option_type.upper()
        atm_strike = round(spot_price / underlying.strike_step) * underlying.strike_step
        matches: List[Tuple[date, float, Dict[str, Any]]] = []

        for row in self._rows:
            if row.get("exch_seg") != "NFO":
                continue
            if row.get("instrumenttype") != "OPTIDX":
                continue
            if row.get("name") != underlying.option_name:
                continue
            symbol = str(row.get("symbol", ""))
            if not symbol.endswith(option_type):
                continue
            expiry = self._parse_expiry(str(row.get("expiry", "")))
            if not expiry or expiry < trade_day:
                continue
            strike = self._parse_strike(str(row.get("strike", "")))
            matches.append((expiry, abs(strike - atm_strike), row))

        if not matches:
            raise RuntimeError(f"No option contracts found for {underlying.option_name}")

        matches.sort(key=lambda item: (item[0], item[1]))
        best_row = matches[0][2]
        expiry = self._parse_expiry(str(best_row.get("expiry", "")))
        strike = self._parse_strike(str(best_row.get("strike", "")))
        tick_size = self._parse_tick_size(str(best_row.get("tick_size", "")))
        lot_size_raw = best_row.get("lotsize") or underlying.default_lot_size
        lot_size = int(float(lot_size_raw))

        return OptionContract(
            exchange="NFO",
            tradingsymbol=str(best_row["symbol"]),
            token=str(best_row["token"]),
            name=underlying.option_name,
            strike=strike,
            expiry=expiry,
            option_type=option_type,
            lot_size=lot_size,
            tick_size=tick_size,
        )


class StrategyScorer:
    def analyze_market(self, market_data: Dict[str, float]) -> Dict[str, float]:
        scores = {"BANKNIFTY_PUT": 0.0, "NIFTY_PUT": 0.0, "FINNIFTY_PUT": 0.0}

        vix = market_data.get("vix", 15.0)
        nifty_change = market_data.get("NIFTY_change", 0.0)
        banknifty_change = market_data.get("BANKNIFTY_change", 0.0)
        finnifty_change = market_data.get("FINNIFTY_change", 0.0)
        hhmm = market_data.get("time", now_ist().hour * 100 + now_ist().minute)

        if vix > 18:
            scores["BANKNIFTY_PUT"] += 35
            scores["FINNIFTY_PUT"] += 25
        elif vix > 15:
            scores["BANKNIFTY_PUT"] += 20

        if banknifty_change < -0.5:
            scores["BANKNIFTY_PUT"] += 40
        elif banknifty_change < -0.2:
            scores["BANKNIFTY_PUT"] += 25

        if nifty_change < -0.5:
            scores["NIFTY_PUT"] += 35
        elif nifty_change < -0.2:
            scores["NIFTY_PUT"] += 20

        if abs(finnifty_change) > 0.8:
            scores["FINNIFTY_PUT"] += 40
        elif abs(finnifty_change) > 0.4:
            scores["FINNIFTY_PUT"] += 25

        if hhmm < 1030 and abs(finnifty_change) > 0.5:
            scores["FINNIFTY_PUT"] += 20

        if vix > 25:
            scores["NIFTY_PUT"] -= 10

        return scores

    def get_best_strategy(self, market_data: Dict[str, float]) -> Tuple[str, int, Dict[str, float]]:
        scores = self.analyze_market(market_data)
        strategy = max(scores, key=scores.get)
        confidence = min(100, int((scores[strategy] / 100.0) * 100))
        return strategy, confidence, scores


class AngelOneConnector:
    def __init__(self):
        self.smart_api: Optional[SmartConnect] = None
        self.auth_token: Optional[str] = None
        self.feed_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self._ltp_cache: Dict[str, float] = {}

    def login(self) -> bool:
        try:
            totp = pyotp.TOTP(config.TOTP_SECRET).now()
            self.smart_api = SmartConnect(api_key=config.API_KEY)
            session = self.smart_api.generateSession(config.CLIENT_ID, config.PASSWORD, totp)
            if not session or not session.get("status"):
                logger.error("Login failed: %s", session)
                return False

            self.auth_token = session["data"]["jwtToken"]
            self.refresh_token = session["data"]["refreshToken"]
            self.feed_token = self.smart_api.getfeedToken()
            logger.info("Angel One connected")
            return True
        except Exception as exc:
            logger.error("Login error: %s", exc)
            return False

    def resolve_spot_instrument(self, key: str, definition: UnderlyingDefinition) -> Instrument:
        override = config.INDEX_OVERRIDES.get(key, {})
        if override:
            return Instrument(
                exchange=str(override["exchange"]),
                tradingsymbol=str(override["tradingsymbol"]),
                token=str(override["token"]),
                name=key,
            )

        if not self.smart_api:
            raise RuntimeError("Smart API not initialized")

        best_candidate: Optional[Dict[str, Any]] = None
        best_score = -1
        for term in definition.search_terms:
            try:
                response = self.smart_api.searchScrip(definition.spot_exchange, term)
            except Exception as exc:
                logger.warning("searchScrip failed for %s: %s", term, exc)
                continue

            for candidate in response.get("data") or []:
                score = self._score_candidate(term, definition, candidate)
                if score > best_score:
                    best_score = score
                    best_candidate = candidate

        if not best_candidate:
            raise RuntimeError(
                f"Could not resolve spot instrument for {key}. "
                f"Set ANGEL_INDEX_MAP_JSON with exchange/tradingsymbol/token overrides."
            )

        return Instrument(
            exchange=str(best_candidate["exchange"]),
            tradingsymbol=str(best_candidate["tradingsymbol"]),
            token=str(best_candidate["symboltoken"]),
            name=key,
        )

    @staticmethod
    def _score_candidate(term: str, definition: UnderlyingDefinition, candidate: Dict[str, Any]) -> int:
        symbol = str(candidate.get("tradingsymbol", "")).upper()
        score = 0
        if candidate.get("exchange") == definition.spot_exchange:
            score += 5
        if symbol == term.upper():
            score += 10
        if definition.option_name in symbol.replace(" ", ""):
            score += 4
        if term.upper().replace(" ", "") in symbol.replace(" ", ""):
            score += 3
        return score

    def get_ltp(self, instrument: Instrument) -> float:
        if not self.smart_api:
            return 0.0
        try:
            response = self.smart_api.ltpData(
                instrument.exchange,
                instrument.tradingsymbol,
                instrument.token,
            )
            if response and response.get("status"):
                ltp = float(response["data"]["ltp"])
                self._ltp_cache[instrument.token] = ltp
                return ltp
        except Exception as exc:
            logger.error("LTP error for %s: %s", instrument.tradingsymbol, exc)
        return self._ltp_cache.get(instrument.token, 0.0)

    def get_previous_close(self, instrument: Instrument) -> float:
        if not self.smart_api:
            return 0.0

        to_dt = now_ist()
        from_dt = to_dt - timedelta(days=7)
        params = {
            "exchange": instrument.exchange,
            "symboltoken": instrument.token,
            "interval": "ONE_DAY",
            "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
            "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
        }

        try:
            response = self.smart_api.getCandleData(params)
            candles = response.get("data") or []
            parsed = []
            for candle in candles:
                if len(candle) < 5:
                    continue
                ts = datetime.fromisoformat(str(candle[0]).replace("Z", "+00:00")).astimezone(IST)
                parsed.append((ts.date(), float(candle[4])))

            today = now_ist().date()
            historical = [close for candle_date, close in parsed if candle_date < today]
            if historical:
                return historical[-1]
            if parsed:
                return parsed[-1][1]
        except Exception as exc:
            logger.warning("Could not fetch previous close for %s: %s", instrument.tradingsymbol, exc)

        return 0.0

    def get_change_pct(self, current: float, reference: float) -> float:
        if current <= 0 or reference <= 0:
            return 0.0
        return round(((current - reference) / reference) * 100.0, 2)

    def round_price(self, price: float, tick_size: float) -> float:
        if tick_size <= 0:
            return round(price, 2)
        steps = round(price / tick_size)
        return round(steps * tick_size, 2)

    def place_order(self, contract: OptionContract, action: str, qty: int, price_hint: float) -> OrderResult:
        if config.TRADE_MODE == "paper":
            return OrderResult(
                order_id=f"PAPER_{int(time.time())}",
                status="complete",
                filled_qty=qty,
                avg_price=price_hint,
            )

        if not self.smart_api:
            return OrderResult(order_id=None, status="failed")

        params: Dict[str, Any] = {
            "variety": "NORMAL",
            "tradingsymbol": contract.tradingsymbol,
            "symboltoken": contract.token,
            "transactiontype": action.upper(),
            "exchange": contract.exchange,
            "ordertype": config.ENTRY_ORDER_TYPE,
            "producttype": "INTRADAY",
            "duration": "DAY",
            "quantity": str(qty),
        }
        if config.ENTRY_ORDER_TYPE == "LIMIT":
            params["price"] = str(self.round_price(price_hint, contract.tick_size))

        try:
            if hasattr(self.smart_api, "placeOrderFullResponse"):
                response = self.smart_api.placeOrderFullResponse(params)
                order_id = response.get("data", {}).get("orderid")
            else:
                response = {"data": {"orderid": self.smart_api.placeOrder(params)}}
                order_id = response["data"]["orderid"]
            if not order_id:
                logger.error("Order placement failed: %s", response)
                return OrderResult(order_id=None, status="failed", raw=response)
            return self.wait_for_order(order_id)
        except Exception as exc:
            logger.error("Order error: %s", exc)
            return OrderResult(order_id=None, status="failed", raw={"error": str(exc)})

    def wait_for_order(self, order_id: str, retries: int = 5, delay_seconds: float = 1.0) -> OrderResult:
        for _ in range(retries):
            details = self.get_order_status(order_id)
            if details.status and details.status.lower() not in {"unknown", "open", "trigger pending"}:
                return details
            time.sleep(delay_seconds)
        return self.get_order_status(order_id)

    def get_order_status(self, order_id: str) -> OrderResult:
        if not self.smart_api:
            return OrderResult(order_id=order_id, status="unknown")

        try:
            details = self.smart_api.individual_order_details(f"?orderid={order_id}")
            parsed = self._parse_order_details(order_id, details)
            if parsed:
                return parsed
        except Exception as exc:
            logger.debug("individual_order_details failed for %s: %s", order_id, exc)

        try:
            book = self.smart_api.orderBook()
            for row in book.get("data") or []:
                if str(row.get("orderid")) == order_id:
                    return self._order_result_from_row(order_id, row)
        except Exception as exc:
            logger.error("orderBook failed for %s: %s", order_id, exc)

        return OrderResult(order_id=order_id, status="unknown")

    def _parse_order_details(self, order_id: str, details: Optional[Dict[str, Any]]) -> Optional[OrderResult]:
        if not details:
            return None
        payload = details.get("data")
        if isinstance(payload, list):
            for row in payload:
                if str(row.get("orderid")) == order_id:
                    return self._order_result_from_row(order_id, row)
        if isinstance(payload, dict):
            return self._order_result_from_row(order_id, payload)
        return None

    @staticmethod
    def _order_result_from_row(order_id: str, row: Dict[str, Any]) -> OrderResult:
        return OrderResult(
            order_id=order_id,
            status=str(row.get("status", "unknown")),
            filled_qty=int(float(row.get("filledshares") or row.get("filledqty") or 0)),
            avg_price=float(row.get("averageprice") or row.get("avgprice") or 0.0),
            raw=row,
        )

    def square_off(self, trade: Trade, price_hint: float) -> OrderResult:
        return self.place_order(trade.contract, "BUY", trade.qty, price_hint)


class TradeState:
    def __init__(self):
        self.active_trade: Optional[Trade] = None
        self.daily_pnl: float = 0.0
        self.trades_today: int = 0
        self.last_trade_time: Optional[datetime] = None
        self.strategy_scores: Dict[str, float] = {}
        self.session_date: date = now_ist().date()

    def roll_day(self) -> None:
        today = now_ist().date()
        if today != self.session_date:
            self.active_trade = None
            self.daily_pnl = 0.0
            self.trades_today = 0
            self.last_trade_time = None
            self.strategy_scores = {}
            self.session_date = today

    def can_trade(self) -> Tuple[bool, str]:
        self.roll_day()
        if self.active_trade:
            return False, "Active trade exists"
        if self.trades_today >= config.MAX_TRADES_PER_DAY:
            return False, "Daily trade limit reached"
        if self.daily_pnl <= -config.MAX_DAILY_LOSS:
            return False, "Daily loss limit reached"
        if self.last_trade_time:
            minutes = (now_ist() - self.last_trade_time).total_seconds() / 60.0
            if minutes < config.TRADE_COOLDOWN_MINUTES:
                remaining = int(config.TRADE_COOLDOWN_MINUTES - minutes)
                return False, f"Cooldown active ({remaining} min)"
        return True, "Ready"

    def start_trade(self, trade: Trade) -> None:
        self.active_trade = trade
        self.last_trade_time = now_ist()

    def close_trade(self, pnl: float) -> None:
        self.daily_pnl += pnl
        self.trades_today += 1
        self.active_trade = None


class WebSocketManager:
    def __init__(self, connector: AngelOneConnector, on_tick):
        self.connector = connector
        self.on_tick = on_tick
        self.ws: Optional[SmartWebSocketV2] = None
        self._requested: Dict[int, set[str]] = {}
        self.running = False

    async def start(self, instruments: Iterable[Instrument]) -> None:
        self.running = True
        self.subscribe_instruments(instruments)
        loop = asyncio.get_running_loop()

        def on_open(wsapp):
            logger.info("WebSocket connected")
            payload = self._subscription_payload()
            if payload:
                wsapp.subscribe("autotrader1", 1, payload)

        def on_data(_wsapp, message):
            loop.call_soon_threadsafe(lambda: asyncio.create_task(self.on_tick(message)))

        def on_error(_wsapp, error):
            logger.error("WebSocket error: %s", error)

        def on_close(_wsapp):
            logger.warning("WebSocket closed")

        self.ws = SmartWebSocketV2(
            self.connector.auth_token,
            config.API_KEY,
            config.CLIENT_ID,
            self.connector.feed_token,
        )
        self.ws.on_open = on_open
        self.ws.on_data = on_data
        self.ws.on_error = on_error
        self.ws.on_close = on_close

        await loop.run_in_executor(None, self.ws.connect)

    def subscribe_instruments(self, instruments: Iterable[Instrument]) -> None:
        new_items: Dict[int, List[str]] = {}
        for instrument in instruments:
            bucket = self._requested.setdefault(instrument.exchange_type, set())
            if instrument.token in bucket:
                continue
            bucket.add(instrument.token)
            new_items.setdefault(instrument.exchange_type, []).append(instrument.token)

        if self.ws and new_items:
            payload = [{"exchangeType": exchange_type, "tokens": tokens} for exchange_type, tokens in new_items.items()]
            self.ws.subscribe("autotrader1", 1, payload)

    def _subscription_payload(self) -> List[Dict[str, Any]]:
        return [
            {"exchangeType": exchange_type, "tokens": sorted(tokens)}
            for exchange_type, tokens in sorted(self._requested.items())
            if tokens
        ]

    def stop(self) -> None:
        self.running = False
        if self.ws:
            self.ws.close_connection()


class AutoTradingEngine:
    def __init__(self):
        self.connector = AngelOneConnector()
        self.instrument_master = InstrumentMaster(config.SCRIP_MASTER_URL, config.SCRIP_MASTER_CACHE)
        self.reference_prices = ReferencePriceStore(config.REFERENCE_PRICE_CACHE)
        self.state = TradeState()
        self.scorer = StrategyScorer()
        self.telegram: Optional[Bot] = None
        self.ws_manager: Optional[WebSocketManager] = None
        self.underlying_instruments: Dict[str, Instrument] = {}
        self.vix_instrument: Optional[Instrument] = None
        self._running = False
        self._peak_profit = 0.0

    async def initialize(self) -> None:
        Config.validate()
        if not self.connector.login():
            raise RuntimeError("Angel One login failed")

        self.instrument_master.load()
        self._resolve_market_instruments()
        self._refresh_reference_prices()

        if config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID:
            self.telegram = Bot(token=config.TELEGRAM_TOKEN)
            await self.send_message(
                "AUTO TRADING BOT STARTED\n\n"
                f"Capital: Rs {config.CAPITAL:,.0f}\n"
                f"Max Loss: Rs {config.MAX_DAILY_LOSS:,.0f}\n"
                f"Max Trades: {config.MAX_TRADES_PER_DAY}\n"
                f"Mode: {config.TRADE_MODE.upper()}\n"
                "Strategies: BANKNIFTY_PUT / NIFTY_PUT / FINNIFTY_PUT"
            )

    def _resolve_market_instruments(self) -> None:
        for key, definition in UNDERLYINGS.items():
            self.underlying_instruments[key] = self.connector.resolve_spot_instrument(key, definition)

        vix_override = config.INDEX_OVERRIDES.get("VIX", {})
        if vix_override:
            self.vix_instrument = Instrument(
                exchange=str(vix_override["exchange"]),
                tradingsymbol=str(vix_override["tradingsymbol"]),
                token=str(vix_override["token"]),
                name="VIX",
            )
        else:
            try:
                temp_definition = UnderlyingDefinition(
                    key="VIX",
                    option_name="INDIA VIX",
                    strategy_name="",
                    strike_step=1,
                    default_lot_size=1,
                    spot_exchange="NSE",
                    search_terms=("INDIA VIX",),
                )
                self.vix_instrument = self.connector.resolve_spot_instrument("VIX", temp_definition)
            except Exception:
                self.vix_instrument = None
                logger.warning("Could not resolve INDIA VIX, using fallback VIX value when unavailable")

    def _refresh_reference_prices(self) -> None:
        for key, instrument in self.underlying_instruments.items():
            if self.reference_prices.get(key) > 0:
                continue
            close_price = self.connector.get_previous_close(instrument)
            if close_price > 0:
                self.reference_prices.update(key, close_price)
                logger.info("Reference close for %s set to %.2f", key, close_price)
            else:
                logger.warning("Reference close unavailable for %s; percent change will remain 0", key)

    async def send_message(self, text: str) -> None:
        if not self.telegram:
            return
        try:
            await self.telegram.send_message(
                chat_id=config.TELEGRAM_CHAT_ID,
                text=text,
            )
        except Exception as exc:
            logger.error("Telegram error: %s", exc)

    async def on_tick(self, message: Dict[str, Any]) -> None:
        if not self.state.active_trade:
            return

        token = str(message.get("token", ""))
        if token != self.state.active_trade.contract.token:
            return

        ltp = float(message.get("last_traded_price", 0)) / 100.0
        if ltp <= 0:
            return
        await self._check_trade_exit(ltp)

    async def run(self) -> None:
        self._running = True
        if not self.ws_manager:
            initial_instruments = list(self.underlying_instruments.values())
            if self.vix_instrument:
                initial_instruments.append(self.vix_instrument)
            self.ws_manager = WebSocketManager(self.connector, self.on_tick)
            asyncio.create_task(self.ws_manager.start(initial_instruments))

        await self.send_message("Bot running and waiting for market conditions.")

        while self._running:
            try:
                self.state.roll_day()
                if any(self.reference_prices.get(key) <= 0 for key in self.underlying_instruments):
                    self._refresh_reference_prices()
                if not self._is_market_open():
                    await asyncio.sleep(30)
                    continue

                can_trade, reason = self.state.can_trade()
                if can_trade:
                    market_data = self.get_market_data()
                    strategy, confidence, scores = self.scorer.get_best_strategy(market_data)
                    self.state.strategy_scores = scores
                    if confidence >= 40:
                        await self._execute_trade(strategy, confidence, market_data)
                    else:
                        logger.info("No trade: low confidence (%s)", confidence)
                else:
                    logger.info("Trade blocked: %s", reason)

                await asyncio.sleep(5)
            except Exception as exc:
                logger.exception("Main loop error: %s", exc)
                await asyncio.sleep(10)

    def get_market_data(self) -> Dict[str, float]:
        market_data: Dict[str, float] = {}
        for key, instrument in self.underlying_instruments.items():
            ltp = self.connector.get_ltp(instrument)
            market_data[key] = ltp
            reference = self.reference_prices.get(key)
            market_data[f"{key}_change"] = self.connector.get_change_pct(ltp, reference)

        if self.vix_instrument:
            vix = self.connector.get_ltp(self.vix_instrument)
        else:
            vix = 15.0
        market_data["vix"] = vix if vix > 0 else 15.0
        market_data["time"] = now_ist().hour * 100 + now_ist().minute
        return market_data

    async def _execute_trade(self, strategy: str, confidence: int, market_data: Dict[str, float]) -> None:
        underlying_key = strategy.replace("_PUT", "")
        definition = UNDERLYINGS[underlying_key]
        spot_price = market_data.get(underlying_key, 0.0)
        if spot_price <= 0:
            logger.warning("Skipping %s: no spot price", strategy)
            return

        contract = self.instrument_master.find_option_contract(
            underlying=definition,
            option_type="PE",
            spot_price=spot_price,
            trade_day=now_ist().date(),
        )
        option_ltp = self.connector.get_ltp(contract)
        if option_ltp <= 0:
            logger.warning("Skipping %s: no option LTP for %s", strategy, contract.tradingsymbol)
            return

        qty = contract.lot_size * config.LOT_MULTIPLIER
        order = self.connector.place_order(contract, "SELL", qty, option_ltp)
        if not order.order_id:
            await self.send_message(f"Order failed for {contract.tradingsymbol}: {order.raw}")
            return

        entry_price = order.avg_price or option_ltp
        sl_price = entry_price * (1 + config.STOP_LOSS_PCT)
        target_price = entry_price * (1 - config.TARGET_PCT)
        trade = Trade(
            strategy=strategy,
            contract=contract,
            order_id=order.order_id,
            qty=qty,
            entry_price=entry_price,
            sl_price=sl_price,
            target_price=target_price,
            confidence=confidence,
            entry_time=now_ist(),
        )
        self.state.start_trade(trade)
        self._peak_profit = 0.0

        if self.ws_manager:
            self.ws_manager.subscribe_instruments([contract])

        await self.send_message(
            "AUTO TRADE EXECUTED\n"
            f"Strategy: {strategy}\n"
            f"Contract: {contract.tradingsymbol}\n"
            f"Spot: Rs {spot_price:.2f}\n"
            f"Entry: Rs {entry_price:.2f}\n"
            f"Qty: {qty}\n"
            f"Confidence: {confidence}%\n"
            f"SL: Rs {sl_price:.2f}\n"
            f"Target: Rs {target_price:.2f}\n"
            f"Order: {order.order_id} ({order.status})"
        )

    async def _check_trade_exit(self, current_ltp: float) -> None:
        trade = self.state.active_trade
        if not trade:
            return

        pnl = (trade.entry_price - current_ltp) * trade.qty
        self._peak_profit = max(self._peak_profit, pnl)

        exit_reason: Optional[str] = None
        if current_ltp <= trade.target_price:
            exit_reason = "TARGET HIT"
        elif current_ltp >= trade.sl_price:
            exit_reason = "STOP LOSS"
        elif self._peak_profit >= config.TRAIL_TRIGGER_PROFIT:
            trail_floor = self._peak_profit * config.TRAIL_LOCK_PERCENT
            if pnl <= trail_floor:
                exit_reason = "TRAILING SL"

        if not exit_reason:
            return

        exit_result = self.connector.square_off(trade, current_ltp)
        if config.TRADE_MODE == "live" and not exit_result.order_id:
            await self.send_message(
                f"Square-off failed for {trade.contract.tradingsymbol}. Manual intervention required."
            )
            return

        self.state.close_trade(pnl)
        await self.send_message(
            f"{exit_reason}\n"
            f"Strategy: {trade.strategy}\n"
            f"Contract: {trade.contract.tradingsymbol}\n"
            f"Exit LTP: Rs {current_ltp:.2f}\n"
            f"P&L: Rs {pnl:+.0f}\n"
            f"Day P&L: Rs {self.state.daily_pnl:+.0f}"
        )

    def _is_market_open(self) -> bool:
        now = now_ist()
        if now.weekday() >= 5:
            return False
        hhmm = now.hour * 100 + now.minute
        return config.MARKET_START <= hhmm <= config.MARKET_END

    async def get_status(self) -> str:
        market_data = self.get_market_data()
        lines = [
            "AUTO TRADER STATUS",
            f"Day P&L: Rs {self.state.daily_pnl:+.0f}",
            f"Trades: {self.state.trades_today}/{config.MAX_TRADES_PER_DAY}",
            f"Mode: {config.TRADE_MODE.upper()}",
            f"VIX: {market_data.get('vix', 0.0):.2f}",
            "",
            "Market:",
            f"BankNifty: {market_data.get('BANKNIFTY', 0.0):,.2f} ({market_data.get('BANKNIFTY_change', 0.0):+.2f}%)",
            f"Nifty: {market_data.get('NIFTY', 0.0):,.2f} ({market_data.get('NIFTY_change', 0.0):+.2f}%)",
            f"FinNifty: {market_data.get('FINNIFTY', 0.0):,.2f} ({market_data.get('FINNIFTY_change', 0.0):+.2f}%)",
        ]
        if self.state.active_trade:
            trade = self.state.active_trade
            lines.extend(
                [
                    "",
                    f"Active Trade: {trade.contract.tradingsymbol}",
                    f"Entry: Rs {trade.entry_price:.2f}",
                    f"Qty: {trade.qty}",
                ]
            )
        if self.state.strategy_scores:
            lines.extend(["", f"Scores: {self.state.strategy_scores}"])
        return "\n".join(lines)


async def status_command(update, context) -> None:
    engine: AutoTradingEngine = context.bot_data.get("engine")
    if engine:
        status = await engine.get_status()
        await update.message.reply_text(status)


async def start_command(update, context) -> None:
    await update.message.reply_text(
        "Auto Trading Bot\n\n"
        "Autonomous paper/live execution\n"
        "Dynamic option contract selection from Angel scrip master\n"
        "Real-time exit checks on the active option leg\n\n"
        "/status - Show bot status"
    )


async def main() -> None:
    engine = AutoTradingEngine()
    await engine.initialize()

    if config.TELEGRAM_TOKEN:
        app = Application.builder().token(config.TELEGRAM_TOKEN).build()
        app.bot_data["engine"] = engine
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("status", status_command))
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("Telegram bot ready")

    await engine.run()


if __name__ == "__main__":
    asyncio.run(main())
