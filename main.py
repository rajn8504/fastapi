"""
Ultra-Fast HFT Trading Bot — Angel One SmartAPI
Async | WebSocket | Telegram UI | Railway.app Ready
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, date
from typing import Optional

import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from strategies import AlphaStrategy
from state_manager import StateManager
from risk_manager import RiskManager

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("TradingBot")


# ─────────────────────────────────────────────
# Config from Environment
# ─────────────────────────────────────────────
class Config:
    API_KEY: str = os.environ["ANGEL_API_KEY"]
    CLIENT_ID: str = os.environ["ANGEL_CLIENT_ID"]
    PASSWORD: str = os.environ["ANGEL_PASSWORD"]
    TOTP_SECRET: str = os.environ["ANGEL_TOTP_SECRET"]
    TELEGRAM_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
    TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]

    CAPITAL: float = float(os.getenv("CAPITAL", "20000"))
    MAX_DAILY_LOSS: float = float(os.getenv("MAX_DAILY_LOSS", "1000"))
    DAILY_TARGET: float = float(os.getenv("DAILY_TARGET", "1500"))
    TRADE_MODE: str = os.getenv("TRADE_MODE", "paper")  # 'paper' | 'live'

    BREAKEVEN_TRIGGER: float = float(os.getenv("BREAKEVEN_TRIGGER", "300"))
    TRAIL_TRIGGER: float = float(os.getenv("TRAIL_TRIGGER", "700"))
    TRAIL_STEP: float = float(os.getenv("TRAIL_STEP", "100"))

    WS_RECONNECT_BASE: float = 1.0
    WS_RECONNECT_MAX: float = 60.0


# ─────────────────────────────────────────────
# Angel One Connector
# ─────────────────────────────────────────────
class AngelOneConnector:
    def __init__(self, config: Config):
        self.config = config
        self.smart_api: Optional[SmartConnect] = None
        self.auth_token: Optional[str] = None
        self.feed_token: Optional[str] = None
        self.refresh_token: Optional[str] = None

    def login(self) -> bool:
        try:
            totp = pyotp.TOTP(self.config.TOTP_SECRET).now()
            self.smart_api = SmartConnect(api_key=self.config.API_KEY)
            session = self.smart_api.generateSession(
                self.config.CLIENT_ID, self.config.PASSWORD, totp
            )
            if session["status"]:
                self.auth_token = session["data"]["jwtToken"]
                self.feed_token = self.smart_api.getfeedToken()
                self.refresh_token = session["data"]["refreshToken"]
                logger.info("✅ Angel One login successful")
                return True
            logger.error(f"Login failed: {session}")
            return False
        except Exception as e:
            logger.exception(f"Login error: {e}")
            return False

    def get_profile(self) -> dict:
        try:
            return self.smart_api.getProfile(self.refresh_token)
        except Exception as e:
            logger.error(f"Profile fetch error: {e}")
            return {}

    def get_funds(self) -> float:
        try:
            resp = self.smart_api.rmsLimit()
            if resp and resp.get("status"):
                return float(resp["data"].get("availablecash", 0))
        except Exception as e:
            logger.error(f"Funds fetch error: {e}")
        return 0.0

    def place_order(
        self,
        symbol: str,
        token: str,
        action: str,
        qty: int,
        price: float,
        order_type: str = "LIMIT",
    ) -> Optional[str]:
        try:
            params = {
                "variety": "NORMAL",
                "tradingsymbol": symbol,
                "symboltoken": token,
                "transactiontype": action,
                "exchange": "NFO",
                "ordertype": order_type,
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": str(price),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": str(qty),
            }
            resp = self.smart_api.placeOrder(params)
            if resp and resp.get("status"):
                order_id = resp["data"]["orderid"]
                logger.info(f"📋 Order placed: {order_id} | {action} {symbol} @ {price}")
                return order_id
            logger.error(f"Order failed: {resp}")
            return None
        except Exception as e:
            logger.exception(f"Order error: {e}")
            return None

    def cancel_order(self, order_id: str, variety: str = "NORMAL") -> bool:
        try:
            resp = self.smart_api.cancelOrder(order_id, variety)
            return bool(resp and resp.get("status"))
        except Exception as e:
            logger.error(f"Cancel order error: {e}")
            return False

    def get_ltp(self, exchange: str, symbol: str, token: str) -> float:
        try:
            resp = self.smart_api.ltpData(exchange, symbol, token)
            if resp and resp.get("status"):
                return float(resp["data"]["ltp"])
        except Exception as e:
            logger.error(f"LTP error: {e}")
        return 0.0

    def get_positions(self) -> list:
        try:
            resp = self.smart_api.position()
            if resp and resp.get("status"):
                return resp["data"] or []
        except Exception as e:
            logger.error(f"Positions error: {e}")
        return []

    def square_off_all(self) -> bool:
        positions = self.get_positions()
        success = True
        for pos in positions:
            net_qty = int(pos.get("netqty", 0))
            if net_qty == 0:
                continue
            action = "SELL" if net_qty > 0 else "BUY"
            ltp = self.get_ltp("NFO", pos["tradingsymbol"], pos["symboltoken"])
            price = round(ltp * 1.002, 1) if action == "SELL" else round(ltp * 0.998, 1)
            oid = self.place_order(
                pos["tradingsymbol"],
                pos["symboltoken"],
                action,
                abs(net_qty),
                price,
            )
            if not oid:
                success = False
        return success


# ─────────────────────────────────────────────
# WebSocket Feed Manager
# ─────────────────────────────────────────────
class WebSocketFeedManager:
    def __init__(self, connector: AngelOneConnector, tick_callback):
        self.connector = connector
        self.tick_callback = tick_callback
        self._ws: Optional[SmartWebSocketV2] = None
        self._running = False
        self._reconnect_delay = Config.WS_RECONNECT_BASE
        self._subscriptions: list = []

    async def start(self, tokens: list):
        """Start WS feed with exponential backoff reconnect."""
        self._subscriptions = tokens
        self._running = True
        while self._running:
            try:
                await self._connect()
                self._reconnect_delay = Config.WS_RECONNECT_BASE
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if self._running:
                    logger.info(
                        f"🔄 Reconnecting in {self._reconnect_delay:.1f}s..."
                    )
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2, Config.WS_RECONNECT_MAX
                    )

    async def _connect(self):
        loop = asyncio.get_event_loop()

        def on_open(ws):
            logger.info("🟢 WebSocket connected")
            token_list = [
                {"exchangeType": 2, "tokens": self._subscriptions}
            ]
            ws.subscribe("ab1234", 3, token_list)

        def on_data(ws, msg):
            loop.call_soon_threadsafe(
                asyncio.ensure_future,
                self.tick_callback(msg),
            )

        def on_error(ws, error):
            logger.error(f"WS Error: {error}")

        def on_close(ws, code, msg):
            logger.warning(f"🔴 WebSocket closed: {code} {msg}")

        self._ws = SmartWebSocketV2(
            self.connector.auth_token,
            self.connector.config.API_KEY,
            self.connector.config.CLIENT_ID,
            self.connector.feed_token,
        )
        self._ws.on_open = on_open
        self._ws.on_data = on_data
        self._ws.on_error = on_error
        self._ws.on_close = on_close

        await asyncio.get_event_loop().run_in_executor(None, self._ws.connect)

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass


# ─────────────────────────────────────────────
# Telegram UI Manager
# ─────────────────────────────────────────────
class TelegramManager:
    def __init__(self, token: str, chat_id: str, bot_engine):
        self.token = token
        self.chat_id = chat_id
        self.engine = bot_engine
        self.app: Optional[Application] = None
        self._bot: Optional[Bot] = None

    async def send(self, text: str, parse_mode: str = "HTML"):
        try:
            await self._bot.send_message(
                chat_id=self.chat_id, text=text, parse_mode=parse_mode
            )
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    async def setup(self):
        self.app = Application.builder().token(self.token).build()
        self._bot = self.app.bot

        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("status", self._cmd_status))
        self.app.add_handler(CommandHandler("stop", self._cmd_stop))
        self.app.add_handler(CallbackQueryHandler(self._button_handler))

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)
        logger.info("📱 Telegram bot started")

    async def shutdown(self):
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [
                InlineKeyboardButton("📊 Check Strategy", callback_data="check_strategy"),
                InlineKeyboardButton("💹 Status", callback_data="status"),
            ],
            [
                InlineKeyboardButton("📝 Paper Trade", callback_data="start_paper"),
                InlineKeyboardButton("💰 Live Trade", callback_data="start_live"),
            ],
            [
                InlineKeyboardButton("⏹️ Stop Bot", callback_data="stop_bot"),
                InlineKeyboardButton("🔍 Positions", callback_data="positions"),
            ],
        ]
        markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "🤖 <b>Ultra-Fast HFT Bot</b> — Angel One\n"
            "BankNifty/FinNifty Options Trader\n\n"
            "Choose an action:",
            reply_markup=markup,
            parse_mode="HTML",
        )

    async def _cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        msg = await self.engine.get_status_message()
        await update.message.reply_text(msg, parse_mode="HTML")

    async def _cmd_stop(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("🛑 Initiating emergency stop...")
        await self.engine.emergency_stop("Manual /stop command")

    async def _button_handler(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "status":
            msg = await self.engine.get_status_message()
            await query.edit_message_text(msg, parse_mode="HTML")

        elif data == "check_strategy":
            await query.edit_message_text("🔍 Analyzing market conditions...", parse_mode="HTML")
            result = await self.engine.analyze_strategy()
            await query.edit_message_text(result, parse_mode="HTML")

        elif data == "start_paper":
            keyboard = [
                [
                    InlineKeyboardButton("✅ Yes, Start Paper Trade", callback_data="confirm_paper"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
                ]
            ]
            await query.edit_message_text(
                "⚠️ <b>Confirm Paper Trade?</b>\nVirtual trades only. No real money.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )

        elif data == "start_live":
            keyboard = [
                [
                    InlineKeyboardButton("✅ CONFIRM LIVE", callback_data="confirm_live"),
                    InlineKeyboardButton("❌ Cancel", callback_data="cancel"),
                ]
            ]
            await query.edit_message_text(
                "🚨 <b>CONFIRM LIVE TRADE?</b>\nReal money at risk! Capital: ₹20,000\n"
                "Max Loss: ₹1,000 | Target: ₹1,500",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML",
            )

        elif data == "confirm_paper":
            await self.engine.set_mode("paper")
            await query.edit_message_text("✅ <b>Paper Trade Mode Active</b>", parse_mode="HTML")

        elif data == "confirm_live":
            await self.engine.set_mode("live")
            await query.edit_message_text("🚀 <b>Live Trade Mode Active!</b>", parse_mode="HTML")

        elif data == "positions":
            msg = await self.engine.get_positions_message()
            await query.edit_message_text(msg, parse_mode="HTML")

        elif data == "stop_bot":
            await query.edit_message_text("🛑 Emergency stop initiated...")
            await self.engine.emergency_stop("Button: Stop Bot")

        elif data == "cancel":
            await query.edit_message_text("❌ Cancelled.")


# ─────────────────────────────────────────────
# Main Trading Engine
# ─────────────────────────────────────────────
class TradingEngine:
    def __init__(self):
        self.config = Config()
        self.connector = AngelOneConnector(self.config)
        self.state = StateManager()
        self.risk = RiskManager(self.config, self.state)
        self.strategy = AlphaStrategy()
        self.telegram: Optional[TelegramManager] = None
        self.ws_manager: Optional[WebSocketFeedManager] = None
        self._mode = self.config.TRADE_MODE
        self._running = False
        self._last_price: dict = {}
        self._last_alert_price: dict = {}

    async def initialize(self):
        if not self.connector.login():
            raise RuntimeError("Angel One login failed!")
        self.state.load()
        self.telegram = TelegramManager(
            self.config.TELEGRAM_TOKEN,
            self.config.TELEGRAM_CHAT_ID,
            self,
        )
        await self.telegram.setup()

    async def set_mode(self, mode: str):
        self._mode = mode
        self.state.set("trade_mode", mode)
        self.state.save()
        await self.telegram.send(
            f"🔄 Mode changed to <b>{mode.upper()}</b>"
        )

    async def run(self):
        """Main event loop."""
        self._running = True
        await self.telegram.send(
            "🤖 <b>Trading Bot Started!</b>\n"
            f"Mode: <b>{self._mode.upper()}</b>\n"
            f"Capital: ₹{self.config.CAPITAL:,.0f}\n"
            f"Max Loss: ₹{self.config.MAX_DAILY_LOSS:,.0f} | Target: ₹{self.config.DAILY_TARGET:,.0f}"
        )

        # Restore open trade if bot restarted
        open_trade = self.state.get("open_trade")
        if open_trade:
            await self.telegram.send(
                f"🔁 Resuming tracking for: <b>{open_trade.get('symbol')}</b>"
            )

        # Start WebSocket and other tasks concurrently
        tokens = self.state.get("ws_tokens", ["26009"])  # BankNifty default

        ws_task = asyncio.create_task(self._start_ws(tokens))
        candle_task = asyncio.create_task(self._candle_aggregator())
        risk_task = asyncio.create_task(self._risk_monitor_loop())
        status_task = asyncio.create_task(self._periodic_status())

        await asyncio.gather(ws_task, candle_task, risk_task, status_task)

    async def _start_ws(self, tokens: list):
        self.ws_manager = WebSocketFeedManager(self.connector, self._on_tick)
        await self.ws_manager.start(tokens)

    async def _on_tick(self, msg: dict):
        """Sub-millisecond tick processor."""
        try:
            token = str(msg.get("token", ""))
            ltp = msg.get("last_traded_price", 0) / 100.0  # paise → rupees
            volume = msg.get("volume_trade_for_the_day", 0)
            timestamp = msg.get("exchange_timestamp", time.time())

            if ltp <= 0:
                return

            prev_price = self._last_price.get(token, ltp)
            self._last_price[token] = ltp

            # Feed to strategy
            signal = self.strategy.on_tick(token, ltp, volume, timestamp)

            # Execute signal
            if signal and self._running:
                await self._execute_signal(signal, ltp)

            # Trailing SL check
            open_trade = self.state.get("open_trade")
            if open_trade and open_trade.get("token") == token:
                await self._manage_trailing_sl(open_trade, ltp)

            # 1% price alert
            last_alert = self._last_alert_price.get(token, ltp)
            if abs(ltp - last_alert) / last_alert >= 0.01:
                self._last_alert_price[token] = ltp
                direction = "📈" if ltp > last_alert else "📉"
                await self.telegram.send(
                    f"{direction} <b>1% Move</b> | {token}\n"
                    f"Price: ₹{ltp:.2f}"
                )

        except Exception as e:
            logger.error(f"Tick error: {e}")

    async def _execute_signal(self, signal: dict, ltp: float):
        """Fire order within 100ms of signal."""
        t0 = time.monotonic()

        if not self.risk.can_trade():
            return

        symbol = signal["symbol"]
        token = signal["token"]
        action = signal["action"]  # BUY or SELL
        qty = signal["qty"]

        # Limit price with buffer for immediate fill
        if action == "BUY":
            price = round(ltp * 1.002, 1)
        else:
            price = round(ltp * 0.998, 1)

        if self._mode == "live":
            order_id = self.connector.place_order(symbol, token, action, qty, price)
        else:
            order_id = f"PAPER-{int(time.time())}"

        latency_ms = (time.monotonic() - t0) * 1000

        if order_id:
            trade = {
                "order_id": order_id,
                "symbol": symbol,
                "token": token,
                "action": action,
                "qty": qty,
                "entry_price": price,
                "sl_price": signal.get("sl"),
                "target_price": signal.get("target"),
                "entry_time": datetime.now().isoformat(),
                "peak_profit": 0,
                "breakeven_moved": False,
                "trailing_active": False,
            }
            self.state.set("open_trade", trade)
            self.state.save()

            await self.telegram.send(
                f"🚀 <b>{'PAPER ' if self._mode == 'paper' else ''}ORDER PLACED</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Action: <b>{action}</b> @ ₹{price:.2f}\n"
                f"Qty: {qty} | SL: ₹{signal.get('sl', 0):.2f}\n"
                f"Target: ₹{signal.get('target', 0):.2f}\n"
                f"⚡ Latency: <b>{latency_ms:.1f}ms</b>"
            )

    async def _manage_trailing_sl(self, trade: dict, ltp: float):
        """Zero-loss trailing stop logic."""
        entry = trade["entry_price"]
        sl = trade["sl_price"]
        action = trade["action"]

        if action == "BUY":
            profit = (ltp - entry) * trade["qty"]
        else:
            profit = (entry - ltp) * trade["qty"]

        trade["peak_profit"] = max(trade.get("peak_profit", 0), profit)

        # Move to breakeven
        if profit >= self.config.BREAKEVEN_TRIGGER and not trade.get("breakeven_moved"):
            trade["sl_price"] = entry
            trade["breakeven_moved"] = True
            self.state.set("open_trade", trade)
            self.state.save()
            await self.telegram.send(
                f"🔒 <b>SL → Break-Even</b>\n"
                f"Symbol: <code>{trade['symbol']}</code>\n"
                f"SL moved to entry: ₹{entry:.2f}\n"
                f"Current P&L: ₹{profit:.0f}"
            )

        # Activate trailing
        elif profit >= self.config.TRAIL_TRIGGER:
            if not trade.get("trailing_active"):
                trade["trailing_active"] = True

            # Trail SL by every ₹100 profit increment
            trail_count = int((profit - self.config.TRAIL_TRIGGER) / self.config.TRAIL_STEP)
            if action == "BUY":
                new_sl = entry + (trail_count * self.config.TRAIL_STEP / trade["qty"])
            else:
                new_sl = entry - (trail_count * self.config.TRAIL_STEP / trade["qty"])

            if action == "BUY" and new_sl > trade["sl_price"]:
                trade["sl_price"] = new_sl
                self.state.set("open_trade", trade)
                self.state.save()
                await self.telegram.send(
                    f"📈 <b>SL Trailed UP</b>\n"
                    f"Symbol: <code>{trade['symbol']}</code>\n"
                    f"New SL: ₹{new_sl:.2f} | P&L: ₹{profit:.0f}"
                )
            elif action == "SELL" and new_sl < trade["sl_price"]:
                trade["sl_price"] = new_sl
                self.state.set("open_trade", trade)
                self.state.save()
                await self.telegram.send(
                    f"📉 <b>SL Trailed DOWN</b>\n"
                    f"Symbol: <code>{trade['symbol']}</code>\n"
                    f"New SL: ₹{new_sl:.2f} | P&L: ₹{profit:.0f}"
                )

        # SL Hit check
        if action == "BUY" and ltp <= sl:
            await self._exit_trade(trade, ltp, "SL HIT 🔴")
        elif action == "SELL" and ltp >= sl:
            await self._exit_trade(trade, ltp, "SL HIT 🔴")
        elif action == "BUY" and ltp >= trade.get("target_price", 1e9):
            await self._exit_trade(trade, ltp, "TARGET HIT 🎯")
        elif action == "SELL" and ltp <= trade.get("target_price", 0):
            await self._exit_trade(trade, ltp, "TARGET HIT 🎯")

    async def _exit_trade(self, trade: dict, ltp: float, reason: str):
        """Exit position and update P&L."""
        symbol = trade["symbol"]
        token = trade["token"]
        action = "SELL" if trade["action"] == "BUY" else "BUY"
        qty = trade["qty"]
        exit_price = round(ltp * 0.998 if action == "SELL" else ltp * 1.002, 1)

        if self._mode == "live":
            self.connector.place_order(symbol, token, action, qty, exit_price)

        entry = trade["entry_price"]
        if trade["action"] == "BUY":
            pnl = (exit_price - entry) * qty
        else:
            pnl = (entry - exit_price) * qty

        self.risk.record_pnl(pnl)
        self.state.set("open_trade", None)
        self.state.save()

        await self.telegram.send(
            f"{'🎯' if 'TARGET' in reason else '🔴'} <b>{reason}</b>\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Exit @ ₹{exit_price:.2f}\n"
            f"P&L: <b>₹{pnl:+.0f}</b>\n"
            f"Day P&L: <b>₹{self.risk.daily_pnl:+.0f}</b>"
        )

        # Check daily loss limit
        if self.risk.daily_pnl <= -self.config.MAX_DAILY_LOSS:
            await self.emergency_stop("Max daily loss reached")
        elif self.risk.daily_pnl >= self.config.DAILY_TARGET:
            await self.emergency_stop("Daily target achieved 🎉")

    async def _candle_aggregator(self):
        """Aggregate ticks into 5-min candles for strategy."""
        while self._running:
            await asyncio.sleep(1)
            self.strategy.aggregate_candles()

    async def _risk_monitor_loop(self):
        """Continuous risk check every second."""
        while self._running:
            await asyncio.sleep(1)
            if self.risk.daily_pnl <= -self.config.MAX_DAILY_LOSS:
                await self.emergency_stop("Risk limit breach!")

    async def _periodic_status(self):
        """Send status every hour."""
        while self._running:
            await asyncio.sleep(3600)
            msg = await self.get_status_message()
            await self.telegram.send(msg)

    async def get_status_message(self) -> str:
        balance = self.connector.get_funds()
        day_pnl = self.risk.daily_pnl
        trade = self.state.get("open_trade")
        vix = await self._get_vix()
        health = self._market_health_index(vix)

        trade_info = "None"
        if trade:
            trade_info = (
                f"{trade['symbol']} {trade['action']} @ ₹{trade['entry_price']:.2f}\n"
                f"   SL: ₹{trade['sl_price']:.2f}"
            )

        return (
            f"📊 <b>Bot Status</b> — {datetime.now().strftime('%H:%M:%S')}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Balance: ₹{balance:,.0f}\n"
            f"📈 Day P&L: <b>₹{day_pnl:+,.0f}</b>\n"
            f"📉 Max Loss: ₹{self.config.MAX_DAILY_LOSS:,.0f} | Target: ₹{self.config.DAILY_TARGET:,.0f}\n"
            f"🎯 Open Trade: {trade_info}\n"
            f"🌡️ VIX: {vix:.2f} | Market Health: {health}\n"
            f"⚙️ Mode: <b>{self._mode.upper()}</b>"
        )

    async def get_positions_message(self) -> str:
        positions = self.connector.get_positions()
        if not positions:
            return "📭 <b>No Open Positions</b>"
        lines = ["📋 <b>Open Positions</b>\n━━━━━━━━━━━━━━━"]
        for p in positions:
            if int(p.get("netqty", 0)) != 0:
                pnl = float(p.get("unrealisedpnl", 0))
                lines.append(
                    f"• <code>{p['tradingsymbol']}</code>\n"
                    f"  Qty: {p['netqty']} | P&L: ₹{pnl:+.0f}"
                )
        return "\n".join(lines)

    async def analyze_strategy(self) -> str:
        signal = self.strategy.get_current_signal()
        vix = await self._get_vix()
        health = self._market_health_index(vix)

        if vix > 20:
            return (
                f"⚠️ <b>Market Unstable — Avoid</b>\n"
                f"VIX: {vix:.2f} (High volatility)\n"
                f"Market Health: {health}\n\n"
                f"Recommendation: Wait for VIX < 20"
            )

        if signal:
            return (
                f"✅ <b>Strong Signal Found!</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"Symbol: <code>{signal.get('symbol', 'N/A')}</code>\n"
                f"Action: <b>{signal.get('action', 'N/A')}</b>\n"
                f"RSI: {signal.get('rsi', 0):.1f} | Supertrend: {signal.get('supertrend', 'N/A')}\n"
                f"Volume Spike: {'✅' if signal.get('volume_spike') else '❌'}\n"
                f"VIX: {vix:.2f} | Health: {health}"
            )
        return (
            f"🔍 <b>No Clear Signal</b>\n"
            f"Conditions not fully met.\n"
            f"VIX: {vix:.2f} | Health: {health}\n\n"
            f"Strategy requires: Supertrend + RSI + Volume Spike"
        )

    async def _get_vix(self) -> float:
        try:
            ltp = self.connector.get_ltp("NSE", "India VIX", "1")
            return ltp if ltp > 0 else 15.0
        except Exception:
            return 15.0

    def _market_health_index(self, vix: float) -> str:
        if vix < 13:
            return "🟢 Excellent"
        elif vix < 17:
            return "🟡 Good"
        elif vix < 20:
            return "🟠 Cautious"
        else:
            return "🔴 Dangerous"

    async def emergency_stop(self, reason: str):
        """Hard stop — liquidate all and shutdown."""
        logger.critical(f"EMERGENCY STOP: {reason}")
        self._running = False

        if self.ws_manager:
            self.ws_manager.stop()

        # Square off all positions
        if self._mode == "live":
            success = self.connector.square_off_all()
            sq_status = "✅ All squared off" if success else "⚠️ Manual check required"
        else:
            sq_status = "📝 Paper mode — no real positions"

        self.state.set("open_trade", None)
        self.state.set("bot_killed_today", date.today().isoformat())
        self.state.save()

        await self.telegram.send(
            f"🛑 <b>BOT EMERGENCY STOP</b>\n"
            f"Reason: {reason}\n"
            f"{sq_status}\n"
            f"Day P&L: <b>₹{self.risk.daily_pnl:+,.0f}</b>\n\n"
            f"Bot process will now terminate."
        )

        await asyncio.sleep(2)
        await self.telegram.shutdown()
        sys.exit(0)


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
async def main():
    engine = TradingEngine()

    # Graceful shutdown on SIGTERM (Railway.app sends this)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(engine.emergency_stop("System signal received")),
        )

    try:
        await engine.initialize()
        await engine.run()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        if engine.telegram:
            await engine.telegram.send(f"💥 <b>FATAL ERROR</b>\n<code>{e}</code>")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
