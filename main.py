
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
    INDEX_OVERRIDES = load_json_env("ANGEL_INDEX_MAP_JSON", {})

    @classmethod
    def validate(cls) -> None:
    def missing_angel_credentials(cls) -> List[str]:
        required = {
            "ANGEL_API_KEY": cls.API_KEY,
            "ANGEL_CLIENT_ID": cls.CLIENT_ID,
            "ANGEL_PASSWORD": cls.PASSWORD,
            "ANGEL_TOTP_SECRET": cls.TOTP_SECRET,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        return [name for name, value in required.items() if not value]

    @classmethod
    def use_demo_mode(cls) -> bool:
        return cls.TRADE_MODE == "paper" and bool(cls.missing_angel_credentials())

    @classmethod
    def validate(cls) -> None:
        if cls.TRADE_MODE not in {"paper", "live"}:
            raise RuntimeError("TRADE_MODE must be 'paper' or 'live'")
        if cls.ENTRY_ORDER_TYPE not in {"MARKET", "LIMIT"}:
            raise RuntimeError("ENTRY_ORDER_TYPE must be MARKET or LIMIT")
        missing = cls.missing_angel_credentials()
        if cls.TRADE_MODE == "live" and missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
        if cls.use_demo_mode():
            logger.warning(
                "Angel credentials missing (%s). Starting in demo simulator mode.",
                ", ".join(missing),
            )


config = Config()
        self.ws_manager: Optional[WebSocketManager] = None
        self.underlying_instruments: Dict[str, Instrument] = {}
        self.vix_instrument: Optional[Instrument] = None
        self.demo_mode = False
        self._simulation_prices: Dict[str, float] = {}
        self._rng = random.Random()
        self._running = False
        self._peak_profit = 0.0

    async def initialize(self) -> None:
        Config.validate()
        if not self.connector.login():
            raise RuntimeError("Angel One login failed")
        self.demo_mode = Config.use_demo_mode()
        if self.demo_mode:
            self._initialize_demo_market()
        else:
            if not self.connector.login():
                raise RuntimeError("Angel One login failed")

        self.instrument_master.load()
        self._resolve_market_instruments()
        self._refresh_reference_prices()
            self.instrument_master.load()
            self._resolve_market_instruments()
            self._refresh_reference_prices()

        if config.TELEGRAM_TOKEN and config.TELEGRAM_CHAT_ID:
            self.telegram = Bot(token=config.TELEGRAM_TOKEN)
                f"Capital: Rs {config.CAPITAL:,.0f}\n"
                f"Max Loss: Rs {config.MAX_DAILY_LOSS:,.0f}\n"
                f"Max Trades: {config.MAX_TRADES_PER_DAY}\n"
                f"Mode: {config.TRADE_MODE.upper()}\n"
                f"Mode: {'DEMO_SIMULATOR' if self.demo_mode else config.TRADE_MODE.upper()}\n"
                "Strategies: BANKNIFTY_PUT / NIFTY_PUT / FINNIFTY_PUT"
            )

    def _initialize_demo_market(self) -> None:
        base_prices = {"BANKNIFTY": 52000.0, "NIFTY": 23500.0, "FINNIFTY": 23000.0}
        self._simulation_prices = {**base_prices, "vix": 15.0}
        self.underlying_instruments = {
            key: Instrument(exchange="NSE", tradingsymbol=f"SIM_{key}", token=f"SIM_{key}", name=key)
            for key in UNDERLYINGS
        }
        self.vix_instrument = Instrument(
            exchange="NSE",
            tradingsymbol="SIM_VIX",
            token="SIM_VIX",
            name="VIX",
        )
        for key, price in base_prices.items():
            self.reference_prices.update(key, price)
        logger.info("Demo simulator mode initialized without Angel credentials")

    def _advance_simulation(self) -> None:
        for key in UNDERLYINGS:
            current = self._simulation_prices.get(key, 0.0)
            move = self._rng.uniform(-0.0025, 0.0025)
            self._simulation_prices[key] = round(max(100.0, current * (1 + move)), 2)
        vix = self._simulation_prices.get("vix", 15.0)
        self._simulation_prices["vix"] = round(min(30.0, max(11.0, vix + self._rng.uniform(-0.35, 0.35))), 2)

    def _get_demo_market_data(self) -> Dict[str, float]:
        self._advance_simulation()
        market_data: Dict[str, float] = {}
        for key in UNDERLYINGS:
            price = self._simulation_prices[key]
            reference = self.reference_prices.get(key) or price
            market_data[key] = price
            market_data[f"{key}_change"] = self.connector.get_change_pct(price, reference)
        market_data["vix"] = self._simulation_prices["vix"]
        market_data["time"] = now_ist().hour * 100 + now_ist().minute
        return market_data

    def _next_demo_expiry(self) -> date:
        today = now_ist().date()
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        return today + timedelta(days=days_ahead)

    def _build_demo_contract(self, definition: UnderlyingDefinition, spot_price: float) -> OptionContract:
        strike = round(spot_price / definition.strike_step) * definition.strike_step
        expiry = self._next_demo_expiry()
        return OptionContract(
            exchange="NFO",
            tradingsymbol=f"SIM-{definition.option_name}-{expiry:%d%b%y}-{int(strike)}PE".upper(),
            token=f"SIM_{definition.key}_{int(strike)}_PE",
            name=definition.option_name,
            strike=float(strike),
            expiry=expiry,
            option_type="PE",
            lot_size=definition.default_lot_size,
            tick_size=0.05,
        )

    def _estimate_demo_option_ltp(
        self,
        definition: UnderlyingDefinition,
        strike: float,
        spot_price: float,
        vix: float,
        change_pct: float,
        elapsed_minutes: float = 0.0,
    ) -> float:
        intrinsic = max(strike - spot_price, 0.0)
        base_time_value = {
            "BANKNIFTY": 220.0,
            "NIFTY": 140.0,
            "FINNIFTY": 110.0,
        }[definition.key]
        volatility_factor = 1.0 + max(vix - 15.0, 0.0) / 18.0 + abs(change_pct) / 6.0
        decay_factor = max(0.45, 1.0 - (elapsed_minutes / 240.0))
        noise_factor = 1.0 + self._rng.uniform(-0.02, 0.02)
        premium = (base_time_value * decay_factor + intrinsic * 0.9) * volatility_factor * noise_factor
        return round(max(5.0, premium), 2)

    def _get_demo_trade_ltp(self, trade: Trade, market_data: Dict[str, float]) -> float:
        underlying_key = trade.strategy.replace("_PUT", "")
        definition = UNDERLYINGS[underlying_key]
        elapsed_minutes = max(0.0, (now_ist() - trade.entry_time).total_seconds() / 60.0)
        return self._estimate_demo_option_ltp(
            definition=definition,
            strike=trade.contract.strike,
            spot_price=market_data.get(underlying_key, trade.contract.strike),
            vix=market_data.get("vix", 15.0),
            change_pct=market_data.get(f"{underlying_key}_change", 0.0),
            elapsed_minutes=elapsed_minutes,
        )

    def _resolve_market_instruments(self) -> None:
        for key, definition in UNDERLYINGS.items():
            self.underlying_instruments[key] = self.connector.resolve_spot_instrument(key, definition)

    async def run(self) -> None:
        self._running = True
        if not self.ws_manager:
        if not self.demo_mode and not self.ws_manager:
            initial_instruments = list(self.underlying_instruments.values())
            if self.vix_instrument:
                initial_instruments.append(self.vix_instrument)
            self.ws_manager = WebSocketManager(self.connector, self.on_tick)
            asyncio.create_task(self.ws_manager.start(initial_instruments))

        await self.send_message("Bot running and waiting for market conditions.")
        await self.send_message(
            "Bot running and waiting for market conditions."
            if not self.demo_mode
            else "Demo simulator running without Angel login."
        )

        while self._running:
            try:
                self.state.roll_day()
                if any(self.reference_prices.get(key) <= 0 for key in self.underlying_instruments):
                if not self.demo_mode and any(self.reference_prices.get(key) <= 0 for key in self.underlying_instruments):
                    self._refresh_reference_prices()

                market_data: Dict[str, float] = {}
                if self.demo_mode:
                    market_data = self.get_market_data()
                    if self.state.active_trade:
                        await self._check_trade_exit(self._get_demo_trade_ltp(self.state.active_trade, market_data))

                if not self._is_market_open():
                    await asyncio.sleep(30)
                    continue

                can_trade, reason = self.state.can_trade()
                if can_trade:
                    market_data = self.get_market_data()
                    if not market_data:
                        market_data = self.get_market_data()
                    strategy, confidence, scores = self.scorer.get_best_strategy(market_data)
                    self.state.strategy_scores = scores
                    if confidence >= 40:
                await asyncio.sleep(10)

    def get_market_data(self) -> Dict[str, float]:
        if self.demo_mode:
            return self._get_demo_market_data()

        market_data: Dict[str, float] = {}
        for key, instrument in self.underlying_instruments.items():
            ltp = self.connector.get_ltp(instrument)
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
        if self.demo_mode:
            contract = self._build_demo_contract(definition, spot_price)
            option_ltp = self._estimate_demo_option_ltp(
                definition=definition,
                strike=contract.strike,
                spot_price=spot_price,
                vix=market_data.get("vix", 15.0),
                change_pct=market_data.get(f"{underlying_key}_change", 0.0),
            )
        else:
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
        self.state.start_trade(trade)
        self._peak_profit = 0.0

        if self.ws_manager:
        if self.ws_manager and not self.demo_mode:
            self.ws_manager.subscribe_instruments([contract])

        await self.send_message(
        )

    def _is_market_open(self) -> bool:
        if self.demo_mode:
            return True
        now = now_ist()
        if now.weekday() >= 5:
            return False
            "AUTO TRADER STATUS",
            f"Day P&L: Rs {self.state.daily_pnl:+.0f}",
            f"Trades: {self.state.trades_today}/{config.MAX_TRADES_PER_DAY}",
            f"Mode: {config.TRADE_MODE.upper()}",
            f"Mode: {'DEMO_SIMULATOR' if self.demo_mode else config.TRADE_MODE.upper()}",
            f"VIX: {market_data.get('vix', 0.0):.2f}",
            "",
            "Market:",
