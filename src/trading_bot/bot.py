"""
Trading Bot Orchestrator – WIRED CORRECTLY WITH FIXED CALLBACKS

This version contains:
- Correct WebSocket callback handling (callback(candle) only)
- Fixed closure capturing for (symbol, timeframe)
- Proper CandleSync callback wiring: callback(symbol, timeframe, candle)
"""

import os
import time
import signal
import sys
import threading
from typing import Dict, Tuple
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify
from concurrent.futures import ThreadPoolExecutor, as_completed

from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api.rest_client import RestClient
from trading_bot.api.ws_client import WebSocketClient
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync
from trading_bot.core.candles.initial_candles_loader import InitialCandlesLoader
from trading_bot.core.logger import get_logger, get_symbol_logger
from trading_bot.storage.parquet_storage import ParquetStorage


class TradingBot:
    """
    Main orchestrator for multi-symbol, multi-timeframe trading engine.
    """

    def __init__(self):
        load_dotenv()
        self.logger = get_logger(__name__)

        # Runtime state
        self.is_running = False
        self.app_config = None
        self.symbols_config = None
        self.rest_client: RestClient | None = None
        self.ws_client: WebSocketClient | None = None
        self.storage: ParquetStorage | None = None

        # Structures
        # key = (symbol, timeframe)
        self.candle_managers: Dict[Tuple[str, str], CandleManager] = {}
        self.candle_syncs: Dict[Tuple[str, str], CandleSync] = {}

        # Health API
        self.health_app: Flask | None = None
        self.health_server_thread: threading.Thread | None = None
        self.health_port = int(os.getenv("HEALTH_PORT", "8080"))

        # Shutdown handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    # -------------------------------------------------------------------------
    # SIGNAL HANDLER
    # -------------------------------------------------------------------------

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    # -------------------------------------------------------------------------
    # INITIALIZATION
    # -------------------------------------------------------------------------

    def initialize(self) -> bool:
        self.logger.info("=" * 80)
        self.logger.info("Initializing Trading Bot")
        self.logger.info("=" * 80)

        # -------------------------------------------------
        # Load configurations
        # -------------------------------------------------
        self.logger.info("[1/6] Loading configurations...")
        try:
            config_dir = os.getenv("CONFIG_DIR", "config")
            self.app_config = load_app_config(f"{config_dir}/app.yml")
            self.symbols_config = load_symbols_config(f"{config_dir}/symbols.yml")

            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]

            if not enabled_symbols:
                self.logger.error("No enabled symbols found.")
                return False

            self.logger.info(f"✓ App config loaded: {self.app_config.app.name}")
            self.logger.info(f"✓ Symbols config loaded: {len(enabled_symbols)} enabled symbols")
            for s in enabled_symbols:
                tfs = [tf.tf for tf in s.timeframes]
                self.logger.info(f"  - {s.name}: {tfs}")

        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            return False

        # -------------------------------------------------
        # Init Parquet storage
        # -------------------------------------------------
        self.logger.info("\n[2/6] Initializing Parquet storage...")
        try:
            self.storage = ParquetStorage(
                base_path=os.getenv("DATA_DIR", "data/live"),
                retention_days=int(os.getenv("PARQUET_RETENTION_DAYS", "7")),
            )
            self.logger.info("✓ Parquet storage initialized.")
        except Exception as e:
            self.logger.error(f"Parquet storage error: {e}")
            return False

        # -------------------------------------------------
        # REST client
        # -------------------------------------------------
        self.logger.info("\n[3/6] Creating REST client...")
        try:
            self.rest_client = RestClient(self.app_config)
            self.logger.info(f"✓ REST client created: {self.app_config.exchange.rest_endpoint}")
        except Exception as e:
            self.logger.error(f"REST client error: {e}")
            return False

        # -------------------------------------------------
        # Build CandleManager + CandleSync for each TF
        # -------------------------------------------------
        self.logger.info("\n[4/6] Creating Candle Managers & Syncs...")
        enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]

        for sym_cfg in enabled_symbols:
            symbol = sym_cfg.name
            get_symbol_logger(symbol)

            for tf_cfg in sym_cfg.timeframes:
                tf = tf_cfg.tf
                key = (symbol, tf)

                try:
                    cm = CandleManager(max_size=tf_cfg.fetch)
                    sync = CandleSync(
                        rest_client=self.rest_client,
                        symbol=symbol,
                        timeframe=tf,
                        candle_manager=cm,
                        app_config=self.app_config,
                    )
                    # IMPORTANT: CandleSync callback is (symbol, timeframe, candle)
                    sync.set_callback(self._on_validated_candle)

                    self.candle_managers[key] = cm
                    self.candle_syncs[key] = sync

                    self.logger.info(f"✓ Managers created for {symbol} {tf}")
                except Exception as e:
                    self.logger.error(f"Failed to create managers for {symbol} {tf}: {e}")

        if not self.candle_managers:
            self.logger.error("No candle managers created!")
            return False

        # -------------------------------------------------
        # Initial candles loading (parallel)
        # -------------------------------------------------
        self.logger.info("\n[5/6] Loading initial historical candles (parallel)...")
        loader = InitialCandlesLoader(self.app_config, self.rest_client)

        with ThreadPoolExecutor(max_workers=len(enabled_symbols)) as executor:
            tasks: Dict[object, str] = {}

            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name

                cm_dict = {
                    tf_cfg.tf: self.candle_managers[(symbol, tf_cfg.tf)]
                    for tf_cfg in sym_cfg.timeframes
                }
                sync_dict = {
                    tf_cfg.tf: self.candle_syncs[(symbol, tf_cfg.tf)]
                    for tf_cfg in sym_cfg.timeframes
                }

                fut = executor.submit(
                    loader.load_initial_for_symbol,
                    symbol_cfg=sym_cfg,
                    candle_managers=cm_dict,
                    candle_syncs=sync_dict,
                    liquidity_maps=None,
                    storage=self.storage,
                )
                tasks[fut] = symbol
                self.logger.info(f"Submitted {symbol} for parallel initial load...")

            for fut in as_completed(tasks):
                symbol = tasks[fut]
                try:
                    ok = fut.result()
                    if ok:
                        self.logger.info(f"✓ {symbol}: initial load complete")
                    else:
                        self.logger.warning(f"{symbol}: initial load incomplete (some TFs may have failed)")
                except Exception as e:
                    self.logger.error(f"{symbol}: initial load error: {e}")

        # -------------------------------------------------
        # Setup WebSocket client & subscriptions
        # -------------------------------------------------
        self.logger.info("\n[6/6] Setting up WebSocket client & subscriptions...")
        try:
            self.ws_client = WebSocketClient(self.app_config)

            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name

                for tf_cfg in sym_cfg.timeframes:
                    tf = tf_cfg.tf

                    # ---- FIXED CALLBACK CLOSURE ----
                    def make_callback(sym: str, timeframe: str):
                        km_key = (sym, timeframe)

                        def cb(candle: dict):
                            """
                            WebSocket callback for one stream.
                            IMPORTANT: WebSocketClient must call this with ONE arg → candle dict.
                            """
                            try:
                                sync_obj = self.candle_syncs[km_key]
                                sync_obj.on_ws_closed_candle(candle, storage=self.storage)
                            except Exception as exc:
                                self.logger.error(
                                    f"[WebSocket] Callback error for {sym} {timeframe}: {exc}"
                                )

                        return cb

                    self.ws_client.subscribe(
                        symbol=symbol,
                        timeframe=tf,
                        callback=make_callback(symbol, tf),
                    )
                    self.logger.info(f"✓ Subscribed WS stream for {symbol} {tf}")

            self.logger.info("✓ WebSocket client configured")

        except Exception as e:
            self.logger.error(f"WebSocket setup error: {e}")
            return False

        self._start_health_api()
        self.logger.info("\nInitialization COMPLETE ✅")
        return True

    # -------------------------------------------------------------------------
    # HEALTH ENDPOINT
    # -------------------------------------------------------------------------

    def _start_health_api(self):
        """Start minimal HTTP health check endpoint."""
        app = Flask(__name__)
        self.health_app = app

        @app.get("/health")
        def health():
            symbols_status: Dict[str, Dict[str, dict]] = {}

            for (symbol, tf), cm in self.candle_managers.items():
                if symbol not in symbols_status:
                    symbols_status[symbol] = {"timeframes": {}}
                symbols_status[symbol]["timeframes"][tf] = {
                    "candles": len(cm.get_all()),
                    "last_ts": cm.last_timestamp(),
                }

            return jsonify(
                {
                    "status": "running" if self.is_running else "stopped",
                    "timestamp": datetime.now().isoformat(),
                    "symbols": list(symbols_status.keys()),
                    "symbols_detail": symbols_status,
                    "websocket": "connected"
                    if (self.ws_client and self.ws_client.is_alive())
                    else "disconnected",
                }
            )

        def run_api():
            app.run(
                host="0.0.0.0",
                port=self.health_port,
                debug=False,
                use_reloader=False,
            )

        self.health_server_thread = threading.Thread(target=run_api, daemon=True)
        self.health_server_thread.start()

        self.logger.info(f"✓ Health API running at http://0.0.0.0:{self.health_port}/health")

    # -------------------------------------------------------------------------
    # CALLBACK FOR VALIDATED CANDLE (FROM CandleSync)
    # -------------------------------------------------------------------------

    def _on_validated_candle(self, symbol: str, timeframe: str, candle: dict):
        """
        Called by CandleSync when a candle is fully validated & gap-filled.

        Here is where future LiquidityMap / Strategy Engine / Execution Layer will attach.
        """
        self.logger.info(
            f"[VALIDATED] {symbol} {timeframe} "
            f"open_ts={candle['ts']} close={candle['close']} vol={candle['volume']}"
        )
        # TODO:
        # - LiquidityMap.on_candle(...)
        # - Strategy evaluation
        # - Signal aggregation & execution

    # -------------------------------------------------------------------------
    # START & RUN
    # -------------------------------------------------------------------------

    def start(self) -> bool:
        """Start the trading bot (WebSocket loop)."""
        if not self.ws_client:
            self.logger.error("Bot not initialized. Call initialize() first.")
            return False

        self.logger.info("\nStarting Trading Bot WebSocket...")
        try:
            self.ws_client.start()
            self.is_running = True
            self.logger.info("🚀 Trading Bot is LIVE and processing candles!")
            return True
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket: {e}")
            return False

    def run(self):
        """Main run loop (status/logging)."""
        try:
            while self.is_running:
                time.sleep(60)
                self._print_status()
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received.")
        finally:
            self.stop()

    def _print_status(self):
        """Periodic status log."""
        self.logger.info("\n" + "─" * 70)
        self.logger.info("Status Update")
        self.logger.info("─" * 70)

        symbols_data: Dict[str, Dict[str, dict]] = {}

        for (symbol, tf), cm in self.candle_managers.items():
            if symbol not in symbols_data:
                symbols_data[symbol] = {}
            last_candle = cm.get_latest_candle()
            symbols_data[symbol][tf] = {
                "candle_count": len(cm.get_all()),
                "last_candle": last_candle,
            }

        for symbol, tfs in symbols_data.items():
            self.logger.info(f"\n{symbol}:")
            for tf, data in tfs.items():
                last_c = data["last_candle"]
                close_val = last_c["close"] if last_c else 0.0
                last_ts = last_c["ts"] if last_c else None
                self.logger.info(
                    f"  {tf}: {data['candle_count']} candles, "
                    f"last_open_ts={last_ts}, close={close_val}"
                )

    # -------------------------------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------------------------------

    def stop(self):
        """Stop the trading bot gracefully."""
        if not self.is_running:
            return

        self.logger.info("\n" + "=" * 80)
        self.logger.info("Stopping Trading Bot...")
        self.logger.info("=" * 80)

        self.is_running = False

        # Stop WebSocket
        if self.ws_client:
            self.logger.info("Stopping WebSocket...")
            self.ws_client.stop()
            self.logger.info("✓ WebSocket stopped")

        # Close REST client
        if self.rest_client:
            self.logger.info("Closing REST client...")
            self.rest_client.close()
            self.logger.info("✓ REST client closed")

        # Shutdown storage executor
        if self.storage:
            self.logger.info("Shutting down storage...")
            try:
                self.storage.shutdown()
                self.logger.info("✓ Storage shutdown complete")
            except Exception as e:
                self.logger.error(f"Error shutting down storage: {e}")

        self.logger.info("\n" + "=" * 80)
        self.logger.info("✓ Trading Bot Stopped")
        self.logger.info("=" * 80)


def main() -> int:
    """Main entry point."""
    logger = get_logger(__name__)
    bot = TradingBot()

    if not bot.initialize():
        logger.error("Failed to initialize bot!")
        return 1

    if not bot.start():
        logger.error("Failed to start bot!")
        return 1

    bot.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
