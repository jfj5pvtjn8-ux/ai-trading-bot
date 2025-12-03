"""
Trading Bot Orchestrator – Unified Candle Pipeline (Pro Edition)

Wires together:
- App + Symbols config
- RestClient (Binance REST)
- WebSocketClient (Binance WS, closed candles only)
- CandleManager (per symbol/timeframe, open-ts based)
- CandleSync (gap-filling + validation)
- ParquetStorage (async parquet writes)
- /health HTTP endpoint (Flask)
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

    One CandleManager + CandleSync per (symbol, timeframe):

        RestClient  → InitialCandlesLoader → CandleManager + CandleSync.seed()
        WebSocket   → WebSocketClient      → CandleSync.on_ws_closed_candle()
                    → CandleManager + ParquetStorage + Strategy Callback
    """

    def __init__(self) -> None:
        load_dotenv()
        self.logger = get_logger(__name__)

        # Runtime state
        self.is_running: bool = False
        self.app_config = None
        self.symbols_config = None
        self.rest_client: RestClient | None = None
        self.ws_client: WebSocketClient | None = None
        self.storage: ParquetStorage | None = None

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

    def _signal_handler(self, signum, frame) -> None:
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
        self.logger.info("[1/7] Loading configurations...")
        try:
            config_dir = os.getenv("CONFIG_DIR", "config")
            self.app_config = load_app_config(f"{config_dir}/app.yml")
            self.symbols_config = load_symbols_config(f"{config_dir}/symbols.yml")

            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]

            if not enabled_symbols:
                self.logger.error("No enabled symbols found.")
                return False

            self.logger.info(f"✓ App config loaded: {self.app_config.app.name}")
            self.logger.info(
                f"✓ Symbols config loaded: {len(enabled_symbols)} enabled symbols"
            )
            for s in enabled_symbols:
                tfs = [tf.tf for tf in s.timeframes]
                self.logger.info(f"  - {s.name}: {tfs}")

        except Exception as e:
            self.logger.error(f"Config load error: {e}")
            return False

        # -------------------------------------------------
        # Init Parquet storage
        # -------------------------------------------------
        self.logger.info("\n[2/7] Initializing Parquet storage...")
        try:
            self.storage = ParquetStorage(
                base_path=os.getenv("DATA_DIR", "data/live"),
                retention_days=int(os.getenv("PARQUET_RETENTION_DAYS", "7")),
                # usually True in dev, False in prod
                fresh_start=os.getenv("PARQUET_FRESH_START", "true").lower() == "true",
            )
            self.logger.info("✓ Parquet storage initialized.")
        except Exception as e:
            self.logger.error(f"Parquet storage error: {e}")
            return False

        # -------------------------------------------------
        # REST client
        # -------------------------------------------------
        self.logger.info("\n[3/7] Creating REST client...")
        try:
            self.rest_client = RestClient(self.app_config)
            self.logger.info(
                f"✓ REST client created: {self.app_config.exchange.rest_endpoint}"
            )
        except Exception as e:
            self.logger.error(f"REST client error: {e}")
            return False

        # -------------------------------------------------
        # Build CandleManager + CandleSync for each TF
        # -------------------------------------------------
        self.logger.info("\n[4/7] Creating Candle Managers & Syncs...")
        enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]

        for sym_cfg in enabled_symbols:
            symbol = sym_cfg.name
            get_symbol_logger(symbol)

            for tf_cfg in sym_cfg.timeframes:
                tf = tf_cfg.tf
                key = (symbol, tf)

                try:
                    tf_sec = self.app_config.get_timeframe_seconds(tf)

                    cm = CandleManager(
                        max_size=tf_cfg.fetch,
                        timeframe_seconds=tf_sec,
                    )
                    sync = CandleSync(
                        rest_client=self.rest_client,
                        symbol=symbol,
                        timeframe=tf,
                        candle_manager=cm,
                        app_config=self.app_config,
                    )
                    # CandleSync callback is (symbol, timeframe, candle)
                    sync.set_callback(self._on_validated_candle)

                    self.candle_managers[key] = cm
                    self.candle_syncs[key] = sync

                    self.logger.info(
                        f"✓ Managers created for {symbol} {tf} "
                        f"(window={tf_cfg.fetch}, step={tf_sec}s)"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to create managers for {symbol} {tf}: {e}"
                    )

        if not self.candle_managers:
            self.logger.error("No candle managers created!")
            return False

        # -------------------------------------------------
        # Initial candles loading (parallel)
        # -------------------------------------------------
        self.logger.info("\n[5/7] Loading initial historical candles (parallel)...")
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
                    liquidity_maps=None,  # plug LM here later
                    storage=self.storage,
                )
                tasks[fut] = symbol
                self.logger.info(
                    f"Submitted {symbol} for parallel initial load..."
                )

            for fut in as_completed(tasks):
                symbol = tasks[fut]
                try:
                    ok = fut.result()
                    if ok:
                        self.logger.info(f"✓ {symbol}: initial load complete")
                    else:
                        self.logger.warning(
                            f"{symbol}: initial load incomplete "
                            f"(some TFs may have failed)"
                        )
                except Exception as e:
                    self.logger.error(f"{symbol}: initial load error: {e}")

        # -------------------------------------------------
        # Optional reverse gap recovery (backwards check)
        # -------------------------------------------------
        self.logger.info("\n[6/7] Optional reverse gap recovery...")
        try:
            for (symbol, tf), sync in self.candle_syncs.items():
                try:
                    sync.reverse_recovery(storage=self.storage)
                except AttributeError:
                    # If you don't use Pro CandleSync, this is a no-op
                    continue
                except Exception as e:
                    self.logger.error(
                        f"[ReverseRecovery] Error for {symbol} {tf}: {e}"
                    )
        except Exception as e:
            self.logger.error(f"Reverse recovery phase error: {e}")

        # -------------------------------------------------
        # Setup WebSocket client & subscriptions
        # -------------------------------------------------
        self.logger.info("\n[7/7] Setting up WebSocket client & subscriptions...")
        try:
            self.ws_client = WebSocketClient(self.app_config)

            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name

                for tf_cfg in sym_cfg.timeframes:
                    tf = tf_cfg.tf

                    # ---- Correct closure over (symbol, timeframe) ----
                    def make_callback(sym: str, timeframe: str):
                        km_key = (sym, timeframe)

                        def cb(candle: dict):
                            """
                            WebSocket callback for one stream.

                            WebSocketClient guarantees:
                                cb(candle_dict)
                            with unified schema:
                                ts      → open timestamp (s)
                                open_ts → same as ts (if WS normalized)
                                close_ts
                            """
                            try:
                                sync_obj = self.candle_syncs[km_key]
                                sync_obj.on_ws_closed_candle(
                                    candle, storage=self.storage
                                )
                            except Exception as exc:
                                self.logger.error(
                                    f"[WebSocket] Callback error for "
                                    f"{sym} {timeframe}: {exc}"
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

    def _start_health_api(self) -> None:
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
                    "candles": len(cm),
                    "last_open_ts": cm.last_open_time(),
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

        self.health_server_thread = threading.Thread(
            target=run_api, daemon=True
        )
        self.health_server_thread.start()

        self.logger.info(
            f"✓ Health API running at http://0.0.0.0:{self.health_port}/health"
        )

    # -------------------------------------------------------------------------
    # CALLBACK FOR VALIDATED CANDLE (FROM CandleSync)
    # -------------------------------------------------------------------------

    def _on_validated_candle(self, symbol: str, timeframe: str, candle: dict) -> None:
        """
        Called by CandleSync when a candle is fully validated & gap-filled.

        This is the hook point for:
        - LiquidityMap.on_candle(...)
        - Strategy evaluation
        - Signal aggregation & execution
        """
        try:
            ts = candle.get("ts")
            close = candle.get("close")
            vol = candle.get("volume")
            self.logger.info(
                f"[VALIDATED] {symbol} {timeframe} "
                f"ts={ts} close={close} vol={vol}"
            )

            # TODO:
            #   lm = self.liquidity_maps[timeframe]
            #   lm.on_candle_close(...)
            #   strategy_engine.on_candle(...)
            #   execution_layer.on_signal(...)

        except Exception as e:
            self.logger.error(
                f"[VALIDATED] Handler error for {symbol} {timeframe}: {e}"
            )

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

    def run(self) -> None:
        """Main run loop (status/logging)."""
        try:
            while self.is_running:
                time.sleep(60)
                self._print_status()
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received.")
        finally:
            self.stop()

    def _print_status(self) -> None:
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
                "candle_count": len(cm),
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
                    f"last_ts={last_ts}, close={close_val}"
                )

    # -------------------------------------------------------------------------
    # SHUTDOWN
    # -------------------------------------------------------------------------

    def stop(self) -> None:
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
