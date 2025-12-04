"""
Trading Bot Orchestrator – Unified Candle Pipeline (DuckDB Edition)

Wires together:
- App + Symbols config
- RestClient (Binance REST)
- DuckDBStorage (gap detection, backward filling, SQL analytics)
- CandleManager (per symbol/timeframe, open-ts based)
- CandleSync (gap-filling + validation)
- WebSocketClient (Binance WS, closed candles only)
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
from trading_bot.core.logger import get_logger, get_symbol_logger
from trading_bot.storage.duckdb_storage import DuckDBStorage


class TradingBot:
    """
    Main orchestrator for multi-symbol, multi-timeframe trading engine.

    Synchronous Startup Flow:
        1. Initialize DuckDB (check last candles, backward fill gaps)
        2. Create CandleManagers + CandleSync per (symbol, timeframe)
        3. Load candles from DuckDB into sliding windows
        4. Seed CandleSync with current state
        5. Start WebSocket for real-time updates
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
        self.duckdb_storage: DuckDBStorage | None = None

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
        # REST client (needed for backward filling)
        # -------------------------------------------------
        self.logger.info("\n[2/6] Creating REST client...")
        try:
            self.rest_client = RestClient(self.app_config)
            self.logger.info(
                f"✓ REST client created: {self.app_config.exchange.rest_endpoint}"
            )
        except Exception as e:
            self.logger.error(f"REST client error: {e}")
            return False

        # -------------------------------------------------
        # Initialize DuckDB and fill gaps (SYNCHRONOUS)
        # -------------------------------------------------
        if not self._initialize_duckdb_storage():
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
        # Load candles from DuckDB into memory (SYNCHRONOUS)
        # -------------------------------------------------
        if not self._load_candles_into_memory():
            return False

        # -------------------------------------------------
        # Seed CandleSync with current state
        # -------------------------------------------------
        self._seed_candle_syncs()

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
                                    candle, storage=self.duckdb_storage
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
    # DUCKDB INITIALIZATION (SYNCHRONOUS)
    # -------------------------------------------------------------------------

    def _initialize_duckdb_storage(self) -> bool:
        """Initialize DuckDB with gap detection and backward filling - SYNCHRONOUS."""
        self.logger.info("\n[3/6] Initializing DuckDB storage...")
        
        try:
            # Create DuckDB storage instance
            db_path = self.app_config.duckdb.database_path
            fresh_start = self.app_config.duckdb.fresh_start
            
            self.duckdb_storage = DuckDBStorage(
                db_path=db_path,
                app_config=self.app_config,
                fresh_start=fresh_start
            )
            
            # Initialize schema
            self.duckdb_storage.initialize_schema()
            
            # If fresh start, clear all data
            if fresh_start:
                self.logger.info("[DuckDB] Fresh start mode - clearing all data")
                self.duckdb_storage.clear_all_candles()
            
            # Process each symbol/timeframe SYNCHRONOUSLY
            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
            
            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name
                
                for tf_cfg in sym_cfg.timeframes:
                    tf = tf_cfg.tf
                    self.logger.info(f"\n[DuckDB] Processing {symbol} {tf}...")
                    
                    # Check last candle in DuckDB
                    last_candle = self.duckdb_storage.get_last_candle(symbol, tf)
                    
                    if not last_candle or fresh_start:
                        # No data or fresh start - load full history
                        count = self.app_config.duckdb.get_initial_candles(tf)
                        self.logger.info(f"  → Loading {count} candles from API...")
                        
                        success = self.duckdb_storage.backward_fill_gap(
                            symbol, tf, self.rest_client, count
                        )
                        
                        if not success:
                            self.logger.error(f"  ✗ Failed to load candles for {symbol} {tf}")
                            return False
                    else:
                        # Incremental - check for gap
                        tf_seconds = self.app_config.get_timeframe_seconds(tf)
                        gap_candles = self.duckdb_storage.calculate_gap(
                            last_candle['open_ts'], tf_seconds
                        )
                        
                        if gap_candles > 0:
                            max_candles = self.app_config.duckdb.get_initial_candles(tf)
                            
                            if self.duckdb_storage.should_full_reload(gap_candles, max_candles):
                                # Gap too large - full reload
                                self.logger.warning(
                                    f"  → Gap too large ({gap_candles} candles), "
                                    f"performing full reload..."
                                )
                                self.duckdb_storage.delete_symbol_timeframe(symbol, tf)
                                success = self.duckdb_storage.backward_fill_gap(
                                    symbol, tf, self.rest_client, max_candles
                                )
                                
                                if not success:
                                    self.logger.error(f"  ✗ Failed to reload {symbol} {tf}")
                                    return False
                            else:
                                # Normal gap fill
                                self.logger.info(f"  → Filling {gap_candles} missing candles...")
                                success = self.duckdb_storage.backward_fill_gap(
                                    symbol, tf, self.rest_client, gap_candles
                                )
                                
                                if not success:
                                    self.logger.warning(f"  ⚠ Gap fill incomplete for {symbol} {tf}")
                        else:
                            self.logger.info(f"  ✓ No gap detected, data is current")
            
            # Log total candles in database
            total_candles = self.duckdb_storage.get_candle_count()
            self.logger.info(f"\n✓ DuckDB initialization complete ({total_candles} total candles)")
            return True
            
        except Exception as e:
            self.logger.error(f"[DuckDB] Initialization failed: {e}")
            return False

    def _load_candles_into_memory(self) -> bool:
        """Load candles from DuckDB into CandleManager sliding windows - SYNCHRONOUS."""
        self.logger.info("\n[5/6] Loading candles into memory (sliding windows)...")
        
        try:
            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
            
            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name
                
                for tf_cfg in sym_cfg.timeframes:
                    tf = tf_cfg.tf
                    key = (symbol, tf)
                    
                    # Fetch candles from DuckDB for sliding window
                    window_size = tf_cfg.fetch  # From symbols.yml
                    
                    self.logger.info(f"  → Loading {window_size} candles for {symbol} {tf}...")
                    candles = self.duckdb_storage.load_candles_for_window(
                        symbol, tf, limit=window_size
                    )
                    
                    if not candles:
                        self.logger.warning(f"  ⚠ No candles found in DuckDB for {symbol} {tf}")
                        continue
                    
                    # Add to CandleManager
                    cm = self.candle_managers[key]
                    for candle in candles:
                        cm.add_candle(candle)
                    
                    self.logger.info(
                        f"  ✓ Loaded {len(candles)} candles into memory for {symbol} {tf}"
                    )
            
            self.logger.info("\n✓ All candles loaded into memory")
            return True
            
        except Exception as e:
            self.logger.error(f"[Memory Load] Failed: {e}")
            return False

    def _seed_candle_syncs(self) -> None:
        """Seed CandleSync with current state - SYNCHRONOUS."""
        self.logger.info("\n[5.5/6] Seeding CandleSync validators...")
        
        for (symbol, tf), sync in self.candle_syncs.items():
            cm = self.candle_managers[(symbol, tf)]
            latest = cm.get_latest_candle()
            
            if latest:
                # Mark CandleSync as seeded with last timestamp
                sync._last_validated_ts = latest['ts']
                self.logger.info(
                    f"  ✓ {symbol} {tf} seeded with ts={latest['ts']}"
                )
        
        self.logger.info("\n✓ CandleSync validators seeded")

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
        - DuckDB storage (async write)
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

            # Save to DuckDB (async write)
            if self.duckdb_storage:
                self.duckdb_storage.save_candle_async(symbol, timeframe, candle)

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

        # Shutdown DuckDB storage
        if self.duckdb_storage:
            self.logger.info("Shutting down DuckDB storage...")
            try:
                self.duckdb_storage.shutdown()
                self.logger.info("✓ DuckDB storage shutdown complete")
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
