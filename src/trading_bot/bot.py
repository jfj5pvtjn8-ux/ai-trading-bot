"""
Trading Bot Orchestrator – FINAL FIXED EDITION

Fully aligned with:
- DuckDBStorage (final)
- CandleManager (final)
- CandleSync (final)
- CandleBootstrapper (final)
- REST Client
- WebSocket Client
"""

import os
import sys
import time
import signal
import threading
from datetime import datetime
from typing import Dict, Tuple

from dotenv import load_dotenv
from flask import Flask, jsonify

from trading_bot.core.logger import get_logger, get_symbol_logger
from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api.rest_client import RestClient
from trading_bot.api.ws_client import WebSocketClient
from trading_bot.storage.duckdb_storage import DuckDBStorage
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync
from trading_bot.core.candles.candle_bootstrapper import CandleBootstrapper


class TradingBot:
    """
    FINAL CORRECT ORCHESTRATOR FLOW:

        1. Load configs
        2. Create REST client
        3. Create DuckDB storage + schema + gap fill
        4. Create CandleManager for each (symbol,timeframe)
        5. Create CandleSync for each (symbol,timeframe)
        6. Run Bootstrapper: DB → Memory (with last-candle drop fix)
        7. Seed CandleSync from CM
        8. Start WebSocket client
        9. Handle real-time candles
    """

    def __init__(self):
        load_dotenv()
        self.logger = get_logger(__name__)

        # Runtime components
        self.app_config = None
        self.symbols_config = None
        self.rest_client: RestClient = None
        self.duckdb_storage: DuckDBStorage = None
        self.ws_client: WebSocketClient = None

        # Candle infra
        self.candle_managers: Dict[Tuple[str, str], CandleManager] = {}
        self.candle_syncs: Dict[Tuple[str, str], CandleSync] = {}

        # Health API
        self.health_app: Flask = None
        self.health_thread: threading.Thread = None
        self.health_port = int(os.getenv("HEALTH_PORT", "8080"))

        # Shutdown control
        self.is_running = False
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
        self.logger.info("\n" + "=" * 100)
        self.logger.info("INITIALIZING TRADING BOT")
        self.logger.info("=" * 100)

        # ---------------------------------------------------------
        # 1. Load configs
        # ---------------------------------------------------------
        try:
            config_dir = os.getenv("CONFIG_DIR", "config")
            self.app_config = load_app_config(f"{config_dir}/app.yml")
            self.symbols_config = load_symbols_config(f"{config_dir}/symbols.yml")

            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
            if not enabled_symbols:
                self.logger.error("No enabled symbols found.")
                return False

            self.logger.info(f"✓ App loaded → {self.app_config.app.name}")
            self.logger.info(f"✓ Symbols loaded: {[s.name for s in enabled_symbols]}")

        except Exception as e:
            self.logger.error(f"Config error: {e}")
            return False

        # ---------------------------------------------------------
        # 2. REST client
        # ---------------------------------------------------------
        try:
            self.rest_client = RestClient(self.app_config)
            self.logger.info("✓ REST client created")
        except Exception as e:
            self.logger.error(f"REST client error: {e}")
            return False

        # ---------------------------------------------------------
        # 3. DuckDB storage
        # ---------------------------------------------------------
        if not self._initialize_duckdb():
            return False

        # ---------------------------------------------------------
        # 4. Create CandleManager & CandleSync
        # ---------------------------------------------------------
        self.logger.info("\n[4/7] Creating Candle Managers + Candle Sync...")
        enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]

        for s_cfg in enabled_symbols:
            symbol = s_cfg.name
            get_symbol_logger(symbol)

            for tf_cfg in s_cfg.timeframes:
                tf = tf_cfg.tf
                key = (symbol, tf)
                tf_sec = self.app_config.get_timeframe_seconds(tf)

                try:
                    # Create CM - always use TF config limit (prevents truncation)
                    # Storage load will fetch exactly this many most recent candles
                    cm = CandleManager(symbol=symbol, tf=tf, maxlen=tf_cfg.fetch)
                    cm.set_tf_seconds(tf_sec)

                    # Create CandleSync
                    sync = CandleSync(
                        symbol=symbol,
                        tf=tf,
                        rest_client=self.rest_client,
                        storage=self.duckdb_storage,
                        cm=cm,
                        validator=None,   # provide validator if needed
                        tf_sec=tf_sec,
                    )

                    self.candle_managers[key] = cm
                    self.candle_syncs[key] = sync

                    self.logger.info(
                        f"  ✓ {symbol} {tf}: CM window={tf_cfg.fetch}, step={tf_sec}s"
                    )
                except Exception as e:
                    self.logger.error(f"CM/Sync creation failed for {symbol} {tf}: {e}")
                    return False

        # ---------------------------------------------------------
        # 5. Bootstrap DB → Memory (with missing candle fix)
        # ---------------------------------------------------------
        self.logger.info("\n[5/7] Running bootstrapper (DB → Memory)...")

        bootstrap = CandleBootstrapper(
            config=self.app_config,
            storage=self.duckdb_storage,
            rest_client=self.rest_client,
            candle_managers=self.candle_managers,
        )

        if not bootstrap.run():
            self.logger.error("Bootstrapper failed.")
            return False

        # ---------------------------------------------------------
        # 6. Seed CandleSyncs
        # ---------------------------------------------------------
        self.logger.info("\n[6/7] Seeding CandleSync objects...")

        for key, sync in self.candle_syncs.items():
            cm = self.candle_managers[key]
            latest_ts = cm.last_ts()
            if latest_ts is not None:
                sync.seed_with_timestamp(latest_ts)
                self.logger.info(f"  ✓ {key} seeded with ts={latest_ts}")
            else:
                self.logger.info(f"  ✓ {key} no candles yet")

        # ---------------------------------------------------------
        # 7. Setup WebSocket
        # ---------------------------------------------------------
        self.logger.info("\n[7/7] Starting WebSocket subscriptions...")

        try:
            self.ws_client = WebSocketClient(self.app_config)

            for (symbol, tf), sync in self.candle_syncs.items():

                def make_cb(sym=symbol, timeframe=tf, sync_obj=sync):
                    def _cb(candle):
                        try:
                            sync_obj.handle_ws_candle(candle)
                        except Exception as e:
                            self.logger.error(
                                f"[WS-Callback] Error for {sym} {timeframe}: {e}"
                            )
                    return _cb

                self.ws_client.subscribe(symbol, tf, make_cb())
                self.logger.info(f"  ✓ WS subscribed → {symbol} {tf}")

            self.logger.info("✓ WebSocket client initialized")
        except Exception as e:
            self.logger.error(f"WebSocket init error: {e}")
            return False

        # ---------------------------------------------------------
        # Start health endpoint
        # ---------------------------------------------------------
        self._start_health_api()

        self.logger.info("\nInitialization COMPLETE ✓")
        return True

    # -------------------------------------------------------------------------
    # DUCKDB INIT
    # -------------------------------------------------------------------------
    def _initialize_duckdb(self) -> bool:
        self.logger.info("\n[3/7] Initializing DuckDB storage...")

        try:
            db_path = self.app_config.duckdb.database_path

            self.duckdb_storage = DuckDBStorage(
                db_path=db_path,
                app_config=self.app_config,
                fresh_start=self.app_config.duckdb.fresh_start,
            )

            self.duckdb_storage.initialize_schema()
            return True

        except Exception as e:
            self.logger.error(f"DuckDB init failed: {e}")
            return False

    # -------------------------------------------------------------------------
    # HEALTH API
    # -------------------------------------------------------------------------
    def _start_health_api(self):
        app = Flask(__name__)
        self.health_app = app

        @app.get("/health")
        def health():
            detail = {}
            for (symbol, tf), cm in self.candle_managers.items():
                if symbol not in detail:
                    detail[symbol] = {}
                detail[symbol][tf] = {
                    "count": cm.size(),
                    "last_ts": cm.last_ts(),
                }

            return jsonify(
                {
                    "status": "running" if self.is_running else "stopped",
                    "timestamp": datetime.utcnow().isoformat(),
                    "symbols": detail,
                }
            )

        def run_server():
            app.run(
                host="0.0.0.0",
                port=self.health_port,
                debug=False,
                use_reloader=False,
            )

        self.health_thread = threading.Thread(
            target=run_server, daemon=True
        )
        self.health_thread.start()

        self.logger.info(
            f"✓ Health API running → http://0.0.0.0:{self.health_port}/health"
        )

    # -------------------------------------------------------------------------
    # START + RUN
    # -------------------------------------------------------------------------
    def start(self):
        if not self.ws_client:
            self.logger.error("Cannot start bot — not initialized")
            return False

        try:
            self.is_running = True
            self.ws_client.start()
            self.logger.info("🚀 Trading Bot LIVE — Streaming candles!")
            return True
        except Exception as e:
            self.logger.error(f"Failed to start WS: {e}")
            return False

    def run(self):
        while self.is_running:
            time.sleep(60)  # Log status every 1 minute
            self._log_status()

    def _log_status(self):
        self.logger.info("\n--- BOT STATUS ---")
        for key, cm in self.candle_managers.items():
            symbol, tf = key
            self.logger.info(
                f"{symbol} {tf}: candles={cm.size()}, last_ts={cm.last_ts()}"
            )

    # -------------------------------------------------------------------------
    # STOP
    # -------------------------------------------------------------------------
    def stop(self):
        if not self.is_running:
            return

        self.is_running = False
        self.logger.info("\nStopping Trading Bot...")

        if self.ws_client:
            try:
                self.ws_client.stop()
                self.logger.info("✓ WebSocket stopped")
            except Exception:
                pass

        if self.duckdb_storage:
            try:
                self.duckdb_storage.shutdown()
                self.logger.info("✓ DuckDB closed")
            except Exception:
                pass

        self.logger.info("✓ Trading Bot stopped")

# -------------------------------------------------------------------------
# MAIN ENTRY
# -------------------------------------------------------------------------
def main():
    logger = get_logger(__name__)
    bot = TradingBot()

    if not bot.initialize():
        logger.error("Bot init failed")
        return 1

    if not bot.start():
        logger.error("Bot startup failed")
        return 1

    bot.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
