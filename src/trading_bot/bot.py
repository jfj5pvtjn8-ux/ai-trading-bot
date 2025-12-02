#!/usr/bin/env python3
"""
TradingBot PRO+ Edition

Features
--------
✓ Full MTF orchestration
✓ Proper wiring between REST → Loader → CandleManager → CandleSync → WS
✓ Gap-safe, restart-safe, alignment-safe pipeline
✓ Correct websocket callback closures (per-symbol, per-tf)
✓ Storage-safe async writes
✓ Health HTTP endpoint
✓ Graceful SIGINT / SIGTERM shutdown
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
from concurrent.futures import ThreadPoolExecutor, as_completed

from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.core.logger import get_logger, get_symbol_logger

from trading_bot.api.rest_client import RestClient
from trading_bot.api.ws_client import WebSocketClient

from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync
from trading_bot.core.candles.initial_candles_loader import InitialCandlesLoader

from trading_bot.storage.parquet_storage import ParquetStorage


class TradingBot:
    """
    PRO+ main orchestrator.
    Manages:
        - REST initial load
        - CandleManager (memory window)
        - CandleSync (RT validation)
        - ParquetStorage async writes
        - WS Client (stream handler)
        - Health Endpoint
    """

    def __init__(self):
        load_dotenv()
        self.logger = get_logger(__name__)

        self.is_running = False

        # Loaded configs
        self.app_config = None
        self.symbols_config = None

        # Core subsystems
        self.rest: RestClient | None = None
        self.ws: WebSocketClient | None = None
        self.storage: ParquetStorage | None = None

        # Per (symbol, timeframe)
        self.cm: Dict[Tuple[str, str], CandleManager] = {}
        self.sync: Dict[Tuple[str, str], CandleSync] = {}

        # Health API server thread
        self.health_app = None
        self.health_thread = None
        self.health_port = int(os.getenv("HEALTH_PORT", "8080"))

        # OS Signals
        signal.signal(signal.SIGINT, self._shutdown_signal)
        signal.signal(signal.SIGTERM, self._shutdown_signal)

    # ======================================================================
    # SIGNAL HANDLER
    # ======================================================================

    def _shutdown_signal(self, signum, frame):
        self.logger.info(f"[TradingBot] Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    # ======================================================================
    # INITIALIZE
    # ======================================================================

    def initialize(self) -> bool:
        self.logger.info("=" * 80)
        self.logger.info("TradingBot PRO+ – Initialization")
        self.logger.info("=" * 80)

        # -------------------------------------------------------
        # Load Configs
        # -------------------------------------------------------
        try:
            config_dir = os.getenv("CONFIG_DIR", "config")

            self.app_config = load_app_config(f"{config_dir}/app.yml")
            self.symbols_config = load_symbols_config(f"{config_dir}/symbols.yml")

            enabled = [s for s in self.symbols_config.symbols if s.enabled]
            if not enabled:
                self.logger.error("No enabled symbols found.")
                return False

            self.logger.info(f"[Config] App loaded: {self.app_config.app.name}")
            for s in enabled:
                tfs = [t.tf for t in s.timeframes]
                self.logger.info(f"[Config] {s.name}: {tfs}")

        except Exception as e:
            self.logger.error(f"[Config] Failed: {e}")
            return False

        # -------------------------------------------------------
        # Parquet Storage
        # -------------------------------------------------------
        try:
            self.storage = ParquetStorage(
                base_path=os.getenv("DATA_DIR", "data/live"),
                retention_days=int(os.getenv("PARQUET_RETENTION_DAYS", "7")),
                fresh_start=False,
            )
            self.logger.info("[Storage] ParquetStorage initialized.")
        except Exception as e:
            self.logger.error(f"[Storage] Failed: {e}")
            return False

        # -------------------------------------------------------
        # REST Client
        # -------------------------------------------------------
        try:
            self.rest = RestClient(self.app_config)
            self.logger.info("[REST] RestClient initialized.")
        except Exception as e:
            self.logger.error(f"[REST] Failed: {e}")
            return False

        # -------------------------------------------------------
        # Create CandleManager + CandleSync
        # -------------------------------------------------------
        for s in enabled:
            sym = s.name
            get_symbol_logger(sym)

            for tf_cfg in s.timeframes:
                tf = tf_cfg.tf
                key = (sym, tf)

                try:
                    cm = CandleManager(
                        max_size=tf_cfg.fetch,
                        timeframe_seconds=self.app_config.get_timeframe_seconds(tf)
                    )
                    sync = CandleSync(
                        rest_client=self.rest,
                        symbol=sym,
                        timeframe=tf,
                        candle_manager=cm,
                        app_config=self.app_config,
                    )

                    sync.set_callback(self._validated_callback)

                    self.cm[key] = cm
                    self.sync[key] = sync

                    self.logger.info(f"[Init] Manager+Sync ready for {sym} {tf}")

                except Exception as e:
                    self.logger.error(f"[Init] Failed for {sym} {tf}: {e}")

        # -------------------------------------------------------
        # Initial REST Load (Parallel per Symbol)
        # -------------------------------------------------------
        loader = InitialCandlesLoader(self.app_config, self.rest)

        with ThreadPoolExecutor(max_workers=len(enabled)) as ex:
            futures = {}
            for s in enabled:
                sym = s.name
                cm_map = {t.tf: self.cm[(sym, t.tf)] for t in s.timeframes}
                sync_map = {t.tf: self.sync[(sym, t.tf)] for t in s.timeframes}

                f = ex.submit(
                    loader.load_initial_for_symbol,
                    symbol_cfg=s,
                    candle_managers=cm_map,
                    candle_syncs=sync_map,
                    liquidity_maps=None,
                    storage=self.storage,
                    use_reverse_recovery=True,
                )
                futures[f] = sym
                self.logger.info(f"[InitialLoad] Submitted {sym}")

            for f in as_completed(futures):
                sym = futures[f]
                try:
                    ok = f.result()
                    if ok:
                        self.logger.info(f"[InitialLoad] {sym} READY")
                    else:
                        self.logger.warning(f"[InitialLoad] {sym} incomplete")
                except Exception as e:
                    self.logger.error(f"[InitialLoad] {sym} FAILED: {e}")

        # -------------------------------------------------------
        # WS Client + Subscriptions
        # -------------------------------------------------------
        try:
            self.ws = WebSocketClient(self.app_config)

            for s in enabled:
                sym = s.name

                for tf_cfg in s.timeframes:
                    tf = tf_cfg.tf

                    # correct closure
                    def make_cb(symbol=sym, timeframe=tf):
                        key = (symbol, timeframe)

                        def cb(candle: dict):
                            try:
                                self.sync[key].on_ws_closed_candle(
                                    candle,
                                    storage=self.storage
                                )
                            except Exception as exc:
                                self.logger.error(
                                    f"[WS] Callback error {symbol} {timeframe}: {exc}"
                                )

                        return cb

                    self.ws.subscribe(
                        symbol=sym,
                        timeframe=tf,
                        callback=make_cb(sym, tf)
                    )
                    self.logger.info(f"[WS] Subscribed {sym} {tf}")

        except Exception as e:
            self.logger.error(f"[WS] Setup failed: {e}")
            return False

        # -------------------------------------------------------
        # Start Health API
        # -------------------------------------------------------
        self._start_health_api()

        self.logger.info("Initialization COMPLETE ✓")
        return True

    # ======================================================================
    # VALIDATED CANDLE CALLBACK
    # ======================================================================

    def _validated_callback(self, symbol: str, timeframe: str, candle: dict):
        self.logger.info(
            f"[VALIDATED] {symbol} {timeframe} ts={candle['ts']} "
            f"close={candle['close']} vol={candle['volume']}"
        )
        # Strategy plug-in point here

    # ======================================================================
    # HEALTH API
    # ======================================================================

    def _start_health_api(self):
        app = Flask("health-check")
        self.health_app = app

        @app.get("/health")
        def health():
            data = {}
            for (sym, tf), cm in self.cm.items():
                if sym not in data:
                    data[sym] = {}
                data[sym][tf] = {
                    "count": len(cm),
                    "last_ts": cm.last_open_time(),
                }

            return jsonify({
                "status": "running" if self.is_running else "stopped",
                "time": datetime.utcnow().isoformat(),
                "symbols": data,
                "ws": "connected" if self.ws and self.ws.is_alive() else "disconnected",
            })

        def run():
            app.run(
                host="0.0.0.0",
                port=self.health_port,
                debug=False,
                use_reloader=False,
            )

        self.health_thread = threading.Thread(target=run, daemon=True)
        self.health_thread.start()

        self.logger.info(
            f"[Health] Running at http://0.0.0.0:{self.health_port}/health"
        )

    # ======================================================================
    # START
    # ======================================================================

    def start(self) -> bool:
        if not self.ws:
            self.logger.error("Initialize before start().")
            return False

        self.logger.info("[TradingBot] Starting WS…")
        try:
            self.ws.start()
            self.is_running = True
            self.logger.info("TradingBot LIVE ✓")
            return True
        except Exception as e:
            self.logger.error(f"[TradingBot] Failed to start WS: {e}")
            return False

    # ======================================================================
    # MAIN LOOP
    # ======================================================================

    def run(self):
        try:
            while self.is_running:
                time.sleep(30)
                self._log_status()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def _log_status(self):
        self.logger.info("─" * 60)
        self.logger.info("STATUS UPDATE")

        for (sym, tf), cm in self.cm.items():
            last = cm.get_latest_candle()
            if last:
                self.logger.info(
                    f"{sym} {tf}: {len(cm)} candles, last_ts={last['ts']} close={last['close']}"
                )

    # ======================================================================
    # STOP
    # ======================================================================

    def stop(self):
        if not self.is_running:
            return

        self.is_running = False
        self.logger.info("Stopping TradingBot…")

        if self.ws:
            try:
                self.ws.stop()
                self.logger.info("[WS] Stopped")
            except Exception as e:
                self.logger.error(f"[WS] Stop error: {e}")

        if self.rest:
            try:
                self.rest.close()
            except:
                pass

        if self.storage:
            try:
                self.storage.shutdown()
                self.logger.info("[Storage] Shutdown complete")
            except Exception as e:
                self.logger.error(f"[Storage] Shutdown failed: {e}")

        self.logger.info("TradingBot STOPPED ✓")


def main() -> int:
    bot = TradingBot()
    if not bot.initialize():
        return 1
    if not bot.start():
        return 1
    bot.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
