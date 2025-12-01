"""
Complete Integration Example: Full Trading Bot Flow

This demonstrates the complete data flow from configuration to real-time candle processing:
1. Load configurations (app + symbols)
2. Create REST and WebSocket clients
3. Create CandleManager for each symbol/timeframe
4. Load initial historical data
5. Start WebSocket for real-time updates
6. Process candles through validation pipeline
"""

import os
import time
import signal
import sys
import threading
from typing import Dict, List
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, jsonify
from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api import RestClient, WebSocketClient
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync
from trading_bot.core.candles.initial_candles_loader import InitialCandlesLoader
from trading_bot.core.logger import get_logger, get_symbol_logger
from trading_bot.storage import ParquetStorage


class TradingBot:
    """
    Main trading bot orchestrator.
    
    Manages multiple symbols, each with multiple timeframes.
    """
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.logger = get_logger(__name__)
        self.is_running = False
        
        # Configuration
        self.app_config = None
        self.symbols_config = None
        
        # Clients
        self.rest_client = None
        self.ws_client = None
        
        # Storage
        self.storage = None
        
        # Candle managers: {(symbol, tf): CandleManager}
        self.candle_managers: Dict[tuple, CandleManager] = {}
        
        # Candle sync managers: {(symbol, tf): CandleSync}
        self.candle_syncs: Dict[tuple, CandleSync] = {}
        
        # Health endpoint
        self.health_app = None
        self.health_server_thread = None
        self.health_port = int(os.getenv("HEALTH_PORT", "8080"))
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def initialize(self):
        """Initialize the trading bot."""
        self.logger.info("=" * 70)
        self.logger.info("Initializing Trading Bot")
        self.logger.info("=" * 70)
        
        # Step 1: Load configurations
        self.logger.info("\n[1/5] Loading configurations...")
        try:
            config_dir = os.getenv("CONFIG_DIR", "config")
            self.app_config = load_app_config(f"{config_dir}/app.yml")
            self.symbols_config = load_symbols_config(f"{config_dir}/symbols.yml")
            
            enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
            
            self.logger.info(f"✓ App config loaded: {self.app_config.app.name}")
            self.logger.info(f"✓ Symbols config loaded: {len(enabled_symbols)} enabled symbols")
            
            for symbol in enabled_symbols:
                tfs = [tf.tf for tf in symbol.timeframes]
                self.logger.info(f"  - {symbol.name}: {tfs}")
                
        except Exception as e:
            self.logger.error(f"Failed to load configurations: {e}")
            return False
        
        # Step 2: Initialize Parquet Storage
        self.logger.info("\n[2/6] Initializing Parquet storage...")
        try:
            data_dir = os.getenv("DATA_DIR", "data/live")
            retention_days = int(os.getenv("PARQUET_RETENTION_DAYS", "7"))
            self.storage = ParquetStorage(base_path=data_dir, retention_days=retention_days)
            self.logger.info("✓ Parquet storage initialized (7-day retention)")
        except Exception as e:
            self.logger.error(f"Failed to initialize storage: {e}")
            return False
        
        # Step 3: Create REST client
        self.logger.info("\n[3/6] Creating REST client...")
        try:
            self.rest_client = RestClient(self.app_config)
            self.logger.info(f"✓ REST client created: {self.app_config.exchange.rest_endpoint}")
        except Exception as e:
            self.logger.error(f"Failed to create REST client: {e}")
            return False
        
        # Step 4: Create candle managers for each enabled symbol/timeframe
        self.logger.info("\n[4/6] Creating Candle Managers...")
        enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
        
        for symbol_cfg in enabled_symbols:
            symbol = symbol_cfg.name
            symbol_logger = get_symbol_logger(symbol)
            
            for tf_cfg in symbol_cfg.timeframes:
                tf = tf_cfg.tf
                key = (symbol, tf)
                
                try:
                    # Create CandleManager for storage (only needs max_size)
                    candle_manager = CandleManager(max_size=tf_cfg.fetch)
                    self.candle_managers[key] = candle_manager
                    
                    # Create CandleSync for validation
                    candle_sync = CandleSync(
                        rest_client=self.rest_client,
                        symbol=symbol,
                        timeframe=tf,
                        candle_manager=candle_manager,
                        app_config=self.app_config
                    )
                    self.candle_syncs[key] = candle_sync
                    
                    self.logger.info(f"✓ Created managers for {symbol} {tf}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to create managers for {symbol} {tf}: {e}")
                    continue
        
        if not self.candle_managers:
            self.logger.error("No candle managers created!")
            return False
        
        # Step 5: Load initial historical data
        self.logger.info("\n[5/6] Loading initial historical data...")
        
        # Create a single loader instance
        loader = InitialCandlesLoader(
            app_config=self.app_config,
            rest_client=self.rest_client
        )
        
        for symbol_cfg in enabled_symbols:
            symbol = symbol_cfg.name
            
            try:
                self.logger.info(f"Loading historical data for {symbol}...")
                
                # Build dicts of managers/syncs for this symbol (keyed by timeframe string)
                symbol_candle_managers = {}
                symbol_candle_syncs = {}
                
                for tf_cfg in symbol_cfg.timeframes:
                    tf = tf_cfg.tf
                    key = (symbol, tf)
                    symbol_candle_managers[tf] = self.candle_managers[key]
                    symbol_candle_syncs[tf] = self.candle_syncs[key]
                
                # Load all timeframes for this symbol in one call
                success = loader.load_initial_for_symbol(
                    symbol_cfg=symbol_cfg,
                    candle_managers=symbol_candle_managers,
                    candle_syncs=symbol_candle_syncs,
                    liquidity_maps=None  # No liquidity maps for now
                )
                
                if success:
                    self.logger.info(f"✓ Successfully loaded all timeframes for {symbol}")
                else:
                    self.logger.warning(f"Failed to load some timeframes for {symbol}")
                    
            except Exception as e:
                self.logger.error(f"Failed to load {symbol}: {e}")
                continue
        
        # Step 6: Setup WebSocket
        self.logger.info("\n[6/6] Setting up WebSocket client...")
        try:
            self.ws_client = WebSocketClient(self.app_config)
            
            # Subscribe to all enabled symbols and timeframes
            for symbol_cfg in enabled_symbols:
                symbol = symbol_cfg.name
                
                for tf_cfg in symbol_cfg.timeframes:
                    tf = tf_cfg.tf
                    key = (symbol, tf)
                    
                    # Create closure to capture symbol and timeframe
                    def make_callback(s, t, k):
                        def callback(candle):
                            """Handle WebSocket closed candle."""
                            try:
                                # Pass to CandleSync for validation
                                candle_sync = self.candle_syncs.get(k)
                                if candle_sync:
                                    candle_sync.on_websocket_candle(candle)
                                else:
                                    self.logger.warning(f"No CandleSync for {s} {t}")
                            except Exception as e:
                                self.logger.error(f"Error processing WS candle {s} {t}: {e}")
                        return callback
                    
                    self.ws_client.subscribe(
                        symbol=symbol,
                        timeframe=tf,
                        callback=make_callback(symbol, tf, key)
                    )
                    
                    self.logger.info(f"✓ Subscribed to {symbol} {tf}")
            
            self.logger.info("\n✓ WebSocket client configured")
            
        except Exception as e:
            self.logger.error(f"Failed to setup WebSocket: {e}")
            return False
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("✓ Initialization Complete!")
        self.logger.info("=" * 70)
        
        # Start health endpoint
        self._start_health_endpoint()
        
        return True
    
    def _start_health_endpoint(self):
        """Start HTTP health check endpoint."""
        try:
            self.health_app = Flask(__name__)
            
            @self.health_app.route('/health', methods=['GET'])
            def health_check():
                """Health check endpoint."""
                symbols_status = {}
                
                # Group by symbol
                for (symbol, tf), candle_manager in self.candle_managers.items():
                    if symbol not in symbols_status:
                        symbols_status[symbol] = {"timeframes": {}}
                    
                    candle_count = len(candle_manager.get_all())
                    last_ts = candle_manager.last_timestamp()
                    
                    symbols_status[symbol]["timeframes"][tf] = {
                        "candles": candle_count,
                        "last_timestamp": last_ts
                    }
                
                return jsonify({
                    "status": "running" if self.is_running else "stopped",
                    "timestamp": datetime.now().isoformat(),
                    "symbols": list(set(s for s, _ in self.candle_managers.keys())),
                    "symbols_detail": symbols_status,
                    "websocket": "connected" if self.ws_client and self.ws_client.is_running else "disconnected"
                })
            
            @self.health_app.route('/stats', methods=['GET'])
            def stats():
                """Detailed statistics endpoint."""
                stats_data = {}
                
                # Group by symbol
                for (symbol, tf), candle_manager in self.candle_managers.items():
                    if symbol not in stats_data:
                        stats_data[symbol] = {"timeframes": {}}
                    
                    recent_candles = candle_manager.last_n(100)
                    last_candle = candle_manager.get_latest_candle()
                    
                    stats_data[symbol]["timeframes"][tf] = {
                        "total_candles": len(candle_manager.get_all()),
                        "last_candle": last_candle,
                        "recent_candles_count": len(recent_candles)
                    }
                
                return jsonify({
                    "timestamp": datetime.now().isoformat(),
                    "symbols": stats_data
                })
            
            # Run Flask in a separate thread
            def run_server():
                self.health_app.run(host='0.0.0.0', port=self.health_port, debug=False, use_reloader=False)
            
            self.health_server_thread = threading.Thread(target=run_server, daemon=True)
            self.health_server_thread.start()
            
            self.logger.info(f"✓ Health endpoint started on http://0.0.0.0:{self.health_port}/health")
            self.logger.info(f"✓ Stats endpoint available at http://0.0.0.0:{self.health_port}/stats")
            
        except Exception as e:
            self.logger.error(f"Failed to start health endpoint: {e}")
    
    def start(self):
        """Start the trading bot."""
        if not self.ws_client:
            self.logger.error("Bot not initialized! Call initialize() first.")
            return False
        
        self.logger.info("\nStarting Trading Bot...")
        
        try:
            self.ws_client.start()
            self.is_running = True
            
            self.logger.info("✓ WebSocket started")
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🚀 Trading Bot is now LIVE and processing candles!")
            self.logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start bot: {e}")
            return False
    
    def _on_validated_candle(self, symbol: str, timeframe: str, candle: dict):
        """
        Callback when a validated candle is received.
        
        This is where you would add trading logic in the future.
        For now, just log the validated candle.
        """
        self.logger.info(
            f"[Validated] {symbol} {timeframe}: "
            f"ts={candle['ts']}, close=${candle['close']:.2f}, "
            f"volume={candle['volume']:.4f}"
        )
        
        # TODO: Add your trading logic here when ready
        # - Detect patterns
        # - Identify trading opportunities
        # - Execute trades based on strategy
    
    def run(self):
        """Main run loop."""
        try:
            while self.is_running:
                # Print status every 60 seconds
                time.sleep(60)
                self._print_status()
                
        except KeyboardInterrupt:
            self.logger.info("\nReceived keyboard interrupt")
        finally:
            self.stop()
    
    def _print_status(self):
        """Print current bot status."""
        self.logger.info("\n" + "─" * 70)
        self.logger.info("Status Update")
        self.logger.info("─" * 70)
        
        # Group by symbol
        symbols_data = {}
        for (symbol, tf), candle_manager in self.candle_managers.items():
            if symbol not in symbols_data:
                symbols_data[symbol] = {}
            
            candle_count = len(candle_manager.get_all())
            last_candle = candle_manager.get_latest_candle()
            
            symbols_data[symbol][tf] = {
                'candle_count': candle_count,
                'last_candle': last_candle
            }
        
        for symbol, tfs_data in symbols_data.items():
            self.logger.info(f"\n{symbol}:")
            for tf, data in tfs_data.items():
                last_candle = data['last_candle']
                close_val = last_candle['close'] if last_candle else 0
                last_ts = last_candle['ts'] if last_candle else None
                
                self.logger.info(
                    f"  {tf}: {data['candle_count']} candles, "
                    f"last_ts={last_ts}, "
                    f"close=${close_val:.2f}"
                )
    
    def stop(self):
        """Stop the trading bot gracefully."""
        if not self.is_running:
            return
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("Stopping Trading Bot...")
        self.logger.info("=" * 70)
        
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
        
        # Note: Flask server runs in daemon thread, will stop automatically
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("✓ Trading Bot Stopped")
        self.logger.info("=" * 70)


def main():
    """Main entry point."""
    logger = get_logger(__name__)
    bot = TradingBot()
    
    # Initialize
    if not bot.initialize():
        logger.error("Failed to initialize bot!")
        return 1
    
    # Start
    if not bot.start():
        logger.error("Failed to start bot!")
        return 1
    
    # Run
    bot.run()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
