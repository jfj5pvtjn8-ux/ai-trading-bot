"""
Complete Integration Example: Full Trading Bot Flow

This demonstrates the complete data flow from configuration to real-time candle processing:
1. Load configurations (app + symbols)
2. Create REST and WebSocket clients
3. Create MultiTFSymbolManager for each enabled symbol
4. Load initial historical data
5. Start WebSocket for real-time updates
6. Process candles through validation pipeline
"""

import os
import time
import signal
import sys
import threading
from typing import Dict
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, jsonify
from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api import RestClient, WebSocketClient
from trading_bot.core.mtf_symbol_manager import MultiTFSymbolManager
from trading_bot.core.logger import get_logger
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
        
        # Symbol managers
        self.mtf_managers: Dict[str, MultiTFSymbolManager] = {}
        
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
        
        # Step 4: Create MTF managers for each enabled symbol
        self.logger.info("\n[4/6] Creating Multi-Timeframe Symbol Managers...")
        enabled_symbols = [s for s in self.symbols_config.symbols if s.enabled]
        
        for symbol_cfg in enabled_symbols:
            try:
                mtf_manager = MultiTFSymbolManager(
                    symbol_cfg=symbol_cfg,
                    app_config=self.app_config,
                    rest_client=self.rest_client,
                    storage=self.storage
                )
                
                # Register callback for validated candles
                mtf_manager.set_on_candle_callback(self._on_validated_candle)
                
                self.mtf_managers[symbol_cfg.name] = mtf_manager
                
                self.logger.info(f"✓ Created MTF manager for {symbol_cfg.name}")
                
            except Exception as e:
                self.logger.error(f"Failed to create MTF manager for {symbol_cfg.name}: {e}")
                continue
        
        if not self.mtf_managers:
            self.logger.error("No symbol managers created!")
            return False
        
        # Step 5: Load initial historical data (parallel loading for speed)
        self.logger.info("\n[5/6] Loading initial historical data...")
        
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        async def load_symbol_async(symbol: str, mtf_manager) -> tuple:
            """Load a symbol in thread pool to avoid blocking."""
            loop = asyncio.get_event_loop()
            try:
                self.logger.info(f"Loading data for {symbol}...")
                success = await loop.run_in_executor(None, mtf_manager.load_initial_data)
                return (symbol, success, mtf_manager)
            except Exception as e:
                self.logger.exception(f"Error loading data for {symbol}: {e}")
                return (symbol, False, None)
        
        async def load_all_symbols():
            """Load all symbols in parallel."""
            tasks = [
                load_symbol_async(symbol, mgr)
                for symbol, mgr in self.mtf_managers.items()
            ]
            return await asyncio.gather(*tasks)
        
        # Run parallel loading
        results = asyncio.run(load_all_symbols())
        
        # Process results
        for symbol, success, mtf_manager in results:
            if success and mtf_manager:
                self.logger.info(f"✓ {symbol} initialized")
                self.logger.info(mtf_manager.get_summary())
            else:
                self.logger.error(f"✗ {symbol} initialization failed")
        
        # Step 6: Setup WebSocket
        self.logger.info("\n[6/6] Setting up WebSocket client...")
        try:
            self.ws_client = WebSocketClient(self.app_config)
            
            # Subscribe to all enabled symbols and timeframes
            for symbol_cfg in enabled_symbols:
                symbol = symbol_cfg.name
                mtf_manager = self.mtf_managers.get(symbol)
                
                if not mtf_manager:
                    continue
                
                for tf_cfg in symbol_cfg.timeframes:
                    tf = tf_cfg.tf
                    
                    # Create closure to capture symbol and timeframe
                    def make_callback(s, t):
                        return lambda candle: self.mtf_managers[s].on_ws_candle(t, candle)
                    
                    self.ws_client.subscribe(
                        symbol=symbol,
                        timeframe=tf,
                        callback=make_callback(symbol, tf)
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
                
                for symbol, mtf_manager in self.mtf_managers.items():
                    status = mtf_manager.get_status()
                    symbols_status[symbol] = {
                        "initialized": status['is_initialized'],
                        "timeframes": {
                            tf: {
                                "candles": tf_data['candle_count'],
                                "last_update": tf_data['last_timestamp']
                            }
                            for tf, tf_data in status['timeframes'].items()
                        }
                    }
                
                return jsonify({
                    "status": "running" if self.is_running else "stopped",
                    "timestamp": datetime.now().isoformat(),
                    "symbols": list(self.mtf_managers.keys()),
                    "symbols_detail": symbols_status,
                    "websocket": "connected" if self.ws_client and self.ws_client.is_running else "disconnected"
                })
            
            @self.health_app.route('/stats', methods=['GET'])
            def stats():
                """Detailed statistics endpoint."""
                stats_data = {}
                
                for symbol, mtf_manager in self.mtf_managers.items():
                    stats_data[symbol] = {
                        "summary": mtf_manager.get_summary(),
                        "status": mtf_manager.get_status()
                    }
                    
                    # Add liquidity map stats if available
                    if mtf_manager.liquidity_map:
                        stats_data[symbol]["liquidity_map"] = mtf_manager.liquidity_map.get_statistics()
                    
                    # Add trend fusion stats if available
                    if mtf_manager.trend_fusion:
                        stats_data[symbol]["trend_fusion"] = mtf_manager.trend_fusion.get_statistics()
                
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
        
        This is where you would trigger your trading logic:
        - Update liquidity maps
        - Detect market structure changes (CHoCH, BOS)
        - Identify trading opportunities
        - Execute trades
        """
        self.logger.info(
            f"[Validated] {symbol} {timeframe}: "
            f"ts={candle['ts']}, close=${candle['close']:.2f}, "
            f"volume={candle['volume']:.4f}"
        )
        
        # Save validated candle to parquet
        if self.storage:
            self.storage.save_candle(symbol, timeframe, candle)
        
        # TODO: Add your trading logic here
        # - Check liquidity maps
        # - Detect order blocks
        # - Identify fair value gaps
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
        
        for symbol, mtf_manager in self.mtf_managers.items():
            status = mtf_manager.get_status()
            
            self.logger.info(f"\n{symbol}:")
            for tf, tf_status in status['timeframes'].items():
                close_val = tf_status['latest_close'] if tf_status['latest_close'] else 0
                self.logger.info(
                    f"  {tf}: {tf_status['candle_count']} candles, "
                    f"last_ts={tf_status['last_timestamp']}, "
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
        
        # Shutdown all MTF managers
        self.logger.info("Shutting down symbol managers...")
        for symbol, mtf_manager in self.mtf_managers.items():
            mtf_manager.shutdown()
            self.logger.info(f"✓ {symbol} manager shutdown")
        
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
