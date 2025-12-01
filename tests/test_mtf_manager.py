"""Test script for MultiTFSymbolManager."""

import time
from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api import RestClient, WebSocketClient
from trading_bot.core.mtf_symbol_manager import MultiTFSymbolManager


def main():
    """Test MultiTFSymbolManager with real data."""
    
    print("=" * 70)
    print("Testing MultiTFSymbolManager")
    print("=" * 70)
    
    # Load configs
    app_config = load_app_config("config/app.yml")
    symbols_config = load_symbols_config("config/symbols.yml")
    
    print(f"\n✓ Loaded configs")
    
    # Get first enabled symbol
    symbol_cfg = next(s for s in symbols_config.symbols if s.enabled)
    print(f"✓ Testing symbol: {symbol_cfg.name}")
    print(f"  Timeframes: {[tf.tf for tf in symbol_cfg.timeframes]}")
    
    # Create REST client
    rest_client = RestClient(app_config)
    print("\n✓ REST client created")
    
    # Create MultiTFSymbolManager
    mtf_manager = MultiTFSymbolManager(
        symbol_cfg=symbol_cfg,
        app_config=app_config,
        rest_client=rest_client
    )
    print(f"✓ MultiTFSymbolManager created for {symbol_cfg.name}")
    
    # Define callback for validated candles
    def on_candle(symbol: str, timeframe: str, candle: dict):
        print(f"\n[Callback] {symbol} {timeframe} validated candle:")
        print(f"  Timestamp: {candle['ts']}")
        print(f"  Close:     ${candle['close']:.2f}")
    
    # Register callback
    mtf_manager.set_on_candle_callback(on_candle)
    print("✓ Callback registered")
    
    # Load initial historical data
    print("\n" + "─" * 70)
    print("Loading initial data...")
    print("─" * 70)
    
    success = mtf_manager.load_initial_data()
    
    if not success:
        print("\n✗ Failed to load initial data")
        return
    
    print("\n✓ Initial data loaded successfully")
    
    # Display status
    print("\n" + "─" * 70)
    print("Status Summary:")
    print("─" * 70)
    print(mtf_manager.get_summary())
    
    # Create WebSocket client
    print("\n" + "─" * 70)
    print("Starting WebSocket for real-time updates...")
    print("─" * 70)
    
    ws_client = WebSocketClient(app_config)
    
    # Subscribe to all timeframes with MTF manager routing
    for tf_cfg in symbol_cfg.timeframes:
        tf = tf_cfg.tf
        
        # Create closure to capture timeframe
        def make_callback(timeframe):
            return lambda candle: mtf_manager.on_ws_candle(timeframe, candle)
        
        ws_client.subscribe(
            symbol=symbol_cfg.name,
            timeframe=tf,
            callback=make_callback(tf)
        )
        print(f"  ✓ Subscribed to {tf}")
    
    # Start WebSocket
    ws_client.start()
    print("\n✓ WebSocket started")
    print("\nMonitoring real-time candles (press Ctrl+C to stop)...")
    print("─" * 70)
    
    try:
        # Monitor for 5 minutes
        timeout = 300
        start_time = time.time()
        last_check = start_time
        
        while time.time() - start_time < timeout:
            # Status update every 30 seconds
            if time.time() - last_check >= 30:
                print(f"\n[Status Update]")
                status = mtf_manager.get_status()
                
                for tf, tf_status in status['timeframes'].items():
                    print(
                        f"  {tf}: {tf_status['candle_count']} candles, "
                        f"last_ts={tf_status['last_timestamp']}, "
                        f"close=${tf_status['latest_close']:.2f if tf_status['latest_close'] else 0}"
                    )
                
                last_check = time.time()
            
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("\n\n✓ Interrupted by user")
    
    finally:
        # Cleanup
        print("\nCleaning up...")
        ws_client.stop()
        mtf_manager.shutdown()
        rest_client.close()
        print("✓ Cleanup complete")
    
    print("\n" + "=" * 70)
    print("Test completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
