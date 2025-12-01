"""Test script for WebSocket client functionality."""

import time
from trading_bot.config.app import load_app_config
from trading_bot.api.ws_client import WebSocketClient


def test_websocket():
    """Test WebSocket client with real-time candle streaming."""
    
    print("=" * 70)
    print("Testing WebSocket Client")
    print("=" * 70)
    
    # Load config
    app_config = load_app_config("config/app.yml")
    print(f"\n✓ Loaded app config: {app_config.app.name}")
    print(f"✓ WebSocket endpoint: {app_config.exchange.ws_endpoint}")
    
    # Create WebSocket client
    ws_client = WebSocketClient(app_config)
    print("\n✓ WebSocket client created")
    
    # Counter for received candles
    candle_count = {"1m": 0, "5m": 0}
    
    # Callback for 1m candles
    def on_1m_candle(candle):
        candle_count["1m"] += 1
        print(f"\n[1m] Candle #{candle_count['1m']} received:")
        print(f"  Timestamp: {candle['ts']}")
        print(f"  Close:     ${candle['close']:.2f}")
        print(f"  High:      ${candle['high']:.2f}")
        print(f"  Low:       ${candle['low']:.2f}")
        print(f"  Volume:    {candle['volume']:.4f}")
    
    # Callback for 5m candles
    def on_5m_candle(candle):
        candle_count["5m"] += 1
        print(f"\n[5m] Candle #{candle_count['5m']} received:")
        print(f"  Timestamp: {candle['ts']}")
        print(f"  Close:     ${candle['close']:.2f}")
        print(f"  High:      ${candle['high']:.2f}")
        print(f"  Low:       ${candle['low']:.2f}")
        print(f"  Volume:    {candle['volume']:.4f}")
    
    # Subscribe to streams
    ws_client.subscribe("BTCUSDT", "1m", on_1m_candle)
    ws_client.subscribe("BTCUSDT", "5m", on_5m_candle)
    print("\n✓ Subscribed to BTCUSDT 1m and 5m streams")
    
    # Start WebSocket
    ws_client.start()
    print("\n✓ WebSocket started, waiting for candles...")
    print("\nListening for closed candles (press Ctrl+C to stop)...")
    print("─" * 70)
    
    try:
        # Wait for candles (or until interrupted)
        # In real application, this would run indefinitely
        timeout = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if not ws_client.is_alive():
                print("\n✗ WebSocket disconnected!")
                break
            time.sleep(1)
        
        print(f"\n\n{'=' * 70}")
        print("Test completed!")
        print(f"Received {candle_count['1m']} candles on 1m")
        print(f"Received {candle_count['5m']} candles on 5m")
        print(f"{'=' * 70}")
        
    except KeyboardInterrupt:
        print("\n\n✓ Interrupted by user")
    finally:
        # Stop WebSocket
        ws_client.stop()
        print("\n✓ WebSocket stopped")


if __name__ == "__main__":
    test_websocket()
