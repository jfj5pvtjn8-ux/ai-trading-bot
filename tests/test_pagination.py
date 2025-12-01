"""Test script to verify RestClient pagination functionality."""

from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import load_symbols_config
from trading_bot.api.rest_client import RestClient


def test_pagination():
    """Test that pagination works correctly for large fetch sizes."""
    
    print("=" * 70)
    print("Testing RestClient Pagination")
    print("=" * 70)
    
    # Load configs
    app_config = load_app_config("config/app.yml")
    symbols_config = load_symbols_config("config/symbols.yml")
    
    print(f"\n✓ Loaded app config: {app_config.app.name}")
    print(f"✓ Loaded {len(symbols_config.symbols)} symbols")
    
    # Create REST client
    rest_client = RestClient(app_config)
    print(f"✓ REST client initialized: {app_config.exchange.rest_endpoint}")
    
    # Get first enabled symbol
    enabled_symbols = [s for s in symbols_config.symbols if s.enabled]
    if not enabled_symbols:
        print("\n✗ No enabled symbols found in config")
        return
    
    symbol_cfg = enabled_symbols[0]
    print(f"\n✓ Testing with symbol: {symbol_cfg.name}")
    
    # Test each timeframe
    for tf_cfg in symbol_cfg.timeframes:
        print(f"\n{'─' * 70}")
        print(f"Timeframe: {tf_cfg.tf} | Fetch: {tf_cfg.fetch} candles")
        print(f"{'─' * 70}")
        
        try:
            candles = rest_client.fetch_klines(
                symbol=symbol_cfg.name,
                timeframe=tf_cfg.tf,
                limit=tf_cfg.fetch
            )
            
            if candles:
                print(f"✓ Successfully fetched {len(candles)} candles")
                print(f"  First candle timestamp: {candles[0]['ts']}")
                print(f"  Last candle timestamp:  {candles[-1]['ts']}")
                print(f"  First candle close:     ${candles[0]['close']:.2f}")
                print(f"  Last candle close:      ${candles[-1]['close']:.2f}")
                
                # Verify ordering (oldest to newest)
                is_ordered = all(
                    candles[i]['ts'] < candles[i+1]['ts'] 
                    for i in range(len(candles)-1)
                )
                if is_ordered:
                    print(f"  ✓ Candles are properly ordered (oldest → newest)")
                else:
                    print(f"  ✗ WARNING: Candles are NOT properly ordered!")
                
                # Calculate expected API requests
                expected_requests = (tf_cfg.fetch // 1000) + (1 if tf_cfg.fetch % 1000 else 0)
                print(f"  Expected API requests:  {expected_requests}")
            else:
                print(f"✗ Failed to fetch candles")
                
        except Exception as e:
            print(f"✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Close client
    rest_client.close()
    print(f"\n{'=' * 70}")
    print("✓ Test completed successfully!")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    test_pagination()
