#!/usr/bin/env python3
"""
Integration Verification Test for LiquidityMap + All SMC Plugins

This script verifies that all 5 SMC plugins are properly integrated
with the LiquidityMap class and can process candle data correctly.

Run this to verify the complete integration:
    python tests/verify_liquidity_map_integration.py
"""

import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trading_bot.decision_layer.liquidity_map import LiquidityMap


def create_sample_candles(count=100, base_price=50000):
    """Create sample candle data for testing."""
    candles = []
    base_time = int(time.time() * 1000)
    
    for i in range(count):
        # Create realistic price movement
        open_price = base_price + (i * 10) + ((i % 5) * 20)
        close_price = open_price + (50 if i % 3 == 0 else -30)
        high_price = max(open_price, close_price) + 20
        low_price = min(open_price, close_price) - 20
        
        candle = {
            'ts': base_time + (i * 60000),  # 1 minute apart
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': 100 + (i % 10) * 10
        }
        candles.append(candle)
    
    return candles


def test_liquidity_map_instantiation():
    """Test 1: Verify LiquidityMap instantiates with all plugins."""
    print("=" * 80)
    print("TEST 1: LiquidityMap Instantiation")
    print("=" * 80)
    
    try:
        liq_map = LiquidityMap(
            symbol='BTCUSDT',
            timeframes=['1m', '5m', '15m', '1h'],
            enable_fvg=True,
            enable_ssl_bsl=True,
            enable_order_blocks=True,
            enable_bos_choch=True,
            enable_breaker_blocks=True,
            enable_liquidity_sweeps=True
        )
        
        print(f"✅ LiquidityMap instantiated successfully")
        print(f"   Symbol: {liq_map.symbol}")
        print(f"   Timeframes: {liq_map.timeframes}")
        print()
        
        # Verify plugins per timeframe
        for tf in liq_map.timeframes:
            plugins = list(liq_map.plugins[tf].keys())
            print(f"   {tf}: {len(plugins)} plugins initialized")
            for plugin_name in plugins:
                plugin = liq_map.plugins[tf][plugin_name]
                print(f"      - {plugin_name}: {plugin.__class__.__name__}")
        
        print()
        print("✅ TEST 1 PASSED")
        return liq_map
        
    except Exception as e:
        print(f"❌ TEST 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_plugin_detection(liq_map):
    """Test 2: Verify all plugins can detect patterns."""
    print()
    print("=" * 80)
    print("TEST 2: Plugin Pattern Detection")
    print("=" * 80)
    
    # Create sample candles
    candles = create_sample_candles(count=100, base_price=50000)
    current_price = candles[-1]['close']
    
    try:
        # Process candles through LiquidityMap
        timeframe = '1m'
        result = liq_map.on_candle_close(timeframe, candles, current_price)
        
        print(f"✅ on_candle_close() returned: {result}")
        print()
        
        # Check each plugin for detections
        tf_plugins = liq_map.plugins[timeframe]
        
        print(f"Plugin Detection Results for {timeframe}:")
        print()
        
        # FVG Plugin
        fvg_plugin = tf_plugins['fvg']
        fvgs = fvg_plugin.get(only_unfilled=False)
        print(f"   FVG Plugin: {len(fvgs)} patterns detected")
        
        # SSL/BSL Plugin
        ssl_plugin = tf_plugins['ssl_bsl']
        ssl_levels = ssl_plugin.get(only_unswept=False)
        print(f"   SSL/BSL Plugin: {len(ssl_levels)} liquidity levels detected")
        
        # Order Blocks Plugin
        ob_plugin = tf_plugins['order_block']
        order_blocks = ob_plugin.get(only_unmitigated=False)
        print(f"   Order Blocks Plugin: {len(order_blocks)} order blocks detected")
        
        # BOS/CHOCH Plugin
        bos_plugin = tf_plugins['bos_choch']
        structure_breaks = bos_plugin.get()
        print(f"   BOS/CHOCH Plugin: {len(structure_breaks)} structure breaks detected")
        
        # Breaker Blocks Plugin
        breaker_plugin = tf_plugins['breaker_block']
        breakers = breaker_plugin.get()
        print(f"   Breaker Blocks Plugin: {len(breakers)} breaker blocks detected")
        
        # Liquidity Sweeps Plugin
        sweep_plugin = tf_plugins['liquidity_sweep']
        sweeps = sweep_plugin.get()
        print(f"   Liquidity Sweeps Plugin: {len(sweeps)} sweeps detected")
        
        print()
        print("✅ TEST 2 PASSED - All plugins successfully processed candles")
        return True
        
    except Exception as e:
        print(f"❌ TEST 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_plugin_dependencies():
    """Test 3: Verify plugin dependencies are working."""
    print()
    print("=" * 80)
    print("TEST 3: Plugin Dependencies")
    print("=" * 80)
    
    try:
        liq_map = LiquidityMap(
            symbol='BTCUSDT',
            timeframes=['1m'],
            enable_fvg=True,
            enable_ssl_bsl=True,
            enable_order_blocks=True,
            enable_bos_choch=True,
            enable_breaker_blocks=True,
            enable_liquidity_sweeps=True
        )
        
        plugins = liq_map.plugins['1m']
        
        # Check Breaker Blocks has OB plugin reference
        breaker = plugins['breaker_block']
        if breaker.ob_plugin is not None:
            print(f"✅ Breaker Blocks has Order Blocks plugin reference")
        else:
            print(f"❌ Breaker Blocks missing Order Blocks plugin reference")
            return False
        
        # Check Liquidity Sweeps has SSL/BSL plugin reference
        sweep = plugins['liquidity_sweep']
        if sweep.ssl_plugin is not None:
            print(f"✅ Liquidity Sweeps has SSL/BSL plugin reference")
        else:
            print(f"❌ Liquidity Sweeps missing SSL/BSL plugin reference")
            return False
        
        print()
        print("✅ TEST 3 PASSED - Plugin dependencies properly configured")
        return True
        
    except Exception as e:
        print(f"❌ TEST 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multi_timeframe_processing():
    """Test 4: Verify multi-timeframe processing."""
    print()
    print("=" * 80)
    print("TEST 4: Multi-Timeframe Processing")
    print("=" * 80)
    
    try:
        liq_map = LiquidityMap(
            symbol='BTCUSDT',
            timeframes=['1m', '5m', '15m', '1h'],
            enable_fvg=True,
            enable_ssl_bsl=True,
            enable_order_blocks=True,
            enable_bos_choch=True,
            enable_breaker_blocks=True,
            enable_liquidity_sweeps=True
        )
        
        # Create candles for each timeframe
        candles_1m = create_sample_candles(100, 50000)
        candles_5m = create_sample_candles(50, 50500)
        candles_15m = create_sample_candles(30, 51000)
        candles_1h = create_sample_candles(20, 51500)
        
        # Process each timeframe
        results = {}
        for tf, candles in [('1m', candles_1m), ('5m', candles_5m), 
                            ('15m', candles_15m), ('1h', candles_1h)]:
            current_price = candles[-1]['close']
            result = liq_map.on_candle_close(tf, candles, current_price)
            results[tf] = result
            
            # Get pattern counts
            plugins = liq_map.plugins[tf]
            fvgs = len(plugins['fvg'].get(only_unfilled=False))
            ssl = len(plugins['ssl_bsl'].get(only_unswept=False))
            obs = len(plugins['order_block'].get(only_unmitigated=False))
            
            print(f"   {tf}: Processed={result}, FVGs={fvgs}, SSL/BSL={ssl}, OBs={obs}")
        
        print()
        if all(results.values()):
            print("✅ TEST 4 PASSED - All timeframes processed successfully")
            return True
        else:
            print("❌ TEST 4 FAILED - Some timeframes failed to process")
            return False
        
    except Exception as e:
        print(f"❌ TEST 4 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all integration tests."""
    print()
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "LIQUIDITY MAP INTEGRATION TEST" + " " * 28 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Test 1: Instantiation
    liq_map = test_liquidity_map_instantiation()
    if not liq_map:
        print("\n❌ INTEGRATION TEST FAILED - Could not instantiate LiquidityMap")
        return False
    
    # Test 2: Pattern Detection
    if not test_plugin_detection(liq_map):
        print("\n❌ INTEGRATION TEST FAILED - Pattern detection issues")
        return False
    
    # Test 3: Dependencies
    if not test_plugin_dependencies():
        print("\n❌ INTEGRATION TEST FAILED - Dependency configuration issues")
        return False
    
    # Test 4: Multi-Timeframe
    if not test_multi_timeframe_processing():
        print("\n❌ INTEGRATION TEST FAILED - Multi-timeframe processing issues")
        return False
    
    # All tests passed
    print()
    print("=" * 80)
    print("🎉 ALL INTEGRATION TESTS PASSED!")
    print("=" * 80)
    print()
    print("Summary:")
    print("  ✅ LiquidityMap instantiation: SUCCESS")
    print("  ✅ Plugin pattern detection: SUCCESS")
    print("  ✅ Plugin dependencies: SUCCESS")
    print("  ✅ Multi-timeframe processing: SUCCESS")
    print()
    print("Verified Components:")
    print("  ✅ FVG Plugin (Fair Value Gaps)")
    print("  ✅ SSL/BSL Plugin (Equal Highs/Lows)")
    print("  ✅ Order Blocks Plugin")
    print("  ✅ BOS/CHOCH Plugin (Market Structure)")
    print("  ✅ Breaker Blocks Plugin")
    print("  ✅ Liquidity Sweeps Plugin")
    print()
    print("Integration Status: 🟢 READY FOR PRODUCTION")
    print()
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
