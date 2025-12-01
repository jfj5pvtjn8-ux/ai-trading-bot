"""
Fair Value Gap (FVG) Detection Demo

Demonstrates how to use the FVG detection feature in LiquidityMap.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from trading_bot.decision_layer.liquidity_map import LiquidityMap


def create_sample_candles_with_fvg():
    """
    Create sample candles that contain FVG patterns.
    
    Creates both bullish and bearish FVGs for demonstration.
    """
    base_ts = 1700000000000
    
    candles = []
    
    # Normal candles
    for i in range(10):
        candles.append({
            "ts": base_ts + i * 60000,
            "open": 50000 + i * 10,
            "high": 50010 + i * 10,
            "low": 49990 + i * 10,
            "close": 50005 + i * 10,
            "volume": 100 + i,
        })
    
    # BULLISH FVG Pattern (3 candles)
    # Candle 1: Down candle
    candles.append({
        "ts": base_ts + 10 * 60000,
        "open": 50100,
        "high": 50110,
        "low": 50050,  # This low...
        "close": 50060,
        "volume": 500,
    })
    
    # Candle 2: Strong impulse UP (creates the gap)
    candles.append({
        "ts": base_ts + 11 * 60000,
        "open": 50060,
        "high": 50200,
        "low": 50055,
        "close": 50195,
        "volume": 2000,  # High volume impulse
    })
    
    # Candle 3: Continuation up
    candles.append({
        "ts": base_ts + 12 * 60000,
        "open": 50195,
        "high": 50210,
        "low": 50100,  # ...is ABOVE this high = GAP!
        "close": 50205,
        "volume": 300,
    })
    
    # More normal candles
    for i in range(13, 20):
        candles.append({
            "ts": base_ts + i * 60000,
            "open": 50200 + i * 5,
            "high": 50210 + i * 5,
            "low": 50190 + i * 5,
            "close": 50205 + i * 5,
            "volume": 150,
        })
    
    # BEARISH FVG Pattern
    # Candle 1: Up candle
    candles.append({
        "ts": base_ts + 20 * 60000,
        "open": 50300,
        "high": 50350,  # This high...
        "low": 50290,
        "close": 50340,
        "volume": 400,
    })
    
    # Candle 2: Strong impulse DOWN (creates the gap)
    candles.append({
        "ts": base_ts + 21 * 60000,
        "open": 50340,
        "high": 50345,
        "low": 50150,
        "close": 50160,
        "volume": 2500,  # High volume impulse
    })
    
    # Candle 3: Continuation down
    candles.append({
        "ts": base_ts + 22 * 60000,
        "open": 50160,
        "high": 50250,  # ...is BELOW this low = GAP!
        "low": 50140,
        "close": 50145,
        "volume": 350,
    })
    
    # Final normal candles
    for i in range(23, 30):
        candles.append({
            "ts": base_ts + i * 60000,
            "open": 50100 - i * 5,
            "high": 50110 - i * 5,
            "low": 50090 - i * 5,
            "close": 50105 - i * 5,
            "volume": 120,
        })
    
    return candles


def demo_fvg_detection():
    """Demonstrate FVG detection capabilities."""
    
    print("=" * 80)
    print("FAIR VALUE GAP (FVG) DETECTION DEMO")
    print("=" * 80)
    print()
    
    # Initialize LiquidityMap
    liq_map = LiquidityMap(
        symbol="BTCUSDT",
        timeframes=["1m", "5m"],
        lookback_candles=50,
    )
    
    # Create sample candles with FVG patterns
    candles = create_sample_candles_with_fvg()
    current_price = candles[-1]["close"]
    
    print(f"Created {len(candles)} sample candles")
    print(f"Current price: ${current_price:,.2f}")
    print()
    
    # Trigger FVG detection
    print("Detecting Fair Value Gaps...")
    liq_map.on_candle_close(
        timeframe="1m",
        candles=candles,
        current_price=current_price
    )
    print()
    
    # Get all FVGs
    all_fvgs = liq_map.get_fvgs(timeframe="1m", only_unfilled=False)
    unfilled_fvgs = liq_map.get_fvgs(timeframe="1m", only_unfilled=True)
    
    print(f"📊 DETECTED FVGs: {len(all_fvgs)} total, {len(unfilled_fvgs)} unfilled")
    print()
    
    # Display FVG details
    if all_fvgs:
        print("-" * 80)
        print("FVG DETAILS:")
        print("-" * 80)
        
        for i, fvg in enumerate(all_fvgs, 1):
            print(f"\n{i}. {fvg.fvg_type.upper()} FVG:")
            print(f"   Gap Range: ${fvg.gap_low:,.2f} - ${fvg.gap_high:,.2f}")
            print(f"   Gap Size: ${fvg.gap_size:,.2f}")
            print(f"   Midpoint: ${fvg.midpoint:,.2f}")
            print(f"   Volume (impulse): {fvg.volume_before:,.0f}")
            print(f"   Status: {'FILLED' if fvg.is_filled else f'{fvg.fill_percentage:.0f}% filled'}")
            print(f"   Touch Count: {fvg.touch_count}")
            
            # Trading interpretation
            if fvg.fvg_type == "bullish":
                if not fvg.is_filled:
                    print(f"   💡 Trading: Price may retrace to ${fvg.gap_low:,.2f}-${fvg.gap_high:,.2f} before continuing up")
                else:
                    print(f"   ✅ Gap filled - pattern complete")
            else:  # bearish
                if not fvg.is_filled:
                    print(f"   💡 Trading: Price may retrace to ${fvg.gap_low:,.2f}-${fvg.gap_high:,.2f} before continuing down")
                else:
                    print(f"   ✅ Gap filled - pattern complete")
    
    print()
    print("-" * 80)
    
    # Find nearest FVGs
    print("\n🎯 NEAREST FVGs TO CURRENT PRICE:")
    print("-" * 80)
    
    fvg_above = liq_map.get_nearest_fvg(current_price, direction="above")
    fvg_below = liq_map.get_nearest_fvg(current_price, direction="below")
    
    if fvg_above:
        distance = fvg_above.gap_low - current_price
        print(f"\nAbove: {fvg_above.fvg_type.upper()} FVG")
        print(f"  Range: ${fvg_above.gap_low:,.2f} - ${fvg_above.gap_high:,.2f}")
        print(f"  Distance: ${distance:,.2f} ({distance/current_price*100:.2f}%)")
    else:
        print("\nAbove: No FVGs found")
    
    if fvg_below:
        distance = current_price - fvg_below.gap_high
        print(f"\nBelow: {fvg_below.fvg_type.upper()} FVG")
        print(f"  Range: ${fvg_below.gap_low:,.2f} - ${fvg_below.gap_high:,.2f}")
        print(f"  Distance: ${distance:,.2f} ({distance/current_price*100:.2f}%)")
    else:
        print("\nBelow: No FVGs found")
    
    print()
    print("-" * 80)
    
    # Get statistics
    stats = liq_map.get_statistics()
    
    print("\n📈 STATISTICS:")
    print("-" * 80)
    for tf, fvg_stats in stats["fvgs_per_tf"].items():
        print(f"{tf}: {fvg_stats['unfilled']} unfilled, {fvg_stats['total']} total FVGs")
    
    print()
    print("=" * 80)
    print("FVG DETECTION COMPLETE")
    print("=" * 80)
    print()
    print("💡 KEY CONCEPTS:")
    print("  - Bullish FVG = Gap where price jumped up (unfilled area below)")
    print("  - Bearish FVG = Gap where price dropped down (unfilled area above)")
    print("  - FVGs act as magnets - price often returns to fill the gap")
    print("  - High-volume impulse candles create stronger FVGs")
    print("  - >75% fill = considered filled")
    print()


if __name__ == "__main__":
    demo_fvg_detection()
