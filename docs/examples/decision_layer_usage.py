"""
Example: Using the Professional Liquidity Map & Trend Fusion System

This example demonstrates how to use the integrated decision layer
components for multi-timeframe analysis.
"""

from trading_bot.core.mtf_symbol_manager import MultiTFSymbolManager
from trading_bot.decision_layer.zone_classifier import ZoneClassifier
from trading_bot.api.rest_client import RestClient
from trading_bot.config.symbols.models import SymbolConfig, TimeframeConfig
from trading_bot.config.app.models import AppConfig

# ============================================================================
# EXAMPLE 1: Basic Setup
# ============================================================================

def example_basic_setup(app_config, rest_client):
    """
    Show how MTFSymbolManager automatically initializes decision layer.
    
    Args:
        app_config: AppConfig instance (from config/app.yml)
        rest_client: RestClient instance for API calls
    """
    
    # Create symbol config
    symbol_cfg = SymbolConfig(
        name="BTCUSDT",
        enabled=True,
        timeframes=[
            TimeframeConfig(tf="1m", fetch=3000),
            TimeframeConfig(tf="5m", fetch=1500),
            TimeframeConfig(tf="15m", fetch=800),
            TimeframeConfig(tf="1h", fetch=400),
        ]
    )
    
    # Create MTF manager (decision layer auto-initialized)
    mtf_manager = MultiTFSymbolManager(
        symbol_cfg=symbol_cfg,
        app_config=app_config,
        rest_client=rest_client,
        storage=None
    )
    
    # Load initial data
    success = mtf_manager.load_initial_data()
    
    if success:
        print("✓ MTF Manager initialized")
        print(f"✓ Liquidity Map: {mtf_manager.liquidity_map is not None}")
        print(f"✓ Trend Fusion: {mtf_manager.trend_fusion is not None}")
    
    return mtf_manager


# ============================================================================
# EXAMPLE 2: Event-Driven Updates (Automatic)
# ============================================================================

def example_event_driven():
    """
    Show how system automatically updates on candle close.
    
    When a candle closes:
    1. CandleSync validates it
    2. CandleManager stores it
    3. LiquidityMap refreshes THAT TF only
    4. TrendFusion updates THAT TF trend
    5. On 1m close: generates fusion signals
    """
    
    # When WebSocket sends a candle:
    # mtf_manager.on_ws_candle("5m", candle_data)
    
    # System automatically:
    # - Queues by TF priority
    # - Validates candle
    # - Updates liquidity zones for 5m
    # - Updates trend state for 5m
    # - Logs results
    
    # When 1m closes (master timeframe):
    # - Updates 1m liquidity zones
    # - Updates 1m trend state
    # - Generates fusion signal (all TFs analyzed)
    # - Logs trade signals if found
    
    print("Event-driven updates happen automatically on candle close!")


# ============================================================================
# EXAMPLE 3: Querying Liquidity Zones
# ============================================================================

def example_query_zones(mtf_manager):
    """Show how to query liquidity zones."""
    
    liq_map = mtf_manager.liquidity_map
    
    if not liq_map:
        print("Liquidity map not available")
        return
    
    # Get zones for specific timeframe
    zones_5m = liq_map.get_zones_for_timeframe("5m")
    print(f"5m zones: {len(zones_5m)}")
    
    # Get confluence zones (multiple TFs agree)
    confluence = liq_map.get_confluence_zones(
        min_timeframes=2,
        min_strength="medium"
    )
    print(f"Confluence zones: {len(confluence)}")
    
    # Get nearest support/resistance
    current_price = 50000.0
    nearest_support = liq_map.get_nearest_support(current_price)
    nearest_resistance = liq_map.get_nearest_resistance(current_price)
    
    if nearest_support:
        print(f"Nearest support: ${nearest_support.price_high:.2f}")
        print(f"  Strength: {nearest_support.strength}")
        print(f"  Touches: {nearest_support.touch_count}")
        print(f"  Confluence: {nearest_support.confluence_count} TFs")
    
    if nearest_resistance:
        print(f"Nearest resistance: ${nearest_resistance.price_low:.2f}")
        print(f"  Strength: {nearest_resistance.strength}")
        print(f"  Touches: {nearest_resistance.touch_count}")
    
    # Get statistics
    stats = liq_map.get_statistics()
    print(f"\nLiquidity Map Stats:")
    print(f"  Total zones created: {stats['total_zones_created']}")
    print(f"  Zones broken: {stats['zones_broken']}")
    for tf, tf_stats in stats['zones_per_tf'].items():
        print(f"  {tf}: {tf_stats['active']} active zones")


# ============================================================================
# EXAMPLE 4: Trend Analysis
# ============================================================================

def example_trend_analysis(mtf_manager):
    """Show how to analyze trends across timeframes."""
    
    fusion = mtf_manager.trend_fusion
    
    if not fusion:
        print("Trend fusion not available")
        return
    
    # Check if trends are aligned
    if fusion.is_aligned():
        direction = fusion.get_dominant_direction()
        print(f"✓ MTF Trend Aligned: {direction.value}")
    else:
        print("⚠ No trend alignment")
    
    # Get trend state for each timeframe
    for tf in ["1h", "15m", "5m", "1m"]:
        trend = fusion.get_trend_state(tf)
        if trend:
            print(f"\n{tf} Trend:")
            print(f"  Direction: {trend.direction.value}")
            print(f"  Strength: {trend.strength.value}")
            print(f"  Momentum: {trend.momentum_score:.1f}")
            print(f"  RSI: {trend.rsi:.1f}")
            print(f"  Structure: HH={trend.higher_highs}, HL={trend.higher_lows}")
    
    # Get latest fusion signal
    signal = fusion.get_fusion_signal()
    if signal:
        print(f"\n🎯 TRADE SIGNAL:")
        print(f"  Type: {signal.signal_type}")
        print(f"  Confidence: {signal.confidence:.2%}")
        print(f"  Aligned TFs: {signal.aligned_timeframes}")
        print(f"  Zone: ${signal.key_zone_price:.2f} ({signal.zone_strength})")
        print(f"  Entry: ${signal.entry_price:.2f}")
        print(f"  Stop Loss: ${signal.stop_loss:.2f}")
        print(f"  Take Profit: ${signal.take_profit:.2f}")
        
        # Calculate risk/reward
        risk = abs(signal.entry_price - signal.stop_loss)
        reward = abs(signal.take_profit - signal.entry_price)
        rr_ratio = reward / risk if risk > 0 else 0
        print(f"  Risk/Reward: 1:{rr_ratio:.2f}")


# ============================================================================
# EXAMPLE 5: Advanced Zone Classification
# ============================================================================

def example_zone_classification(mtf_manager):
    """Show advanced zone classification and ranking."""
    
    liq_map = mtf_manager.liquidity_map
    
    if not liq_map:
        return
    
    # Get all zones from all timeframes
    all_zones = []
    for tf in ["1h", "15m", "5m", "1m"]:
        zones = liq_map.get_zones_for_timeframe(tf)
        all_zones.extend(zones)
    
    # Create classifier
    classifier = ZoneClassifier(
        fresh_threshold_hours=24,
        recent_threshold_days=7,
        aged_threshold_days=30,
        min_confidence=0.5,
    )
    
    # Classify all zones
    current_price = 50000.0
    classified = classifier.classify_zones(
        zones=all_zones,
        current_price=current_price
    )
    
    print(f"\nClassified {len(classified)} zones")
    
    # Get summary
    summary = classifier.get_zone_summary(classified)
    print(f"\nZone Summary:")
    print(f"  By Quality:")
    for quality, count in summary['by_quality'].items():
        print(f"    {quality}: {count}")
    print(f"  By Time:")
    for time_rel, count in summary['by_time'].items():
        print(f"    {time_rel}: {count}")
    print(f"  Avg Confidence: {summary['avg_confidence']:.2f}")
    print(f"  Avg Risk: {summary['avg_risk']:.2f}")
    print(f"  With Confluence: {summary['with_confluence']}")
    
    # Get top 5 zones
    top_zones = classifier.get_top_zones(classified, max_zones=5)
    print(f"\nTop 5 Priority Zones:")
    for i, zone in enumerate(top_zones, 1):
        print(f"\n{i}. {zone.zone_type.upper()} @ ${zone.price_low:.2f}-${zone.price_high:.2f}")
        print(f"   TF: {zone.timeframe}, Quality: {zone.quality.value}")
        print(f"   Confidence: {zone.confidence_score:.2%}")
        print(f"   Priority: {zone.priority_score:.1f}/100")
        print(f"   Risk: {zone.risk_score:.2%}")
        print(f"   Distance: {zone.distance_from_price_pct:.2%}")
        print(f"   Confluence: {zone.confluence_count} TFs")
    
    # Get only excellent zones
    excellent = classifier.get_excellent_zones(classified)
    print(f"\n{len(excellent)} excellent quality zones")
    
    # Get low-risk zones
    low_risk = classifier.get_low_risk_zones(classified, max_risk=0.3)
    print(f"{len(low_risk)} low-risk zones (risk < 30%)")
    
    # Get nearby zones
    nearby = classifier.get_nearby_zones(classified, max_distance_pct=0.05)
    print(f"{len(nearby)} zones within 5% of current price")
    
    # Rank by risk/reward
    best_rr = classifier.rank_by_risk_reward(classified)[:3]
    print(f"\nBest Risk/Reward Zones:")
    for zone in best_rr:
        rr_score = zone.confidence_score * (1.0 - zone.risk_score)
        print(f"  {zone.zone_type} @ ${zone.price_low:.2f}, RR Score: {rr_score:.2f}")


# ============================================================================
# EXAMPLE 6: Complete Trading Decision Flow
# ============================================================================

def example_trading_decision(mtf_manager):
    """
    Complete example of making a trading decision.
    
    This is what your execution layer would do.
    """
    
    liq_map = mtf_manager.liquidity_map
    fusion = mtf_manager.trend_fusion
    
    if not liq_map or not fusion:
        print("Decision layer not ready")
        return
    
    # Step 1: Check trend alignment
    if not fusion.is_aligned():
        print("❌ No trend alignment - skip trading")
        return
    
    direction = fusion.get_dominant_direction()
    print(f"✓ Trends aligned: {direction.value}")
    
    # Step 2: Get fusion signal
    signal = fusion.get_fusion_signal()
    
    if not signal:
        print("❌ No fusion signal - wait for setup")
        return
    
    print(f"✓ Signal: {signal.signal_type} (confidence: {signal.confidence:.2%})")
    
    # Step 3: Check signal confidence
    if signal.confidence < 0.65:  # 65% minimum
        print(f"❌ Confidence too low ({signal.confidence:.2%}) - skip")
        return
    
    print("✓ Confidence acceptable")
    
    # Step 4: Classify the zone
    classifier = ZoneClassifier(min_confidence=0.5)
    
    # Get the zone from liquidity map
    zones = liq_map.get_confluence_zones(min_timeframes=2, min_strength="medium")
    classified = classifier.classify_zones(
        zones=zones,
        current_price=signal.entry_price
    )
    
    # Find the signal's zone
    signal_zone = None
    for zone in classified:
        zone_mid = (zone.price_low + zone.price_high) / 2
        if abs(zone_mid - signal.key_zone_price) < 1.0:
            signal_zone = zone
            break
    
    if not signal_zone:
        print("❌ Could not classify signal zone")
        return
    
    print(f"✓ Zone quality: {signal_zone.quality.value}")
    print(f"✓ Zone risk: {signal_zone.risk_score:.2%}")
    
    # Step 5: Final decision
    if signal_zone.quality.value in ["excellent", "good"] and signal_zone.risk_score < 0.4:
        print("\n✅ EXECUTE TRADE:")
        print(f"   Symbol: {signal.symbol}")
        print(f"   Direction: {signal.dominant_direction.value}")
        print(f"   Entry: ${signal.entry_price:.2f}")
        print(f"   Stop Loss: ${signal.stop_loss:.2f}")
        print(f"   Take Profit: ${signal.take_profit:.2f}")
        print(f"   Zone: ${signal.key_zone_price:.2f} ({signal.zone_strength})")
        print(f"   Confluence: {len(signal.aligned_timeframes)} TFs")
        
        # Calculate position sizing based on risk
        account_balance = 10000.0
        risk_per_trade = 0.02  # 2% risk
        risk_amount = account_balance * risk_per_trade
        
        price_risk = abs(signal.entry_price - signal.stop_loss)
        position_size = risk_amount / price_risk
        
        print(f"   Position Size: {position_size:.4f} BTC")
        print(f"   Risk: ${risk_amount:.2f} (2% of account)")
        
        # This is where you'd send order to execution layer
        # execution_manager.place_order(...)
    else:
        print(f"❌ Zone quality insufficient - skip trade")
        print(f"   Quality: {signal_zone.quality.value}")
        print(f"   Risk: {signal_zone.risk_score:.2%}")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("Professional Liquidity Map & Trend Fusion System")
    print("=" * 60)
    
    print("\nNOTE: This is a documentation/reference file showing usage patterns.")
    print("To run these examples, you need to:")
    print("  1. Initialize app_config from your config/app.yml")
    print("  2. Create a RestClient instance with API credentials")
    print("  3. Pass these to example_basic_setup(app_config, rest_client)")
    print()
    print("Example:")
    print("  from trading_bot.config.app.loader import load_app_config")
    print("  app_config = load_app_config('config/app.yml')")
    print("  rest_client = RestClient(api_key, api_secret)")
    print("  mtf_manager = example_basic_setup(app_config, rest_client)")
    
    print("\n\nSystem Features:")
    print("✓ Event-driven refresh (only updates changed TF)")
    print("✓ TF priority system (1h → 15m → 5m → 1m master)")
    print("✓ Swing pivot detection with confirmation")
    print("✓ Volume cluster analysis")
    print("✓ Multi-timeframe confluence detection")
    print("✓ Zone strength classification")
    print("✓ EMA/RSI/MACD trend analysis")
    print("✓ MTF trend alignment detection")
    print("✓ Fusion signals with confidence scoring")
    print("✓ Advanced zone classification (quality/risk/priority)")
    print("✓ Risk/reward analysis")
    print("✓ Complete trade decision pipeline")
    
    print("\n" + "=" * 60)
    print("Ready for production trading!")
