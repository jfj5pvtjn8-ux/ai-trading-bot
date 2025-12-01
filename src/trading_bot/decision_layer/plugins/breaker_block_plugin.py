"""
Breaker Block Plugin

Detects Breaker Blocks - Order Blocks that failed and flipped polarity.

Architecture:
- Uses BreakerBlock model from models/
- Implements detection and update logic
- Requires OrderBlockPlugin dependency
- Provides query interface for decision layer
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import BreakerBlock


class BreakerBlockPlugin(LiquidityPlugin):
    """
    Breaker Block detection plugin.
    
    Detection Logic:
    1. Monitor Order Blocks from OrderBlockPlugin
    2. Detect when OB is broken through (not just mitigated)
    3. OB flips polarity and becomes a breaker
    4. Bullish OB broken → Bearish breaker (resistance)
    5. Bearish OB broken → Bullish breaker (support)
    
    Break Criteria:
    - Price closes beyond OB zone (not just wick)
    - Break is decisive (> X% beyond OB)
    - Volume confirmation
    
    Strength Factors:
    - Original OB strength
    - Break decisiveness
    - Number of retests
    - Volume on retests
    
    Invalidation:
    - Breaker is broken again (same direction)
    - Becomes too old without retest
    
    Usage:
        # Requires OrderBlockPlugin data
        ob_plugin = OrderBlockPlugin(...)
        breaker_plugin = BreakerBlockPlugin(
            symbol="BTCUSDT",
            timeframe="15m",
            ob_plugin=ob_plugin  # Reference to OB plugin
        )
        
        breaker_plugin.on_candle_close(candles, current_price)
        
        # Query breakers
        bullish_breakers = breaker_plugin.get(breaker_type="bullish", only_active=True)
        nearest = breaker_plugin.get_nearest(current_price)
    
    TODO Implementation Steps:
    1. Monitor OrderBlock patterns from OB plugin
    2. Detect decisive breaks through OB zones
    3. Create breaker block (flip polarity)
    4. Track retests
    5. Detect invalidation
    6. Calculate strength
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None,
        ob_plugin: Any = None  # OrderBlockPlugin reference
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[BreakerBlock] = []
        self.ob_plugin = ob_plugin  # Need OB data
        self.min_break_percent = 0.5  # Must break by 0.5%
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[BreakerBlock]:
        """
        Detect breaker blocks from broken Order Blocks.
        
        Algorithm:
        1. Get all Order Blocks from OB plugin
        2. Find which OBs became breakers (is_breaker=True)
        3. Create BreakerBlock with flipped polarity
        4. Breaker strength inherits from original OB
        """
        if not self.ob_plugin or not candles:
            return []
        
        breakers = []
        
        # Get all OBs (including breakers)
        all_obs = self.ob_plugin.get(only_unmitigated=False)
        
        # Find newly broken OBs
        for ob in all_obs:
            if not ob.is_breaker:
                continue
            
            # Check if we already have this breaker
            existing_ids = {b.breaker_id for b in self._patterns}
            breaker_id = f"{self.symbol}_{self.timeframe}_breaker_{ob.created_ts}"
            
            if breaker_id in existing_ids:
                continue
            
            # Create breaker with flipped polarity
            if ob.ob_type == "bullish":
                breaker_type = "bearish"  # Bullish OB broken → bearish breaker
            else:
                breaker_type = "bullish"  # Bearish OB broken → bullish breaker
            
            breaker = BreakerBlock(
                breaker_id=breaker_id,
                timeframe=self.timeframe,
                breaker_type=breaker_type,
                price_high=ob.price_high,
                price_low=ob.price_low,
                original_ob_type=ob.ob_type,
                break_candle_idx=ob.candle_idx,  # Approx
                break_ts=candles[-1]["ts"] if candles else 0,
                created_ts=ob.created_ts,
                strength=ob.strength
            )
            breakers.append(breaker)
        
        return breakers
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update breakers: retests and invalidation.
        
        Algorithm:
        1. Check for retests (price returns to zone)
        2. Track test counts
        3. Detect invalidation (broken again)
        """
        if not candles or not self._patterns:
            return
        
        recent = candles[-20:] if len(candles) > 20 else candles
        
        for breaker in self._patterns:
            if breaker.is_invalidated:
                continue
            
            for candle in recent:
                # Check for retest
                in_zone = (candle["low"] <= breaker.price_high and 
                          candle["high"] >= breaker.price_low)
                
                if not breaker.is_tested and in_zone:
                    breaker.is_tested = True
                    breaker.test_count = 1
                    breaker.last_test_ts = candle["ts"]
                elif breaker.is_tested and in_zone and breaker.last_test_ts != candle["ts"]:
                    breaker.test_count += 1
                    breaker.last_test_ts = candle["ts"]
                
                # Check invalidation
                if breaker.breaker_type == "bullish" and candle["close"] < breaker.price_low:
                    breaker.is_invalidated = True
                elif breaker.breaker_type == "bearish" and candle["close"] > breaker.price_high:
                    breaker.is_invalidated = True
    
    def get(
        self,
        breaker_type: Optional[str] = None,
        only_active: bool = True
    ) -> List[BreakerBlock]:
        """
        Query breaker blocks.
        
        Args:
            breaker_type: "bullish", "bearish", or None
            only_active: Exclude invalidated breakers
        """
        breakers = list(self._patterns)
        
        if only_active:
            breakers = [b for b in breakers if not b.is_invalidated]
        
        if breaker_type:
            breakers = [b for b in breakers if b.breaker_type == breaker_type]
        
        return breakers
    
    def get_nearest(
        self,
        current_price: float,
        direction: str = "both"
    ) -> Optional[BreakerBlock]:
        """Find nearest breaker to price."""
        breakers = self.get(only_active=True)
        
        if not breakers:
            return None
        
        if direction == "above":
            breakers = [b for b in breakers if b.price_low > current_price]
            if not breakers:
                return None
            return min(breakers, key=lambda x: x.price_low)
        elif direction == "below":
            breakers = [b for b in breakers if b.price_high < current_price]
            if not breakers:
                return None
            return max(breakers, key=lambda x: x.price_high)
        else:
            return min(breakers, key=lambda x: abs(x.midpoint - current_price))
    
    def _merge_patterns(self, new_patterns: List[BreakerBlock]) -> None:
        """Merge new breakers with existing."""
        existing_ids = {b.breaker_id for b in self._patterns}
        
        for new_breaker in new_patterns:
            if new_breaker.breaker_id not in existing_ids:
                self._patterns.append(new_breaker)
        
        # Cleanup: Keep max 100, prioritize non-invalidated
        self._patterns = sorted(
            self._patterns,
            key=lambda x: (x.is_invalidated, -x.break_ts)
        )[:100]
