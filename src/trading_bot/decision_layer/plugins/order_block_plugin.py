"""
Order Block (OB) Plugin

Detects Order Blocks - the last bullish/bearish candle before
a strong impulsive move, indicating institutional order placement.

Architecture:
- Uses OrderBlock model from models/
- Implements detection and update logic
- Provides query interface for decision layer
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import OrderBlock


class OrderBlockPlugin(LiquidityPlugin):
    """
    Order Block detection plugin.
    
    Detection Logic:
    1. Find strong impulsive moves (large candles with high volume)
    2. Identify the candle BEFORE the impulse
    3. That candle is the Order Block
    4. Bullish OB = last down candle before up move
    5. Bearish OB = last up candle before down move
    
    Strength Criteria:
    - Volume: Higher volume = stronger
    - Size: Larger impulse move = stronger
    - Retest: More retests = stronger
    
    Mitigation:
    - OB is "mitigated" when price returns and fills 50%+
    
    Breaker Blocks:
    - OB that gets broken through becomes opposite polarity
    - Bullish OB broken = becomes bearish breaker
    - Bearish OB broken = becomes bullish breaker
    
    Usage:
        plugin = OrderBlockPlugin(symbol="BTCUSDT", timeframe="15m")
        plugin.on_candle_close(candles, current_price)
        
        # Query OBs
        bullish_obs = plugin.get(ob_type="bullish", only_unmitigated=True)
        breakers = plugin.get_breakers()
        nearest = plugin.get_nearest(current_price, direction="below")
    
    TODO Implementation Steps:
    1. Detect impulsive candles (size > ATR * 1.5, volume > avg)
    2. Find the candle before impulse
    3. Classify as bullish/bearish OB
    4. Track mitigation (price returning)
    5. Detect breaker blocks (OB broken)
    6. Calculate strength score
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[OrderBlock] = []
        self.min_impulse_multiplier = 1.5  # Candle must be 1.5x ATR
        self.min_volume_multiplier = 1.2  # Volume must be 1.2x average
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[OrderBlock]:
        """
        Detect Order Blocks.
        
        Algorithm:
        1. Calculate average body size for displacement detection
        2. Find impulsive candles (large displacement moves)
        3. Identify last opposite candle before impulse = Order Block
        4. Calculate strength based on displacement magnitude
        """
        if len(candles) < 20:
            return []
        
        order_blocks = []
        lookback = min(100, len(candles))
        recent = candles[-lookback:]
        
        # Calculate average candle body for reference
        avg_body = sum(abs(c["close"] - c["open"]) for c in recent) / len(recent)
        displacement_threshold = avg_body * self.min_impulse_multiplier
        
        i = 1
        while i < len(recent) - 1:
            candle = recent[i]
            body_size = abs(candle["close"] - candle["open"])
            
            # Bullish displacement detected
            if body_size > displacement_threshold and candle["close"] > candle["open"]:
                # Find last bearish candle before displacement
                for j in range(i - 1, max(0, i - 10), -1):
                    prev = recent[j]
                    if prev["close"] < prev["open"]:
                        ob_id = f"{self.symbol}_{self.timeframe}_ob_bull_{prev['ts']}"
                        move_pct = (candle["close"] - candle["open"]) / candle["open"]
                        strength = self._calc_strength(move_pct)
                        
                        ob = OrderBlock(
                            ob_id=ob_id,
                            timeframe=self.timeframe,
                            ob_type="bullish",
                            price_high=prev["high"],
                            price_low=prev["low"],
                            candle_idx=len(candles) - lookback + j,
                            created_ts=prev["ts"],
                            volume=prev.get("volume", 0),
                            strength=strength
                        )
                        order_blocks.append(ob)
                        i += 5
                        break
            
            # Bearish displacement detected
            elif body_size > displacement_threshold and candle["close"] < candle["open"]:
                # Find last bullish candle before displacement
                for j in range(i - 1, max(0, i - 10), -1):
                    prev = recent[j]
                    if prev["close"] > prev["open"]:
                        ob_id = f"{self.symbol}_{self.timeframe}_ob_bear_{prev['ts']}"
                        move_pct = abs((candle["close"] - candle["open"]) / candle["open"])
                        strength = self._calc_strength(move_pct)
                        
                        ob = OrderBlock(
                            ob_id=ob_id,
                            timeframe=self.timeframe,
                            ob_type="bearish",
                            price_high=prev["high"],
                            price_low=prev["low"],
                            candle_idx=len(candles) - lookback + j,
                            created_ts=prev["ts"],
                            volume=prev.get("volume", 0),
                            strength=strength
                        )
                        order_blocks.append(ob)
                        i += 5
                        break
            
            i += 1
        
        return order_blocks
    
    def _calc_strength(self, move_pct: float) -> str:
        """Calculate OB strength from displacement magnitude."""
        score = min(abs(move_pct) * 50, 1.0)
        if score > 0.7:
            return "strong"
        elif score > 0.4:
            return "medium"
        return "weak"
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update OBs: mitigation and breaker detection.
        
        Algorithm:
        1. Check if price returned to OB zone (mitigation)
        2. Check if OB was fully broken (becomes breaker)
        3. Update touch counts
        4. Track test timestamps
        """
        if not candles or not self._patterns:
            return
        
        recent = candles[-20:] if len(candles) > 20 else candles
        
        for ob in self._patterns:
            if ob.is_breaker:
                continue
            
            # Check each recent candle
            for candle in recent:
                # Check for mitigation (price returned to OB)
                if not ob.is_mitigated:
                    if ob.ob_type == "bullish":
                        # Price came back down to bullish OB
                        if candle["low"] <= ob.price_high and candle["low"] >= ob.price_low:
                            ob.is_mitigated = True
                            ob.touch_count += 1
                            ob.last_test_ts = candle["ts"]
                    elif ob.ob_type == "bearish":
                        # Price came back up to bearish OB
                        if candle["high"] >= ob.price_low and candle["high"] <= ob.price_high:
                            ob.is_mitigated = True
                            ob.touch_count += 1
                            ob.last_test_ts = candle["ts"]
                
                # Check for breaker (OB was broken through)
                if ob.ob_type == "bullish":
                    # Bullish OB broken when price closes below its low
                    if candle["close"] < ob.price_low:
                        ob.is_breaker = True
                elif ob.ob_type == "bearish":
                    # Bearish OB broken when price closes above its high
                    if candle["close"] > ob.price_high:
                        ob.is_breaker = True
                
                # Count additional tests
                if ob.is_mitigated and not ob.is_breaker:
                    if ob.ob_type == "bullish":
                        if candle["low"] <= ob.price_high and candle["low"] >= ob.price_low:
                            if ob.last_test_ts != candle["ts"]:
                                ob.touch_count += 1
                                ob.last_test_ts = candle["ts"]
                    elif ob.ob_type == "bearish":
                        if candle["high"] >= ob.price_low and candle["high"] <= ob.price_high:
                            if ob.last_test_ts != candle["ts"]:
                                ob.touch_count += 1
                                ob.last_test_ts = candle["ts"]
    
    def get(
        self,
        ob_type: Optional[str] = None,
        only_unmitigated: bool = True,
        min_strength: Optional[str] = None
    ) -> List[OrderBlock]:
        """
        Query Order Blocks.
        
        Args:
            ob_type: "bullish", "bearish", or None
            only_unmitigated: Only return active OBs
            min_strength: "weak", "medium", or "strong"
        """
        obs = list(self._patterns)
        
        if only_unmitigated:
            obs = [o for o in obs if not o.is_mitigated and not o.is_breaker]
        
        if ob_type:
            obs = [o for o in obs if o.ob_type == ob_type]
        
        if min_strength:
            strength_order = {"weak": 0, "medium": 1, "strong": 2}
            min_val = strength_order.get(min_strength, 0)
            obs = [o for o in obs if strength_order[o.strength] >= min_val]
        
        return obs
    
    def get_breakers(self) -> List[OrderBlock]:
        """Get breaker blocks."""
        return [o for o in self._patterns if o.is_breaker]
    
    def get_nearest(
        self,
        current_price: float,
        direction: str = "both"
    ) -> Optional[OrderBlock]:
        """Find nearest OB to price."""
        obs = self.get(only_unmitigated=True)
        
        if not obs:
            return None
        
        if direction == "above":
            obs = [o for o in obs if o.price_low > current_price]
            if not obs:
                return None
            return min(obs, key=lambda x: x.price_low)
        elif direction == "below":
            obs = [o for o in obs if o.price_high < current_price]
            if not obs:
                return None
            return max(obs, key=lambda x: x.price_high)
        else:
            return min(obs, key=lambda x: abs(x.midpoint - current_price))
    
    def _merge_patterns(self, new_patterns: List[OrderBlock]) -> None:
        """Merge new OBs with existing."""
        existing_ids = {o.ob_id for o in self._patterns}
        
        for new_ob in new_patterns:
            if new_ob.ob_id not in existing_ids:
                self._patterns.append(new_ob)
        
        # Cleanup: Keep max 100, prioritize unmitigated
        self._patterns = sorted(
            self._patterns,
            key=lambda x: (x.is_mitigated, -x.created_ts)
        )[:100]
