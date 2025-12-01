"""
Fair Value Gap (FVG) Plugin

Detects and tracks Fair Value Gaps - price imbalance zones where
price moved so quickly that no trading occurred.

Features:
- Bullish FVG detection (gap below price)
- Bearish FVG detection (gap above price)
- Fill tracking (0-100%)
- Touch counting
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import FairValueGap


class FVGPlugin(LiquidityPlugin):
    """
    Fair Value Gap detection plugin.
    
    Usage:
        plugin = FVGPlugin(symbol="BTCUSDT", timeframe="1m")
        
        # On candle close
        plugin.on_candle_close(candles, current_price)
        
        # Query FVGs
        unfilled = plugin.get(only_unfilled=True, fvg_type="bullish")
        nearest = plugin.get_nearest(current_price, direction="above")
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[FairValueGap] = []
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[FairValueGap]:
        """
        Detect Fair Value Gaps in recent candles.
        
        A FVG is a 3-candle pattern:
        - Bullish: candle[i-1].low > candle[i+1].high
        - Bearish: candle[i-1].high < candle[i+1].low
        """
        fvgs = []
        
        if len(candles) < 3:
            return fvgs
        
        # Only check recent candles
        start_idx = max(1, len(candles) - self.config.lookback_candles)
        
        for i in range(start_idx, len(candles) - 1):
            prev_candle = candles[i - 1]
            curr_candle = candles[i]
            next_candle = candles[i + 1]
            
            # Bullish FVG
            if prev_candle["low"] > next_candle["high"]:
                gap_high = prev_candle["low"]
                gap_low = next_candle["high"]
                
                if gap_high > gap_low:
                    fvg = FairValueGap(
                        fvg_id=f"{self.timeframe}_bull_{curr_candle['ts']}",
                        timeframe=self.timeframe,
                        fvg_type="bullish",
                        gap_high=gap_high,
                        gap_low=gap_low,
                        created_idx=i,
                        created_ts=curr_candle["ts"],
                        volume_before=curr_candle.get("volume", 0),
                    )
                    fvgs.append(fvg)
            
            # Bearish FVG
            elif prev_candle["high"] < next_candle["low"]:
                gap_high = next_candle["low"]
                gap_low = prev_candle["high"]
                
                if gap_high > gap_low:
                    fvg = FairValueGap(
                        fvg_id=f"{self.timeframe}_bear_{curr_candle['ts']}",
                        timeframe=self.timeframe,
                        fvg_type="bearish",
                        gap_high=gap_high,
                        gap_low=gap_low,
                        created_idx=i,
                        created_ts=curr_candle["ts"],
                        volume_before=curr_candle.get("volume", 0),
                    )
                    fvgs.append(fvg)
        
        return fvgs
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """Update FVG fill status."""
        for fvg in self._patterns:
            if fvg.is_filled:
                continue
            
            # Check recent candles
            for candle in candles[-10:]:
                candle_high = candle["high"]
                candle_low = candle["low"]
                
                # Check overlap
                if candle_low <= fvg.gap_high and candle_high >= fvg.gap_low:
                    fvg.touch_count += 1
                    fvg.last_test_ts = candle["ts"]
                    
                    # Calculate fill percentage
                    if fvg.fvg_type == "bullish":
                        fill_level = min(candle_low, fvg.gap_high)
                        filled_amount = fvg.gap_high - fill_level
                    else:
                        fill_level = max(candle_high, fvg.gap_low)
                        filled_amount = fill_level - fvg.gap_low
                    
                    fvg.fill_percentage = min(100, (filled_amount / fvg.gap_size) * 100)
                    
                    if fvg.fill_percentage >= 75:
                        fvg.is_filled = True
                        break
    
    def get(
        self,
        only_unfilled: bool = True,
        fvg_type: Optional[str] = None
    ) -> List[FairValueGap]:
        """
        Query FVGs with filters.
        
        Args:
            only_unfilled: Only return unfilled FVGs
            fvg_type: Filter by "bullish" or "bearish"
        """
        fvgs = list(self._patterns)
        
        if only_unfilled:
            fvgs = [f for f in fvgs if not f.is_filled]
        
        if fvg_type:
            fvgs = [f for f in fvgs if f.fvg_type == fvg_type]
        
        fvgs.sort(key=lambda x: x.created_ts, reverse=True)
        return fvgs
    
    def get_nearest(
        self,
        current_price: float,
        direction: str = "both",
        only_unfilled: bool = True
    ) -> Optional[FairValueGap]:
        """
        Find nearest FVG to current price.
        
        Args:
            current_price: Current market price
            direction: "above", "below", or "both"
            only_unfilled: Only consider unfilled FVGs
        """
        fvgs = self.get(only_unfilled=only_unfilled)
        
        if not fvgs:
            return None
        
        if direction == "above":
            fvgs = [f for f in fvgs if f.gap_low > current_price]
            if not fvgs:
                return None
            return min(fvgs, key=lambda x: x.gap_low)
        
        elif direction == "below":
            fvgs = [f for f in fvgs if f.gap_high < current_price]
            if not fvgs:
                return None
            return max(fvgs, key=lambda x: x.gap_high)
        
        else:  # both
            return min(fvgs, key=lambda x: abs(x.midpoint - current_price))
    
    def _merge_patterns(self, new_patterns: List[FairValueGap]) -> None:
        """Merge new FVGs, avoiding duplicates."""
        existing_ids = {fvg.fvg_id for fvg in self._patterns}
        
        for new_fvg in new_patterns:
            if new_fvg.fvg_id not in existing_ids:
                self._patterns.append(new_fvg)
        
        # Cleanup: Keep last 50 filled FVGs
        filled = [f for f in self._patterns if f.is_filled]
        if len(filled) > 50:
            filled.sort(key=lambda x: x.created_ts, reverse=True)
            filled_ids_to_keep = {f.fvg_id for f in filled[:50]}
            
            self._patterns = [
                f for f in self._patterns
                if not f.is_filled or f.fvg_id in filled_ids_to_keep
            ]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get FVG statistics."""
        stats = super().get_statistics()
        
        unfilled = [f for f in self._patterns if not f.is_filled]
        bullish = [f for f in unfilled if f.fvg_type == "bullish"]
        bearish = [f for f in unfilled if f.fvg_type == "bearish"]
        
        stats.update({
            "total_fvgs": len(self._patterns),
            "unfilled": len(unfilled),
            "bullish_unfilled": len(bullish),
            "bearish_unfilled": len(bearish),
        })
        
        return stats
