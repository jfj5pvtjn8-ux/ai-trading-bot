"""
Break of Structure (BOS) / Change of Character (CHOCH) Plugin

Detects market structure changes:
- BOS: Break of Structure (trend continuation)
- CHOCH: Change of Character (trend reversal)

Architecture:
- Uses StructureBreak and StructurePoint models from models/
- Implements detection and update logic
- Provides query interface for decision layer
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import StructureBreak, StructurePoint


class BOSCHOCHPlugin(LiquidityPlugin):
    """
    Market structure break detection plugin.
    
    Detection Logic:
    1. Identify swing highs and lows
    2. Track current market structure
    3. Detect when structure is broken
    4. Classify as BOS or CHOCH
    
    Swing Point Criteria:
    - Swing High: Price higher than N candles left and right
    - Swing Low: Price lower than N candles left and right
    - Default N = 5 (configurable)
    
    BOS Detection:
    - Uptrend: Break above last swing high → Bullish BOS
    - Downtrend: Break below last swing low → Bearish BOS
    
    CHOCH Detection:
    - Uptrend: Break below last swing low → Bearish CHOCH
    - Downtrend: Break above last swing high → Bullish CHOCH
    
    Trend Classification:
    - Uptrend: Higher highs and higher lows
    - Downtrend: Lower highs and lower lows
    - Ranging: No clear structure
    
    Strength Factors:
    - Break decisiveness (how far through)
    - Volume on break candle
    - Confirmation (price stays beyond structure)
    
    Usage:
        plugin = BOSCHOCHPlugin(symbol="BTCUSDT", timeframe="1h")
        plugin.on_candle_close(candles, current_price)
        
        # Query breaks
        recent_bos = plugin.get(break_type="BOS", direction="bullish")
        chochs = plugin.get(break_type="CHOCH")
        trend = plugin.get_current_trend()
        
        # Structure analysis
        highs = plugin.get_swing_highs()
        lows = plugin.get_swing_lows()
    
    TODO Implementation Steps:
    1. Implement swing high/low detection (N-bar pattern)
    2. Track market structure (highs and lows list)
    3. Detect BOS (break of last structure point in trend)
    4. Detect CHOCH (break of structure counter to trend)
    5. Calculate trend state (HH/HL vs LH/LL)
    6. Calculate break strength
    7. Confirm breaks (price stays beyond)
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[StructureBreak] = []
        self._swing_highs: List[StructurePoint] = []
        self._swing_lows: List[StructurePoint] = []
        self.swing_lookback = 5  # Candles left/right for swing
        self.current_trend = "ranging"
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[StructureBreak]:
        """
        Detect structure breaks (BOS/CHOCH).
        
        Algorithm:
        1. Identify swing highs and lows
        2. Determine current trend direction
        3. Detect when structure is broken
        4. Classify as BOS (continuation) or CHOCH (reversal)
        """
        if len(candles) < self.swing_lookback * 2 + 5:
            return []
        
        structure_breaks = []
        self._update_swing_points(candles)
        
        recent = candles[-50:] if len(candles) > 50 else candles
        
        for i in range(len(recent) - 1):
            candle = recent[i]
            candle_idx = len(candles) - len(recent) + i
            
            # Check bullish breaks (breaking above swing high)
            for swing_high in self._swing_highs:
                if not swing_high.confirmed:
                    continue
                
                if candle["close"] > swing_high.price:
                    previous_trend = self.current_trend
                    if self.current_trend == "up":
                        break_type = "BOS"  # Continuation
                        direction = "bullish"
                    else:
                        break_type = "CHOCH"  # Reversal
                        direction = "bullish"
                        self.current_trend = "up"
                    
                    break_id = f"{self.symbol}_{self.timeframe}_break_{candle['ts']}"
                    sb = StructureBreak(
                        break_id=break_id,
                        timeframe=self.timeframe,
                        break_type=break_type,
                        direction=direction,
                        break_price=candle["close"],
                        structure_price=swing_high.price,
                        candle_idx=candle_idx,
                        timestamp=candle["ts"],
                        previous_trend=previous_trend,
                        confirmed=True
                    )
                    structure_breaks.append(sb)
                    break
            
            # Check bearish breaks (breaking below swing low)
            for swing_low in self._swing_lows:
                if not swing_low.confirmed:
                    continue
                
                if candle["close"] < swing_low.price:
                    previous_trend = self.current_trend
                    if self.current_trend == "down":
                        break_type = "BOS"  # Continuation
                        direction = "bearish"
                    else:
                        break_type = "CHOCH"  # Reversal
                        direction = "bearish"
                        self.current_trend = "down"
                    
                    break_id = f"{self.symbol}_{self.timeframe}_break_{candle['ts']}"
                    sb = StructureBreak(
                        break_id=break_id,
                        timeframe=self.timeframe,
                        break_type=break_type,
                        direction=direction,
                        break_price=candle["close"],
                        structure_price=swing_low.price,
                        candle_idx=candle_idx,
                        timestamp=candle["ts"],
                        previous_trend=previous_trend,
                        confirmed=True
                    )
                    structure_breaks.append(sb)
                    break
        
        return structure_breaks
    
    def _update_swing_points(self, candles: List[Dict[str, Any]]) -> None:
        """Identify swing highs and lows."""
        if len(candles) < self.swing_lookback * 2 + 1:
            return
        
        lookback = min(100, len(candles))
        recent = candles[-lookback:]
        
        for i in range(self.swing_lookback, len(recent) - self.swing_lookback):
            candle = recent[i]
            
            # Swing high check
            is_swing_high = True
            for j in range(1, self.swing_lookback + 1):
                if recent[i - j]["high"] >= candle["high"] or recent[i + j]["high"] >= candle["high"]:
                    is_swing_high = False
                    break
            
            if is_swing_high:
                swing_high = StructurePoint(
                    point_type="high",
                    price=candle["high"],
                    candle_idx=len(candles) - lookback + i,
                    timestamp=candle["ts"],
                    confirmed=True
                )
                if not any(sh.timestamp == swing_high.timestamp for sh in self._swing_highs):
                    self._swing_highs.append(swing_high)
            
            # Swing low check
            is_swing_low = True
            for j in range(1, self.swing_lookback + 1):
                if recent[i - j]["low"] <= candle["low"] or recent[i + j]["low"] <= candle["low"]:
                    is_swing_low = False
                    break
            
            if is_swing_low:
                swing_low = StructurePoint(
                    point_type="low",
                    price=candle["low"],
                    candle_idx=len(candles) - lookback + i,
                    timestamp=candle["ts"],
                    confirmed=True
                )
                if not any(sl.timestamp == swing_low.timestamp for sl in self._swing_lows):
                    self._swing_lows.append(swing_low)
        
        # Keep recent points only
        self._swing_highs = self._swing_highs[-50:]
        self._swing_lows = self._swing_lows[-50:]
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update structure state.
        
        Algorithm:
        1. Refresh swing points
        2. Keep pattern storage manageable
        3. Trend updated during detect()
        """
        if not candles:
            return
        
        # Update swing points
        self._update_swing_points(candles)
        
        # Limit pattern storage
        if len(self._patterns) > 100:
            self._patterns = self._patterns[-100:]
    
    def get(
        self,
        break_type: Optional[str] = None,
        direction: Optional[str] = None,
        only_recent: bool = True
    ) -> List[StructureBreak]:
        """
        Query structure breaks.
        
        Args:
            break_type: "BOS", "CHOCH", or None
            direction: "bullish", "bearish", or None
            only_recent: Only last 20 breaks
        """
        breaks = list(self._patterns)
        
        if break_type:
            breaks = [b for b in breaks if b.break_type == break_type]
        
        if direction:
            breaks = [b for b in breaks if b.direction == direction]
        
        if only_recent:
            breaks = breaks[-20:]
        
        return breaks
    
    def get_swing_highs(self, confirmed_only: bool = True) -> List[StructurePoint]:
        """Get swing high points."""
        if confirmed_only:
            return [h for h in self._swing_highs if h.confirmed]
        return list(self._swing_highs)
    
    def get_swing_lows(self, confirmed_only: bool = True) -> List[StructurePoint]:
        """Get swing low points."""
        if confirmed_only:
            return [l for l in self._swing_lows if l.confirmed]
        return list(self._swing_lows)
    
    def get_current_trend(self) -> str:
        """Get current trend state: "up", "down", or "ranging"."""
        return self.current_trend
    
    def get_last_bos(self, direction: Optional[str] = None) -> Optional[StructureBreak]:
        """Get most recent BOS."""
        bos_list = self.get(break_type="BOS", direction=direction)
        return bos_list[-1] if bos_list else None
    
    def get_last_choch(self, direction: Optional[str] = None) -> Optional[StructureBreak]:
        """Get most recent CHOCH."""
        choch_list = self.get(break_type="CHOCH", direction=direction)
        return choch_list[-1] if choch_list else None
    
    def _merge_patterns(self, new_patterns: List[StructureBreak]) -> None:
        """Merge new breaks with existing."""
        existing_ids = {b.break_id for b in self._patterns}
        
        for new_break in new_patterns:
            if new_break.break_id not in existing_ids:
                self._patterns.append(new_break)
        
        # Keep last 50 breaks
        self._patterns = self._patterns[-50:]
