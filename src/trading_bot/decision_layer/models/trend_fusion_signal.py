"""
TrendFusionSignal - Combined signal from multi-timeframe trend analysis.
"""

from typing import List, Optional
from dataclasses import dataclass

from ..enums import TrendDirection


@dataclass
class TrendFusionSignal:
    """
    Combined signal from multi-timeframe trend analysis + liquidity zones.
    
    This is what gets passed to execution layer for trade decisions.
    Combines trend alignment across multiple timeframes with key liquidity zones.
    """
    symbol: str
    signal_type: str  # "bullish_confluence", "bearish_confluence", "reversal_setup", etc.
    confidence: float  # 0.0 to 1.0
    
    # Trend information
    aligned_timeframes: List[str]  # Which TFs agree on direction
    dominant_direction: TrendDirection
    
    # Zone information
    key_zone_price: float
    zone_strength: str
    zone_type: str
    
    # Trade recommendation
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    
    # Metadata
    timestamp: int = 0
    
    def is_bullish_signal(self) -> bool:
        """Check if this is a bullish signal."""
        return self.dominant_direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]
    
    def is_bearish_signal(self) -> bool:
        """Check if this is a bearish signal."""
        return self.dominant_direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]
    
    def is_high_confidence(self, threshold: float = 0.7) -> bool:
        """Check if signal meets high confidence threshold."""
        return self.confidence >= threshold
    
    def get_risk_reward_ratio(self) -> Optional[float]:
        """Calculate risk:reward ratio."""
        if not all([self.entry_price, self.stop_loss, self.take_profit]):
            return None
        
        risk = abs(self.entry_price - self.stop_loss)
        reward = abs(self.take_profit - self.entry_price)
        
        if risk == 0:
            return None
        
        return reward / risk
    
    def __repr__(self) -> str:
        return (
            f"TrendFusionSignal({self.signal_type}, confidence={self.confidence:.2f}, "
            f"aligned_tfs={len(self.aligned_timeframes)}, zone=${self.key_zone_price:.2f})"
        )
