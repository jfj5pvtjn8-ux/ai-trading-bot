"""
TrendState - Represents the trend state for a single timeframe.
"""

from typing import Optional
from dataclasses import dataclass

from ..enums import TrendDirection, TrendStrength


@dataclass
class TrendState:
    """
    Trend state for a single timeframe.
    
    Captures complete trend information including direction, strength,
    momentum, and structure for use in multi-timeframe trend analysis.
    """
    timeframe: str
    direction: TrendDirection
    strength: TrendStrength
    momentum_score: float = 0.0  # -100 to +100
    
    # EMA/SMA analysis
    price_above_ema20: bool = False
    price_above_ema50: bool = False
    ema20_above_ema50: bool = False
    
    # Structure analysis
    higher_highs: bool = False
    higher_lows: bool = False
    lower_highs: bool = False
    lower_lows: bool = False
    
    # Momentum indicators
    rsi: Optional[float] = None
    macd_histogram: Optional[float] = None
    
    # Metadata
    last_update_ts: Optional[int] = None
    candles_analyzed: int = 0
    
    def is_bullish(self) -> bool:
        """Check if trend is bullish (any degree)."""
        return self.direction in [TrendDirection.BULLISH, TrendDirection.STRONG_BULLISH]
    
    def is_bearish(self) -> bool:
        """Check if trend is bearish (any degree)."""
        return self.direction in [TrendDirection.BEARISH, TrendDirection.STRONG_BEARISH]
    
    def is_strong(self) -> bool:
        """Check if trend is strong (not weak or very weak)."""
        return self.strength in [TrendStrength.STRONG, TrendStrength.VERY_STRONG]
    
    def is_neutral(self) -> bool:
        """Check if trend is neutral."""
        return self.direction == TrendDirection.NEUTRAL
    
    def __repr__(self) -> str:
        return (
            f"TrendState({self.timeframe}, {self.direction.value}, "
            f"{self.strength.value}, momentum={self.momentum_score:.1f})"
        )
