"""
Displacement Model

Represents a strong directional move in price action, typically characterized by:
- Multiple consecutive candles moving in the same direction (3+)
- Above-average volume
- Large candle bodies (minimal wicks)
- Momentum/urgency in the move

Displacement is a key SMC concept indicating institutional order flow.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Displacement:
    """
    Represents a displacement move in price action.
    
    A displacement is a strong, sustained directional move that indicates
    institutional activity. Common characteristics:
    - 3+ consecutive candles in same direction
    - Volume surge (above average)
    - Large bodied candles
    - Minimal retracement during the move
    
    Attributes:
        symbol: Trading symbol
        timeframe: Timeframe where displacement occurred
        direction: "bullish" or "bearish"
        start_price: Price at start of displacement
        end_price: Price at end of displacement
        start_ts: Timestamp when displacement started
        end_ts: Timestamp when displacement ended
        num_candles: Number of candles in displacement
        total_move: Total price move (pips/points)
        move_pct: Move as percentage
        avg_volume: Average volume during displacement
        volume_surge_ratio: Volume compared to baseline (1.5 = 50% above avg)
        created_ts: When this displacement was detected
    """
    symbol: str
    timeframe: str
    direction: str  # "bullish" or "bearish"
    start_price: float
    end_price: float
    start_ts: int
    end_ts: int
    num_candles: int
    total_move: float
    move_pct: float
    avg_volume: float
    volume_surge_ratio: float
    created_ts: int
    
    @property
    def is_bullish(self) -> bool:
        """Check if displacement is bullish."""
        return self.direction == "bullish"
    
    @property
    def is_bearish(self) -> bool:
        """Check if displacement is bearish."""
        return self.direction == "bearish"
    
    @property
    def midpoint(self) -> float:
        """Calculate midpoint of displacement move."""
        return (self.start_price + self.end_price) / 2.0
    
    @property
    def duration_seconds(self) -> int:
        """Duration of displacement in seconds."""
        return self.end_ts - self.start_ts
    
    def is_strong_displacement(self, min_candles: int = 5, min_volume_ratio: float = 2.0) -> bool:
        """
        Check if this is a strong displacement.
        
        Args:
            min_candles: Minimum candles for strong displacement
            min_volume_ratio: Minimum volume surge ratio
            
        Returns:
            True if displacement is considered strong
        """
        return (
            self.num_candles >= min_candles and
            self.volume_surge_ratio >= min_volume_ratio
        )
    
    def contains_price(self, price: float) -> bool:
        """Check if price falls within displacement range."""
        low = min(self.start_price, self.end_price)
        high = max(self.start_price, self.end_price)
        return low <= price <= high
