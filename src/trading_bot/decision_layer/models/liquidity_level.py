"""
LiquidityLevel - Represents SSL/BSL liquidity levels.
"""

from typing import Optional, List
from dataclasses import dataclass, field


@dataclass
class LiquidityLevel:
    """
    Liquidity Level (SSL/BSL).
    
    SSL (Sell-Side Liquidity): Equal lows - stops clustered below support
    BSL (Buy-Side Liquidity): Equal highs - stops clustered above resistance
    
    These represent areas where stop losses are concentrated, making them
    prime targets for institutional "liquidity hunts."
    """
    level_id: str                    # Unique identifier
    timeframe: str                   # Timeframe where level was detected
    level_type: str                  # "SSL" (equal lows) or "BSL" (equal highs)
    price: float                     # Price level
    touch_count: int                 # How many times price touched this level
    touches: List[tuple] = field(default_factory=list)  # [(candle_idx, timestamp), ...]
    is_swept: bool = False           # Has level been swept/taken out
    sweep_candle_idx: Optional[int] = None
    sweep_timestamp: Optional[int] = None
    created_ts: int = 0              # When level was first created
    strength: str = "medium"         # "weak", "medium", "strong"
    
    def add_touch(self, candle_idx: int, timestamp: int) -> None:
        """Record a touch of this level."""
        self.touches.append((candle_idx, timestamp))
        self.touch_count = len(self.touches)
    
    def mark_swept(self, candle_idx: int, timestamp: int) -> None:
        """Mark this level as swept."""
        self.is_swept = True
        self.sweep_candle_idx = candle_idx
        self.sweep_timestamp = timestamp
    
    def is_near_price(self, price: float, tolerance_pct: float = 0.001) -> bool:
        """Check if a price is near this level (within tolerance)."""
        return abs(price - self.price) / self.price <= tolerance_pct
    
    def __repr__(self) -> str:
        status = "SWEPT" if self.is_swept else f"{self.touch_count} touches"
        return f"{self.level_type}(${self.price:.2f}, {status}, {self.timeframe})"
