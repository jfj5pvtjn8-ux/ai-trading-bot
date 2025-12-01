"""
BreakerBlock - Represents a failed order block that has flipped polarity.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class BreakerBlock:
    """
    Breaker Block pattern.
    
    A breaker is a failed order block that has flipped polarity:
    - Bullish OB that got broken → Becomes bearish breaker (resistance)
    - Bearish OB that got broken → Becomes bullish breaker (support)
    
    Breakers often provide stronger support/resistance than original OBs
    because they represent a shift in market structure.
    """
    breaker_id: str                  # Unique identifier
    timeframe: str                   # Timeframe where breaker exists
    breaker_type: str                # "bullish" or "bearish"
    price_high: float                # Upper boundary of breaker
    price_low: float                 # Lower boundary of breaker
    original_ob_type: str            # Original OB type before flip
    break_candle_idx: int            # Index of candle that broke OB
    break_ts: int                    # Timestamp when OB was broken
    created_ts: int                  # Original OB creation timestamp
    is_tested: bool = False          # Has breaker been tested
    test_count: int = 0              # Number of retests
    last_test_ts: Optional[int] = None
    strength: str = "medium"         # "weak", "medium", "strong"
    is_invalidated: bool = False     # Has breaker been invalidated
    
    @property
    def midpoint(self) -> float:
        """Get the midpoint price of the breaker block."""
        return (self.price_high + self.price_low) / 2
    
    @property
    def size(self) -> float:
        """Get the size of the breaker block."""
        return self.price_high - self.price_low
    
    def contains_price(self, price: float) -> bool:
        """Check if a price is within this breaker block."""
        return self.price_low <= price <= self.price_high
    
    def mark_tested(self, timestamp: int) -> None:
        """Record a test of this breaker."""
        self.is_tested = True
        self.test_count += 1
        self.last_test_ts = timestamp
    
    def invalidate(self) -> None:
        """Mark this breaker as invalidated."""
        self.is_invalidated = True
    
    def __repr__(self) -> str:
        status = "INVALID" if self.is_invalidated else (
            f"TESTED({self.test_count}x)" if self.is_tested else "ACTIVE"
        )
        return (
            f"Breaker({self.breaker_type}, "
            f"${self.price_low:.2f}-${self.price_high:.2f}, "
            f"{status}, {self.timeframe})"
        )
