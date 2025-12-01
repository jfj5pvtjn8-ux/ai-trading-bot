"""
OrderBlock - Represents an institutional order block.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class OrderBlock:
    """
    Order Block pattern.
    
    An Order Block is the last bullish/bearish candle before a strong
    impulsive move, indicating where institutional orders were placed.
    
    Bullish OB: Last down candle before strong up move
    Bearish OB: Last up candle before strong down move
    
    These zones often provide strong support/resistance on retest.
    """
    ob_id: str                       # Unique identifier
    timeframe: str                   # Timeframe where OB was detected
    ob_type: str                     # "bullish" or "bearish"
    price_high: float                # Upper boundary of OB
    price_low: float                 # Lower boundary of OB
    candle_idx: int                  # Index of the OB candle
    created_ts: int                  # Timestamp when OB was created
    volume: float                    # Volume of the OB candle
    is_mitigated: bool = False       # Has price returned to fill 50%+
    is_breaker: bool = False         # Has become a breaker block
    touch_count: int = 0             # Number of retests
    last_test_ts: Optional[int] = None
    strength: str = "medium"         # "weak", "medium", "strong"
    
    @property
    def midpoint(self) -> float:
        """Get the midpoint price of the order block."""
        return (self.price_high + self.price_low) / 2
    
    @property
    def size(self) -> float:
        """Get the size of the order block."""
        return self.price_high - self.price_low
    
    def contains_price(self, price: float) -> bool:
        """Check if a price is within this order block."""
        return self.price_low <= price <= self.price_high
    
    def calculate_fill_percentage(self, price: float) -> float:
        """Calculate how much of the OB has been filled by current price."""
        if not self.contains_price(price):
            return 0.0
        
        if self.ob_type == "bullish":
            # For bullish OB, filling from top down
            filled = self.price_high - price
        else:
            # For bearish OB, filling from bottom up
            filled = price - self.price_low
        
        return (filled / self.size) * 100
    
    def __repr__(self) -> str:
        status = "BREAKER" if self.is_breaker else ("MITIGATED" if self.is_mitigated else "ACTIVE")
        return f"OB({self.ob_type}, ${self.price_low:.2f}-${self.price_high:.2f}, {status})"
