"""
LiquidityZone - Represents a support/resistance liquidity zone.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class LiquidityZone:
    """
    Represents a single liquidity zone (support/resistance area).
    
    A zone is an area where significant trading activity occurred,
    indicating potential future price reactions.
    """
    zone_id: str                      # Unique identifier
    timeframe: str                    # Timeframe where zone was detected
    zone_type: str                    # "support", "resistance", "supply", "demand"
    price_low: float                  # Lower boundary of zone
    price_high: float                 # Upper boundary of zone
    volume: float                     # Total volume in this zone
    touch_count: int = 0              # Number of times price touched this zone
    last_touch_ts: Optional[int] = None  # Last time price touched zone
    created_ts: int = 0               # When zone was first created
    strength: str = "weak"            # "weak", "medium", "strong"
    is_active: bool = True            # False if zone broken/consumed
    confluence_count: int = 0         # How many TFs have zone here
    confluence_weight: float = 0.0    # Weighted confluence score (higher TF = more weight)
    pivot_high: Optional[float] = None  # Swing high that created zone
    pivot_low: Optional[float] = None   # Swing low that created zone
    pd_position: str = "equilibrium"  # "premium", "equilibrium", "discount"
    equilibrium_distance: float = 0.0  # % distance from 50% level (+ = premium, - = discount)
    
    @property
    def midpoint(self) -> float:
        """Get the midpoint price of the zone."""
        return (self.price_high + self.price_low) / 2
    
    @property
    def size(self) -> float:
        """Get the size of the zone."""
        return self.price_high - self.price_low
    
    def contains_price(self, price: float) -> bool:
        """Check if a price is within this zone."""
        return self.price_low <= price <= self.price_high
    
    def is_premium(self) -> bool:
        """Check if zone is in premium area (above equilibrium)."""
        return self.pd_position == "premium"
    
    def is_discount(self) -> bool:
        """Check if zone is in discount area (below equilibrium)."""
        return self.pd_position == "discount"
    
    def is_equilibrium(self) -> bool:
        """Check if zone is at equilibrium (near 50% level)."""
        return self.pd_position == "equilibrium"
    
    def __repr__(self) -> str:
        return (
            f"Zone({self.zone_type}, {self.price_low:.2f}-{self.price_high:.2f}, "
            f"{self.timeframe}, strength={self.strength}, touches={self.touch_count}, "
            f"pd={self.pd_position})"
        )
