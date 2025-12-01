"""
VolumeProfile - Volume distribution across price levels.
"""

from dataclasses import dataclass


@dataclass
class VolumeProfile:
    """Volume distribution across price levels for a zone."""
    price_level: float
    volume: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    
    @property
    def order_imbalance(self) -> float:
        """
        Calculate order flow imbalance (-1 to 1, positive = bullish).
        
        Returns:
            -1.0: Pure selling pressure
             0.0: Balanced
            +1.0: Pure buying pressure
        """
        total = self.buy_volume + self.sell_volume
        if total == 0:
            return 0.0
        return (self.buy_volume - self.sell_volume) / total
    
    @property
    def is_bullish(self) -> bool:
        """Check if order flow is bullish (more buying than selling)."""
        return self.order_imbalance > 0.2
    
    @property
    def is_bearish(self) -> bool:
        """Check if order flow is bearish (more selling than buying)."""
        return self.order_imbalance < -0.2
