"""
FairValueGap - Represents a Fair Value Gap (FVG) price imbalance zone.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class FairValueGap:
    """
    Fair Value Gap (FVG) - Price imbalance zone.
    
    A FVG occurs when there's a gap between three consecutive candles:
    - Bullish FVG: Gap between candle[i-1].low and candle[i+1].high (price jumped up)
    - Bearish FVG: Gap between candle[i-1].high and candle[i+1].low (price dropped down)
    
    FVGs act as magnets - price often returns to fill the gap.
    """
    fvg_id: str                      # Unique identifier
    timeframe: str                   # Timeframe where FVG was detected
    fvg_type: str                    # "bullish" or "bearish"
    gap_high: float                  # Upper boundary of gap
    gap_low: float                   # Lower boundary of gap
    created_idx: int                 # Candle index where FVG formed
    created_ts: int                  # Timestamp when FVG formed
    is_filled: bool = False          # True if price has filled the gap
    fill_percentage: float = 0.0     # How much of gap is filled (0-100%)
    last_test_ts: Optional[int] = None  # Last time price tested the FVG
    touch_count: int = 0             # Number of times price touched FVG
    volume_before: float = 0.0       # Volume of impulse candle
    
    @property
    def gap_size(self) -> float:
        """Size of the gap."""
        return self.gap_high - self.gap_low
    
    @property
    def midpoint(self) -> float:
        """Midpoint of the gap."""
        return (self.gap_high + self.gap_low) / 2
    
    def overlaps_with_price(self, price: float) -> bool:
        """Check if price overlaps with this FVG."""
        return self.gap_low <= price <= self.gap_high
    
    def __repr__(self) -> str:
        status = "FILLED" if self.is_filled else f"{self.fill_percentage:.0f}% filled"
        return (
            f"FVG({self.fvg_type}, {self.gap_low:.2f}-{self.gap_high:.2f}, "
            f"{self.timeframe}, {status})"
        )
