"""
LiquiditySweep - Represents a liquidity hunt/sweep pattern.
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class LiquiditySweep:
    """
    Liquidity sweep pattern.
    
    A sweep occurs when price takes out a key level (SSL/BSL, swing high/low,
    order block, etc.) to trigger stops and collect liquidity, then reverses.
    
    Types:
    - Buy-side sweep: Take out resistance/highs, then reverse down
    - Sell-side sweep: Take out support/lows, then reverse up
    
    This is a common institutional tactic to accumulate liquidity before
    a significant move in the opposite direction.
    """
    sweep_id: str                    # Unique identifier
    timeframe: str                   # Timeframe where sweep occurred
    sweep_type: str                  # "buy_side" or "sell_side"
    level_price: float               # Original level that was swept
    sweep_price: float               # How far price swept beyond level
    sweep_candle_idx: int            # Index of sweeping candle
    sweep_ts: int                    # Timestamp of sweep
    reversal_price: Optional[float] = None  # Price of reversal
    reversal_ts: Optional[int] = None       # Timestamp of reversal
    confirmed: bool = False          # Has reversal occurred
    level_type: str = "ssl_bsl"      # "ssl_bsl", "swing", "ob", "fvg"
    strength: str = "medium"         # "weak", "medium", "strong"
    volume_spike: bool = False       # Was there volume spike on sweep
    
    @property
    def sweep_distance(self) -> float:
        """How far price swept beyond the level."""
        return abs(self.sweep_price - self.level_price)
    
    @property
    def sweep_percentage(self) -> float:
        """Sweep distance as percentage of level price."""
        return (self.sweep_distance / self.level_price) * 100
    
    @property
    def reversal_distance(self) -> Optional[float]:
        """How far price reversed after the sweep."""
        if self.reversal_price:
            return abs(self.reversal_price - self.sweep_price)
        return None
    
    @property
    def reversal_percentage(self) -> Optional[float]:
        """Reversal distance as percentage of sweep price."""
        if self.reversal_distance:
            return (self.reversal_distance / self.sweep_price) * 100
        return None
    
    def confirm_reversal(self, reversal_price: float, reversal_ts: int) -> None:
        """Confirm the sweep with reversal details."""
        self.confirmed = True
        self.reversal_price = reversal_price
        self.reversal_ts = reversal_ts
    
    def is_buy_side_sweep(self) -> bool:
        """Check if this is a buy-side sweep (took out highs)."""
        return self.sweep_type == "buy_side"
    
    def is_sell_side_sweep(self) -> bool:
        """Check if this is a sell-side sweep (took out lows)."""
        return self.sweep_type == "sell_side"
    
    def __repr__(self) -> str:
        status = "CONFIRMED" if self.confirmed else "UNCONFIRMED"
        reversal_info = f" → ${self.reversal_price:.2f}" if self.reversal_price else ""
        return (
            f"Sweep({self.sweep_type}, "
            f"${self.level_price:.2f} → ${self.sweep_price:.2f}{reversal_info}, "
            f"{status}, {self.timeframe})"
        )
