"""
StructureBreak - Represents BOS (Break of Structure) or CHOCH (Change of Character).
"""

from typing import Optional
from dataclasses import dataclass


@dataclass
class StructurePoint:
    """
    Swing high or low point in market structure.
    """
    point_type: str                  # "high" or "low"
    price: float                     # Price of the swing point
    candle_idx: int                  # Index of the candle
    timestamp: int                   # Timestamp
    confirmed: bool = False          # Has structure been confirmed


@dataclass
class StructureBreak:
    """
    Structure break pattern (BOS or CHOCH).
    
    BOS (Break of Structure):
    - In uptrend: Break above previous high (trend continuation)
    - In downtrend: Break below previous low (trend continuation)
    - Confirms existing trend is still valid
    
    CHOCH (Change of Character):
    - In uptrend: Break below previous low (potential reversal)
    - In downtrend: Break above previous high (potential reversal)
    - Signals potential trend reversal or weakening
    """
    break_id: str                    # Unique identifier
    timeframe: str                   # Timeframe where break occurred
    break_type: str                  # "BOS" or "CHOCH"
    direction: str                   # "bullish" or "bearish"
    break_price: float               # Price where structure was broken
    structure_price: float           # Previous high/low that was broken
    candle_idx: int                  # Index of breaking candle
    timestamp: int                   # Timestamp of break
    previous_trend: str              # "up", "down", or "ranging"
    confirmed: bool = True           # Is break confirmed
    strength: str = "medium"         # "weak", "medium", "strong"
    
    @property
    def break_size(self) -> float:
        """How far price broke through structure."""
        return abs(self.break_price - self.structure_price)
    
    @property
    def break_percentage(self) -> float:
        """Break size as percentage of structure price."""
        return (self.break_size / self.structure_price) * 100
    
    def is_trend_continuation(self) -> bool:
        """Check if this is a trend continuation (BOS)."""
        return self.break_type == "BOS"
    
    def is_trend_reversal(self) -> bool:
        """Check if this is a potential trend reversal (CHOCH)."""
        return self.break_type == "CHOCH"
    
    def __repr__(self) -> str:
        return (
            f"{self.break_type}({self.direction}, "
            f"${self.structure_price:.2f} → ${self.break_price:.2f}, "
            f"{self.timeframe})"
        )
