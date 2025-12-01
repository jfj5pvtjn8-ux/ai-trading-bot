"""
ClassifiedZone - Enhanced liquidity zone with quality classification.
"""

from typing import Optional
from dataclasses import dataclass

from ..enums import ZoneQuality, TimeRelevance, ZoneReaction


@dataclass
class ClassifiedZone:
    """
    Enhanced liquidity zone with quality classification and scoring.
    
    Extends basic zone information with quality assessment, time relevance,
    reaction history, and confidence scoring for trading decisions.
    """
    # Basic zone information
    zone_id: str
    timeframe: str
    zone_type: str  # "support", "resistance", "supply", "demand"
    price_low: float
    price_high: float
    
    # Classification
    quality: ZoneQuality
    time_relevance: TimeRelevance
    last_reaction: ZoneReaction = ZoneReaction.PENDING
    
    # Scoring
    confidence_score: float = 0.0  # 0.0 to 1.0
    strength_score: float = 0.0    # 0.0 to 1.0
    
    # Historical data
    touch_count: int = 0
    successful_bounces: int = 0
    volume: float = 0.0
    created_ts: int = 0
    last_touch_ts: Optional[int] = None
    
    # Multi-timeframe
    confluence_count: int = 0  # How many TFs have zone here
    timeframes_present: list = None
    
    # Status
    is_active: bool = True
    is_invalidated: bool = False
    
    def __post_init__(self):
        """Initialize mutable defaults."""
        if self.timeframes_present is None:
            self.timeframes_present = [self.timeframe]
    
    @property
    def midpoint(self) -> float:
        """Get the midpoint price of the zone."""
        return (self.price_high + self.price_low) / 2
    
    @property
    def size(self) -> float:
        """Get the size of the zone."""
        return self.price_high - self.price_low
    
    @property
    def success_rate(self) -> float:
        """Calculate bounce success rate."""
        if self.touch_count == 0:
            return 0.0
        return self.successful_bounces / self.touch_count
    
    def contains_price(self, price: float) -> bool:
        """Check if a price is within this zone."""
        return self.price_low <= price <= self.price_high
    
    def is_high_quality(self) -> bool:
        """Check if zone is high quality."""
        return self.quality in [ZoneQuality.EXCELLENT, ZoneQuality.GOOD]
    
    def is_fresh(self) -> bool:
        """Check if zone is fresh (recently created)."""
        return self.time_relevance in [TimeRelevance.FRESH, TimeRelevance.RECENT]
    
    def is_valid_for_trading(self) -> bool:
        """Check if zone is valid for trading decisions."""
        return (
            self.is_active and 
            not self.is_invalidated and
            self.quality != ZoneQuality.INVALID and
            self.confidence_score >= 0.5
        )
    
    def __repr__(self) -> str:
        return (
            f"ClassifiedZone({self.zone_type}, ${self.price_low:.2f}-${self.price_high:.2f}, "
            f"{self.quality.value}, conf={self.confidence_score:.2f})"
        )
