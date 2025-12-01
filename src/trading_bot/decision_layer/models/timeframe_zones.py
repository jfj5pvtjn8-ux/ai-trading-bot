"""
TimeframeZones - Container for all zones in a specific timeframe.
"""

from typing import List, Optional
from dataclasses import dataclass, field

from .liquidity_zone import LiquidityZone
from .fair_value_gap import FairValueGap
from .volume_profile import VolumeProfile


@dataclass
class TimeframeZones:
    """Container for all zones in a specific timeframe."""
    timeframe: str
    zones: List[LiquidityZone] = field(default_factory=list)
    fvgs: List[FairValueGap] = field(default_factory=list)  # Fair Value Gaps
    last_update_ts: Optional[int] = None
    volume_profile: List[VolumeProfile] = field(default_factory=list)
    
    def get_active_zones(self) -> List[LiquidityZone]:
        """Get only active (unbroken) zones."""
        return [z for z in self.zones if z.is_active]
    
    def get_unfilled_fvgs(self) -> List[FairValueGap]:
        """Get FVGs that haven't been fully filled yet."""
        return [fvg for fvg in self.fvgs if not fvg.is_filled]
    
    def get_support_zones(self) -> List[LiquidityZone]:
        """Get active support zones."""
        return [z for z in self.zones if z.is_active and z.zone_type in ["support", "demand"]]
    
    def get_resistance_zones(self) -> List[LiquidityZone]:
        """Get active resistance zones."""
        return [z for z in self.zones if z.is_active and z.zone_type in ["resistance", "supply"]]
    
    def add_zone(self, zone: LiquidityZone):
        """Add a zone to this timeframe."""
        self.zones.append(zone)
    
    def add_fvg(self, fvg: FairValueGap):
        """Add an FVG to this timeframe."""
        self.fvgs.append(fvg)
    
    def clear_filled_fvgs(self, keep_recent: int = 10):
        """Remove old filled FVGs, keeping only recent ones."""
        filled = [fvg for fvg in self.fvgs if fvg.is_filled]
        unfilled = [fvg for fvg in self.fvgs if not fvg.is_filled]
        
        # Keep all unfilled + recent filled (sorted by timestamp)
        recent_filled = sorted(filled, key=lambda x: x.created_ts, reverse=True)[:keep_recent]
        self.fvgs = unfilled + recent_filled
