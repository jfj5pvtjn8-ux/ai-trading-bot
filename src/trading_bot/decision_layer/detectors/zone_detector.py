"""
Zone Detector - Creates liquidity zones from pivots and volume clusters.
"""

from typing import List, Dict, Any, Tuple
from ..models import LiquidityZone


class ZoneDetector:
    """
    Creates liquidity zones by combining pivot points with volume analysis.
    
    Zones are areas where significant price reactions are expected due to:
    - Historical swing highs/lows (pivots)
    - High volume trading activity
    - Confluence of multiple factors
    """
    
    def __init__(
        self,
        symbol: str,
        zone_buffer_pct: float = 0.001,
        proximity_threshold: float = 0.005
    ):
        """
        Initialize zone detector.
        
        Args:
            symbol: Trading symbol for zone IDs
            zone_buffer_pct: Buffer percentage around pivot price (0.1% default)
            proximity_threshold: How close volume must be to pivot (0.5% default)
        """
        self.symbol = symbol
        self.zone_buffer_pct = zone_buffer_pct
        self.proximity_threshold = proximity_threshold
    
    def create_zones_from_pivots_and_volume(
        self,
        timeframe: str,
        pivots: Dict[str, List[Tuple[int, float]]],
        volume_clusters: List[Tuple[float, float]],
        candles: List[Dict[str, Any]]
    ) -> List[LiquidityZone]:
        """
        Create liquidity zones from detected pivots and volume clusters.
        
        Combines technical pivots with volume analysis for robust zone identification.
        
        Args:
            timeframe: Timeframe for the zones
            pivots: Dict with "highs" and "lows" lists of (idx, price) tuples
            volume_clusters: List of (price, volume) tuples
            candles: Candle data for timestamps
            
        Returns:
            List of LiquidityZone objects
        """
        zones = []
        zone_counter = 0
        
        # Create resistance zones from swing highs
        for idx, price in pivots["highs"]:
            # Check if there's volume support near this price
            nearby_volume = sum(
                vol for p, vol in volume_clusters
                if abs(p - price) / price < self.proximity_threshold
            )
            
            if nearby_volume > 0:
                zone_id = f"{self.symbol}_{timeframe}_R{zone_counter}"
                zone_counter += 1
                
                # Create zone with buffer
                zone = LiquidityZone(
                    zone_id=zone_id,
                    timeframe=timeframe,
                    zone_type="resistance",
                    price_low=price * (1 - self.zone_buffer_pct),
                    price_high=price * (1 + self.zone_buffer_pct),
                    volume=nearby_volume,
                    created_ts=candles[idx]["ts"],
                    pivot_high=price,
                )
                zones.append(zone)
        
        # Create support zones from swing lows
        for idx, price in pivots["lows"]:
            nearby_volume = sum(
                vol for p, vol in volume_clusters
                if abs(p - price) / price < self.proximity_threshold
            )
            
            if nearby_volume > 0:
                zone_id = f"{self.symbol}_{timeframe}_S{zone_counter}"
                zone_counter += 1
                
                zone = LiquidityZone(
                    zone_id=zone_id,
                    timeframe=timeframe,
                    zone_type="support",
                    price_low=price * (1 - self.zone_buffer_pct),
                    price_high=price * (1 + self.zone_buffer_pct),
                    volume=nearby_volume,
                    created_ts=candles[idx]["ts"],
                    pivot_low=price,
                )
                zones.append(zone)
        
        return zones
    
    def update_zone_touches(
        self,
        zones: List[LiquidityZone],
        candles: List[Dict[str, Any]],
        stats: Dict[str, Any]
    ):
        """
        Update existing zones: check for touches and breaks.
        
        - Touch: Price entered zone but respected it (bounced)
        - Break: Price closed through zone (zone is now inactive)
        
        Args:
            zones: List of zones to update
            candles: Recent candle data (last 20 typically)
            stats: Statistics dictionary to update
        """
        for zone in zones:
            if not zone.is_active:
                continue
            
            for candle in candles:
                # Check if price touched this zone
                if zone.contains_price(candle["low"]) or zone.contains_price(candle["high"]):
                    zone.touch_count += 1
                    zone.last_touch_ts = candle["ts"]
                    
                    # Check if zone was broken (closed through it)
                    if zone.zone_type in ["resistance", "supply"]:
                        if candle["close"] > zone.price_high:
                            zone.is_active = False
                            stats["zones_broken"] += 1
                    else:  # support/demand
                        if candle["close"] < zone.price_low:
                            zone.is_active = False
                            stats["zones_broken"] += 1
    
    def merge_zones(
        self,
        existing_zones: List[LiquidityZone],
        new_zones: List[LiquidityZone]
    ) -> List[LiquidityZone]:
        """
        Merge new zones with existing ones, avoiding duplicates.
        
        Args:
            existing_zones: Current zones
            new_zones: Newly detected zones
            
        Returns:
            Combined list without duplicates
        """
        # Create set of existing zone IDs
        existing_ids = {zone.zone_id for zone in existing_zones}
        
        # Add new zones that don't already exist
        for new_zone in new_zones:
            if new_zone.zone_id not in existing_ids:
                existing_zones.append(new_zone)
        
        return existing_zones
    
    def calculate_zone_strength(
        self,
        zones: List[LiquidityZone],
        touch_threshold_strong: int = 3,
        volume_threshold_high: float = None
    ):
        """
        Calculate and update zone strength based on touches and volume.
        
        Args:
            zones: List of zones to update
            touch_threshold_strong: Touches needed for "strong" rating
            volume_threshold_high: Volume threshold for strong zones
        """
        if not zones:
            return
        
        # Calculate volume threshold if not provided (top 30%)
        if volume_threshold_high is None:
            volumes = [z.volume for z in zones if z.is_active]
            if volumes:
                volume_threshold_high = sorted(volumes)[int(len(volumes) * 0.7)]
            else:
                volume_threshold_high = 0
        
        for zone in zones:
            if not zone.is_active:
                continue
            
            # Classify based on touches and volume
            if zone.touch_count >= touch_threshold_strong and zone.volume >= volume_threshold_high:
                zone.strength = "strong"
            elif zone.touch_count >= 2 or zone.volume >= volume_threshold_high * 0.5:
                zone.strength = "medium"
            else:
                zone.strength = "weak"
    
    def calculate_pd_position(
        self,
        zones: List[LiquidityZone],
        candles: List[Dict[str, Any]],
        equilibrium_threshold: float = 0.05
    ):
        """
        Calculate Premium/Discount position for zones based on recent range.
        
        Uses the swing high/low range from recent candles to determine if zones
        are in premium (expensive, above 50%), equilibrium (near 50%), or 
        discount (cheap, below 50%) areas.
        
        Args:
            zones: List of zones to update with PD position
            candles: Recent candles to determine range
            equilibrium_threshold: % range around 50% to consider equilibrium (default 5%)
        """
        if not zones or not candles:
            return
        
        # Calculate swing high and swing low from recent candles
        swing_high = max(c["high"] for c in candles)
        swing_low = min(c["low"] for c in candles)
        range_size = swing_high - swing_low
        
        if range_size == 0:
            # No range, all zones are equilibrium
            for zone in zones:
                zone.pd_position = "equilibrium"
                zone.equilibrium_distance = 0.0
            return
        
        # Calculate 50% equilibrium level
        equilibrium_level = swing_low + (range_size * 0.5)
        equilibrium_upper = swing_low + (range_size * (0.5 + equilibrium_threshold))
        equilibrium_lower = swing_low + (range_size * (0.5 - equilibrium_threshold))
        
        # Classify each zone
        for zone in zones:
            zone_midpoint = zone.midpoint
            
            # Calculate distance from equilibrium as percentage of range
            distance_from_eq = ((zone_midpoint - equilibrium_level) / range_size) * 100.0
            zone.equilibrium_distance = distance_from_eq
            
            # Classify position
            if zone_midpoint > equilibrium_upper:
                zone.pd_position = "premium"
            elif zone_midpoint < equilibrium_lower:
                zone.pd_position = "discount"
            else:
                zone.pd_position = "equilibrium"
