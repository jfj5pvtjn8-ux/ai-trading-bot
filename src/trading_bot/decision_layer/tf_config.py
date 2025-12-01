"""
Timeframe-Specific Configuration for Liquidity Map

Provides adaptive parameters per timeframe for production-grade zone detection.
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class TimeframeConfig:
    """
    Configuration for a specific timeframe's liquidity detection.
    
    All parameters are tuned to the characteristics of each timeframe:
    - Higher TFs (1h): Stricter filters, wider zones, longer confirmation
    - Lower TFs (1m): Faster response, tighter zones, looser filters
    """
    # Core pivot detection
    pivot_left: int
    pivot_right: int
    
    # Lookback and history
    lookback_candles: int
    max_zone_age_candles: int  # Age-based filtering
    
    # Volume filters
    min_volume_percentile: float
    volume_spike_multiplier: float  # Must exceed X times recent average
    
    # Zone geometry
    zone_buffer_pct: float  # Zone width around pivot
    merge_radius_pct: float  # Multi-TF merge tolerance
    min_zone_distance_pct: float  # Minimum distance from current price
    
    # ATR-based dynamic thresholds
    atr_min_multiplier: float  # Skip if ATR too low (dead market)
    atr_max_multiplier: float  # Skip if ATR too high (slippage risk)
    
    # Sweep detection (per TF sensitivity)
    sweep_penetration_pct: float  # How far wick must penetrate level
    sweep_rejection_pct: float  # How much must reject back
    
    # Multi-TF weighting
    tf_weight: int  # Higher TF = higher weight (1h=4, 15m=3, 5m=2, 1m=1)
    
    # Metadata
    description: str
    
    def __post_init__(self):
        """Validate configuration parameters."""
        assert 0 < self.pivot_left <= 20, "pivot_left must be 1-20"
        assert 0 < self.pivot_right <= 20, "pivot_right must be 1-20"
        assert 10 <= self.lookback_candles <= 500, "lookback_candles must be 10-500"
        assert 10 <= self.max_zone_age_candles <= 1000, "max_zone_age_candles must be 10-1000"
        assert 0 < self.min_volume_percentile <= 100, "min_volume_percentile must be 0-100"
        assert 1.0 <= self.volume_spike_multiplier <= 5.0, "volume_spike_multiplier must be 1.0-5.0"
        assert 0 < self.zone_buffer_pct <= 0.01, "zone_buffer_pct must be 0-1%"
        assert 0 < self.merge_radius_pct <= 0.01, "merge_radius_pct must be 0-1%"
        assert 0 <= self.min_zone_distance_pct <= 0.01, "min_zone_distance_pct must be 0-1%"
        assert 0.1 <= self.atr_min_multiplier <= 2.0, "atr_min_multiplier must be 0.1-2.0"
        assert 0.5 <= self.atr_max_multiplier <= 5.0, "atr_max_multiplier must be 0.5-5.0"
        assert 0.0001 <= self.sweep_penetration_pct <= 0.01, "sweep_penetration_pct must be 0.01%-1%"
        assert 0.0001 <= self.sweep_rejection_pct <= 0.01, "sweep_rejection_pct must be 0.01%-1%"
        assert 1 <= self.tf_weight <= 10, "tf_weight must be 1-10"


# Production-Grade Configurations
TIMEFRAME_CONFIGS: Dict[str, TimeframeConfig] = {
    "1h": TimeframeConfig(
        # Core detection
        pivot_left=8,
        pivot_right=8,
        
        # History
        lookback_candles=120,
        max_zone_age_candles=200,  # ~8 days
        
        # Volume
        min_volume_percentile=75.0,
        volume_spike_multiplier=2.2,
        
        # Geometry
        zone_buffer_pct=0.002,  # ±0.2%
        merge_radius_pct=0.0025,
        min_zone_distance_pct=0.0015,
        
        # ATR volatility filters
        atr_min_multiplier=0.8,
        atr_max_multiplier=2.5,
        
        # Sweep sensitivity
        sweep_penetration_pct=0.002,
        sweep_rejection_pct=0.0012,
        
        # Weight
        tf_weight=4,
        description="Major structure - Weekly/daily key levels"
    ),
    
    "15m": TimeframeConfig(
        # Core detection
        pivot_left=5,
        pivot_right=5,
        
        # History
        lookback_candles=100,
        max_zone_age_candles=150,  # ~37 hours
        
        # Volume
        min_volume_percentile=70.0,
        volume_spike_multiplier=1.8,
        
        # Geometry
        zone_buffer_pct=0.0015,  # ±0.15%
        merge_radius_pct=0.0018,
        min_zone_distance_pct=0.001,
        
        # ATR volatility filters
        atr_min_multiplier=0.7,
        atr_max_multiplier=2.0,
        
        # Sweep sensitivity
        sweep_penetration_pct=0.0012,
        sweep_rejection_pct=0.0008,
        
        # Weight
        tf_weight=3,
        description="Swing structure - Session highs/lows"
    ),
    
    "5m": TimeframeConfig(
        # Core detection
        pivot_left=4,
        pivot_right=4,
        
        # History
        lookback_candles=100,
        max_zone_age_candles=100,  # ~8 hours
        
        # Volume
        min_volume_percentile=70.0,
        volume_spike_multiplier=1.6,
        
        # Geometry
        zone_buffer_pct=0.001,  # ±0.1%
        merge_radius_pct=0.0015,
        min_zone_distance_pct=0.0008,
        
        # ATR volatility filters
        atr_min_multiplier=0.6,
        atr_max_multiplier=1.8,
        
        # Sweep sensitivity
        sweep_penetration_pct=0.0008,
        sweep_rejection_pct=0.0006,
        
        # Weight
        tf_weight=2,
        description="Intraday structure - Day trading zones"
    ),
    
    "1m": TimeframeConfig(
        # Core detection
        pivot_left=3,
        pivot_right=3,
        
        # History
        lookback_candles=80,
        max_zone_age_candles=40,  # ~40 minutes
        
        # Volume
        min_volume_percentile=65.0,
        volume_spike_multiplier=1.5,
        
        # Geometry
        zone_buffer_pct=0.0008,  # ±0.08%
        merge_radius_pct=0.0012,
        min_zone_distance_pct=0.0005,
        
        # ATR volatility filters
        atr_min_multiplier=0.5,
        atr_max_multiplier=1.5,
        
        # Sweep sensitivity
        sweep_penetration_pct=0.0006,
        sweep_rejection_pct=0.0005,
        
        # Weight
        tf_weight=1,
        description="Micro structure - Scalping zones"
    ),
}


def get_timeframe_config(timeframe: str) -> TimeframeConfig:
    """
    Get configuration for a specific timeframe.
    
    Args:
        timeframe: Timeframe string (e.g., "5m", "1h")
        
    Returns:
        TimeframeConfig object with optimal parameters
    """
    # Return config if exists, else default to 5m
    return TIMEFRAME_CONFIGS.get(timeframe, TIMEFRAME_CONFIGS["5m"])


__all__ = ["TimeframeConfig", "TIMEFRAME_CONFIGS", "get_timeframe_config"]
