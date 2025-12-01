"""
Decision Layer - Advanced Trading Intelligence

Professional-grade modules for market analysis and decision making:
- LiquidityMap: Multi-timeframe liquidity zone detection and tracking
- TrendFusion: MTF trend alignment and confluence analysis  
- ZoneClassifier: Liquidity zone strength and type classification

Architecture:
- enums: Centralized enumerations (5 enums)
- models: Pure data structures (13 models)
- plugins: SMC pattern detection (6 plugins)
- detectors: Helper algorithms (3 detectors)

All modules are MTF-aware and integrate with the candle close event system.
"""

# Main components
from .liquidity_map import LiquidityMap
from .trend_detector import TrendFusion
from .zone_classifier import ZoneClassifier

# Enumerations
from .enums import (
    TrendDirection,
    TrendStrength,
    ZoneQuality,
    TimeRelevance,
    ZoneReaction,
)

# Models (all data structures)
from .models import (
    # Core models
    LiquidityZone,
    FairValueGap,
    VolumeProfile,
    TimeframeZones,
    # SMC pattern models
    LiquidityLevel,
    OrderBlock,
    StructureBreak,
    StructurePoint,
    BreakerBlock,
    LiquiditySweep,
    # Trend analysis models
    TrendState,
    TrendFusionSignal,
    # Zone classification models
    ClassifiedZone,
    # Displacement models
    Displacement,
)

# Plugins
from .plugins import (
    LiquidityPlugin,
    PluginConfig,
    FVGPlugin,
    SSLBSLPlugin,
    OrderBlockPlugin,
    BOSCHOCHPlugin,
    BreakerBlockPlugin,
    LiquiditySweepPlugin,
)

# Detectors
from .detectors import (
    PivotDetector,
    VolumeClusterDetector,
    ZoneDetector,
    DisplacementDetector,
)

__all__ = [
    # Main components
    "LiquidityMap",
    "TrendFusion",
    "ZoneClassifier",
    
    # Enums
    "TrendDirection",
    "TrendStrength",
    "ZoneQuality",
    "TimeRelevance",
    "ZoneReaction",
    
    # Models
    "LiquidityZone",
    "FairValueGap",
    "VolumeProfile",
    "TimeframeZones",
    "LiquidityLevel",
    "OrderBlock",
    "StructureBreak",
    "StructurePoint",
    "BreakerBlock",
    "LiquiditySweep",
    "TrendState",
    "TrendFusionSignal",
    "ClassifiedZone",
    "Displacement",
    
    # Plugins
    "LiquidityPlugin",
    "PluginConfig",
    "FVGPlugin",
    "SSLBSLPlugin",
    "OrderBlockPlugin",
    "BOSCHOCHPlugin",
    "BreakerBlockPlugin",
    "LiquiditySweepPlugin",
    
    # Detectors
    "PivotDetector",
    "VolumeClusterDetector",
    "ZoneDetector",
    "DisplacementDetector",
]
