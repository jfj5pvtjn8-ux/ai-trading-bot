"""
Liquidity Map Plugins

Modular SMC (Smart Money Concepts) feature plugins that can be
independently developed and integrated into LiquidityMap.

Architecture:
- Plugins contain ONLY detection logic (no data structures)
- All data models are imported from models/
- Each plugin follows the LiquidityPlugin interface:
  - detect() - Detect new patterns
  - update() - Update existing patterns
  - get() - Query patterns

Separation of Concerns:
  models/    → Data structures (WHAT the data is)
  plugins/   → Business logic (HOW to detect/update)
  detectors/ → Helper algorithms (pivot detection, volume analysis)
"""

from .base import LiquidityPlugin, PluginConfig
from .fvg_plugin import FVGPlugin
from .ssl_bsl_plugin import SSLBSLPlugin
from .order_block_plugin import OrderBlockPlugin
from .bos_choch_plugin import BOSCHOCHPlugin
from .breaker_block_plugin import BreakerBlockPlugin
from .liquidity_sweep_plugin import LiquiditySweepPlugin

__all__ = [
    # Base classes
    "LiquidityPlugin",
    "PluginConfig",
    
    # Plugin implementations
    "FVGPlugin",           # Fair Value Gaps
    "SSLBSLPlugin",        # Equal highs/lows
    "OrderBlockPlugin",    # Institutional blocks
    "BOSCHOCHPlugin",      # Structure breaks
    "BreakerBlockPlugin",  # Failed order blocks
    "LiquiditySweepPlugin", # Liquidity hunts
]
