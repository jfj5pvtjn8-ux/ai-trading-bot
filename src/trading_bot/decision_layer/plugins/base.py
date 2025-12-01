"""
Base Plugin Interface

All liquidity map plugins must inherit from this base class.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class PluginConfig:
    """Configuration for a plugin."""
    enabled: bool = True
    lookback_candles: int = 100
    min_significance: float = 0.0  # Minimum significance threshold


class LiquidityPlugin(ABC):
    """
    Base class for all liquidity map plugins.
    
    Each plugin implements SMC features independently:
    - FVG Plugin
    - SSL/BSL Plugin (Equal Highs/Lows)
    - Order Block Plugin
    - BOS/CHOCH Plugin (Market Structure)
    - Breaker Block Plugin
    - Liquidity Sweep Plugin
    
    Interface:
    - detect() - Detect new patterns in recent candles
    - update() - Update existing patterns (fill status, breaks, etc.)
    - get() - Query patterns with filters
    - get_statistics() - Get plugin-specific stats
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None
    ):
        """
        Initialize plugin.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            timeframe: Timeframe for this plugin (e.g., "1m")
            config: Plugin configuration
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.config = config or PluginConfig()
        self.is_enabled = self.config.enabled
        
        # Internal storage (implementation-specific)
        self._patterns = []
        
    @abstractmethod
    def detect(self, candles: List[Dict[str, Any]]) -> List[Any]:
        """
        Detect new patterns in recent candles.
        
        Args:
            candles: List of candle dictionaries
            
        Returns:
            List of newly detected patterns
        """
        pass
    
    @abstractmethod
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update existing patterns (check fill status, breaks, etc.).
        
        Args:
            candles: Recent candles for context
            current_price: Current market price
        """
        pass
    
    @abstractmethod
    def get(self, **filters) -> List[Any]:
        """
        Query patterns with optional filters.
        
        Args:
            **filters: Plugin-specific filter criteria
            
        Returns:
            List of patterns matching filters
        """
        pass
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about this plugin's patterns.
        
        Returns:
            Dictionary of statistics
        """
        return {
            "total_patterns": len(self._patterns),
            "enabled": self.is_enabled,
        }
    
    def enable(self):
        """Enable this plugin."""
        self.is_enabled = True
    
    def disable(self):
        """Disable this plugin."""
        self.is_enabled = False
    
    def clear(self):
        """Clear all stored patterns."""
        self._patterns.clear()
    
    def on_candle_close(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> bool:
        """
        Main entry point called when candle closes.
        
        Orchestrates detect() and update() calls.
        
        Args:
            candles: All candles for this timeframe
            current_price: Current market price
            
        Returns:
            True if plugin was updated
        """
        if not self.is_enabled:
            return False
        
        try:
            # Detect new patterns
            new_patterns = self.detect(candles)
            
            # Update existing patterns
            self.update(candles, current_price)
            
            # Merge new patterns
            if new_patterns:
                self._merge_patterns(new_patterns)
            
            return True
            
        except Exception as e:
            print(f"Error in {self.__class__.__name__}: {e}")
            return False
    
    @abstractmethod
    def _merge_patterns(self, new_patterns: List[Any]) -> None:
        """
        Merge newly detected patterns with existing ones.
        
        Implementation-specific logic for avoiding duplicates.
        
        Args:
            new_patterns: List of newly detected patterns
        """
        pass
    
    @property
    def name(self) -> str:
        """Return plugin name."""
        return self.__class__.__name__.replace("Plugin", "")
