"""
Base Plugin Interface for SMC Features.

All liquidity detection plugins (FVG, Order Blocks, SSL/BSL, etc.)
inherit from this base class to ensure consistent interface.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class BaseLiquidityPlugin(ABC):
    """
    Abstract base class for all SMC feature detection plugins.
    
    Each plugin is responsible for:
    1. Detecting specific patterns (FVG, OB, SSL/BSL, etc.)
    2. Updating existing patterns (filling, breaking, etc.)
    3. Providing query methods for detected patterns
    4. Managing pattern lifecycle (creation, update, cleanup)
    
    Plugins are called by LiquidityMap during candle close events.
    """
    
    def __init__(self, symbol: str, timeframe: str, enabled: bool = True):
        """
        Initialize the plugin.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            timeframe: Timeframe this plugin tracks (e.g., "5m")
            enabled: Whether this plugin is active
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.enabled = enabled
        self.name = self.__class__.__name__
    
    @abstractmethod
    def detect(self, candles: List[Dict[str, Any]], current_price: float) -> List[Any]:
        """
        Detect new patterns in the candle data.
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            current_price: Current market price
            
        Returns:
            List of newly detected patterns (e.g., FVGs, Order Blocks, etc.)
        """
        pass
    
    @abstractmethod
    def update(self, candles: List[Dict[str, Any]], current_price: float):
        """
        Update existing patterns based on new price action.
        
        This checks if existing patterns have been:
        - Filled/mitigated
        - Broken/invalidated
        - Retested
        
        Args:
            candles: Recent candle data
            current_price: Current market price
        """
        pass
    
    @abstractmethod
    def get_active_patterns(self) -> List[Any]:
        """
        Get all active (unfilled/unbroken) patterns.
        
        Returns:
            List of active patterns detected by this plugin
        """
        pass
    
    @abstractmethod
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get plugin statistics (pattern counts, accuracy, etc.).
        
        Returns:
            Dictionary with plugin-specific statistics
        """
        pass
    
    def cleanup(self, keep_recent: int = 20):
        """
        Clean up old/filled patterns to prevent memory bloat.
        
        Args:
            keep_recent: Number of recent filled patterns to keep
        """
        pass
    
    def enable(self):
        """Enable this plugin."""
        self.enabled = True
    
    def disable(self):
        """Disable this plugin."""
        self.enabled = False
    
    def __repr__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return f"{self.name}({self.symbol}, {self.timeframe}, {status})"
