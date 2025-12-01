"""
Pivot Detector - Detects swing highs and lows using pivot confirmation.
"""

from typing import List, Dict, Any, Tuple


class PivotDetector:
    """
    Detects swing highs and lows using pivot confirmation logic.
    
    A swing high requires:
    - High > all highs in pivot_left candles before
    - High > all highs in pivot_right candles after
    
    Similarly for swing lows.
    """
    
    def __init__(self, pivot_left: int = 5, pivot_right: int = 5):
        """
        Initialize pivot detector.
        
        Args:
            pivot_left: Number of candles to the left for confirmation
            pivot_right: Number of candles to the right for confirmation
        """
        self.pivot_left = pivot_left
        self.pivot_right = pivot_right
    
    def detect_swing_pivots(
        self,
        candles: List[Dict[str, Any]]
    ) -> Dict[str, List[Tuple[int, float]]]:
        """
        Detect swing highs and lows using pivot confirmation.
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            
        Returns:
            {"highs": [(idx, price), ...], "lows": [(idx, price), ...]}
        """
        highs = []
        lows = []
        
        n = len(candles)
        
        # Need enough candles on both sides
        for i in range(self.pivot_left, n - self.pivot_right):
            current_high = candles[i]["high"]
            current_low = candles[i]["low"]
            
            # Check if it's a pivot high
            is_pivot_high = True
            for j in range(i - self.pivot_left, i):
                if candles[j]["high"] >= current_high:
                    is_pivot_high = False
                    break
            
            if is_pivot_high:
                for j in range(i + 1, i + self.pivot_right + 1):
                    if candles[j]["high"] >= current_high:
                        is_pivot_high = False
                        break
            
            if is_pivot_high:
                highs.append((i, current_high))
            
            # Check if it's a pivot low
            is_pivot_low = True
            for j in range(i - self.pivot_left, i):
                if candles[j]["low"] <= current_low:
                    is_pivot_low = False
                    break
            
            if is_pivot_low:
                for j in range(i + 1, i + self.pivot_right + 1):
                    if candles[j]["low"] <= current_low:
                        is_pivot_low = False
                        break
            
            if is_pivot_low:
                lows.append((i, current_low))
        
        return {"highs": highs, "lows": lows}
    
    def get_most_recent_pivot_high(
        self,
        candles: List[Dict[str, Any]]
    ) -> Tuple[int, float]:
        """
        Get the most recent pivot high.
        
        Returns:
            (index, price) or None if no pivot found
        """
        pivots = self.detect_swing_pivots(candles)
        if pivots["highs"]:
            return pivots["highs"][-1]
        return None
    
    def get_most_recent_pivot_low(
        self,
        candles: List[Dict[str, Any]]
    ) -> Tuple[int, float]:
        """
        Get the most recent pivot low.
        
        Returns:
            (index, price) or None if no pivot found
        """
        pivots = self.detect_swing_pivots(candles)
        if pivots["lows"]:
            return pivots["lows"][-1]
        return None
