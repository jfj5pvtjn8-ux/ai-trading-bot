"""
Displacement Detector

Detects displacement moves - strong directional price action indicating
institutional order flow. Displacement is characterized by:
- Multiple consecutive candles in same direction (typically 3+)
- Above-average volume
- Large candle bodies (strong momentum)
- Minimal retracement during move
"""

from typing import List, Dict, Any, Optional
import numpy as np
from ..models.displacement import Displacement


class DisplacementDetector:
    """
    Detects displacement moves in price action.
    
    A displacement is a key SMC concept representing institutional
    order flow - strong, sustained moves that break through levels.
    
    Configuration:
        min_candles: Minimum consecutive candles for displacement (default 3)
        min_volume_ratio: Minimum volume surge vs baseline (default 1.5 = 50% above avg)
        min_body_pct: Minimum body size as % of candle range (default 0.6 = 60%)
        volume_lookback: Candles to use for baseline volume calc (default 20)
    """
    
    def __init__(
        self,
        min_candles: int = 3,
        min_volume_ratio: float = 1.5,
        min_body_pct: float = 0.6,
        volume_lookback: int = 20,
    ):
        """
        Initialize displacement detector.
        
        Args:
            min_candles: Minimum consecutive candles for displacement
            min_volume_ratio: Minimum volume surge vs average
            min_body_pct: Minimum body size as % of total range
            volume_lookback: Candles for baseline volume calculation
        """
        self.min_candles = min_candles
        self.min_volume_ratio = min_volume_ratio
        self.min_body_pct = min_body_pct
        self.volume_lookback = volume_lookback
    
    def detect_displacements(
        self,
        candles: List[Dict[str, Any]],
        symbol: str,
        timeframe: str,
    ) -> List[Displacement]:
        """
        Detect all displacement moves in the candle data.
        
        Args:
            candles: List of candle dicts with OHLCV data
            symbol: Trading symbol
            timeframe: Timeframe of candles
            
        Returns:
            List of detected Displacement objects
        """
        if len(candles) < self.min_candles + self.volume_lookback:
            return []
        
        displacements = []
        
        # Calculate baseline volume
        baseline_volume = self._calculate_baseline_volume(candles)
        
        # Scan for displacement sequences
        i = self.volume_lookback
        while i < len(candles):
            displacement = self._check_displacement_at_index(
                candles, i, symbol, timeframe, baseline_volume
            )
            
            if displacement:
                displacements.append(displacement)
                # Skip past this displacement to avoid overlaps
                i += displacement.num_candles
            else:
                i += 1
        
        return displacements
    
    def _check_displacement_at_index(
        self,
        candles: List[Dict[str, Any]],
        start_idx: int,
        symbol: str,
        timeframe: str,
        baseline_volume: float,
    ) -> Optional[Displacement]:
        """
        Check if a displacement starts at the given index.
        
        Args:
            candles: All candles
            start_idx: Index to check from
            symbol: Trading symbol
            timeframe: Timeframe
            baseline_volume: Average volume for comparison
            
        Returns:
            Displacement object if detected, None otherwise
        """
        if start_idx >= len(candles):
            return None
        
        start_candle = candles[start_idx]
        
        # Determine initial direction
        is_bullish = start_candle["close"] > start_candle["open"]
        
        # Check if starting candle qualifies
        if not self._is_displacement_candle(start_candle, baseline_volume):
            return None
        
        # Count consecutive candles in same direction with volume
        consecutive_count = 1
        end_idx = start_idx
        total_volume = start_candle["volume"]
        
        for i in range(start_idx + 1, len(candles)):
            candle = candles[i]
            candle_is_bullish = candle["close"] > candle["open"]
            
            # Check if continues in same direction with sufficient quality
            if candle_is_bullish == is_bullish and self._is_displacement_candle(candle, baseline_volume):
                consecutive_count += 1
                end_idx = i
                total_volume += candle["volume"]
            else:
                break
        
        # Check if we have enough candles for a displacement
        if consecutive_count < self.min_candles:
            return None
        
        # Calculate displacement metrics
        start_price = start_candle["open"]
        end_price = candles[end_idx]["close"]
        total_move = abs(end_price - start_price)
        move_pct = (total_move / start_price) * 100.0
        avg_volume = total_volume / consecutive_count
        volume_surge_ratio = avg_volume / baseline_volume if baseline_volume > 0 else 1.0
        
        # Create displacement object
        return Displacement(
            symbol=symbol,
            timeframe=timeframe,
            direction="bullish" if is_bullish else "bearish",
            start_price=start_price,
            end_price=end_price,
            start_ts=start_candle["ts"],
            end_ts=candles[end_idx]["ts"],
            num_candles=consecutive_count,
            total_move=total_move,
            move_pct=move_pct,
            avg_volume=avg_volume,
            volume_surge_ratio=volume_surge_ratio,
            created_ts=candles[-1]["ts"],
        )
    
    def _is_displacement_candle(
        self,
        candle: Dict[str, Any],
        baseline_volume: float,
    ) -> bool:
        """
        Check if a single candle qualifies as a displacement candle.
        
        Args:
            candle: Candle to check
            baseline_volume: Average volume for comparison
            
        Returns:
            True if candle qualifies
        """
        # Check volume surge
        if candle["volume"] < baseline_volume * self.min_volume_ratio:
            return False
        
        # Check body size (strong momentum)
        body = abs(candle["close"] - candle["open"])
        total_range = candle["high"] - candle["low"]
        
        if total_range == 0:
            return False
        
        body_pct = body / total_range
        
        return body_pct >= self.min_body_pct
    
    def _calculate_baseline_volume(self, candles: List[Dict[str, Any]]) -> float:
        """
        Calculate baseline (average) volume from recent candles.
        
        Args:
            candles: List of candles
            
        Returns:
            Average volume
        """
        if len(candles) < self.volume_lookback:
            volumes = [c["volume"] for c in candles]
        else:
            volumes = [c["volume"] for c in candles[-self.volume_lookback:]]
        
        return float(np.mean(volumes)) if volumes else 1.0
    
    def get_recent_displacements(
        self,
        displacements: List[Displacement],
        lookback_candles: int = 50,
    ) -> List[Displacement]:
        """
        Filter displacements to only recent ones.
        
        Args:
            displacements: All displacements
            lookback_candles: How many candles to look back
            
        Returns:
            Recent displacements only
        """
        if not displacements:
            return []
        
        # Sort by end timestamp
        sorted_displacements = sorted(displacements, key=lambda d: d.end_ts, reverse=True)
        
        # Return up to lookback_candles worth
        return sorted_displacements[:lookback_candles]
    
    def get_strongest_displacement(
        self,
        displacements: List[Displacement],
        metric: str = "move_pct",
    ) -> Optional[Displacement]:
        """
        Get the strongest displacement by chosen metric.
        
        Args:
            displacements: List of displacements
            metric: "move_pct", "volume_surge_ratio", or "num_candles"
            
        Returns:
            Strongest displacement or None
        """
        if not displacements:
            return None
        
        if metric == "move_pct":
            return max(displacements, key=lambda d: d.move_pct)
        elif metric == "volume_surge_ratio":
            return max(displacements, key=lambda d: d.volume_surge_ratio)
        elif metric == "num_candles":
            return max(displacements, key=lambda d: d.num_candles)
        else:
            return displacements[0]
