"""
Volume Cluster Detector - Identifies price levels with high volume concentration.
"""

from typing import List, Dict, Any, Tuple
import numpy as np
from trading_bot.indicators import Indicators


class VolumeClusterDetector:
    """
    Identifies price levels with high volume concentration using volume profile analysis.
    
    Uses binning to create a volume profile and identifies high-volume nodes
    where significant trading activity occurred.
    """
    
    def __init__(
        self,
        num_bins: int = 50,
        min_volume_percentile: float = 70.0
    ):
        """
        Initialize volume cluster detector.
        
        Args:
            num_bins: Number of price bins for volume profile
            min_volume_percentile: Only consider bins above this percentile
        """
        self.num_bins = num_bins
        self.min_volume_percentile = min_volume_percentile
    
    def identify_volume_clusters(
        self,
        candles: List[Dict[str, Any]]
    ) -> List[Tuple[float, float]]:
        """
        Identify price levels with high volume concentration.
        
        Uses volume profile binning to find where most trading occurred.
        
        Args:
            candles: List of candle dictionaries with OHLCV data
            
        Returns:
            List of (price, volume) tuples for high-volume areas
        """
        if not candles:
            return []
        
        # Extract price and volume data
        prices = []
        volumes = []
        
        for candle in candles:
            # Use VWAP of the candle for more accurate price
            vwap = Indicators.calculate_vwap(candle)
            prices.append(vwap)
            volumes.append(candle["volume"])
        
        prices = np.array(prices)
        volumes = np.array(volumes)
        
        # Create price bins
        price_min, price_max = prices.min(), prices.max()
        bins = np.linspace(price_min, price_max, self.num_bins)
        
        # Aggregate volume per bin
        volume_per_bin = np.zeros(self.num_bins - 1)
        
        for price, volume in zip(prices, volumes):
            bin_idx = np.digitize(price, bins) - 1
            if 0 <= bin_idx < len(volume_per_bin):
                volume_per_bin[bin_idx] += volume
        
        # Find high-volume bins (above percentile threshold)
        volume_threshold = np.percentile(volume_per_bin, self.min_volume_percentile)
        
        clusters = []
        for i, vol in enumerate(volume_per_bin):
            if vol >= volume_threshold:
                # Use bin center as price
                price_level = (bins[i] + bins[i + 1]) / 2.0
                clusters.append((price_level, vol))
        
        return clusters
    
    def get_highest_volume_node(
        self,
        candles: List[Dict[str, Any]]
    ) -> Tuple[float, float]:
        """
        Get the single highest volume price level (Point of Control).
        
        Args:
            candles: List of candle dictionaries
            
        Returns:
            (price, volume) tuple for the POC or None
        """
        clusters = self.identify_volume_clusters(candles)
        if not clusters:
            return None
        
        # Return the cluster with highest volume
        return max(clusters, key=lambda x: x[1])
    
    def get_value_area(
        self,
        candles: List[Dict[str, Any]],
        value_area_pct: float = 0.70
    ) -> Tuple[float, float]:
        """
        Get the value area (range containing X% of volume).
        
        Args:
            candles: List of candle dictionaries
            value_area_pct: Percentage of volume to include (default 70%)
            
        Returns:
            (low, high) price range containing the value area
        """
        clusters = self.identify_volume_clusters(candles)
        if not clusters:
            return None
        
        # Sort by volume descending
        sorted_clusters = sorted(clusters, key=lambda x: x[1], reverse=True)
        
        # Calculate total volume
        total_volume = sum(c[1] for c in clusters)
        target_volume = total_volume * value_area_pct
        
        # Accumulate highest volume clusters until we reach target
        accumulated = 0
        value_area_prices = []
        
        for price, vol in sorted_clusters:
            value_area_prices.append(price)
            accumulated += vol
            if accumulated >= target_volume:
                break
        
        if value_area_prices:
            return (min(value_area_prices), max(value_area_prices))
        
        return None
