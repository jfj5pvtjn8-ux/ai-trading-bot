"""
SSL/BSL Plugin (Equal Highs/Lows)

Detects Sell-Side Liquidity (SSL) and Buy-Side Liquidity (BSL) - 
areas where equal highs or equal lows indicate liquidity pools.

Architecture:
- Uses LiquidityLevel model from models/
- Implements detection and update logic
- Provides query interface for decision layer
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import LiquidityLevel


class SSLBSLPlugin(LiquidityPlugin):
    """
    SSL/BSL detection plugin.
    
    Detects equal highs and equal lows that represent liquidity pools
    where stop losses are clustered.
    
    Detection Logic:
    - Equal Lows (SSL): Multiple candles with same/similar lows
    - Equal Highs (BSL): Multiple candles with same/similar highs
    - Tolerance: Within 0.1% is considered "equal"
    
    Sweep Detection:
    - SSL Sweep: Price takes out equal lows (bearish hunt)
    - BSL Sweep: Price takes out equal highs (bullish hunt)
    
    Usage:
        plugin = SSLBSLPlugin(symbol="BTCUSDT", timeframe="5m")
        plugin.on_candle_close(candles, current_price)
        
        # Query levels
        ssl_levels = plugin.get(level_type="SSL", only_unswept=True)
        bsl_levels = plugin.get(level_type="BSL", only_unswept=True)
        
        # Check for recent sweeps
        recent_sweeps = plugin.get_recent_sweeps(lookback=10)
    
    TODO Implementation Steps:
    1. Group highs/lows by price proximity (0.1% tolerance)
    2. Identify levels with 2+ touches
    3. Track sweep events (price closing beyond level)
    4. Maintain touch history per level
    5. Remove stale levels (no touches in X candles)
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None,
        timeframe_config: Any = None  # ENHANCEMENT E: Per-TF sweep sensitivity
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[LiquidityLevel] = []
        self.price_tolerance_pct = 0.001  # 0.1% tolerance for "equal"
        self.min_touches = 2  # Minimum touches to be significant
        
        # ENHANCEMENT E: Use timeframe-specific sweep penetration threshold
        if timeframe_config:
            self.sweep_penetration_pct = timeframe_config.sweep_penetration_pct
        else:
            self.sweep_penetration_pct = 0.0005  # Legacy default 0.05%
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[LiquidityLevel]:
        """
        Detect equal highs/lows in recent candles.
        
        Algorithm:
        1. Extract all swing highs and swing lows
        2. Group prices within tolerance (0.1%)
        3. Find groups with min_touches or more
        4. Create LiquidityLevel for significant clusters
        """
        if len(candles) < 5:
            return []
        
        levels = []
        
        # Extract highs and lows with their indices
        highs = [(i, c["high"], c["ts"]) for i, c in enumerate(candles)]
        lows = [(i, c["low"], c["ts"]) for i, c in enumerate(candles)]
        
        # Detect BSL (Buy-Side Liquidity) - Equal Highs
        bsl_clusters = self._find_price_clusters(highs, "high")
        for cluster in bsl_clusters:
            if len(cluster["touches"]) >= self.min_touches:
                level_id = f"{self.symbol}_{self.timeframe}_bsl_{cluster['price']:.2f}_{cluster['touches'][0][0]}"
                level = LiquidityLevel(
                    level_id=level_id,
                    timeframe=self.timeframe,
                    level_type="BSL",
                    price=cluster["price"],
                    touch_count=len(cluster["touches"]),
                    touches=cluster["touches"],
                    created_ts=cluster["touches"][0][1]
                )
                levels.append(level)
        
        # Detect SSL (Sell-Side Liquidity) - Equal Lows
        ssl_clusters = self._find_price_clusters(lows, "low")
        for cluster in ssl_clusters:
            if len(cluster["touches"]) >= self.min_touches:
                level_id = f"{self.symbol}_{self.timeframe}_ssl_{cluster['price']:.2f}_{cluster['touches'][0][0]}"
                level = LiquidityLevel(
                    level_id=level_id,
                    timeframe=self.timeframe,
                    level_type="SSL",
                    price=cluster["price"],
                    touch_count=len(cluster["touches"]),
                    touches=cluster["touches"],
                    created_ts=cluster["touches"][0][1]
                )
                levels.append(level)
        
        return levels
    
    def _find_price_clusters(
        self,
        price_data: List[tuple],  # [(idx, price, ts), ...]
        price_type: str
    ) -> List[Dict]:
        """
        Group prices within tolerance into clusters.
        
        Args:
            price_data: List of (index, price, timestamp) tuples
            price_type: "high" or "low"
            
        Returns:
            List of clusters with touches
        """
        if not price_data:
            return []
        
        clusters = []
        used_indices = set()
        
        for i, (idx1, price1, ts1) in enumerate(price_data):
            if idx1 in used_indices:
                continue
            
            # Find all prices within tolerance
            cluster_touches = [(idx1, ts1)]
            tolerance = price1 * self.price_tolerance_pct
            
            for j, (idx2, price2, ts2) in enumerate(price_data):
                if i == j or idx2 in used_indices:
                    continue
                
                if abs(price1 - price2) <= tolerance:
                    cluster_touches.append((idx2, ts2))
                    used_indices.add(idx2)
            
            if len(cluster_touches) >= self.min_touches:
                used_indices.add(idx1)
                avg_price = sum(price_data[ct[0]][1] for ct in cluster_touches) / len(cluster_touches)
                clusters.append({
                    "price": avg_price,
                    "touches": cluster_touches
                })
        
        return clusters
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update levels: check for sweeps and new touches.
        
        Algorithm:
        1. Check if recent candles swept any levels
        2. Mark swept levels with sweep details
        3. Check for new touches on existing levels
        4. Remove very old stale levels
        """
        if not candles or not self._patterns:
            return
        
        # Get last few candles to check for sweeps
        recent_candles = candles[-10:] if len(candles) > 10 else candles
        
        for level in self._patterns:
            if level.is_swept:
                continue
            
            # Check for sweep (ENHANCEMENT E: Use adaptive penetration threshold)
            for i, candle in enumerate(recent_candles):
                if level.level_type == "BSL":
                    # BSL sweep: price breaks ABOVE the level
                    if candle["high"] > level.price * (1 + self.sweep_penetration_pct):
                        level.is_swept = True
                        level.sweep_candle_idx = len(candles) - len(recent_candles) + i
                        level.sweep_timestamp = candle["ts"]
                        break
                elif level.level_type == "SSL":
                    # SSL sweep: price breaks BELOW the level
                    if candle["low"] < level.price * (1 - self.sweep_penetration_pct):
                        level.is_swept = True
                        level.sweep_candle_idx = len(candles) - len(recent_candles) + i
                        level.sweep_timestamp = candle["ts"]
                        break
            
            # Check for new touches (if not swept)
            if not level.is_swept:
                for i, candle in enumerate(recent_candles):
                    candle_idx = len(candles) - len(recent_candles) + i
                    
                    # Skip if already recorded
                    if any(touch[0] == candle_idx for touch in level.touches):
                        continue
                    
                    tolerance = level.price * self.price_tolerance_pct
                    
                    if level.level_type == "BSL":
                        # Touch if high is near level
                        if abs(candle["high"] - level.price) <= tolerance:
                            level.touches.append((candle_idx, candle["ts"]))
                            level.touch_count += 1
                    elif level.level_type == "SSL":
                        # Touch if low is near level
                        if abs(candle["low"] - level.price) <= tolerance:
                            level.touches.append((candle_idx, candle["ts"]))
                            level.touch_count += 1
        
        # Remove stale levels (older than 500 candles and not swept)
        if candles:
            current_idx = len(candles)
            self._patterns = [
                level for level in self._patterns
                if level.is_swept or (current_idx - level.touches[0][0]) < 500
            ]
    
    def get(
        self,
        level_type: Optional[str] = None,
        only_unswept: bool = True
    ) -> List[LiquidityLevel]:
        """
        Query liquidity levels.
        
        Args:
            level_type: "SSL", "BSL", or None for both
            only_unswept: Only return levels that haven't been swept
        """
        levels = list(self._patterns)
        
        if only_unswept:
            levels = [l for l in levels if not l.is_swept]
        
        if level_type:
            levels = [l for l in levels if l.level_type == level_type]
        
        return levels
    
    def get_recent_sweeps(self, lookback: int = 10) -> List[LiquidityLevel]:
        """Get recently swept levels."""
        swept = [l for l in self._patterns if l.is_swept]
        swept.sort(key=lambda x: x.sweep_timestamp or 0, reverse=True)
        return swept[:lookback]
    
    def _merge_patterns(self, new_patterns: List[LiquidityLevel]) -> None:
        """Merge new levels with existing."""
        existing_ids = {l.level_id for l in self._patterns}
        
        for new_level in new_patterns:
            if new_level.level_id not in existing_ids:
                self._patterns.append(new_level)
