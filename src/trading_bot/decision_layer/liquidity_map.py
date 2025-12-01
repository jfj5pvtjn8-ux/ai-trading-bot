"""
Professional-Grade Multi-Timeframe Liquidity Map (Refactored & Modular)

This refactored version uses a plugin architecture for SMC features and
modular detectors for cleaner, maintainable code.

Architecture:
- Models: Data structures (LiquidityZone, FVG, VolumeProfile, TimeframeZones)
- Detectors: Pivot, Volume Cluster, Zone detection logic
- Plugins: Modular SMC features (FVG, Order Blocks, SSL/BSL, etc.)
- Main LiquidityMap: Orchestrates all components

Each timeframe has independent zones stored separately.
Zones are refreshed ONLY when that timeframe's candle closes.
"""

from typing import Dict, List, Optional, Any
from collections import defaultdict

# Import modular components
from .models import LiquidityZone, VolumeProfile, TimeframeZones, FairValueGap, Displacement
from .detectors import PivotDetector, VolumeClusterDetector, ZoneDetector, DisplacementDetector
from .plugins.fvg_plugin import FVGPlugin
from .plugins.base import PluginConfig
from .tf_config import TimeframeConfig, get_timeframe_config

# Import all SMC plugins
from .plugins.ssl_bsl_plugin import SSLBSLPlugin
from .plugins.order_block_plugin import OrderBlockPlugin
from .plugins.bos_choch_plugin import BOSCHOCHPlugin
from .plugins.breaker_block_plugin import BreakerBlockPlugin
from .plugins.liquidity_sweep_plugin import LiquiditySweepPlugin

# Import indicators for ATR and volatility analysis
from trading_bot.utils.indicators import (
    calculate_atr,
    calculate_volume_spike_ratio,
    is_high_volatility,
    is_low_volatility
)


class LiquidityMap:
    """
    Professional multi-timeframe liquidity zone tracker (Refactored).
    
    Uses plugin architecture for extensibility and modular detectors
    for better code organization.
    
    Usage:
        liq_map = LiquidityMap(symbol="BTCUSDT", timeframes=["1m", "5m", "15m", "1h"])
        
        # On candle close (called by MTFSymbolManager)
        liq_map.on_candle_close(
            timeframe="5m",
            candles=candle_manager.get_all(),
            current_price=50000.0
        )
        
        # Get zones for decision making
        zones = liq_map.get_confluence_zones(min_timeframes=2)
        fvgs = liq_map.get_fvgs(timeframe="5m", only_unfilled=True)
    """
    
    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        timeframe_config: Optional[TimeframeConfig] = None,
        # Legacy parameters (kept for backward compatibility)
        lookback_candles: Optional[int] = None,
        zone_buffer_pct: Optional[float] = None,
        min_volume_percentile: Optional[float] = None,
        pivot_left: Optional[int] = None,
        pivot_right: Optional[int] = None,
        # Plugin toggles
        enable_fvg: bool = True,
        enable_ssl_bsl: bool = True,
        enable_order_blocks: bool = True,
        enable_bos_choch: bool = True,
        enable_breaker_blocks: bool = True,
        enable_liquidity_sweeps: bool = True,
        # Trend context integration
        trend_state: Optional[Any] = None,
    ):
        """
        Initialize adaptive liquidity map with production-grade enhancements.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            timeframes: List of timeframes to track (usually single TF)
            timeframe_config: TimeframeConfig with adaptive parameters (RECOMMENDED)
            lookback_candles: Legacy parameter (overridden by config if provided)
            zone_buffer_pct: Legacy parameter (overridden by config if provided)
            min_volume_percentile: Legacy parameter (overridden by config if provided)
            pivot_left: Legacy parameter (overridden by config if provided)
            pivot_right: Legacy parameter (overridden by config if provided)
            enable_fvg: Enable FVG plugin
            enable_ssl_bsl: Enable SSL/BSL plugin
            enable_order_blocks: Enable Order Blocks plugin
            enable_bos_choch: Enable BOS/CHOCH plugin
            enable_breaker_blocks: Enable Breaker Blocks plugin
            enable_liquidity_sweeps: Enable Liquidity Sweeps plugin
            trend_state: Current trend state for context-aware adaptation
        """
        self.symbol = symbol
        self.timeframes = timeframes
        
        # Get adaptive configuration (per timeframe if provided, else use first TF)
        if timeframe_config:
            self.config = timeframe_config
        else:
            # Fallback to legacy parameters or default config
            tf = timeframes[0] if timeframes else "5m"
            self.config = get_timeframe_config(tf)
            
            # Override with legacy parameters if provided
            if lookback_candles is not None:
                self.config.lookback_candles = lookback_candles
            if zone_buffer_pct is not None:
                self.config.zone_buffer_pct = zone_buffer_pct
            if min_volume_percentile is not None:
                self.config.min_volume_percentile = min_volume_percentile
            if pivot_left is not None:
                self.config.pivot_left = pivot_left
            if pivot_right is not None:
                self.config.pivot_right = pivot_right
        
        # Store for convenience
        self.lookback_candles = self.config.lookback_candles
        
        # Trend context for adaptive behavior
        self.trend_state = trend_state
        
        # Per-timeframe zone storage
        self.tf_zones: Dict[str, TimeframeZones] = {
            tf: TimeframeZones(timeframe=tf) for tf in timeframes
        }
        
        # TF priority mapping
        self.tf_priority = {
            "1h": 1,
            "15m": 2,
            "5m": 3,
            "1m": 4,
        }
        
        # Initialize detectors with adaptive parameters
        self.pivot_detector = PivotDetector(
            pivot_left=self.config.pivot_left,
            pivot_right=self.config.pivot_right
        )
        self.volume_detector = VolumeClusterDetector(
            num_bins=50,
            min_volume_percentile=self.config.min_volume_percentile
        )
        self.zone_detector = ZoneDetector(
            symbol=symbol,
            zone_buffer_pct=self.config.zone_buffer_pct
        )
        self.displacement_detector = DisplacementDetector(
            min_candles=3,
            min_volume_ratio=1.5,
            min_body_pct=0.6,
            volume_lookback=20
        )
        
        # Per-timeframe displacement storage
        self.tf_displacements: Dict[str, List[Displacement]] = {
            tf: [] for tf in timeframes
        }
        
        # Initialize plugins (SMC features)
        self.plugins: Dict[str, Dict] = {}
        
        # Plugin configurations
        fvg_config = PluginConfig(enabled=enable_fvg, lookback_candles=self.lookback_candles)
        ssl_config = PluginConfig(enabled=enable_ssl_bsl, lookback_candles=self.lookback_candles)
        ob_config = PluginConfig(enabled=enable_order_blocks, lookback_candles=self.lookback_candles)
        bos_config = PluginConfig(enabled=enable_bos_choch, lookback_candles=self.lookback_candles)
        breaker_config = PluginConfig(enabled=enable_breaker_blocks, lookback_candles=self.lookback_candles)
        sweep_config = PluginConfig(enabled=enable_liquidity_sweeps, lookback_candles=self.lookback_candles)
        
        for tf in timeframes:
            if tf not in self.plugins:
                self.plugins[tf] = {}
            
            # FVG Plugin
            self.plugins[tf]["fvg"] = FVGPlugin(
                symbol=symbol,
                timeframe=tf,
                config=fvg_config
            )
            
            # SSL/BSL Plugin (Equal Highs/Lows)
            # ENHANCEMENT E: Pass timeframe_config for per-TF sweep sensitivity
            self.plugins[tf]["ssl_bsl"] = SSLBSLPlugin(
                symbol=symbol,
                timeframe=tf,
                config=ssl_config,
                timeframe_config=self.config  # Pass adaptive config
            )
            
            # Order Blocks Plugin
            self.plugins[tf]["order_block"] = OrderBlockPlugin(
                symbol=symbol,
                timeframe=tf,
                config=ob_config
            )
            
            # BOS/CHOCH Plugin (Market Structure)
            self.plugins[tf]["bos_choch"] = BOSCHOCHPlugin(
                symbol=symbol,
                timeframe=tf,
                config=bos_config
            )
            
            # Breaker Blocks Plugin (depends on Order Blocks)
            self.plugins[tf]["breaker_block"] = BreakerBlockPlugin(
                symbol=symbol,
                timeframe=tf,
                config=breaker_config,
                ob_plugin=self.plugins[tf]["order_block"]
            )
            
            # Liquidity Sweeps Plugin (depends on SSL/BSL)
            # ENHANCEMENT E: Pass timeframe_config for per-TF sweep sensitivity
            self.plugins[tf]["liquidity_sweep"] = LiquiditySweepPlugin(
                symbol=symbol,
                timeframe=tf,
                config=sweep_config,
                ssl_plugin=self.plugins[tf]["ssl_bsl"],
                timeframe_config=self.config  # Pass our adaptive config
            )
        
        # Statistics
        self.stats = {
            "total_zones_created": 0,
            "zones_broken": 0,
            "zones_respected": 0,
            "zones_filtered_atr": 0,  # Filtered by ATR
            "zones_filtered_age": 0,  # Filtered by age
            "zones_filtered_distance": 0,  # Too close to price
            "zones_filtered_volume": 0,  # Volume spike too low
            "last_refresh_ts": {},
        }
    
    # ========================================================================
    # EVENT-DRIVEN REFRESH (CORE LOGIC)
    # ========================================================================
    
    def on_candle_close(
        self,
        timeframe: str,
        candles: List[Dict[str, Any]],
        current_price: float,
    ) -> bool:
        """
        MAIN ENTRY POINT: Called when a specific timeframe candle closes.
        
        This refreshes liquidity zones ONLY for the timeframe that just closed.
        No unnecessary recalculations - event-driven updates only.
        
        Args:
            timeframe: The TF that just closed (e.g., "5m")
            candles: All candles for this TF from CandleManager
            current_price: Current market price
            
        Returns:
            True if zones were updated, False if no update needed
        """
        if timeframe not in self.tf_zones:
            return False
        
        # Get recent candles for analysis
        recent_candles = candles[-self.lookback_candles:] if len(candles) > self.lookback_candles else candles
        
        if len(recent_candles) < self.pivot_detector.pivot_left + self.pivot_detector.pivot_right + 1:
            return False
        
        # Refresh zones for this specific timeframe only
        self._refresh_timeframe_zones(timeframe, recent_candles, current_price)
        
        # Update statistics
        self.stats["last_refresh_ts"][timeframe] = recent_candles[-1]["ts"]
        
        return True
    
    def _refresh_timeframe_zones(
        self,
        timeframe: str,
        candles: List[Dict[str, Any]],
        current_price: float,
    ):
        """
        Refresh liquidity zones with production-grade enhancements.
        
        Enhanced Steps:
        1. Calculate ATR and check volatility state
        2. Adapt parameters based on trend context (if available)
        3. Detect swing pivots (adaptive confirmation)
        4. Identify volume clusters
        5. Run plugin detections (FVG, etc.)
        6. Create zones from pivots + volume
        7. Apply ATR-based filters
        8. Apply volume spike filters
        9. Apply age-based filters
        10. Apply distance filters
        11. Update existing zones (touches/breaks)
        12. Merge new zones
        13. Calculate zone strength and PD positions
        """
        tf_zones = self.tf_zones[timeframe]
        
        # =====================================================================
        # ENHANCEMENT A: ATR-Based Dynamic Thresholds
        # =====================================================================
        atr = calculate_atr(candles, period=14)
        current_high_volatility = is_high_volatility(candles)
        current_low_volatility = is_low_volatility(candles)
        
        # Check if we should skip zone detection based on ATR
        if atr and current_low_volatility:
            # Market too dead - ATR below minimum threshold
            if atr < (self.config.atr_min_multiplier * calculate_atr(candles[:-14], 14) if len(candles) > 28 else atr):
                self.stats["zones_filtered_atr"] += 1
                return  # Skip detection in dead market
        
        if atr and current_high_volatility:
            # Market too volatile - risk of slippage
            avg_atr = calculate_atr(candles[:-14], 14) if len(candles) > 28 else atr
            if avg_atr and atr > (self.config.atr_max_multiplier * avg_atr):
                self.stats["zones_filtered_atr"] += 1
                return  # Skip detection in extremely volatile conditions
        
        # =====================================================================
        # ENHANCEMENT B: Trend Context Adaptation
        # =====================================================================
        # Adjust parameters based on trend state (if available)
        adapted_pivot_left = self.config.pivot_left
        adapted_pivot_right = self.config.pivot_right
        adapted_buffer = self.config.zone_buffer_pct
        adapted_volume_pct = self.config.min_volume_percentile
        
        if self.trend_state:
            trend_strength = getattr(self.trend_state, 'strength', None)
            if trend_strength and trend_strength == "strong":
                # Strong trend: faster confirmation, tighter zones, stricter filter
                adapted_pivot_left = int(adapted_pivot_left * 0.7)
                adapted_pivot_right = int(adapted_pivot_right * 0.7)
                adapted_buffer *= 0.8
                adapted_volume_pct += 5
            elif trend_strength and trend_strength == "weak":
                # Ranging market: looser requirements for reversal zones
                adapted_pivot_left = int(adapted_pivot_left * 1.2)
                adapted_pivot_right = int(adapted_pivot_right * 1.2)
                adapted_buffer *= 1.2
                adapted_volume_pct -= 5
        
        # Temporarily update detectors with adapted parameters
        original_pivot_left = self.pivot_detector.pivot_left
        original_pivot_right = self.pivot_detector.pivot_right
        original_buffer = self.zone_detector.zone_buffer_pct
        original_volume_pct = self.volume_detector.min_volume_percentile
        
        self.pivot_detector.pivot_left = max(1, adapted_pivot_left)
        self.pivot_detector.pivot_right = max(1, adapted_pivot_right)
        self.zone_detector.zone_buffer_pct = adapted_buffer
        self.volume_detector.min_volume_percentile = max(50, min(90, adapted_volume_pct))
        
        # Step 1: Detect swing pivots (with adapted parameters)
        pivots = self.pivot_detector.detect_swing_pivots(candles)
        
        # Step 2: Identify volume clusters
        volume_clusters = self.volume_detector.identify_volume_clusters(candles)
        
        # Restore original parameters
        self.pivot_detector.pivot_left = original_pivot_left
        self.pivot_detector.pivot_right = original_pivot_right
        self.zone_detector.zone_buffer_pct = original_buffer
        self.volume_detector.min_volume_percentile = original_volume_pct
        
        # Step 2.5: Detect displacement moves
        displacements = self.displacement_detector.detect_displacements(
            candles, self.symbol, timeframe
        )
        self.tf_displacements[timeframe] = displacements
        
        # Step 3: Run ALL plugin detections and updates
        if timeframe in self.plugins:
            # FVG Plugin
            if "fvg" in self.plugins[timeframe]:
                fvg_plugin = self.plugins[timeframe]["fvg"]
                fvg_plugin.on_candle_close(candles, current_price)
            
            # SSL/BSL Plugin (Equal Highs/Lows)
            if "ssl_bsl" in self.plugins[timeframe]:
                ssl_plugin = self.plugins[timeframe]["ssl_bsl"]
                ssl_plugin.on_candle_close(candles, current_price)
            
            # Order Blocks Plugin
            if "order_block" in self.plugins[timeframe]:
                ob_plugin = self.plugins[timeframe]["order_block"]
                ob_plugin.on_candle_close(candles, current_price)
            
            # BOS/CHOCH Plugin (Market Structure)
            if "bos_choch" in self.plugins[timeframe]:
                bos_plugin = self.plugins[timeframe]["bos_choch"]
                bos_plugin.on_candle_close(candles, current_price)
            
            # Breaker Blocks Plugin (depends on Order Blocks)
            if "breaker_block" in self.plugins[timeframe]:
                breaker_plugin = self.plugins[timeframe]["breaker_block"]
                breaker_plugin.on_candle_close(candles, current_price)
            
            # Liquidity Sweeps Plugin (depends on SSL/BSL)
            if "liquidity_sweep" in self.plugins[timeframe]:
                sweep_plugin = self.plugins[timeframe]["liquidity_sweep"]
                sweep_plugin.on_candle_close(candles, current_price)
        
        # Step 4: Create zones from pivots and volume
        new_zones = self.zone_detector.create_zones_from_pivots_and_volume(
            timeframe, pivots, volume_clusters, candles
        )
        
        # =====================================================================
        # ENHANCEMENT F: Volume Spike Confirmation
        # =====================================================================
        filtered_zones = []
        for zone in new_zones:
            # Get candle at zone creation
            zone_candle_idx = None
            for i, candle in enumerate(candles):
                if candle["ts"] == zone.created_ts:
                    zone_candle_idx = i
                    break
            
            if zone_candle_idx is not None and zone_candle_idx > 0:
                zone_candle = candles[zone_candle_idx]
                lookback_candles = candles[max(0, zone_candle_idx - 20):zone_candle_idx]
                
                if lookback_candles:
                    spike_ratio = calculate_volume_spike_ratio(
                        lookback_candles,
                        zone_candle["volume"],
                        lookback=min(20, len(lookback_candles))
                    )
                    
                    if spike_ratio and spike_ratio >= self.config.volume_spike_multiplier:
                        filtered_zones.append(zone)
                    else:
                        self.stats["zones_filtered_volume"] += 1
                else:
                    # Not enough lookback data, keep zone
                    filtered_zones.append(zone)
            else:
                # Can't find candle, keep zone
                filtered_zones.append(zone)
        
        new_zones = filtered_zones
        
        # =====================================================================
        # ENHANCEMENT G: Minimum Distance Filter
        # =====================================================================
        distance_filtered_zones = []
        for zone in new_zones:
            zone_mid = (zone.price_low + zone.price_high) / 2
            distance_pct = abs(zone_mid - current_price) / current_price
            
            if distance_pct >= self.config.min_zone_distance_pct:
                distance_filtered_zones.append(zone)
            else:
                self.stats["zones_filtered_distance"] += 1
        
        new_zones = distance_filtered_zones
        
        # =====================================================================
        # ENHANCEMENT C: Zone Age-Based Filtering
        # =====================================================================
        # Filter old zones from existing zones
        current_ts = candles[-1]["ts"]
        age_filtered_zones = []
        
        for zone in tf_zones.zones:
            # Calculate age in candles (approximate)
            age_ms = current_ts - zone.created_ts
            
            # Estimate candle duration based on timeframe
            tf_to_ms = {
                "1m": 60 * 1000,
                "5m": 5 * 60 * 1000,
                "15m": 15 * 60 * 1000,
                "1h": 60 * 60 * 1000,
            }
            candle_duration = tf_to_ms.get(timeframe, 60 * 1000)
            age_in_candles = age_ms / candle_duration
            
            if age_in_candles <= self.config.max_zone_age_candles:
                age_filtered_zones.append(zone)
            else:
                self.stats["zones_filtered_age"] += 1
        
        tf_zones.zones = age_filtered_zones
        
        # Step 5: Update existing zones (check touches/breaks)
        self.zone_detector.update_zone_touches(
            tf_zones.zones,
            candles[-20:],  # Last 20 candles
            self.stats
        )
        
        # Step 6: Merge new zones
        tf_zones.zones = self.zone_detector.merge_zones(tf_zones.zones, new_zones)
        self.stats["total_zones_created"] += len(new_zones)
        
        # Step 7: Calculate zone strength
        self.zone_detector.calculate_zone_strength(tf_zones.zones)
        
        # Step 8: Calculate Premium/Discount positions
        self.zone_detector.calculate_pd_position(tf_zones.zones, candles)
        
        # Update timestamp
        tf_zones.last_update_ts = candles[-1]["ts"]
    
    # ========================================================================
    # ZONE QUERIES
    # ========================================================================
    
    def get_zones_for_timeframe(self, timeframe: str) -> List[LiquidityZone]:
        """Get all zones for a specific timeframe."""
        if timeframe in self.tf_zones:
            return self.tf_zones[timeframe].zones
        return []
    
    def get_active_zones(self, timeframe: Optional[str] = None) -> List[LiquidityZone]:
        """Get all active zones, optionally filtered by timeframe."""
        if timeframe:
            if timeframe in self.tf_zones:
                return self.tf_zones[timeframe].get_active_zones()
            return []
        
        # Get from all timeframes
        zones = []
        for tf_zones in self.tf_zones.values():
            zones.extend(tf_zones.get_active_zones())
        return zones
    
    def get_nearest_support(self, current_price: float) -> Optional[LiquidityZone]:
        """Get nearest support zone below current price."""
        supports = []
        for tf_zones in self.tf_zones.values():
            supports.extend(tf_zones.get_support_zones())
        
        # Filter zones below current price
        below = [z for z in supports if z.price_high < current_price]
        
        if not below:
            return None
        
        # Return closest
        return min(below, key=lambda z: current_price - z.price_high)
    
    def get_nearest_resistance(self, current_price: float) -> Optional[LiquidityZone]:
        """Get nearest resistance zone above current price."""
        resistances = []
        for tf_zones in self.tf_zones.values():
            resistances.extend(tf_zones.get_resistance_zones())
        
        # Filter zones above current price
        above = [z for z in resistances if z.price_low > current_price]
        
        if not above:
            return None
        
        # Return closest
        return min(above, key=lambda z: z.price_low - current_price)
    
    def get_confluence_zones(
        self,
        min_timeframes: int = 2,
        min_strength: str = "weak"
    ) -> List[LiquidityZone]:
        """
        Get zones where multiple timeframes agree (confluence).
        Uses weighted scoring: higher TFs contribute more weight.
        
        Args:
            min_timeframes: Minimum number of TFs that must have zone at same price
            min_strength: Minimum strength ("weak", "medium", "strong")
            
        Returns:
            List of zones with confluence across timeframes, weighted by TF importance
        """
        from .tf_config import get_timeframe_config
        
        strength_order = {"weak": 1, "medium": 2, "strong": 3}
        min_strength_value = strength_order.get(min_strength, 1)
        
        # Group zones by price proximity (uses dynamic merge_radius_pct)
        groups = self._group_zones_by_price()
        
        confluence_zones = []
        for zones in groups:
            if len(zones) >= min_timeframes:
                # ENHANCEMENT D: Calculate weighted confluence score
                # Higher TF zones contribute more weight (1h=4, 15m=3, 5m=2, 1m=1)
                total_weight = 0
                for zone in zones:
                    tf_config = get_timeframe_config(zone.timeframe)
                    total_weight += tf_config.tf_weight
                
                # Select best zone: prioritize higher TF, then strength, then touches
                best_zone = max(zones, key=lambda z: (
                    get_timeframe_config(z.timeframe).tf_weight,
                    strength_order[z.strength],
                    z.touch_count,
                    z.volume
                ))
                
                if strength_order[best_zone.strength] >= min_strength_value:
                    best_zone.confluence_count = len(zones)
                    # Store weighted score for ranking
                    best_zone.confluence_weight = total_weight
                    confluence_zones.append(best_zone)
        
        # Sort by weighted confluence (higher TF confluence ranked higher)
        confluence_zones.sort(key=lambda z: (z.confluence_weight, z.confluence_count), reverse=True)
        
        return confluence_zones
    
    def _group_zones_by_price(
        self,
        tolerance_pct: float = None
    ) -> List[List[LiquidityZone]]:
        """
        Group zones from different timeframes at similar price levels.
        
        ENHANCEMENT D: Uses dynamic merge_radius_pct from each TF's config.
        Calculates weighted average tolerance based on TF weights.
        """
        from .tf_config import get_timeframe_config
        
        all_zones = []
        for tf_zones in self.tf_zones.values():
            all_zones.extend(tf_zones.get_active_zones())
        
        if not all_zones:
            return []
        
        # Calculate dynamic tolerance if not provided
        if tolerance_pct is None:
            # Use weighted average of merge_radius_pct across all TFs
            total_weight = 0
            weighted_radius = 0
            for tf in self.tf_zones.keys():
                tf_config = get_timeframe_config(tf)
                weighted_radius += tf_config.merge_radius_pct * tf_config.tf_weight
                total_weight += tf_config.tf_weight
            
            tolerance_pct = weighted_radius / total_weight if total_weight > 0 else 0.01
        
        # Sort by midpoint price
        all_zones.sort(key=lambda z: z.midpoint)
        
        # Group nearby zones
        groups = []
        current_group = [all_zones[0]]
        
        for i in range(1, len(all_zones)):
            zone = all_zones[i]
            prev_zone = current_group[-1]
            
            distance_pct = abs(zone.midpoint - prev_zone.midpoint) / prev_zone.midpoint
            
            if distance_pct <= tolerance_pct:
                current_group.append(zone)
            else:
                groups.append(current_group)
                current_group = [zone]
        
        if current_group:
            groups.append(current_group)
        
        return groups
    
    # ========================================================================
    # FVG QUERIES (Via Plugin)
    # ========================================================================
    
    def get_fvgs(
        self,
        timeframe: Optional[str] = None,
        only_unfilled: bool = True,
        fvg_type: Optional[str] = None,
    ) -> List[FairValueGap]:
        """
        Get Fair Value Gaps from FVG plugin.
        
        Args:
            timeframe: Specific TF (None = all TFs)
            only_unfilled: Only return unfilled FVGs
            fvg_type: Filter by type ("bullish" or "bearish")
            
        Returns:
            List of FVGs matching criteria
        """
        fvgs = []
        
        tfs_to_check = [timeframe] if timeframe else self.timeframes
        
        for tf in tfs_to_check:
            if tf in self.plugins and "fvg" in self.plugins[tf]:
                plugin = self.plugins[tf]["fvg"]
                tf_fvgs = plugin.get(only_unfilled=only_unfilled, fvg_type=fvg_type)
                fvgs.extend(tf_fvgs)
        
        # Sort by created timestamp (newest first)
        fvgs.sort(key=lambda x: x.created_ts, reverse=True)
        
        return fvgs
    
    def get_nearest_fvg(
        self,
        current_price: float,
        direction: str = "both",
        only_unfilled: bool = True,
    ) -> Optional[FairValueGap]:
        """
        Find the nearest FVG to current price.
        
        Args:
            current_price: Current market price
            direction: "above", "below", or "both"
            only_unfilled: Only consider unfilled FVGs
            
        Returns:
            Nearest FVG or None
        """
        fvgs = self.get_fvgs(only_unfilled=only_unfilled)
        
        if not fvgs:
            return None
        
        if direction == "above":
            fvgs = [fvg for fvg in fvgs if fvg.gap_low > current_price]
            if not fvgs:
                return None
            return min(fvgs, key=lambda x: x.gap_low)
        
        elif direction == "below":
            fvgs = [fvg for fvg in fvgs if fvg.gap_high < current_price]
            if not fvgs:
                return None
            return max(fvgs, key=lambda x: x.gap_high)
        
        else:  # both
            return min(fvgs, key=lambda x: abs(x.midpoint - current_price))
    
    # ========================================================================
    # DISPLACEMENT QUERIES
    # ========================================================================
    
    def get_displacements(
        self,
        timeframe: Optional[str] = None,
        direction: Optional[str] = None,
        min_candles: Optional[int] = None,
    ) -> List[Displacement]:
        """
        Get displacement moves.
        
        Args:
            timeframe: Specific TF (None = all TFs)
            direction: Filter by "bullish" or "bearish" (None = both)
            min_candles: Minimum candles in displacement
            
        Returns:
            List of Displacement objects
        """
        displacements = []
        
        tfs_to_check = [timeframe] if timeframe else self.timeframes
        
        for tf in tfs_to_check:
            if tf in self.tf_displacements:
                displacements.extend(self.tf_displacements[tf])
        
        # Apply filters
        if direction:
            displacements = [d for d in displacements if d.direction == direction]
        
        if min_candles:
            displacements = [d for d in displacements if d.num_candles >= min_candles]
        
        # Sort by end timestamp (newest first)
        displacements.sort(key=lambda d: d.end_ts, reverse=True)
        
        return displacements
    
    def get_recent_displacements(
        self,
        timeframe: Optional[str] = None,
        lookback: int = 10,
    ) -> List[Displacement]:
        """
        Get most recent displacement moves.
        
        Args:
            timeframe: Specific TF (None = all TFs)
            lookback: Number of recent displacements to return
            
        Returns:
            Recent displacements
        """
        all_displacements = self.get_displacements(timeframe=timeframe)
        return all_displacements[:lookback]
    
    def get_strongest_displacement(
        self,
        timeframe: Optional[str] = None,
        metric: str = "move_pct",
    ) -> Optional[Displacement]:
        """
        Get the strongest displacement by chosen metric.
        
        Args:
            timeframe: Specific TF (None = all TFs)
            metric: "move_pct", "volume_surge_ratio", or "num_candles"
            
        Returns:
            Strongest displacement or None
        """
        displacements = self.get_displacements(timeframe=timeframe)
        
        return self.displacement_detector.get_strongest_displacement(
            displacements, metric=metric
        )
    
    # ========================================================================
    # STATISTICS
    # ========================================================================
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the liquidity map."""
        stats = self.stats.copy()
        
        # Add per-TF zone stats
        stats["zones_per_tf"] = {}
        for tf, tf_zones in self.tf_zones.items():
            active = len(tf_zones.get_active_zones())
            total = len(tf_zones.zones)
            
            stats["zones_per_tf"][tf] = {
                "active": active,
                "total": total,
                "last_update": tf_zones.last_update_ts
            }
        
        # Add per-TF FVG stats (from plugins)
        stats["fvgs_per_tf"] = {}
        for tf in self.timeframes:
            if tf in self.plugins and "fvg" in self.plugins[tf]:
                plugin = self.plugins[tf]["fvg"]
                unfilled = len(plugin.get(only_unfilled=True))
                total = len(plugin.get(only_unfilled=False))
                stats["fvgs_per_tf"][tf] = {
                    "unfilled": unfilled,
                    "total": total
                }
        
        # Add per-TF displacement stats
        stats["displacements_per_tf"] = {}
        for tf in self.timeframes:
            if tf in self.tf_displacements:
                displacements = self.tf_displacements[tf]
                bullish = len([d for d in displacements if d.is_bullish])
                bearish = len([d for d in displacements if d.is_bearish])
                stats["displacements_per_tf"][tf] = {
                    "total": len(displacements),
                    "bullish": bullish,
                    "bearish": bearish
                }
        
        return stats
    
    # ========================================================================
    # PLUGIN MANAGEMENT
    # ========================================================================
    
    def enable_plugin(self, plugin_name: str, timeframe: Optional[str] = None):
        """Enable a specific plugin for one or all timeframes."""
        tfs = [timeframe] if timeframe else self.timeframes
        
        for tf in tfs:
            if tf in self.plugins and plugin_name in self.plugins[tf]:
                self.plugins[tf][plugin_name].enable()
    
    def disable_plugin(self, plugin_name: str, timeframe: Optional[str] = None):
        """Disable a specific plugin for one or all timeframes."""
        tfs = [timeframe] if timeframe else self.timeframes
        
        for tf in tfs:
            if tf in self.plugins and plugin_name in self.plugins[tf]:
                self.plugins[tf][plugin_name].disable()
    
    def get_plugin_status(self) -> Dict[str, Dict[str, bool]]:
        """Get enabled/disabled status of all plugins per timeframe."""
        status = {}
        
        for tf in self.timeframes:
            status[tf] = {}
            if tf in self.plugins:
                for plugin_name, plugin in self.plugins[tf].items():
                    status[tf][plugin_name] = plugin.enabled
        
        return status
