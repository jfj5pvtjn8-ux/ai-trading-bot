"""
Multi-Timeframe Trend Fusion & Detection

Combines trend analysis across multiple timeframes with liquidity zones
to identify high-probability trading setups.

Key Features:
- MTF trend alignment detection (all TFs trending same direction)
- Trend strength measurement per timeframe
- Integration with liquidity zones (trend + zone = signal)
- Trend reversal detection at key zones
- Momentum confluence across timeframes
- EMA/SMA trend analysis
- Higher high / higher low structure detection

Architecture:
- Each timeframe has independent trend state
- Trends are calculated when TF candle closes (event-driven)
- 1m master triggers final trend fusion across all TFs
- Combines with liquidity zones for trade setups
"""

from typing import Dict, List, Optional, Tuple, Any
import numpy as np

from trading_bot.indicators import Indicators
from .enums import TrendDirection, TrendStrength
from .models import TrendState, TrendFusionSignal


# ============================================================================
# TREND FUSION CLASS
# ============================================================================

class TrendFusion:
    """
    Professional multi-timeframe trend detector and fusion engine.
    
    Analyzes trends across multiple timeframes and combines with liquidity
    zones to generate high-probability trading signals.
    
    Usage:
        fusion = TrendFusion(symbol="BTCUSDT", timeframes=["1m", "5m", "15m", "1h"])
        
        # On candle close
        fusion.on_candle_close(
            timeframe="5m",
            candles=candle_manager.get_all(),
            liquidity_map=liq_map
        )
        
        # Check for signals
        signal = fusion.get_fusion_signal()
    """
    
    def __init__(
        self,
        symbol: str,
        timeframes: List[str],
        ema_fast: int = 20,
        ema_slow: int = 50,
        rsi_period: int = 14,
        structure_lookback: int = 20,
    ):
        """
        Initialize trend fusion engine.
        
        Args:
            symbol: Trading symbol
            timeframes: List of timeframes to analyze
            ema_fast: Fast EMA period (default 20)
            ema_slow: Slow EMA period (default 50)
            rsi_period: RSI calculation period (default 14)
            structure_lookback: Candles to analyze for structure (default 20)
        """
        self.symbol = symbol
        self.timeframes = timeframes
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.rsi_period = rsi_period
        self.structure_lookback = structure_lookback
        
        # Per-timeframe trend state
        self.trend_states: Dict[str, TrendState] = {}
        
        # Latest fusion signal
        self.latest_signal: Optional[TrendFusionSignal] = None
        
        # Statistics
        self.stats = {
            "signals_generated": 0,
            "bullish_signals": 0,
            "bearish_signals": 0,
            "last_update_ts": {},
        }
    
    # ========================================================================
    # EVENT-DRIVEN TREND ANALYSIS
    # ========================================================================
    
    def on_candle_close(
        self,
        timeframe: str,
        candles: List[Dict[str, Any]],
        liquidity_map = None,
        current_price: Optional[float] = None,
    ) -> bool:
        """
        Analyze trend for specific timeframe when its candle closes.
        
        This is called by MTFSymbolManager after each validated candle.
        Updates trend state for this TF only.
        
        Args:
            timeframe: TF that just closed
            candles: All candles for this TF
            liquidity_map: LiquidityMap instance for zone integration
            current_price: Current market price
            
        Returns:
            True if trend state was updated
        """
        if len(candles) < max(self.ema_slow, self.rsi_period, self.structure_lookback):
            # Not enough data for analysis
            return False
        
        # Calculate trend state for this timeframe
        trend_state = self._calculate_trend_state(timeframe, candles)
        self.trend_states[timeframe] = trend_state
        self.stats["last_update_ts"][timeframe] = candles[-1]["ts"]
        
        # If 1m (master) closed, generate fusion signal
        if timeframe == "1m" and liquidity_map:
            self._generate_fusion_signal(liquidity_map, current_price or candles[-1]["close"])
        
        return True
    
    def _calculate_trend_state(
        self,
        timeframe: str,
        candles: List[Dict[str, Any]]
    ) -> TrendState:
        """
        Calculate complete trend state for a timeframe.
        
        Analyzes:
        - EMA crossovers and alignment
        - Price structure (higher highs/lows, lower highs/lows)
        - RSI momentum
        - MACD histogram
        - Overall trend direction and strength
        """
        # Extract price data
        closes = np.array([c["close"] for c in candles])
        highs = np.array([c["high"] for c in candles])
        lows = np.array([c["low"] for c in candles])
        
        current_price = closes[-1]
        
        # Calculate EMAs
        ema20 = self._calculate_ema(closes, self.ema_fast)
        ema50 = self._calculate_ema(closes, self.ema_slow)
        
        # EMA analysis
        price_above_ema20 = current_price > ema20
        price_above_ema50 = current_price > ema50
        ema20_above_ema50 = ema20 > ema50
        
        # Structure analysis
        structure = self._analyze_structure(highs, lows)
        
        # RSI
        rsi = self._calculate_rsi(closes, self.rsi_period)
        
        # MACD histogram (simplified)
        macd_hist = self._calculate_macd_histogram(closes)
        
        # Determine trend direction
        direction = self._determine_trend_direction(
            price_above_ema20, price_above_ema50, ema20_above_ema50,
            structure, rsi
        )
        
        # Calculate trend strength
        strength = self._calculate_trend_strength(
            price_above_ema20, price_above_ema50, ema20_above_ema50,
            structure, rsi, macd_hist
        )
        
        # Momentum score (-100 to +100)
        momentum = self._calculate_momentum_score(rsi, macd_hist, structure)
        
        return TrendState(
            timeframe=timeframe,
            direction=direction,
            strength=strength,
            momentum_score=momentum,
            price_above_ema20=price_above_ema20,
            price_above_ema50=price_above_ema50,
            ema20_above_ema50=ema20_above_ema50,
            higher_highs=structure["higher_highs"],
            higher_lows=structure["higher_lows"],
            lower_highs=structure["lower_highs"],
            lower_lows=structure["lower_lows"],
            rsi=rsi,
            macd_histogram=macd_hist,
            last_update_ts=candles[-1]["ts"],
            candles_analyzed=len(candles),
        )
    
    # ========================================================================
    # TECHNICAL INDICATORS
    # ========================================================================
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> float:
        """Calculate Exponential Moving Average."""
        return Indicators.calculate_ema(prices, period)
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """Calculate Relative Strength Index."""
        return Indicators.calculate_rsi(prices, period)
    
    def _calculate_macd_histogram(self, prices: np.ndarray) -> float:
        """Calculate MACD histogram (simplified)."""
        if len(prices) < 26:
            return 0.0
        
        ema12 = self._calculate_ema(prices, 12)
        ema26 = self._calculate_ema(prices, 26)
        macd_line = ema12 - ema26
        
        # Signal line (9-period EMA of MACD)
        # Simplified: just use recent MACD values
        signal = macd_line * 0.8  # Approximation
        
        histogram = macd_line - signal
        return histogram
    
    def _analyze_structure(
        self,
        highs: np.ndarray,
        lows: np.ndarray
    ) -> Dict[str, bool]:
        """
        Analyze price structure for higher/lower highs and lows.
        
        Returns:
            Dict with higher_highs, higher_lows, lower_highs, lower_lows flags
        """
        lookback = min(self.structure_lookback, len(highs))
        recent_highs = highs[-lookback:]
        recent_lows = lows[-lookback:]
        
        # Simple structure detection
        # Check if recent highs are trending up or down
        high_trend = np.polyfit(range(len(recent_highs)), recent_highs, 1)[0]
        low_trend = np.polyfit(range(len(recent_lows)), recent_lows, 1)[0]
        
        higher_highs = high_trend > 0
        higher_lows = low_trend > 0
        lower_highs = high_trend < 0
        lower_lows = low_trend < 0
        
        return {
            "higher_highs": higher_highs,
            "higher_lows": higher_lows,
            "lower_highs": lower_highs,
            "lower_lows": lower_lows,
        }
    
    # ========================================================================
    # TREND CLASSIFICATION
    # ========================================================================
    
    def _determine_trend_direction(
        self,
        price_above_ema20: bool,
        price_above_ema50: bool,
        ema20_above_ema50: bool,
        structure: Dict[str, bool],
        rsi: float,
    ) -> TrendDirection:
        """Determine overall trend direction."""
        
        # Strong bullish: All aligned + structure + RSI
        if (price_above_ema20 and price_above_ema50 and ema20_above_ema50 and
            structure["higher_highs"] and structure["higher_lows"] and rsi > 55):
            return TrendDirection.STRONG_BULLISH
        
        # Bullish: Most indicators aligned
        if ((price_above_ema20 and ema20_above_ema50) or
            (structure["higher_highs"] and structure["higher_lows"])):
            return TrendDirection.BULLISH
        
        # Strong bearish: All aligned + structure + RSI
        if (not price_above_ema20 and not price_above_ema50 and not ema20_above_ema50 and
            structure["lower_highs"] and structure["lower_lows"] and rsi < 45):
            return TrendDirection.STRONG_BEARISH
        
        # Bearish: Most indicators aligned
        if ((not price_above_ema20 and not ema20_above_ema50) or
            (structure["lower_highs"] and structure["lower_lows"])):
            return TrendDirection.BEARISH
        
        # Neutral: Mixed signals
        return TrendDirection.NEUTRAL
    
    def _calculate_trend_strength(
        self,
        price_above_ema20: bool,
        price_above_ema50: bool,
        ema20_above_ema50: bool,
        structure: Dict[str, bool],
        rsi: float,
        macd_hist: float,
    ) -> TrendStrength:
        """Calculate trend strength score."""
        
        score = 0
        
        # EMA alignment (0-3 points)
        if price_above_ema20 or not price_above_ema20:  # Direction doesn't matter
            score += 1
        if ema20_above_ema50 or not ema20_above_ema50:
            score += 1
        if price_above_ema50 or not price_above_ema50:
            score += 1
        
        # Structure (0-2 points)
        if structure["higher_highs"] and structure["higher_lows"]:
            score += 2
        elif structure["lower_highs"] and structure["lower_lows"]:
            score += 2
        
        # RSI extremes (0-2 points)
        if rsi > 60 or rsi < 40:
            score += 1
        if rsi > 70 or rsi < 30:
            score += 1
        
        # MACD (0-2 points)
        if abs(macd_hist) > 10:
            score += 1
        if abs(macd_hist) > 20:
            score += 1
        
        # Classify strength
        if score >= 8:
            return TrendStrength.VERY_STRONG
        elif score >= 6:
            return TrendStrength.STRONG
        elif score >= 4:
            return TrendStrength.MODERATE
        elif score >= 2:
            return TrendStrength.WEAK
        else:
            return TrendStrength.VERY_WEAK
    
    def _calculate_momentum_score(
        self,
        rsi: float,
        macd_hist: float,
        structure: Dict[str, bool],
    ) -> float:
        """Calculate momentum score (-100 to +100)."""
        
        score = 0.0
        
        # RSI contribution (-50 to +50)
        score += (rsi - 50.0)
        
        # MACD contribution (-25 to +25)
        score += np.clip(macd_hist * 2, -25, 25)
        
        # Structure contribution (-25 to +25)
        if structure["higher_highs"] and structure["higher_lows"]:
            score += 25
        elif structure["lower_highs"] and structure["lower_lows"]:
            score -= 25
        
        return np.clip(score, -100, 100)
    
    # ========================================================================
    # TREND FUSION & SIGNAL GENERATION
    # ========================================================================
    
    def _generate_fusion_signal(
        self,
        liquidity_map,
        current_price: float,
    ):
        """
        Generate fusion signal by combining MTF trends with liquidity zones.
        
        This is called when 1m (master) closes - aggregates all TF trends
        and combines with liquidity zones for high-probability setups.
        """
        # Get trend alignment
        alignment = self._analyze_trend_alignment()
        
        if not alignment["is_aligned"]:
            self.latest_signal = None
            return
        
        # Get confluence zones from liquidity map
        confluence_zones = liquidity_map.get_confluence_zones(
            min_timeframes=2,
            min_strength="medium"
        )
        
        if not confluence_zones:
            self.latest_signal = None
            return
        
        # Find nearest zone in trend direction
        if alignment["direction"].is_bullish():
            # Look for support zone below price
            nearest_zone = liquidity_map.get_nearest_support(current_price)
            signal_type = "bullish_confluence"
        else:
            # Look for resistance zone above price
            nearest_zone = liquidity_map.get_nearest_resistance(current_price)
            signal_type = "bearish_confluence"
        
        if not nearest_zone:
            self.latest_signal = None
            return
        
        # Calculate confidence (0.0 to 1.0)
        confidence = self._calculate_signal_confidence(
            alignment, nearest_zone, current_price
        )
        
        # Generate signal
        self.latest_signal = TrendFusionSignal(
            symbol=self.symbol,
            signal_type=signal_type,
            confidence=confidence,
            aligned_timeframes=alignment["aligned_tfs"],
            dominant_direction=alignment["direction"],
            key_zone_price=(nearest_zone.price_low + nearest_zone.price_high) / 2,
            zone_strength=nearest_zone.strength,
            zone_type=nearest_zone.zone_type,
            entry_price=self._calculate_entry_price(nearest_zone, alignment["direction"]),
            stop_loss=self._calculate_stop_loss(nearest_zone, alignment["direction"]),
            take_profit=self._calculate_take_profit(nearest_zone, alignment["direction"], current_price),
            timestamp=int(liquidity_map.stats["last_refresh_ts"].get("1m", 0)),
        )
        
        # Update statistics
        self.stats["signals_generated"] += 1
        if alignment["direction"].is_bullish():
            self.stats["bullish_signals"] += 1
        else:
            self.stats["bearish_signals"] += 1
    
    def _analyze_trend_alignment(self) -> Dict[str, Any]:
        """
        Analyze if multiple timeframes are aligned in same direction.
        
        Returns:
            Dict with is_aligned, direction, aligned_tfs, strength_score
        """
        if not self.trend_states:
            return {"is_aligned": False}
        
        # Count bullish/bearish/neutral across TFs
        bullish_count = sum(1 for ts in self.trend_states.values() if ts.is_bullish())
        bearish_count = sum(1 for ts in self.trend_states.values() if ts.is_bearish())
        
        total_tfs = len(self.trend_states)
        
        # Check for alignment (at least 75% agreement)
        if bullish_count >= total_tfs * 0.75:
            aligned_tfs = [tf for tf, ts in self.trend_states.items() if ts.is_bullish()]
            return {
                "is_aligned": True,
                "direction": TrendDirection.BULLISH,
                "aligned_tfs": aligned_tfs,
                "strength_score": bullish_count / total_tfs,
            }
        
        if bearish_count >= total_tfs * 0.75:
            aligned_tfs = [tf for tf, ts in self.trend_states.items() if ts.is_bearish()]
            return {
                "is_aligned": True,
                "direction": TrendDirection.BEARISH,
                "aligned_tfs": aligned_tfs,
                "strength_score": bearish_count / total_tfs,
            }
        
        return {"is_aligned": False}
    
    def _calculate_signal_confidence(
        self,
        alignment: Dict[str, Any],
        zone,
        current_price: float,
    ) -> float:
        """Calculate confidence score for signal (0.0 to 1.0)."""
        
        confidence = 0.0
        
        # Trend alignment strength (0.0 to 0.4)
        confidence += alignment["strength_score"] * 0.4
        
        # Zone strength (0.0 to 0.3)
        zone_strength_map = {"weak": 0.1, "medium": 0.2, "strong": 0.3}
        confidence += zone_strength_map.get(zone.strength, 0.1)
        
        # Zone confluence (0.0 to 0.2)
        confidence += min(0.2, zone.confluence_count * 0.05)
        
        # Distance to zone (0.0 to 0.1) - closer = better
        distance_pct = abs(current_price - (zone.price_low + zone.price_high) / 2) / current_price
        if distance_pct < 0.02:  # Within 2%
            confidence += 0.1 - (distance_pct * 5)
        
        return min(1.0, confidence)
    
    def _calculate_entry_price(self, zone, direction: TrendDirection) -> float:
        """Calculate suggested entry price based on zone."""
        if direction.is_bullish():
            return zone.price_high  # Enter at top of support zone
        else:
            return zone.price_low  # Enter at bottom of resistance zone
    
    def _calculate_stop_loss(self, zone, direction: TrendDirection) -> float:
        """Calculate stop loss below/above zone."""
        buffer = 0.001  # 0.1% buffer
        if direction.is_bullish():
            return zone.price_low * (1 - buffer)  # Below support
        else:
            return zone.price_high * (1 + buffer)  # Above resistance
    
    def _calculate_take_profit(self, zone, direction: TrendDirection, current_price: float) -> float:
        """Calculate take profit target (2:1 risk-reward minimum)."""
        zone_mid = (zone.price_low + zone.price_high) / 2
        risk = abs(current_price - zone_mid)
        
        if direction.is_bullish():
            return current_price + (risk * 2)
        else:
            return current_price - (risk * 2)
    
    # ========================================================================
    # QUERY INTERFACE
    # ========================================================================
    
    def get_fusion_signal(self) -> Optional[TrendFusionSignal]:
        """Get latest fusion signal (if any)."""
        return self.latest_signal
    
    def get_trend_state(self, timeframe: str) -> Optional[TrendState]:
        """Get trend state for specific timeframe."""
        return self.trend_states.get(timeframe)
    
    def get_all_trend_states(self) -> Dict[str, TrendState]:
        """Get all timeframe trend states."""
        return self.trend_states.copy()
    
    def is_aligned(self) -> bool:
        """Check if timeframes are aligned."""
        alignment = self._analyze_trend_alignment()
        return alignment.get("is_aligned", False)
    
    def get_dominant_direction(self) -> Optional[TrendDirection]:
        """Get dominant trend direction across all TFs."""
        alignment = self._analyze_trend_alignment()
        return alignment.get("direction") if alignment.get("is_aligned") else None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about trend fusion."""
        return self.stats.copy()
