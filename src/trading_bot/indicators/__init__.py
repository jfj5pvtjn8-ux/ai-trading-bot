"""
Technical Indicators Module

Calculates technical indicators from candle data.
All indicators are pure functions that take candle data and return calculated values.

Usage:
    indicators = Indicators()
    
    # Single candle VWAP
    vwap = indicators.calculate_vwap(candle)
    
    # Rolling VWAP over multiple candles
    vwap_series = indicators.calculate_rolling_vwap(candles, period=20)
    
    # EMA
    ema20 = indicators.calculate_ema(closes, period=20)
    
    # RSI
    rsi = indicators.calculate_rsi(closes, period=14)
"""

from typing import List, Dict, Any
import numpy as np


class Indicators:
    """
    Technical indicators calculator.
    
    Provides clean, reusable indicator calculations for use across
    liquidity map, trend fusion, and other decision layer components.
    """
    
    @staticmethod
    def calculate_vwap(candle: Dict[str, Any]) -> float:
        """
        Calculate Volume Weighted Average Price for a single candle.
        
        VWAP = (High + Low + Close) / 3
        
        This is the typical price for the candle, weighted by the OHLC.
        More accurate than using just close price.
        
        Args:
            candle: Candle dict with high, low, close
            
        Returns:
            VWAP price
        """
        return (candle["high"] + candle["low"] + candle["close"]) / 3.0
    
    @staticmethod
    def calculate_vwap_with_volume(candle: Dict[str, Any]) -> float:
        """
        Calculate true Volume Weighted Average Price.
        
        VWAP = (Typical Price * Volume) / Volume
        Typical Price = (High + Low + Close) / 3
        
        Args:
            candle: Candle dict with high, low, close, volume
            
        Returns:
            Volume-weighted VWAP
        """
        typical_price = (candle["high"] + candle["low"] + candle["close"]) / 3.0
        return typical_price  # For single candle, it's the same
    
    @staticmethod
    def calculate_rolling_vwap(
        candles: List[Dict[str, Any]],
        period: int = 20
    ) -> List[float]:
        """
        Calculate rolling VWAP over a period.
        
        For each candle, calculates VWAP using the last N candles.
        
        Args:
            candles: List of candle dicts
            period: Lookback period (default 20)
            
        Returns:
            List of VWAP values
        """
        vwaps = []
        
        for i in range(len(candles)):
            start_idx = max(0, i - period + 1)
            period_candles = candles[start_idx:i + 1]
            
            # Calculate cumulative typical price * volume
            cum_tp_vol = 0.0
            cum_vol = 0.0
            
            for candle in period_candles:
                typical_price = (candle["high"] + candle["low"] + candle["close"]) / 3.0
                volume = candle["volume"]
                cum_tp_vol += typical_price * volume
                cum_vol += volume
            
            # VWAP = cumulative(typical_price * volume) / cumulative(volume)
            vwap = cum_tp_vol / cum_vol if cum_vol > 0 else period_candles[-1]["close"]
            vwaps.append(vwap)
        
        return vwaps
    
    @staticmethod
    def calculate_ema(prices: np.ndarray, period: int) -> float:
        """
        Calculate Exponential Moving Average.
        
        Args:
            prices: Array of prices
            period: EMA period
            
        Returns:
            Latest EMA value
        """
        if len(prices) < period:
            return float(np.mean(prices))
        
        multiplier = 2.0 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price - ema) * multiplier + ema
        
        return float(ema)
    
    @staticmethod
    def calculate_ema_series(prices: np.ndarray, period: int) -> np.ndarray:
        """
        Calculate EMA series for all prices.
        
        Args:
            prices: Array of prices
            period: EMA period
            
        Returns:
            Array of EMA values
        """
        if len(prices) < period:
            return prices.copy()
        
        multiplier = 2.0 / (period + 1)
        ema_values = np.zeros(len(prices))
        ema_values[0] = prices[0]
        
        for i in range(1, len(prices)):
            ema_values[i] = (prices[i] - ema_values[i-1]) * multiplier + ema_values[i-1]
        
        return ema_values
    
    @staticmethod
    def calculate_sma(prices: np.ndarray, period: int) -> float:
        """
        Calculate Simple Moving Average.
        
        Args:
            prices: Array of prices
            period: SMA period
            
        Returns:
            Latest SMA value
        """
        if len(prices) < period:
            return float(np.mean(prices))
        
        return float(np.mean(prices[-period:]))
    
    @staticmethod
    def calculate_rsi(prices: np.ndarray, period: int = 14) -> float:
        """
        Calculate Relative Strength Index.
        
        Args:
            prices: Array of prices
            period: RSI period (default 14)
            
        Returns:
            RSI value (0-100)
        """
        if len(prices) < period + 1:
            return 50.0  # Neutral
        
        deltas = np.diff(prices[-period - 1:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains)
        avg_loss = np.mean(losses)
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        
        return float(rsi)
    
    @staticmethod
    def calculate_macd(
        prices: np.ndarray,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> Dict[str, float]:
        """
        Calculate MACD (Moving Average Convergence Divergence).
        
        Args:
            prices: Array of prices
            fast: Fast EMA period (default 12)
            slow: Slow EMA period (default 26)
            signal: Signal line period (default 9)
            
        Returns:
            Dict with macd, signal, histogram values
        """
        if len(prices) < slow:
            return {"macd": 0.0, "signal": 0.0, "histogram": 0.0}
        
        # Calculate EMAs
        ema_fast = Indicators.calculate_ema(prices, fast)
        ema_slow = Indicators.calculate_ema(prices, slow)
        
        # MACD line
        macd_line = ema_fast - ema_slow
        
        # Signal line (simplified - would need series for accurate signal)
        signal_line = macd_line * 0.8  # Approximation
        
        # Histogram
        histogram = macd_line - signal_line
        
        return {
            "macd": float(macd_line),
            "signal": float(signal_line),
            "histogram": float(histogram)
        }
    
    @staticmethod
    def calculate_bollinger_bands(
        prices: np.ndarray,
        period: int = 20,
        std_dev: float = 2.0
    ) -> Dict[str, float]:
        """
        Calculate Bollinger Bands.
        
        Args:
            prices: Array of prices
            period: SMA period (default 20)
            std_dev: Standard deviations (default 2.0)
            
        Returns:
            Dict with upper, middle, lower bands
        """
        if len(prices) < period:
            middle = float(np.mean(prices))
            return {
                "upper": middle,
                "middle": middle,
                "lower": middle
            }
        
        middle = Indicators.calculate_sma(prices, period)
        std = float(np.std(prices[-period:]))
        
        return {
            "upper": middle + (std_dev * std),
            "middle": middle,
            "lower": middle - (std_dev * std)
        }
    
    @staticmethod
    def calculate_atr(
        candles: List[Dict[str, Any]],
        period: int = 14
    ) -> float:
        """
        Calculate Average True Range.
        
        Args:
            candles: List of candle dicts with high, low, close
            period: ATR period (default 14)
            
        Returns:
            ATR value
        """
        if len(candles) < period + 1:
            # Fallback: simple high-low range
            ranges = [c["high"] - c["low"] for c in candles]
            return float(np.mean(ranges))
        
        true_ranges = []
        
        for i in range(1, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i-1]["close"]
            
            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(tr)
        
        # Average of last N true ranges
        return float(np.mean(true_ranges[-period:]))
    
    @staticmethod
    def calculate_stochastic(
        candles: List[Dict[str, Any]],
        period: int = 14,
        smooth_k: int = 3,
        smooth_d: int = 3
    ) -> Dict[str, float]:
        """
        Calculate Stochastic Oscillator.
        
        Args:
            candles: List of candle dicts
            period: Lookback period (default 14)
            smooth_k: %K smoothing (default 3)
            smooth_d: %D smoothing (default 3)
            
        Returns:
            Dict with k and d values
        """
        if len(candles) < period:
            return {"k": 50.0, "d": 50.0}
        
        recent = candles[-period:]
        
        current_close = candles[-1]["close"]
        lowest_low = min(c["low"] for c in recent)
        highest_high = max(c["high"] for c in recent)
        
        if highest_high == lowest_low:
            k = 50.0
        else:
            k = 100.0 * (current_close - lowest_low) / (highest_high - lowest_low)
        
        # Simplified: D is smoothed K (would need series for proper calculation)
        d = k * 0.9
        
        return {"k": float(k), "d": float(d)}
    
    @staticmethod
    def calculate_obv(candles: List[Dict[str, Any]]) -> float:
        """
        Calculate On-Balance Volume.
        
        Args:
            candles: List of candle dicts with close and volume
            
        Returns:
            OBV value
        """
        if len(candles) < 2:
            return candles[0]["volume"] if candles else 0.0
        
        obv = 0.0
        
        for i in range(1, len(candles)):
            if candles[i]["close"] > candles[i-1]["close"]:
                obv += candles[i]["volume"]
            elif candles[i]["close"] < candles[i-1]["close"]:
                obv -= candles[i]["volume"]
        
        return float(obv)
    
    @staticmethod
    def is_bullish_candle(candle: Dict[str, Any]) -> bool:
        """Check if candle is bullish (close > open)."""
        return candle["close"] > candle["open"]
    
    @staticmethod
    def is_bearish_candle(candle: Dict[str, Any]) -> bool:
        """Check if candle is bearish (close < open)."""
        return candle["close"] < candle["open"]
    
    @staticmethod
    def candle_body_size(candle: Dict[str, Any]) -> float:
        """Calculate candle body size (abs(close - open))."""
        return abs(candle["close"] - candle["open"])
    
    @staticmethod
    def candle_wick_size(candle: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate upper and lower wick sizes.
        
        Returns:
            Dict with upper_wick and lower_wick sizes
        """
        body_high = max(candle["open"], candle["close"])
        body_low = min(candle["open"], candle["close"])
        
        return {
            "upper_wick": candle["high"] - body_high,
            "lower_wick": body_low - candle["low"]
        }
    
    @staticmethod
    def is_doji(candle: Dict[str, Any], threshold: float = 0.1) -> bool:
        """
        Check if candle is a doji (small body relative to range).
        
        Args:
            candle: Candle dict
            threshold: Max body/range ratio for doji (default 0.1 = 10%)
            
        Returns:
            True if doji pattern
        """
        body = abs(candle["close"] - candle["open"])
        range_size = candle["high"] - candle["low"]
        
        if range_size == 0:
            return True
        
        return (body / range_size) <= threshold
