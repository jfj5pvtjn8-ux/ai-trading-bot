"""
Technical Indicators Utility Module

Provides common technical indicators for liquidity map analysis:
- ATR (Average True Range)
- EMA (Exponential Moving Average)
- RSI (Relative Strength Index)
- Volume Average
"""

from typing import List, Dict, Any, Optional


def calculate_atr(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """
    Calculate Average True Range (ATR).
    
    ATR measures market volatility by decomposing the entire range of an asset
    price for that period. It's the average of true ranges over a period.
    
    True Range = max(high - low, abs(high - prev_close), abs(low - prev_close))
    
    Args:
        candles: List of candle dictionaries with OHLCV data
        period: ATR calculation period (default 14)
        
    Returns:
        ATR value or None if insufficient data
    """
    if len(candles) < period + 1:
        return None
    
    true_ranges = []
    
    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]
        
        high_low = current["high"] - current["low"]
        high_close = abs(current["high"] - previous["close"])
        low_close = abs(current["low"] - previous["close"])
        
        true_range = max(high_low, high_close, low_close)
        true_ranges.append(true_range)
    
    # Calculate simple moving average of true ranges
    if len(true_ranges) < period:
        return None
    
    atr = sum(true_ranges[-period:]) / period
    return atr


def calculate_ema(candles: List[Dict[str, Any]], period: int, price_key: str = "close") -> Optional[float]:
    """
    Calculate Exponential Moving Average (EMA).
    
    EMA gives more weight to recent prices, making it more responsive
    to new information.
    
    Args:
        candles: List of candle dictionaries
        period: EMA period
        price_key: Which price to use ("close", "high", "low", "open")
        
    Returns:
        EMA value or None if insufficient data
    """
    if len(candles) < period:
        return None
    
    prices = [c[price_key] for c in candles]
    
    # Calculate initial SMA
    sma = sum(prices[:period]) / period
    
    # Calculate multiplier
    multiplier = 2 / (period + 1)
    
    # Calculate EMA
    ema = sma
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    
    return ema


def calculate_rsi(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    """
    Calculate Relative Strength Index (RSI).
    
    RSI is a momentum oscillator that measures the speed and magnitude
    of price changes (0-100 range).
    
    Args:
        candles: List of candle dictionaries
        period: RSI period (default 14)
        
    Returns:
        RSI value (0-100) or None if insufficient data
    """
    if len(candles) < period + 1:
        return None
    
    closes = [c["close"] for c in candles]
    
    # Calculate price changes
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    
    # Separate gains and losses
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    
    # Calculate average gains and losses
    if len(gains) < period:
        return None
    
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    
    if avg_loss == 0:
        return 100.0
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_volume_average(candles: List[Dict[str, Any]], period: int = 20) -> Optional[float]:
    """
    Calculate average volume over a period.
    
    Args:
        candles: List of candle dictionaries
        period: Period for average calculation
        
    Returns:
        Average volume or None if insufficient data
    """
    if len(candles) < period:
        return None
    
    volumes = [c["volume"] for c in candles[-period:]]
    return sum(volumes) / period


def calculate_volume_spike_ratio(
    candles: List[Dict[str, Any]],
    current_volume: float,
    lookback: int = 20
) -> Optional[float]:
    """
    Calculate volume spike ratio (current volume / average volume).
    
    Args:
        candles: Historical candles
        current_volume: Current candle's volume
        lookback: Period for average calculation
        
    Returns:
        Volume spike ratio (e.g., 2.0 = 2x average) or None
    """
    avg_volume = calculate_volume_average(candles, lookback)
    
    if not avg_volume or avg_volume == 0:
        return None
    
    return current_volume / avg_volume


def is_high_volatility(candles: List[Dict[str, Any]], atr_period: int = 14, threshold_multiplier: float = 1.5) -> bool:
    """
    Check if market is in high volatility state.
    
    Args:
        candles: List of candles
        atr_period: ATR calculation period
        threshold_multiplier: ATR multiplier for high volatility
        
    Returns:
        True if high volatility
    """
    if len(candles) < atr_period * 2:
        return False
    
    current_atr = calculate_atr(candles, atr_period)
    historical_atr = calculate_atr(candles[:-atr_period], atr_period)
    
    if not current_atr or not historical_atr:
        return False
    
    return current_atr > (historical_atr * threshold_multiplier)


def is_low_volatility(candles: List[Dict[str, Any]], atr_period: int = 14, threshold_multiplier: float = 0.7) -> bool:
    """
    Check if market is in low volatility state (dead/ranging).
    
    Args:
        candles: List of candles
        atr_period: ATR calculation period
        threshold_multiplier: ATR multiplier for low volatility
        
    Returns:
        True if low volatility
    """
    if len(candles) < atr_period * 2:
        return False
    
    current_atr = calculate_atr(candles, atr_period)
    historical_atr = calculate_atr(candles[:-atr_period], atr_period)
    
    if not current_atr or not historical_atr:
        return False
    
    return current_atr < (historical_atr * threshold_multiplier)


__all__ = [
    "calculate_atr",
    "calculate_ema",
    "calculate_rsi",
    "calculate_volume_average",
    "calculate_volume_spike_ratio",
    "is_high_volatility",
    "is_low_volatility",
]
