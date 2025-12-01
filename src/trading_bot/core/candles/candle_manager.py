from collections import deque
from typing import List, Dict, Any, Optional


class CandleManager:
    """
    Efficient sliding-window storage for Multi-Timeframe candles.
    Supports O(1) append/pop, timestamp ordering, last-candle lookup,
    and fast access for LiquidityMap and Feature extraction.
    """

    def __init__(self, max_size: int):
        """
        Args:
            max_size: Maximum number of candles to store (from timeframe config)
        """
        self._max_size = max_size
        self._candles: deque = deque(maxlen=self._max_size)

    # ---------------------------------------------------------
    # INITIAL LOAD
    # ---------------------------------------------------------
    def load_initial(self, candles: List[Dict[str, Any]]):
        """
        Loads initial historical candles into sliding window.

        - Assumes candles are ordered oldest → newest.
        - Used ONLY during Initial Load via REST.
        """
        for c in candles:
            self._candles.append(c)

    # ---------------------------------------------------------
    # REAL-TIME APPEND (via CandleSync)
    # ---------------------------------------------------------
    def add_closed_candle(self, candle: Dict[str, Any]):
        """
        Add a new closed candle, ensuring proper timestamp order.
        Should ONLY be called by CandleSync or Initial Loader.
        """
        if len(self._candles) == 0:
            self._candles.append(candle)
            return

        last_ts = self._candles[-1]["ts"]

        # Prevent duplicates, out-of-order data
        if candle["ts"] <= last_ts:
            return

        self._candles.append(candle)

    # ---------------------------------------------------------
    # BASIC GETTERS
    # ---------------------------------------------------------
    def get_all(self) -> List[Dict[str, Any]]:
        """Returns all candles as a list (copy)."""
        return list(self._candles)

    def get_window(self) -> deque:
        """Returns underlying deque WITHOUT copying (fast)."""
        return self._candles

    def last_timestamp(self) -> Optional[int]:
        """Returns timestamp of latest closed candle."""
        if not self._candles:
            return None
        return self._candles[-1]["ts"]

    def get_latest_candle(self) -> Optional[Dict[str, Any]]:
        if not self._candles:
            return None
        return self._candles[-1]

    def get_latest_close(self) -> Optional[float]:
        if not self._candles:
            return None
        return self._candles[-1]["close"]

    def get_latest_high(self) -> Optional[float]:
        if not self._candles:
            return None
        return self._candles[-1]["high"]

    def get_latest_low(self) -> Optional[float]:
        if not self._candles:
            return None
        return self._candles[-1]["low"]

    # ---------------------------------------------------------
    # UTILITY HELPERS FOR LIQUIDITY MAP
    # ---------------------------------------------------------
    def last_n(self, n: int) -> List[Dict[str, Any]]:
        """Return last N candles (default dictionary objects)."""
        if n >= len(self._candles):
            return list(self._candles)
        return list(self._candles)[-n:]

    def highest_in_last(self, n: int) -> float:
        """Highest high in last N candles."""
        window = self.last_n(n)
        return max(c["high"] for c in window)

    def lowest_in_last(self, n: int) -> float:
        """Lowest low in last N candles."""
        window = self.last_n(n)
        return min(c["low"] for c in window)

    def last_two(self):
        """Returns last two candles (useful for CHOCH/BOS, liquidity sweeps)."""
        if len(self._candles) < 2:
            return None, None
        return self._candles[-2], self._candles[-1]
