from collections import deque
from typing import Deque, List, Dict, Any, Optional, Tuple

Candle = Dict[str, Any]


class CandleManager:
    """
    Efficient sliding-window storage for Multi-Timeframe candles.

    Assumptions about candle dict format:
      - 'ts'      → OPEN timestamp in seconds (same as 'open_ts')
      - 'open_ts' → OPEN timestamp in seconds (optional alias, but recommended)
      - 'close_ts'→ CLOSE timestamp in seconds (optional)
      - 'open', 'high', 'low', 'close', 'volume' present

    This matches both RestClient and WebSocketClient normalization.
    """

    def __init__(self, max_size: int):
        """
        Args:
            max_size: Maximum number of candles to store (from timeframe config)
        """
        self._max_size: int = max_size
        self._candles: Deque[Candle] = deque(maxlen=self._max_size)

    # ---------------------------------------------------------
    # INITIAL LOAD
    # ---------------------------------------------------------

    def load_initial(self, candles: List[Candle]) -> None:
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

    def add_closed_candle(self, candle: Candle) -> None:
        """
        Add a new closed candle, ensuring proper timestamp order.
        Should ONLY be called by CandleSync or Initial Loader.

        Uses candle['ts'] as the master time index (open timestamp).
        """
        if not self._candles:
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

    def get_all(self) -> List[Candle]:
        """Returns all candles as a list (copy)."""
        return list(self._candles)

    def get_window(self) -> Deque[Candle]:
        """
        Returns the underlying deque WITHOUT copying.

        NOTE: This is a live view. Callers SHOULD treat it as read-only.
        Mutating it directly can break CandleSync / LiquidityMap assumptions.
        """
        return self._candles

    def last_timestamp(self) -> Optional[int]:
        """
        Returns timestamp of latest closed candle (OPEN time in seconds).

        This is the same as candle['ts'] and, by convention, candle['open_ts'].
        """
        if not self._candles:
            return None
        return int(self._candles[-1]["ts"])

    def last_open_time(self) -> Optional[int]:
        """
        Alias for last_timestamp() to make intent explicit:
        last OPEN candle time (seconds).

        InitialCandlesLoader / CandleSync can call either.
        """
        return self.last_timestamp()

    def get_latest_candle(self) -> Optional[Candle]:
        if not self._candles:
            return None
        return self._candles[-1]

    def get_latest_close(self) -> Optional[float]:
        if not self._candles:
            return None
        return float(self._candles[-1]["close"])

    def get_latest_high(self) -> Optional[float]:
        if not self._candles:
            return None
        return float(self._candles[-1]["high"])

    def get_latest_low(self) -> Optional[float]:
        if not self._candles:
            return None
        return float(self._candles[-1]["low"])

    # ---------------------------------------------------------
    # UTILITY HELPERS FOR LIQUIDITY MAP
    # ---------------------------------------------------------

    def last_n(self, n: int) -> List[Candle]:
        """Return last N candles (default dictionary objects)."""
        if n >= len(self._candles):
            return list(self._candles)
        # slicing a list copy is OK; deque slicing is not supported
        return list(self._candles)[-n:]

    def highest_in_last(self, n: int) -> Optional[float]:
        """
        Highest high in last N candles.

        Returns None if there are no candles.
        """
        window = self.last_n(n)
        if not window:
            return None
        return max(float(c["high"]) for c in window)

    def lowest_in_last(self, n: int) -> Optional[float]:
        """
        Lowest low in last N candles.

        Returns None if there are no candles.
        """
        window = self.last_n(n)
        if not window:
            return None
        return min(float(c["low"]) for c in window)

    def last_two(self) -> Tuple[Optional[Candle], Optional[Candle]]:
        """
        Returns last two candles (useful for CHOCH/BOS, sweeps).

        Returns:
            (prev_candle, last_candle)
            If there are fewer than 2 candles, one or both will be None.
        """
        if len(self._candles) == 0:
            return None, None
        if len(self._candles) == 1:
            return None, self._candles[-1]
        return self._candles[-2], self._candles[-1]

    def get_by_timestamp(self, ts: int) -> Optional[Candle]:
        """
        Return the candle whose ts (open timestamp) equals the given value.

        This is O(n) over the sliding window but cheap for typical sizes.
        """
        for c in reversed(self._candles):
            if int(c["ts"]) == int(ts):
                return c
        return None

    # ---------------------------------------------------------
    # DUNDER HELPERS
    # ---------------------------------------------------------

    def __len__(self) -> int:
        return len(self._candles)

    def __bool__(self) -> bool:
        return bool(self._candles)


__all__ = ["CandleManager"]
