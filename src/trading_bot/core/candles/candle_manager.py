from __future__ import annotations

from collections import deque
from typing import Deque, List, Dict, Any, Optional, Tuple

Candle = Dict[str, Any]


class CandleManager:
    """
    High-performance sliding-window candle storage (one CM per symbol–timeframe).

    Features:
    ----------
    • O(1) append via deque(maxlen)
    • Strictly increasing OPEN timestamp enforcement
    • Binary-search O(log N) candle lookup by timestamp
    • Rich helpers for LiquidityMap / strategy engines
    • Backfill utilities (reverse gap cleanup)
    • Optional timeframe integrity diagnostics (non-blocking)

    Primary timestamp field:
        ts = candle open timestamp (seconds)
    """

    # =====================================================================
    # INIT
    # =====================================================================

    def __init__(self, max_size: int, timeframe_seconds: Optional[int] = None):
        """
        Args:
            max_size: Maximum candles to keep in memory.
            timeframe_seconds: Optional, used for optional gap diagnostics.
        """
        self._max_size = max_size
        self._tf_sec = timeframe_seconds
        self._candles: Deque[Candle] = deque(maxlen=max_size)

    # =====================================================================
    # INTERNAL HELPERS
    # =====================================================================

    @staticmethod
    def _get_ts(c: Candle) -> int:
        """Return candle open timestamp (ts > open_ts)."""
        if "ts" in c and c["ts"] is not None:
            return int(c["ts"])
        if "open_ts" in c and c["open_ts"] is not None:
            return int(c["open_ts"])
        raise KeyError(f"Candle missing 'ts'/'open_ts': {c}")

    @staticmethod
    def _ensure_ts(c: Candle) -> Candle:
        """Ensure candle has canonical ts field."""
        if "ts" in c:
            return c
        if "open_ts" in c:
            cpy = dict(c)
            cpy["ts"] = int(c["open_ts"])
            return cpy
        raise KeyError("Candle missing both ts and open_ts")

    # =====================================================================
    # INITIAL LOAD
    # =====================================================================

    def load_initial(self, candles: List[Candle]) -> None:
        """
        Load historical candles (REST). Clears previous data.

        • Sorts by open timestamp ascending
        • Normalizes ts / open_ts
        • Trims to last max_size candles
        """
        self._candles.clear()

        if not candles:
            return

        normalized = [self._ensure_ts(c) for c in candles]
        normalized.sort(key=self._get_ts)

        if len(normalized) > self._max_size:
            normalized = normalized[-self._max_size:]

        self._candles.extend(normalized)

    # =====================================================================
    # REAL-TIME APPEND
    # =====================================================================

    def add_closed_candle(self, candle: Candle) -> None:
        """
        Add new closed candle (WS).  
        Enforces strictly increasing ts.

        Out-of-order or duplicate candles are silently ignored.
        """
        c = self._ensure_ts(candle)
        new_ts = self._get_ts(c)

        if self._candles:
            last_ts = self._get_ts(self._candles[-1])
            if new_ts <= last_ts:
                return  # ignore duplicate or older

        self._candles.append(c)

    # =====================================================================
    # BASIC GETTERS
    # =====================================================================

    def get_all(self) -> List[Candle]:
        return list(self._candles)

    def get_window(self) -> Deque[Candle]:
        """Returns the live deque — treat as read-only."""
        return self._candles

    def __len__(self) -> int:
        return len(self._candles)

    def __bool__(self) -> bool:
        return bool(self._candles)

    # ---------------------------------------------------------------------

    def last_timestamp(self) -> Optional[int]:
        if not self._candles:
            return None
        return self._get_ts(self._candles[-1])

    def first_timestamp(self) -> Optional[int]:
        if not self._candles:
            return None
        return self._get_ts(self._candles[0])

    def last_open_time(self) -> Optional[int]:
        """Alias for clarity."""
        return self.last_timestamp()

    def get_latest_candle(self) -> Optional[Candle]:
        if not self._candles:
            return None
        return self._candles[-1]

    # =====================================================================
    # LOOKUPS (Binary Search)
    # =====================================================================

    def get_by_timestamp(self, ts: int) -> Optional[Candle]:
        """
        Binary search inside deque.
        O(log N), highly efficient for windows up to 10k+.
        """
        ts = int(ts)
        if not self._candles:
            return None

        lo, hi = 0, len(self._candles) - 1
        while lo <= hi:
            m = (lo + hi) // 2
            mid_ts = self._get_ts(self._candles[m])

            if mid_ts == ts:
                return self._candles[m]
            if mid_ts < ts:
                lo = m + 1
            else:
                hi = m - 1

        return None

    def contains_timestamp(self, ts: int) -> bool:
        """Fast existence check."""
        return self.get_by_timestamp(ts) is not None

    # =====================================================================
    # RANGE / STRUCTURAL HELPERS
    # =====================================================================

    def last_n(self, n: int) -> List[Candle]:
        """Return last N candles as list."""
        size = len(self._candles)
        if n >= size:
            return list(self._candles)
        start = size - n
        return [self._candles[i] for i in range(start, size)]

    def highest_in_last(self, n: int) -> Optional[float]:
        win = self.last_n(n)
        if not win:
            return None
        return max(float(c["high"]) for c in win)

    def lowest_in_last(self, n: int) -> Optional[float]:
        win = self.last_n(n)
        if not win:
            return None
        return min(float(c["low"]) for c in win)

    def last_two(self) -> Tuple[Optional[Candle], Optional[Candle]]:
        if len(self._candles) == 0:
            return None, None
        if len(self._candles) == 1:
            return None, self._candles[-1]
        return self._candles[-2], self._candles[-1]

    # =====================================================================
    # ADVANCED UTILITIES (Optional but useful)
    # =====================================================================

    def drop_until(self, ts: int) -> None:
        """
        Remove candles until next >= ts.
        Used during reverse-sync on bot restart if needed.
        """
        ts = int(ts)
        while self._candles and self._get_ts(self._candles[0]) < ts:
            self._candles.popleft()

    def has_gap(self) -> bool:
        """
        Optional diagnostic:
        Check if internal deque contains gaps.

        Not enforced — CandleSync owns gap logic.
        """
        if len(self._candles) < 2 or self._tf_sec is None:
            return False

        expected = self._get_ts(self._candles[0])
        for c in self._candles:
            ts = self._get_ts(c)
            if ts != expected:
                return True
            expected += self._tf_sec

        return False


__all__ = ["CandleManager"]
