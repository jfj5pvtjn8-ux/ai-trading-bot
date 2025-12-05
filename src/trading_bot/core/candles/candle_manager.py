from __future__ import annotations

from collections import deque
from typing import Deque, List, Dict, Any, Optional

Candle = Dict[str, Any]


class CandleManager:
    """
    High-performance sliding-window candle store for each symbol+timeframe.

    Responsibilities:
    -----------------
    • Maintain strictly increasing OPEN_TS sequence
    • Provide O(1) append operations
    • Offer "overwrite_last" for WS candle updates
    • Perform safe drop/remove operations for reverse recovery
    • Enable fast access for LM, Trend, and bot logic
    """

    def __init__(self, symbol: str, tf: str, maxlen: int):
        self.symbol = symbol
        self.tf = tf
        self._candles: Deque[Candle] = deque(maxlen=maxlen)
        self._tf_sec: Optional[int] = None  # set later via setter

    # ------------------------------------------------------------------
    # SETUP
    # ------------------------------------------------------------------
    def set_tf_seconds(self, tf_sec: int):
        self._tf_sec = tf_sec

    # ------------------------------------------------------------------
    # INTROSPECTION
    # ------------------------------------------------------------------
    def last_ts(self) -> Optional[int]:
        """Return the OPEN timestamp of the last candle."""
        if not self._candles:
            return None
        return self._candles[-1]["ts"]

    def first_ts(self) -> Optional[int]:
        if not self._candles:
            return None
        return self._candles[0]["ts"]

    def size(self) -> int:
        return len(self._candles)

    def get_all(self) -> List[Candle]:
        return list(self._candles)

    # ------------------------------------------------------------------
    # LOAD INITIAL LIST
    # ------------------------------------------------------------------
    def load_from_list(self, candles: List[Candle]):
        """
        Load an entire list (used on startup from DuckDB).
        Ensures strict timestamp ordering.
        """
        self._candles.clear()

        last_ts = None
        for c in candles:
            ts = c["ts"]
            if last_ts is not None and ts <= last_ts:
                raise ValueError(
                    f"Candle sequence error for {self.symbol} {self.tf}: "
                    f"{ts} not greater than {last_ts}"
                )
            self._candles.append(c)
            last_ts = ts

    # ------------------------------------------------------------------
    # ADD CLOSED CANDLE
    # ------------------------------------------------------------------
    def add_closed_candle(self, c: Candle):
        """
        Append a fully closed candle.
        Ensures strict timestamp progression.
        """
        ts = c["ts"]
        last_ts = self.last_ts()

        # First ever candle
        if last_ts is None:
            self._candles.append(c)
            return

        # Normal case
        if self._tf_sec is not None:
            expected = last_ts + self._tf_sec
            if ts != expected:
                raise ValueError(
                    f"[CandleManager] Timestamp gap for {self.symbol} {self.tf}: "
                    f"got {ts}, expected {expected}"
                )

        self._candles.append(c)

    # ------------------------------------------------------------------
    # OVERWRITE LAST (for websocket "kline update")
    # ------------------------------------------------------------------
    def overwrite_last(self, c: Candle):
        """
        Replace last candle only if timestamps match.
        """
        if not self._candles:
            raise RuntimeError("overwrite_last called with empty deque")

        if c["ts"] != self._candles[-1]["ts"]:
            raise ValueError(
                f"[CandleManager] overwrite_last mismatch: incoming ts={c['ts']} "
                f"does not match last ts={self._candles[-1]['ts']}"
            )

        self._candles[-1] = c

    # ------------------------------------------------------------------
    # DROP UNTIL
    # ------------------------------------------------------------------
    def drop_until(self, target_ts: int):
        """
        Remove all candles with ts >= target_ts.
        Used during reverse recovery or replacing last candle after restart.
        """
        while self._candles and self._candles[-1]["ts"] >= target_ts:
            self._candles.pop()

    # ------------------------------------------------------------------
    # GAP DETECTION
    # ------------------------------------------------------------------
    def has_gap(self) -> bool:
        """
        Returns True if the stored candles contain timestamp gaps.
        """
        if len(self._candles) < 2 or self._tf_sec is None:
            return False

        expected = self._candles[0]["ts"]

        for c in self._candles:
            ts = c["ts"]
            if ts != expected:
                return True
            expected += self._tf_sec

        return False


__all__ = ["CandleManager"]
