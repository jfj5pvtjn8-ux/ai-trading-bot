from __future__ import annotations

from typing import Optional, Dict, Any, Callable, List
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class CandleSync:
    """
    PRO EDITION — Real-time candle synchronizer.

    Responsibilities:
    -----------------
    • Enforce strict OPEN timestamp sequencing
    • WS candles are validated, normalized, and gap-filled
    • Missing candles are fetched from REST (forward gaps)
    • Supports optional REVERSE gap recovery (bot restart)
    • Emits validated candles to CandleManager + callback
    • Handles storage layer safely (async)
    """

    # =====================================================================
    # INIT
    # =====================================================================

    def __init__(
        self,
        rest_client,
        symbol: str,
        timeframe: str,
        candle_manager,
        app_config: AppConfig,
    ):
        self.logger = get_logger(__name__)

        self.rest = rest_client
        self.symbol = symbol
        self.tf = timeframe
        self.cm = candle_manager
        self.app_config = app_config

        # interval seconds (1m=60, 5m=300...)
        self.step = self.app_config.get_timeframe_seconds(timeframe)

        # last known OPEN timestamp (seconds)
        self.last_open_ts: Optional[int] = None

        # callback(symbol, timeframe, candle)
        self.callback: Optional[Callable[[str, str, Dict[str, Any]], Any]] = None

    # =====================================================================
    # SETUP
    # =====================================================================

    def set_initial_last_ts(self, ts: Optional[int]) -> None:
        """Seed the last_open_ts (from REST initial load)."""
        self.last_open_ts = ts
        self.logger.info(f"[CandleSync] Seed last_open_ts={ts} for {self.symbol} {self.tf}")

    def set_callback(self, fn: Callable[[str, str, Dict[str, Any]], Any]) -> None:
        """User-defined validated-candle callback."""
        self.callback = fn

    # =====================================================================
    # ENTRYPOINT: WebSocket CLOSED candle
    # =====================================================================

    def on_ws_closed_candle(self, candle: Dict[str, Any], storage=None):
        """
        WS candle must contain:
            { "open_ts": int, ... }
        And may contain:
            { "ts": int } (optional)
        """
        try:
            c = self._normalize_ws_candle(candle)
        except Exception as e:
            self.logger.error(f"[CandleSync] Invalid WS candle for {self.symbol} {self.tf}: {e}")
            return

        incoming_ts = c["ts"]

        # First-ever event
        if self.last_open_ts is None:
            return self._accept(c, storage)

        # Duplicate / out-of-order
        if incoming_ts <= self.last_open_ts:
            return

        expected = self.last_open_ts + self.step

        # Perfect next candle
        if incoming_ts == expected:
            return self._accept(c, storage)

        # Forward gap
        if incoming_ts > expected:
            self._forward_fill_expected_gap(expected, incoming_ts, storage)
            return self._accept(c, storage)

    # =====================================================================
    # NORMALIZATION
    # =====================================================================

    def _normalize_ws_candle(self, c: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize candle dict to ensure 'ts' exists."""
        if "ts" not in c:
            if "open_ts" not in c:
                raise ValueError("WS candle missing both ts and open_ts")
            c = dict(c)
            c["ts"] = int(c["open_ts"])
        else:
            c["ts"] = int(c["ts"])
        return c

    # =====================================================================
    # ACCEPT VALID CANDLE
    # =====================================================================

    def _accept(self, candle: Dict[str, Any], storage=None) -> None:
        """Push candle to CM and propagate callback + storage."""
        self.cm.add_closed_candle(candle)
        self.last_open_ts = candle["ts"]

        if storage:
            try:
                storage.save_candle_async(self.symbol, self.tf, candle)
            except Exception as e:
                self.logger.error(f"[CandleSync] Storage error: {e}")

        if self.callback:
            try:
                self.callback(self.symbol, self.tf, candle)
            except Exception as e:
                self.logger.error(
                    f"[CandleSync] Callback error for {self.symbol} {self.tf}: {e}"
                )

    # =====================================================================
    # FORWARD GAP FILLING
    # =====================================================================

    def _forward_fill_expected_gap(
        self,
        start_ts: int,
        incoming_ts: int,
        storage=None,
    ):
        """
        Fill missing candles:
        For expected=start_ts until incoming_ts-step.
        """
        self.logger.warning(
            f"[CandleSync] GAP detected {self.symbol} {self.tf}: "
            f"expected={start_ts}, ws_ts={incoming_ts}"
        )

        ts = start_ts
        end_ts = incoming_ts

        # Example: expected=100, incoming=160
        # step=60 → fetch missing candles: 100,160-step=100,140 => only 100
        while ts < end_ts:
            missing = self._fetch_exact_open_candle(ts)

            if missing:
                self._accept(missing, storage)
            else:
                self.logger.error(
                    f"[CandleSync] Missing candle not found via REST @ ts={ts}"
                )

            ts += self.step

    # =====================================================================
    # REST SINGLE-CANDLE FETCH
    # =====================================================================

    def _fetch_exact_open_candle(self, open_ts: int) -> Optional[Dict[str, Any]]:
        """
        Fetch EXACT candle for open_ts using REST API.

        Request window:
            start = open_ts * 1000
            end   = (open_ts + step) * 1000
        """
        try:
            candles = self.rest.fetch_klines(
                symbol=self.symbol,
                timeframe=self.tf,
                limit=2,
                start_time=open_ts * 1000,
                end_time=(open_ts + self.step) * 1000,
            )
        except Exception as e:
            self.logger.error(
                f"[CandleSync] REST fetch error {self.symbol} {self.tf}: {e}"
            )
            return None

        if not candles:
            return None

        # find candle with exact open timestamp
        for c in candles:
            try:
                ts = c.get("ts") or c.get("open_ts")
                if int(ts) == open_ts:
                    # ensure ts field exists
                    if "ts" not in c:
                        c = dict(c)
                        c["ts"] = int(ts)
                    return c
            except Exception:
                pass

        return None

    # =====================================================================
    # OPTIONAL: REVERSE GAP RECOVERY (for bot restart)
    # =====================================================================

    def reverse_recovery(self, storage=None):
        """
        OPTIONAL — Call at bot startup after initial REST load.
        Ensures the CM window has no gaps by looking backwards.

        Logic:
        ------
        • Look at the earliest stored candle
        • If first timestamp > (last_timestamp - N*step)
          → fetch missing backwards using REST.

        This is used ONLY when running with large CM windows
        and for operators who want full historical continuity.
        """
        if len(self.cm) < 2 or self.cm.first_timestamp() is None:
            return

        # current earliest known candle
        first_ts = self.cm.first_timestamp()

        # we expect candles spaced by step:
        # e.g., if window is 1500 candles of 1m, earliest should be last_ts - 1499*60
        expected_ts = (
            self.last_open_ts - (len(self.cm) - 1) * self.step
            if self.last_open_ts is not None
            else None
        )

        if expected_ts is None:
            return

        if first_ts == expected_ts:
            return  # perfect, no reverse gaps

        if first_ts > expected_ts:
            missing_count = int((first_ts - expected_ts) / self.step)
            self.logger.warning(
                f"[CandleSync] Reverse gap detected {self.symbol} {self.tf}: "
                f"missing={missing_count}"
            )

            ts = expected_ts
            missing_candles: List[Dict[str, Any]] = []

            # Fetch backwards
            while ts < first_ts:
                c = self._fetch_exact_open_candle(ts)
                if c:
                    missing_candles.append(c)
                ts += self.step

            # Sort ascending & prepend
            missing_candles.sort(key=lambda x: x["ts"])
            for c in missing_candles:
                self.cm.drop_until(c["ts"])   # ensure no duplicates
                self.cm.add_closed_candle(c)
                if storage:
                    storage.save_candle_async(self.symbol, self.tf, c)

            if missing_candles:
                self.logger.info(
                    f"[CandleSync] Reverse recovery added {len(missing_candles)} candles"
                )


__all__ = ["CandleSync"]
