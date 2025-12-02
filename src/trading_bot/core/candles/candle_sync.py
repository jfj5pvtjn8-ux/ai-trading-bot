# =====================================================================================
# CandleSync with Optimized Batch Gap Filling
# =====================================================================================

from typing import Optional, Dict, Any, Callable
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class CandleSync:
    """
    Ensures closed candles are:
      - Sequential
      - Using open timestamps (ts divisible by timeframe)
      - Gap-filled efficiently using batch REST calls
      - Forwarded to CandleManager + Strategy Layer
    """

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

        # TF step in seconds
        self.step = self.app_config.get_timeframe_seconds(timeframe)

        # Last known OPEN timestamp
        self.last_open_ts: Optional[int] = None

        # callback(symbol, timeframe, candle)
        self.callback: Optional[Callable[[str, str, Dict[str, Any]], Any]] = None

    # -------------------------------------------------------------------------
    # SETUP
    # -------------------------------------------------------------------------

    def set_initial_last_ts(self, last_ts: Optional[int]):
        """Set the last known open timestamp from initial REST load."""
        self.last_open_ts = last_ts
        self.logger.info(f"[CandleSync] Seed ts={last_ts} for {self.symbol} {self.tf}")

    def set_callback(self, fn):
        self.callback = fn

    # -------------------------------------------------------------------------
    # ENTRY: Closed candle from WebSocket
    # -------------------------------------------------------------------------

    def on_ws_closed_candle(self, candle: Dict[str, Any], storage=None):
        """
        WS candle dict MUST contain: {"open_ts": int, ...}
        """

        incoming_open_ts = candle["open_ts"]

        # First candle ever
        if self.last_open_ts is None:
            self._accept(candle, storage)
            return

        expected = self.last_open_ts + self.step

        # Duplicate / out-of-order
        if incoming_open_ts <= self.last_open_ts:
            return

        # Perfect next candle
        if incoming_open_ts == expected:
            self._accept(candle, storage)
            return

        # Missing gap(s) → batch recovery
        if incoming_open_ts > expected:
            self._fill_missing_batch(expected, incoming_open_ts, storage)
            self._accept(candle, storage)
            return

    # -------------------------------------------------------------------------
    # ACCEPT A SINGLE CANDLE
    # -------------------------------------------------------------------------

    def _accept(self, candle: Dict[str, Any], storage=None):
        self.cm.add_closed_candle(candle)
        self.last_open_ts = candle["open_ts"]

        if storage:
            storage.save_candle_async(self.symbol, self.tf, candle)

        if self.callback:
            try:
                self.callback(self.symbol, self.tf, candle)
            except Exception as e:
                self.logger.error(f"CandleSync callback error {self.symbol} {self.tf}: {e}")

    # -------------------------------------------------------------------------
    # BATCH GAP FILLER (Optimized — PRODUCTION READY)
    # -------------------------------------------------------------------------

    def _fill_missing_batch(self, start_ts: int, incoming_ts: int, storage=None):
        """
        Fill missing candles between start_ts (expected) and incoming_ts (WS open_ts).
        
        Example:
            expected=10:00 open_ts
            incoming=10:07 open_ts
            Missing: 10:01 ... 10:06

        Fetch ALL missing candles in ONE REST CALL.
        """

        missing_count = (incoming_ts - start_ts) // self.step

        self.logger.warning(
            f"[CandleSync] Missing {missing_count} candles for "
            f"{self.symbol} {self.tf} ({start_ts} → {incoming_ts - self.step})"
        )

        # REST batch fetch
        batch = self.rest.fetch_klines(
            symbol=self.symbol,
            timeframe=self.tf,
            limit=missing_count,
            start_time=start_ts * 1000,
            end_time=(incoming_ts * 1000),
        )

        if not batch:
            self.logger.error(
                f"[CandleSync] REST returned empty batch for {self.symbol} {self.tf}"
            )
            return

        # Build lookup map by open timestamp
        lookup = {c["open_ts"]: c for c in batch}

        # Sequential acceptance
        ts = start_ts
        recovered = 0

        while ts < incoming_ts:
            c = lookup.get(ts)

            if not c:
                # REST missed something — rare but possible
                self.logger.error(
                    f"[CandleSync] Missing candle {ts} even after batch fetch"
                )
            else:
                self._accept(c, storage)
                recovered += 1

            ts += self.step

        self.logger.info(
            f"[CandleSync] Batch gap recovery complete for "
            f"{self.symbol} {self.tf}: recovered={recovered}/{missing_count}"
        )
