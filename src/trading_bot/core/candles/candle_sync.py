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
    • Reverse recovery handles WS → DB synchronization
    """

    def __init__(self, symbol: str, tf: str, rest_client, storage, cm, validator, tf_sec: int):
        self.symbol = symbol
        self.tf = tf
        self.rest = rest_client
        self.storage = storage
        self.cm = cm
        self.validator = validator  # Optional - WS already normalizes
        self.logger = get_logger(__name__)
        self.tf_sec = tf_sec
        self.last_open_ts: Optional[int] = None

    def seed_with_timestamp(self, ts: int):
        """
        Initialize last_open_ts with the timestamp from bootstrap.
        This prevents gap detection on the first WebSocket candle.
        """
        self.last_open_ts = ts
        self.logger.info(f"[CandleSync] {self.symbol} {self.tf} seeded with ts={ts}")

    # -------------------------------------------------------------
    # WS Candle Handler
    # -------------------------------------------------------------
    def handle_ws_candle(self, raw: Dict[str, Any]):
        """
        raw → parsed WS structure:
        {
            "ts": open_timestamp,
            "open": ...,
            "high": ...,
            "low": ...,
            "close": ...,
            "volume": ...,
            "closed": True|False
        }
        """
        # Validator is optional - WebSocketClient already normalizes data
        if self.validator:
            c = self.validator.normalize_ws(self.symbol, self.tf, raw)
            if not c:
                return
        else:
            c = raw  # Use already-normalized data from WebSocket

        # Debug: Log incoming timestamps for 1h timeframe
        if self.tf == "1h":
            self.logger.debug(
                f"[CandleSync] {self.symbol} {self.tf} received WS candle: "
                f"ts={c['ts']}, open_ts={c.get('open_ts')}, close_ts={c.get('close_ts')}"
            )

        last_ts = self.cm.last_ts()

        # First candle ever?
        if last_ts is None:
            self.cm.add_closed_candle(c)
            # WebSocket normalized format doesn't have 'closed' field - all WS candles are closed
            self.storage.save_candle_async(self.symbol, self.tf, c)
            return

        # -------------------------------------------------------------
        # Case A: Same timestamp (WS overwriting last candle)
        # -------------------------------------------------------------
        if c["ts"] == last_ts:
            self.cm.overwrite_last(c)
            self.storage.save_candle_async(self.symbol, self.tf, c)
            return

        # -------------------------------------------------------------
        # Case B: Proper next candle → append
        # -------------------------------------------------------------
        if c["ts"] == last_ts + self.tf_sec:
            self.cm.add_closed_candle(c)
            self.storage.save_candle_async(self.symbol, self.tf, c)
            return

        # -------------------------------------------------------------
        # Case C: Gap detected → reverse recovery
        # -------------------------------------------------------------
        if c["ts"] > last_ts + self.tf_sec:
            self.logger.warning(
                f"[CandleSync] Gap detected {self.symbol} {self.tf}: "
                f"{last_ts} → {c['ts']}."
            )
            self.reverse_recovery(c["ts"])

        # After recovery, append this candle
        self.cm.add_closed_candle(c)
        self.storage.save_candle_async(self.symbol, self.tf, c)

    # -------------------------------------------------------------
    # Reverse Recovery — Missing Candle Fix
    # -------------------------------------------------------------
    def reverse_recovery(self, target_ts: int):
        """
        Fetch missing candles from REST and append them
        until memory TS reaches target_ts.
        """
        last_ts = self.cm.last_ts()
        missing = self.rest.fetch_candles_between(self.symbol, self.tf, last_ts, target_ts)

        if not missing:
            self.logger.warning(f"[CandleSync] No missing candles found for reverse recovery")
            return

        missing.sort(key=lambda x: x["ts"])

        for c in missing:
            # ------------------------------------------------------
            # FIX: DROP-UNTIL SAFETY
            # Only drop if the incoming candle is strictly newer.
            # Prevents deleting last candle after restart.
            # ------------------------------------------------------
            if c["ts"] > self.cm.last_ts():
                self.cm.drop_until(c["ts"])

            self.cm.add_closed_candle(c)
            self.storage.save_candle_async(self.symbol, self.tf, c)

        self.logger.info(
            f"[CandleSync] Reverse recovery added {len(missing)} candles for "
            f"{self.symbol} {self.tf}"
        )


__all__ = ["CandleSync"]
