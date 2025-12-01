from typing import Optional, Dict, Any, Callable
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class CandleSync:
    """
    Ensures that incoming closed candles are:
    - Strictly sequential in timestamp
    - Missing gaps are filled via REST
    - No duplicates
    - Delivered to CandleManager + MultiTFSymbolManager correctly

    WS → CandleSync → (validated) → CandleManager → LiquidityMap → Fusion → Decision Engine
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
        self.step = app_config.get_timeframe_seconds(timeframe)

        self.last_closed_ts: Optional[int] = None
        self.callback: Optional[Callable[[str, Dict[str, Any]], Any]] = None

    # ---------------------------------------------------------
    # SETTERS
    # ---------------------------------------------------------
    def set_initial_last_ts(self, ts: Optional[int]):
        """Set last known candle during initial load."""
        self.last_closed_ts = ts
        self.logger.info(
            f"[CandleSync] {self.symbol} {self.tf} initial last_ts={ts}"
        )

    def set_callback(self, fn: Callable[[str, Dict[str, Any]], Any]):
        """Callback to MultiTFSymbolManager.on_valid_candle(tf, candle)."""
        self.callback = fn

    # ---------------------------------------------------------
    # MAIN ENTRY: CLOSED CANDLE FROM WEBSOCKET
    # ---------------------------------------------------------
    def on_ws_closed_candle(self, candle: Dict[str, Any]):
        """
        Handles incoming closed candles from WS stream:
        - validates timestamp order
        - fetches missing candles
        - pushes valid candles to CandleManager
        - notifies callback
        """
        incoming_ts = candle["ts"]

        # Case 1: First candle ever
        if self.last_closed_ts is None:
            self._accept(candle)
            return

        expected_ts = self.last_closed_ts + self.step

        # Case 2: Duplicate or old candle
        if incoming_ts <= self.last_closed_ts:
            self.logger.debug(
                f"[CandleSync] Skip duplicate/out-of-order: {self.symbol} {self.tf} incoming_ts={incoming_ts}"
            )
            return

        # Case 3: Exact next candle → perfect
        if incoming_ts == expected_ts:
            self._accept(candle)
            return

        # Case 4: Missing candles detected → fetch from REST
        if incoming_ts > expected_ts:
            self._fill_missing(expected_ts, incoming_ts)
            self._accept(candle)
            return

    # ---------------------------------------------------------
    # ACCEPT VALID CANDLE
    # ---------------------------------------------------------
    def _accept(self, candle: Dict[str, Any]):
        """Accept a validated closed candle, push to CandleManager, call callback."""

        self.cm.add_closed_candle(candle)
        self.last_closed_ts = candle["ts"]

        self.logger.info(
            f"[CandleSync] ACCEPTED {self.symbol} {self.tf} ts={candle['ts']} close={candle['close']}"
        )

        # Notify the MultiTFSymbolManager that a valid closed candle is ready
        if self.callback:
            self.callback(self.tf, candle)

    # ---------------------------------------------------------
    # MISSING CANDLES HANDLING (REST BACKFILL)
    # ---------------------------------------------------------
    def _fill_missing(self, start_ts: int, incoming_ts: int):
        """
        If the expected timestamp is less than the incoming timestamp,
        it means we missed one or more WS candles.
        """
        self.logger.warning(
            f"[CandleSync] Missing candles for {self.symbol} {self.tf} "
            f"expected from {start_ts} to {incoming_ts - self.step}"
        )

        cur_ts = start_ts

        while cur_ts < incoming_ts:
            # Fetch exact candle by timestamp
            missing_candle = self.rest.fetch_kline_exact(
                self.symbol, self.tf, cur_ts
            )

            if not missing_candle:
                # If exact match fails, try smaller window fetch
                self.logger.warning(
                    f"[CandleSync] Could not fetch exact candle ts={cur_ts}. "
                    f"Trying fallback batch..."
                )
                batch = self.rest.fetch_klines(
                    self.symbol, self.tf, limit=10
                )
                matched = next(
                    (x for x in batch if x["ts"] == cur_ts), None
                )

                if matched:
                    missing_candle = matched
                else:
                    self.logger.error(
                        f"[CandleSync] Missing candle ts={cur_ts} NOT RECOVERED."
                    )
                    break  # avoid infinite loop

            self.logger.info(
                f"[CandleSync] RECOVERED missing candle ts={cur_ts}"
            )
            self._accept(missing_candle)

            cur_ts += self.step
