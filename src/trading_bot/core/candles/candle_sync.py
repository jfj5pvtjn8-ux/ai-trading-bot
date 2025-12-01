from typing import Optional, Dict, Any, Callable
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class CandleSync:
    """
    Ensures closed candles are:
      - Sequential
      - Gap-filled when needed
      - Using OPEN TIMESTAMPS (t divisible by timeframe)
      - Synchronized across TFs
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

        # timeframe step in seconds
        self.step = self.app_config.get_timeframe_seconds(timeframe)

        # We store OPEN timestamp, not CLOSE timestamp.
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

    def set_callback(self, fn: Callable[[str, str, Dict[str, Any]], Any]):
        self.callback = fn

    # -------------------------------------------------------------------------
    # ENTRY: CLOSED CANDLE FROM WEBSOCKET
    # -------------------------------------------------------------------------

    def on_ws_closed_candle(self, candle: Dict[str, Any], storage=None):
        """
        WS candle dict MUST contain:
            {"open_ts": int, ...}
        Not close timestamp!
        """

        incoming_open_ts = candle["open_ts"]

        # First candle ever
        if self.last_open_ts is None:
            self._accept(candle, storage)
            return

        expected = self.last_open_ts + self.step

        # Duplicate / old
        if incoming_open_ts <= self.last_open_ts:
            return

        # Perfect next candle
        if incoming_open_ts == expected:
            self._accept(candle, storage)
            return

        # Missing gaps — fetch via REST
        if incoming_open_ts > expected:
            self._fill_missing(expected, incoming_open_ts)
            self._accept(candle, storage)
            return

    # -------------------------------------------------------------------------
    # ACCEPT VALID CANDLE
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
                self.logger.error(f"Candle callback error {self.symbol} {self.tf}: {e}")

    # -------------------------------------------------------------------------
    # GAP FILLING
    # -------------------------------------------------------------------------

    def _fill_missing(self, start_ts: int, incoming_ts: int):
        """
        If we expected ts=X but WS gives ts=Y, fetch missing candles:
        X, X+step, X+2step ... until Y-step.
        """
        self.logger.warning(
            f"[CandleSync] Missing candles for {self.symbol} {self.tf} "
            f"from {start_ts} to {incoming_ts - self.step}"
        )

        ts = start_ts

        while ts < incoming_ts:
            missing = self._fetch_exact_open_candle(ts)

            if not missing:
                self.logger.error(
                    f"[CandleSync] Could not recover missing candle @ {ts}"
                )
                ts += self.step
                continue

            self._accept(missing)
            ts += self.step

    # -------------------------------------------------------------------------
    # REST HELPERS
    # -------------------------------------------------------------------------

    def _fetch_exact_open_candle(self, open_ts: int) -> Optional[Dict[str, Any]]:
        """
        Fetch EXACT candle matching open timestamp.

        - Start time = open_ts * 1000
        - End time   = (open_ts + step) * 1000
        """

        batch = self.rest.fetch_klines(
            symbol=self.symbol,
            timeframe=self.tf,
            limit=2,
            start_time=open_ts * 1000,
            end_time=(open_ts + self.step) * 1000,
        )

        if not batch:
            return None

        # Find candle by open timestamp match
        for c in batch:
            if c["open_ts"] == open_ts:
                return c

        return None
