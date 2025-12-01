"""
InitialCandlesLoader:
- Loads historical candles from REST
- Injects them into CandleManager
- Updates CandleSync last timestamp
- Builds initial LiquidityMap (optional)
"""

from typing import Dict, List, Any
from trading_bot.core.logger import get_logger
from trading_bot.config.symbols.models import SymbolConfig, TimeframeConfig
from trading_bot.config.app.models import AppConfig
from trading_bot.api.rest_client import RestClient


class InitialCandlesLoader:
    """Proper initial loader with CandleManager + CandleSync integration."""

    def __init__(self, app_config: AppConfig, rest_client: RestClient):
        """
        Args:
            app_config: Application configuration
            rest_client: RestClient instance for fetching candles
        """
        self.logger = get_logger(__name__)
        self.app_config = app_config
        self.rest_client = rest_client

    # -------------------------------------------------------------------------
    # MAIN ENTRY POINT
    # -------------------------------------------------------------------------
    def load_initial_for_symbol(
        self,
        symbol_cfg: SymbolConfig,
        candle_managers: Dict[str, Any],    # {"1m": CandleManager, ...}
        candle_syncs: Dict[str, Any],       # {"1m": CandleSync, ...}
        liquidity_maps: Dict[str, Any] = None  # Dict of LiquidityMap per TF
    ) -> bool:
        """
        Loads and seeds all structures for a single symbol.

        Args:
            symbol_cfg      → includes name + TF configs
            candle_managers → CandleManager per timeframe
            candle_syncs    → CandleSync per timeframe
            liquidity_maps  → Dict of LiquidityMap instances (one per TF)

        Returns:
            True when symbol is fully initialized
        """

        symbol = symbol_cfg.name
        self.logger.info(f"[InitialLoad] Start for {symbol}")

        for tf_cfg in symbol_cfg.timeframes:
            tf = tf_cfg.tf
            fetch = tf_cfg.fetch

            cm = candle_managers[tf]
            sync = candle_syncs[tf]

            # 1️⃣ Fetch REST candles
            candles = self._fetch_candles(symbol, tf_cfg)

            if not candles:
                self.logger.error(f"[InitialLoad] No candles loaded for {symbol} {tf}")
                continue

            # 2️⃣ Load into CandleManager
            cm.load_initial(candles)

            # 3️⃣ Set last closed timestamp for CandleSync
            last_ts = cm.last_timestamp()
            sync.set_initial_last_ts(last_ts)

            # 4️⃣ Build initial LiquidityMap for this timeframe
            if liquidity_maps is not None and tf in liquidity_maps:
                try:
                    # Get this timeframe's independent liquidity map
                    lm = liquidity_maps[tf]
                    current_price = candles[-1]["close"] if candles else 0
                    
                    lm.on_candle_close(
                        timeframe=tf,
                        candles=cm.get_all(),
                        current_price=current_price
                    )
                    
                    self.logger.info(
                        f"[InitialLoad] {symbol} {tf} LiquidityMap initialized"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[InitialLoad] LiquidityMap update failed for {symbol} {tf}: {e}"
                    )

            self.logger.info(
                f"[InitialLoad] {symbol} {tf}: loaded={len(candles)} last_ts={last_ts}"
            )

        self.logger.info(f"[InitialLoad] Completed for {symbol}")
        return True

    # -------------------------------------------------------------------------
    # REST FETCHER (Using RestClient)
    # -------------------------------------------------------------------------
    def _fetch_candles(self, symbol: str, tf_cfg: TimeframeConfig) -> List[Dict[str, Any]]:
        """
        Fetch candles from Binance REST using RestClient.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            tf_cfg: Timeframe configuration with tf and fetch limit

        Returns:
            List of normalized candle dictionaries
        """
        # Calculate end_time to ensure we only fetch closed candles
        # Get current time and round down to last closed candle for this timeframe
        import time
        from datetime import datetime
        current_time = int(time.time())
        tf_seconds = self.rest_client.app_config.get_timeframe_seconds(tf_cfg.tf)
        
        # Round down to last complete candle close time, then subtract 1ms
        # to ensure we don't fetch the current incomplete candle
        last_closed_candle = ((current_time - 1) // tf_seconds) * tf_seconds
        end_time_ms = (last_closed_candle - 1) * 1000  # Subtract 1 second to be safe
        
        self.logger.info(
            f"[InitialLoad] Fetching {tf_cfg.fetch} candles for {symbol} {tf_cfg.tf} "
            f"(end_time={datetime.fromtimestamp(last_closed_candle - 1)})"
        )

        candles = self.rest_client.fetch_klines(
            symbol=symbol,
            timeframe=tf_cfg.tf,
            limit=tf_cfg.fetch,
            end_time=end_time_ms
        )

        if not candles:
            self.logger.error(
                f"[InitialLoad] Failed to fetch candles for {symbol} {tf_cfg.tf}"
            )
            return []

        self.logger.info(
            f"[InitialLoad] Successfully fetched {len(candles)} candles for {symbol} {tf_cfg.tf}"
        )
        return candles


__all__ = ["InitialCandlesLoader"]
