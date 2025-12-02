"""
InitialCandlesLoader:
- Loads historical candles from REST
- Injects them into CandleManager
- Updates CandleSync last timestamp (open_ts of last CLOSED candle)
- Optionally builds initial LiquidityMap
"""

import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from trading_bot.core.logger import get_logger
from trading_bot.config.symbols.models import SymbolConfig, TimeframeConfig
from trading_bot.config.app.models import AppConfig
from trading_bot.api.rest_client import RestClient
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync


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
        candle_managers: Dict[str, CandleManager],  # {"1m": CandleManager, ...}
        candle_syncs: Dict[str, CandleSync],        # {"1m": CandleSync, ...}
        liquidity_maps: Optional[Dict[str, Any]] = None,  # Dict of LiquidityMap per TF
        storage=None,  # ParquetStorage for async writes
    ) -> bool:
        """
        Loads and seeds all structures for a single symbol.

        Returns:
            True if at least one timeframe was successfully initialized,
            False if all timeframes failed.
        """
        symbol = symbol_cfg.name
        self.logger.info(f"[InitialLoad] Start for {symbol}")

        success_count = 0

        for tf_cfg in symbol_cfg.timeframes:
            tf = tf_cfg.tf

            cm = candle_managers.get(tf)
            sync = candle_syncs.get(tf)

            if cm is None or sync is None:
                self.logger.error(
                    f"[InitialLoad] Missing CandleManager or CandleSync for {symbol} {tf}"
                )
                continue

            # 1️⃣ Fetch REST candles (only closed, via end_time logic)
            candles = self._fetch_candles(symbol, tf_cfg)

            if not candles:
                self.logger.error(
                    f"[InitialLoad] No candles loaded for {symbol} {tf}"
                )
                continue

            # Simple alignment sanity check using open_ts/ts
            tf_seconds = self.rest_client.app_config.get_timeframe_seconds(tf)
            base_ts = candles[0]["open_ts"]
            aligned = [
                c for c in candles
                if (c["open_ts"] - base_ts) % tf_seconds == 0
            ]

            if len(aligned) != len(candles):
                self.logger.warning(
                    f"[InitialLoad] Alignment issue for {symbol} {tf}: "
                    f"{len(aligned)}/{len(candles)} candles aligned"
                )
            else:
                self.logger.debug(
                    f"[InitialLoad] Alignment OK for {symbol} {tf}: {len(aligned)} candles"
                )

            candles = aligned
            if not candles:
                self.logger.error(
                    f"[InitialLoad] All candles misaligned for {symbol} {tf}"
                )
                continue

            # 2️⃣ Load into CandleManager (oldest → newest)
            cm.load_initial(candles)

            # 3️⃣ Set last CLOSED candle OPEN timestamp for CandleSync
            last_ts = cm.last_open_time()
            sync.set_initial_last_ts(last_ts)

            # 4️⃣ Save to Parquet storage (async)
            if storage:
                try:
                    storage.save_candles_batch_async(symbol, tf, candles)
                    self.logger.debug(
                        f"[InitialLoad] Submitted {len(candles)} candles to storage "
                        f"for {symbol} {tf}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[InitialLoad] Storage save_candles_batch_async failed for "
                        f"{symbol} {tf}: {e}"
                    )

            # 5️⃣ Build initial LiquidityMap for this timeframe (if provided)
            if liquidity_maps is not None and tf in liquidity_maps:
                try:
                    lm = liquidity_maps[tf]
                    current_price = candles[-1]["close"]
                    lm.on_candle_close(
                        timeframe=tf,
                        candles=cm.get_all(),
                        current_price=current_price,
                    )
                    self.logger.info(
                        f"[InitialLoad] {symbol} {tf} LiquidityMap initialized"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[InitialLoad] LiquidityMap update failed for "
                        f"{symbol} {tf}: {e}"
                    )

            self.logger.info(
                f"[InitialLoad] {symbol} {tf}: loaded={len(candles)} last_ts={last_ts}"
            )
            success_count += 1

        if success_count == 0:
            self.logger.error(
                f"[InitialLoad] No timeframes successfully loaded for {symbol}"
            )
            return False

        self.logger.info(
            f"[InitialLoad] Completed for {symbol}, timeframes_ok={success_count}"
        )
        return True

    # -------------------------------------------------------------------------
    # REST FETCHER (Using RestClient)
    # -------------------------------------------------------------------------

    def _fetch_candles(self, symbol: str, tf_cfg: TimeframeConfig) -> List[Dict[str, Any]]:
        """
        Fetch candles from Binance REST using RestClient.

        We ALWAYS fetch only fully CLOSED candles, by setting end_time
        to the last fully completed candle's CLOSE time.
        """
        tf_seconds = self.rest_client.app_config.get_timeframe_seconds(tf_cfg.tf)
        now_sec = int(time.time())

        # Last fully CLOSED candle has:
        #   open_ts = floor(now / tf) * tf - tf
        #   close_ts = open_ts + tf - 1
        last_closed_open_ts = (now_sec // tf_seconds) * tf_seconds - tf_seconds
        last_closed_close_ts = last_closed_open_ts + tf_seconds - 1

        end_time_ms = last_closed_close_ts * 1000

        self.logger.info(
            f"[InitialLoad] Fetching {tf_cfg.fetch} candles for {symbol} {tf_cfg.tf} "
            f"(last_closed_open_ts={last_closed_open_ts}, "
            f"close_time={datetime.fromtimestamp(last_closed_close_ts)})"
        )

        candles = self.rest_client.fetch_klines(
            symbol=symbol,
            timeframe=tf_cfg.tf,
            limit=tf_cfg.fetch,
            end_time=end_time_ms,
        )

        if not candles:
            self.logger.error(
                f"[InitialLoad] Failed to fetch candles for {symbol} {tf_cfg.tf}"
            )
            return []

        self.logger.info(
            f"[InitialLoad] Successfully fetched {len(candles)} candles for "
            f"{symbol} {tf_cfg.tf}"
        )
        return candles


__all__ = ["InitialCandlesLoader"]
