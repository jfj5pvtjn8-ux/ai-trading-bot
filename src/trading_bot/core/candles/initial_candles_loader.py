from __future__ import annotations

"""
InitialCandlesLoader PRO+ Edition

Responsibilities
----------------
• For each (symbol, timeframe):
    - Fetch a clean block of historical candles from REST
    - Enforce OPEN-timestamp semantics (ts/open_ts)
    - Enforce alignment to timeframe interval (no weird partials)
    - Deduplicate & sort oldest → newest
    - Detect & log gaps (for observability)
    - Seed CandleManager window
    - Seed CandleSync.last_open_ts
    - Optionally persist batch to Parquet
    - Optionally build initial LiquidityMap
    - Optionally trigger reverse gap recovery (CandleSync.reverse_recovery)

Assumptions
-----------
• RestClient.fetch_klines returns a list of normalized dicts:
    {
        "open_ts": int,       # seconds, candle OPEN time
        "close_ts": int,      # seconds, candle CLOSE time
        "ts": int,            # alias to open_ts (master index)
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        ...
    }

• CandleManager:
    - load_initial(candles: List[Dict[str, Any]])
    - last_open_time() -> Optional[int]
    - get_all() -> List[Dict[str, Any]]

• CandleSync:
    - set_initial_last_ts(ts: Optional[int])
    - reverse_recovery(storage=None)  # optional, used only if requested
"""

import time
from typing import Dict, List, Any, Optional

from trading_bot.core.logger import get_logger
from trading_bot.config.symbols.models import SymbolConfig, TimeframeConfig
from trading_bot.config.app.models import AppConfig
from trading_bot.api.rest_client import RestClient
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync


class InitialCandlesLoader:
    """PRO+ initial loader with CandleManager + CandleSync + Storage integration."""

    def __init__(self, app_config: AppConfig, rest_client: RestClient):
        """
        Args:
            app_config: Application configuration
            rest_client: RestClient instance for fetching candles
        """
        self.logger = get_logger(__name__)
        self.app_config = app_config
        self.rest_client = rest_client

    # ======================================================================
    # PUBLIC API
    # ======================================================================

    def load_initial_for_symbol(
        self,
        symbol_cfg: SymbolConfig,
        candle_managers: Dict[str, CandleManager],   # {"1m": CandleManager, ...}
        candle_syncs: Dict[str, CandleSync],         # {"1m": CandleSync, ...}
        liquidity_maps: Optional[Dict[str, Any]] = None,  # Dict of LiquidityMap per TF
        storage=None,  # ParquetStorage for async writes
        use_reverse_recovery: bool = False,
    ) -> bool:
        """
        Load & seed all structures for a single symbol.

        Args:
            symbol_cfg       → includes name + TF configs
            candle_managers  → CandleManager per timeframe (key = timeframe string)
            candle_syncs     → CandleSync per timeframe (key = timeframe string)
            liquidity_maps   → Dict of LiquidityMap instances (one per TF, optional)
            storage          → ParquetStorage or similar (optional)
            use_reverse_recovery → if True, call CandleSync.reverse_recovery()

        Returns:
            True if at least one timeframe was successfully initialized,
            False if all timeframes failed.
        """

        symbol = symbol_cfg.name
        self.logger.info(f"[InitialLoad] ===== START for {symbol} =====")

        success_count = 0

        for tf_cfg in symbol_cfg.timeframes:
            tf = tf_cfg.tf
            fetch = tf_cfg.fetch

            cm = candle_managers.get(tf)
            sync = candle_syncs.get(tf)

            if cm is None or sync is None:
                self.logger.error(
                    f"[InitialLoad] Missing CandleManager or CandleSync "
                    f"for {symbol} {tf}"
                )
                continue

            # 1️⃣ Fetch REST candles (normalized, open-time semantics)
            candles = self._fetch_candles(symbol, tf_cfg)
            if not candles:
                self.logger.error(
                    f"[InitialLoad] No candles loaded for {symbol} {tf}"
                )
                continue

            # 2️⃣ Clean + validate (alignment, duplicates, ordering, gaps)
            cleaned = self._sanitize_candles(symbol, tf_cfg, candles)
            if not cleaned:
                self.logger.error(
                    f"[InitialLoad] All candles filtered out for {symbol} {tf}"
                )
                continue

            # 3️⃣ Load into CandleManager (sliding window)
            cm.load_initial(cleaned)

            # 4️⃣ Seed CandleSync last_open_ts
            last_ts = cm.last_open_time()
            sync.set_initial_last_ts(last_ts)

            # 5️⃣ Persist to Parquet (async)
            if storage:
                try:
                    storage.save_candles_batch_async(symbol, tf, cleaned)
                    self.logger.debug(
                        f"[InitialLoad] Submitted {len(cleaned)} candles to storage "
                        f"for {symbol} {tf}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[InitialLoad] Storage save_candles_batch_async failed for "
                        f"{symbol} {tf}: {e}"
                    )

            # 6️⃣ Initialize LiquidityMap for this TF (if provided)
            if liquidity_maps is not None and tf in liquidity_maps:
                try:
                    lm = liquidity_maps[tf]
                    current_price = cleaned[-1]["close"]
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

            # 7️⃣ Optional reverse gap recovery (backwards fill after restart)
            if use_reverse_recovery:
                try:
                    sync.reverse_recovery(storage=storage)
                except AttributeError:
                    # Older CandleSync without reverse_recovery – safe to ignore
                    self.logger.debug(
                        f"[InitialLoad] reverse_recovery not supported for {symbol} {tf}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[InitialLoad] reverse_recovery failed for {symbol} {tf}: {e}"
                    )

            self.logger.info(
                f"[InitialLoad] {symbol} {tf}: "
                f"loaded={len(cleaned)} last_ts={last_ts}"
            )

            success_count += 1

        if success_count == 0:
            self.logger.error(
                f"[InitialLoad] No timeframes successfully loaded for {symbol}"
            )
            self.logger.info(f"[InitialLoad] ===== FAILED for {symbol} =====")
            return False

        self.logger.info(
            f"[InitialLoad] Completed for {symbol}, timeframes_ok={success_count}"
        )
        self.logger.info(f"[InitialLoad] ===== DONE for {symbol} =====")
        return True

    # ======================================================================
    # REST FETCHER (Using RestClient)
    # ======================================================================

    def _fetch_candles(self, symbol: str, tf_cfg: TimeframeConfig) -> List[Dict[str, Any]]:
        """
        Fetch candles from Binance REST using RestClient (open-time semantics).

        Logic:
        ------
        • Compute "now" in seconds
        • tf_seconds = get_timeframe_seconds(tf)
        • closed_ceiling = floor(now / tf_sec) * tf_sec
          → this is the first *open_ts* that is definitely not in-progress
        • REST end_time_ms is closed_ceiling * 1000
          → Binance returns candles with:
                open_time_ms < end_time_ms
            so the latest open_ts we get is <= closed_ceiling - step

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            tf_cfg: Timeframe configuration with tf and fetch limit

        Returns:
            List of normalized candle dictionaries (possibly empty).
        """
        from datetime import datetime

        tf = tf_cfg.tf
        fetch = tf_cfg.fetch

        tf_seconds = self.rest_client.app_config.get_timeframe_seconds(tf)
        now_sec = int(time.time())

        # First non-in-progress open_ts (upper bound, exclusive)
        closed_ceiling_open_ts = (now_sec // tf_seconds) * tf_seconds

        end_time_ms = closed_ceiling_open_ts * 1000

        self.logger.info(
            f"[InitialLoad] Fetching {fetch} candles for {symbol} {tf} "
            f"(end_open_ts<{closed_ceiling_open_ts}, "
            f"end_time={datetime.fromtimestamp(closed_ceiling_open_ts)})"
        )

        try:
            candles = self.rest_client.fetch_klines(
                symbol=symbol,
                timeframe=tf,
                limit=fetch,
                end_time=end_time_ms,
            )
        except Exception as e:
            self.logger.error(
                f"[InitialLoad] REST fetch_klines failed for {symbol} {tf}: {e}"
            )
            return []

        if not candles:
            self.logger.error(
                f"[InitialLoad] REST returned no data for {symbol} {tf}"
            )
            return []

        self.logger.info(
            f"[InitialLoad] Successfully fetched {len(candles)} raw candles for "
            f"{symbol} {tf}"
        )
        return candles

    # ======================================================================
    # SANITIZATION + DIAGNOSTICS
    # ======================================================================

    def _sanitize_candles(
        self,
        symbol: str,
        tf_cfg: TimeframeConfig,
        candles: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Normalize + validate REST candles:

        Steps:
        ------
        • Ensure ts field exists (ts = open_ts)
        • Sort by ts ascending
        • Drop duplicates by ts
        • Enforce alignment to timeframe interval
        • Log gap stats (but do not "fix" gaps here)
        """
        tf = tf_cfg.tf
        tf_seconds = self.rest_client.app_config.get_timeframe_seconds(tf)

        if not candles:
            return []

        # Ensure ts exists & normalize type
        normalized: List[Dict[str, Any]] = []
        for c in candles:
            if "ts" not in c:
                if "open_ts" in c:
                    c = dict(c)
                    c["ts"] = int(c["open_ts"])
                else:
                    self.logger.warning(
                        f"[InitialLoad] Dropping candle without ts/open_ts for "
                        f"{symbol} {tf}: {c}"
                    )
                    continue
            else:
                c = dict(c)
                c["ts"] = int(c["ts"])
            normalized.append(c)

        if not normalized:
            return []

        # Sort oldest → newest by open timestamp
        normalized.sort(key=lambda x: x["ts"])

        # Deduplicate by ts (keep last occurrence to favor latest REST data)
        unique_map: Dict[int, Dict[str, Any]] = {}
        for c in normalized:
            unique_map[c["ts"]] = c
        deduped = list(unique_map.values())
        deduped.sort(key=lambda x: x["ts"])

        # Alignment check: ts % tf_seconds == 0
        aligned: List[Dict[str, Any]] = []
        misaligned_ts: List[int] = []

        for c in deduped:
            ts = c["ts"]
            if ts % tf_seconds != 0:
                misaligned_ts.append(ts)
                continue
            aligned.append(c)

        if misaligned_ts:
            self.logger.warning(
                f"[InitialLoad] {symbol} {tf}: {len(misaligned_ts)} misaligned candles "
                f"filtered out. Examples: {misaligned_ts[:5]}"
            )

        if not aligned:
            self.logger.error(
                f"[InitialLoad] {symbol} {tf}: all candles misaligned, nothing to load."
            )
            return []

        # Gap diagnostics (we don't fix gaps here; CandleSync will handle forward gaps)
        gaps = 0
        worst_gap = 0
        prev_ts = aligned[0]["ts"]

        for c in aligned[1:]:
            ts = c["ts"]
            diff = ts - prev_ts
            if diff != tf_seconds:
                gaps += 1
                missing = int(diff / tf_seconds) - 1
                worst_gap = max(worst_gap, missing)
            prev_ts = ts

        if gaps == 0:
            self.logger.info(
                f"[InitialLoad] {symbol} {tf}: alignment OK, no gaps in {len(aligned)} candles."
            )
        else:
            self.logger.warning(
                f"[InitialLoad] {symbol} {tf}: detected {gaps} gaps, "
                f"worst_missing={worst_gap} candles. "
                f"Forward gaps will be handled by CandleSync at runtime."
            )

        # Optionally truncate to timeframe fetch limit again (safety)
        if len(aligned) > tf_cfg.fetch:
            aligned = aligned[-tf_cfg.fetch:]

        return aligned


__all__ = ["InitialCandlesLoader"]
