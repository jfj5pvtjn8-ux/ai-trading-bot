"""
RestClient: Centralized REST API client for Binance with proper error handling,
rate limiting, retry logic, and exponential backoff.
"""

import time
import requests
from typing import List, Dict, Any, Optional
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class RestClient:
    """
    Binance REST API client with:
    - Automatic retry with exponential backoff
    - Rate limit handling (429)
    - Request timeout management
    - Proper error logging
    - Normalization of Binance kline format
    - open_ts-aligned timestamps for Liquidity Map compatibility
    """

    def __init__(self, app_config: AppConfig):
        self.logger = get_logger(__name__)
        self.app_config = app_config
        
        self.rest_endpoint = app_config.exchange.rest_endpoint
        self.request_timeout = app_config.exchange.request_timeout
        self.max_retries = app_config.candles.initial_retries
        self.retry_delay = app_config.candles.retry_delay
        
        # Delay to avoid Binance API bans during pagination
        self.per_request_delay = 0.1
        
        # Session pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; TradingBot/1.0)'
        })

    # =========================================================================
    # PUBLIC METHODS
    # =========================================================================

    def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch klines, auto-paginating if limit > 1000.
        Returns fully normalized, deduped, aligned candles.
        """
        if limit <= 1000:
            return self._fetch_single_batch(symbol, timeframe, limit, start_time, end_time)

        return self._fetch_with_pagination(symbol, timeframe, limit, start_time, end_time)

    # =========================================================================
    # SINGLE-BATCH FETCH
    # =========================================================================

    def _fetch_single_batch(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int],
        end_time: Optional[int]
    ) -> List[Dict[str, Any]]:
        
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "limit": min(limit, 1000),
        }

        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        raw = self._request_with_retry(params, context=f"{symbol} {timeframe}")
        if not raw:
            return []

        return self._normalize_klines(raw)

    # =========================================================================
    # PAGINATION
    # =========================================================================

    def _fetch_with_pagination(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int],
        end_time: Optional[int]
    ) -> List[Dict[str, Any]]:

        self.logger.info(
            f"[RestClient] Paginated fetch: {limit} candles for {symbol} {timeframe}"
        )

        all_candles = []
        remaining = limit

        direction_backwards = end_time is not None
        next_start = start_time
        next_end = end_time

        tf_ms = self.app_config.get_timeframe_seconds(timeframe) * 1000

        while remaining > 0:
            batch_limit = min(remaining, 1000)

            batch = self._fetch_single_batch(
                symbol=symbol,
                timeframe=timeframe,
                limit=batch_limit,
                start_time=next_start,
                end_time=next_end
            )

            if not batch:
                break

            all_candles.extend(batch)
            remaining -= len(batch)

            if len(batch) < batch_limit:
                break  # no more data

            # --- Update pagination windows ---
            if direction_backwards:
                oldest_open_ms = batch[0]["open_ts"] * 1000
                next_end = oldest_open_ms - 1
            else:
                latest_open_ms = batch[-1]["open_ts"] * 1000
                next_start = latest_open_ms + tf_ms

            time.sleep(self.per_request_delay)

        # --- Deduplicate ---
        unique = {c["ts"]: c for c in all_candles}
        cleaned = list(unique.values())

        # --- Sort oldest → newest ---
        cleaned.sort(key=lambda x: x["ts"])

        # --- Validate alignment ---
        self._check_alignment(cleaned, timeframe)

        return cleaned[:limit]

    # =========================================================================
    # EXACT CANDLE FETCH
    # =========================================================================

    def fetch_kline_exact(self, symbol: str, timeframe: str, timestamp: int):
        """
        Fetch exact closed candle by timestamp (seconds).
        """
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)
        open_time_ms = (timestamp - tf_sec) * 1000
        close_time_ms = timestamp * 1000

        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "startTime": open_time_ms,
            "endTime": close_time_ms,
            "limit": 2,
        }

        raw = self._request_with_retry(params, context=f"{symbol} {timeframe} ts={timestamp}")
        if not raw:
            return None

        candles = self._normalize_klines(raw)
        for c in candles:
            if c["ts"] == timestamp:
                return c

        return candles[0] if candles else None

    # =========================================================================
    # LOW LEVEL REQUEST + RETRY
    # =========================================================================

    def _request_with_retry(self, params: Dict[str, Any], context: str):
        url = self.rest_endpoint
        backoff = self.retry_delay

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    timeout=self.request_timeout
                )

                # --- Rate limited (429) ---
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", backoff))
                    wait = min(retry_after, backoff)
                    self.logger.warning(
                        f"[RestClient] 429 for {context}, sleeping {wait}s "
                        f"(attempt {attempt}/{self.max_retries})"
                    )
                    time.sleep(wait)
                    backoff *= 2
                    continue

                # --- Server errors ---
                if 500 <= resp.status_code < 600:
                    self.logger.warning(
                        f"[RestClient] Server error {resp.status_code} for {context}, retrying..."
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, list):
                    return data

                self.logger.error(f"[RestClient] Unexpected response type {type(data)}")
                return None

            except requests.exceptions.Timeout:
                self.logger.warning(f"[RestClient] Timeout for {context}, retrying...")
                time.sleep(backoff)
                backoff *= 2

            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"[RestClient] Connection error: {e}, retrying...")
                time.sleep(backoff)
                backoff *= 2

            except Exception as e:
                self.logger.error(f"[RestClient] Fatal error: {e}")
                return None

        self.logger.error(f"[RestClient] FAILED after {self.max_retries} retries for {context}")
        return None

    # =========================================================================
    # NORMALIZATION (LM-READY)
    # =========================================================================

    def _normalize_klines(self, raw: List[List[Any]]):
        candles = []
        for k in raw:
            try:
                open_ts = int(k[0]) // 1000
                close_ts = int(k[6]) // 1000

                candles.append({
                    # MASTER TIME INDEX (open_ts) — safe for LM structure
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "ts": open_ts,

                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),

                    "quote_volume": float(k[7]),
                    "trades": int(k[8]),
                    "taker_buy_base": float(k[9]),
                    "taker_buy_quote": float(k[10]),
                })

            except Exception as e:
                self.logger.error(f"[RestClient] Kline normalization error: {e}")
                continue

        return candles

    # =========================================================================
    # ALIGNMENT CHECK
    # =========================================================================

    def _check_alignment(self, candles, timeframe):
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)

        for c in candles:
            if c["open_ts"] % tf_sec != 0:
                self.logger.warning(
                    f"[RestClient] Misaligned candle detected: ts={c['ts']} tf={timeframe}"
                )

    # =========================================================================

    def close(self):
        self.session.close()
        self.logger.debug("[RestClient] Session closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


__all__ = ["RestClient"]
