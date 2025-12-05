"""
RestClient: Centralized REST API client for Binance with proper error handling,
rate limiting, retry logic, and exponential backoff.

Enhanced with robust pagination supporting 10,000+ candle fetching.
"""

import time
from typing import List, Dict, Any, Optional

import requests

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
    - Robust pagination supporting limit > 1000 (e.g., 10000 candles)
    
    Key Features:
    - fetch_klines(symbol, timeframe, limit, start_time, end_time)
      returns up to `limit` normalized candles (oldest -> newest)
    - Automatic pagination for limit > 1000
    - Order-independent window movement using raw Binance openTime
    - Proper deduplication and alignment checks
    
    Normalized candle schema:
        "open_ts" (seconds), "close_ts" (seconds), "ts" == open_ts,
        "open", "high", "low", "close", "volume", plus extras
    """

    def __init__(self, app_config: AppConfig):
        self.logger = get_logger(__name__)
        self.app_config = app_config

        self.rest_endpoint = app_config.exchange.rest_endpoint
        self.request_timeout = app_config.exchange.request_timeout
        self.max_retries = max(1, int(getattr(app_config.candles, "initial_retries", 5)))
        self.retry_delay = float(getattr(app_config.candles, "retry_delay", 1.0))

        # Delay between pagination requests to be polite (can be tuned)
        self.per_request_delay = float(getattr(app_config.candles, "per_request_delay", 0.12))

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"})

    # --------------------------
    # Public
    # --------------------------
    def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 500,
        start_time: Optional[int] = None,  # milliseconds
        end_time: Optional[int] = None,    # milliseconds
    ) -> List[Dict[str, Any]]:
        """
        Fetch klines, auto-paginating when limit > 1000.
        Returns candles oldest -> newest (ts=open_ts in seconds).
        `start_time` and `end_time` are milliseconds (Binance API).
        If both None, this fetches most recent `limit` closed candles (uses current time ceiling).
        """
        if limit <= 1000:
            raw = self._fetch_single_batch(symbol, timeframe, limit, start_time, end_time)
            return self._normalize_and_validate(raw, timeframe)

        # pagination for >1000
        return self._fetch_with_pagination(symbol, timeframe, limit, start_time, end_time)

    # --------------------------
    # Single batch
    # --------------------------
    def _fetch_single_batch(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int],
        end_time: Optional[int],
    ) -> Optional[List[List[Any]]]:
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "limit": max(1, min(limit, 1000)),
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)

        return self._request_with_retry(params, context=f"{symbol} {timeframe}")

    # --------------------------
    # Pagination
    # --------------------------
    def _fetch_with_pagination(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int],
        end_time: Optional[int],
    ) -> List[Dict[str, Any]]:
        """
        Robust pagination:
         - If end_time is provided (ms) we page *backwards* from end_time (most recent).
         - Otherwise we page forwards starting at start_time if given, or
           we compute an end_time = closed_ceiling and page backwards to get most recent candles.
        Returns up to `limit` normalized candles (oldest -> newest).
        """
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)
        tf_ms = tf_sec * 1000

        # If neither provided, set end_time to last fully closed candle (ms)
        if start_time is None and end_time is None:
            now = int(time.time())
            closed_ceiling_open_ts = (now // tf_sec) * tf_sec  # seconds
            end_time = closed_ceiling_open_ts * 1000

        direction_backwards = end_time is not None

        all_raw: List[List[Any]] = []
        remaining = limit
        next_start = start_time
        next_end = end_time

        while remaining > 0:
            batch_limit = min(remaining, 1000)

            raw_batch = self._fetch_single_batch(
                symbol=symbol,
                timeframe=timeframe,
                limit=batch_limit,
                start_time=next_start,
                end_time=next_end,
            )

            if not raw_batch:
                # nothing returned
                break

            # append raw rows (we will dedupe & normalize later)
            all_raw.extend(raw_batch)
            remaining -= len(raw_batch)

            # if we received fewer than asked, server exhausted the range
            if len(raw_batch) < batch_limit:
                break

            # update pagination window
            # convert raw_batch -> normalized open times to move window safely
            # raw_batch items: [openTime_ms, open, high, low, close, volume, closeTime_ms, ...]
            # find min and max openTime in this batch
            try:
                open_times_ms = [int(r[0]) for r in raw_batch]
            except Exception:
                # defensive: if parsing fails, stop to avoid infinite loop
                break

            min_open_ms = min(open_times_ms)
            max_open_ms = max(open_times_ms)

            if direction_backwards:
                # move end backwards to just before the earliest open in this batch
                next_end = min_open_ms - 1
            else:
                # move start forwards to just after the latest open in this batch
                next_start = max_open_ms + 1

            # small delay between paged requests
            time.sleep(self.per_request_delay)

        # Normalize & dedupe by open_ts
        normalized = self._normalize_klines(all_raw)
        # Build unique by open_ts (ts)
        unique_map = {c["open_ts"]: c for c in normalized}
        cleaned = list(unique_map.values())
        cleaned.sort(key=lambda x: x["open_ts"])

        # If we paged backwards to get most recent N, cleaned now contains all candidate candles oldest->newest.
        # Return the last `limit` items (most recent N) to match expectation.
        if len(cleaned) > limit:
            cleaned = cleaned[-limit:]

        # Final alignment check
        self._check_alignment(cleaned, timeframe)

        return cleaned

    # --------------------------
    # Exact fetch
    # --------------------------
    def fetch_kline_exact(self, symbol: str, timeframe: str, open_ts: int) -> Optional[Dict[str, Any]]:
        """
        Fetch exact candle by open timestamp (seconds).
        Returns normalized candle dict or None.
        """
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)
        start_ms = open_ts * 1000
        end_ms = (open_ts + tf_sec) * 1000 - 1

        raw = self._fetch_single_batch(symbol, timeframe, limit=2, start_time=start_ms, end_time=end_ms)
        if not raw:
            return None

        normalized = self._normalize_klines(raw)
        for c in normalized:
            if c["open_ts"] == open_ts:
                return c
        return None

    def fetch_candles_between(self, symbol: str, timeframe: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
        """
        Fetch candles between start_ts and end_ts (exclusive).
        Used for gap filling during WebSocket recovery.
        Handles large gaps by fetching in multiple batches (1000 candles per request).
        
        Parameters:
        ----------
        start_ts: Start timestamp in seconds (exclusive - will fetch from start_ts + 1 interval)
        end_ts: End timestamp in seconds (exclusive)
        
        Returns:
        -------
        List of normalized candles between the timestamps
        """
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)
        
        # Calculate how many candles we need
        gap_seconds = end_ts - start_ts
        expected_candles = max(1, (gap_seconds + tf_sec - 1) // tf_sec)  # Ceiling division
        
        all_candles = []
        current_start_ts = start_ts
        
        # Fetch in batches of 1000 (Binance API limit)
        while current_start_ts < end_ts - tf_sec:
            batch_start_ms = (current_start_ts + tf_sec) * 1000
            batch_end_ms = (end_ts - 1) * 1000
            
            remaining_candles = (end_ts - current_start_ts) // tf_sec
            batch_limit = min(int(remaining_candles), 1000)
            
            batch = self.fetch_klines(
                symbol=symbol,
                timeframe=timeframe,
                limit=batch_limit,
                start_time=batch_start_ms,
                end_time=batch_end_ms
            )
            
            if not batch:
                break
            
            all_candles.extend(batch)
            
            # Move to the last candle's timestamp for next batch
            current_start_ts = batch[-1]["ts"]
            
            # If we got less than requested, we've reached the end
            if len(batch) < batch_limit:
                break
        
        self.logger.info(
            f"[RestClient] fetch_candles_between {symbol} {timeframe}: "
            f"requested {expected_candles}, got {len(all_candles)} candles in {(len(all_candles) + 999) // 1000} batch(es)"
        )
        
        return all_candles

    # --------------------------
    # Request + Retry
    # --------------------------
    def _request_with_retry(self, params: Dict[str, Any], context: str) -> Optional[List[Any]]:
        url = self.rest_endpoint
        backoff = float(self.retry_delay)

        for attempt in range(1, self.max_retries + 1):
            start_t = time.time()
            try:
                resp = self.session.get(url, params=params, timeout=self.request_timeout)
                duration = time.time() - start_t

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else backoff
                    self.logger.warning(f"[RestClient] 429 for {context}, sleeping {wait}s (attempt {attempt})")
                    time.sleep(wait)
                    backoff *= 2
                    continue

                if resp.status_code >= 500:
                    self.logger.warning(f"[RestClient] Server error {resp.status_code} for {context} (attempt {attempt})")
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list):
                    return data
                self.logger.error(f"[RestClient] Unexpected response format for {context}: {type(data)}")
                return None

            except requests.exceptions.Timeout:
                self.logger.warning(f"[RestClient] Timeout for {context}, retrying (attempt {attempt})")
                time.sleep(backoff)
                backoff *= 2

            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"[RestClient] Connection error for {context}: {e}, retrying (attempt {attempt})")
                time.sleep(backoff)
                backoff *= 2

            except Exception as e:
                self.logger.error(f"[RestClient] Fatal error for {context}: {e}")
                return None

        self.logger.error(f"[RestClient] Exhausted retries for {context}")
        return None

    # --------------------------
    # Normalization & checks
    # --------------------------
    def _normalize_klines(self, raw: List[List[Any]]) -> List[Dict[str, Any]]:
        """
        Normalize Binance /api/v3/klines response.

        raw element layout:
        [
            0  openTime,
            1  open,
            2  high,
            3  low,
            4  close,
            5  volume,
            6  closeTime,
            7  quoteAssetVolume,
            8  numberOfTrades,
            9  takerBuyBaseAssetVolume,
            10 takerBuyQuoteAssetVolume,
            11 ignore
        ]
        """
        candles: List[Dict[str, Any]] = []
        
        for k in raw:
            try:
                open_ms = int(k[0])
                close_ms = int(k[6])
                open_ts = open_ms // 1000
                close_ts = close_ms // 1000
                
                candles.append({
                    # MASTER TIME INDEX (open_ts) — safe for LM & CandleSync
                    "open_ts": open_ts,
                    "close_ts": close_ts,
                    "ts": open_ts,  # alias used across pipeline
                    
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    
                    "quote_volume": float(k[7]) if len(k) > 7 else 0.0,
                    "trades": int(k[8]) if len(k) > 8 else 0,
                    "taker_buy_base": float(k[9]) if len(k) > 9 else 0.0,
                    "taker_buy_quote": float(k[10]) if len(k) > 10 else 0.0,
                })
            except Exception as e:
                self.logger.error(f"[RestClient] Kline normalization error: {e}")
                continue
                
        return candles

    def _normalize_and_validate(self, raw: Optional[List[List[Any]]], timeframe: str) -> List[Dict[str, Any]]:
        if not raw:
            return []
        normalized = self._normalize_klines(raw)
        self._check_alignment(normalized, timeframe)
        # Ensure sorted oldest -> newest
        normalized.sort(key=lambda x: x["open_ts"])
        return normalized

    def _check_alignment(self, candles: List[Dict[str, Any]], timeframe: str) -> None:
        tf_sec = self.app_config.get_timeframe_seconds(timeframe)
        if not candles:
            return
        misaligned = 0
        for c in candles:
            if c["open_ts"] % tf_sec != 0:
                misaligned += 1
                self.logger.warning(f"[RestClient] Misaligned candle detected: open_ts={c['open_ts']} tf={timeframe}")
        if misaligned:
            self.logger.warning(f"[RestClient] Alignment: {misaligned}/{len(candles)} misaligned for {timeframe}")

    def close(self):
        self.session.close()
        self.logger.debug("[RestClient] Session closed")
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


__all__ = ["RestClient"]
