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
    """

    def __init__(self, app_config: AppConfig):
        """
        Args:
            app_config: AppConfig instance containing exchange settings
        """
        self.logger = get_logger(__name__)
        self.app_config = app_config
        
        self.rest_endpoint = app_config.exchange.rest_endpoint
        self.request_timeout = app_config.exchange.request_timeout
        self.max_retries = app_config.candles.initial_retries
        self.retry_delay = app_config.candles.retry_delay
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; TradingBot/1.0)'
        })

    # -------------------------------------------------------------------------
    # PUBLIC API METHODS
    # -------------------------------------------------------------------------

    def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch candles (klines) from Binance REST API with automatic pagination.
        
        If limit > 1000, automatically makes multiple requests to fetch all requested candles.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Interval (e.g., "1m", "5m", "15m", "1h")
            limit: Number of candles to fetch (automatically paginated if > 1000)
            start_time: Start timestamp in milliseconds (optional)
            end_time: End timestamp in milliseconds (optional)

        Returns:
            List of normalized candle dictionaries
        """
        # If limit is within single request, fetch directly
        if limit <= 1000:
            return self._fetch_single_batch(symbol, timeframe, limit, start_time, end_time)
        
        # Otherwise, use pagination
        return self._fetch_with_pagination(symbol, timeframe, limit, start_time, end_time)

    def _fetch_single_batch(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a single batch of candles (internal method).
        
        Args:
            symbol: Trading pair
            timeframe: Interval
            limit: Number of candles (must be <= 1000)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            
        Returns:
            List of normalized candles
        """
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "limit": min(limit, 1000)
        }

        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        raw_klines = self._request_with_retry(params, context=f"{symbol} {timeframe}")
        
        if not raw_klines:
            return []

        return self._normalize_klines(raw_klines)

    def _fetch_with_pagination(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch candles with automatic pagination for requests > 1000 candles.
        
        Args:
            symbol: Trading pair
            timeframe: Interval
            limit: Total number of candles to fetch
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            
        Returns:
            List of normalized candles (oldest to newest)
        """
        all_candles = []
        remaining = limit
        current_end_time = end_time
        
        self.logger.info(
            f"[RestClient] Paginated fetch: {limit} candles for {symbol} {timeframe} "
            f"(~{limit // 1000 + 1} requests)"
        )
        
        while remaining > 0:
            # Fetch up to 1000 candles per request
            batch_limit = min(remaining, 1000)
            
            batch = self._fetch_single_batch(
                symbol=symbol,
                timeframe=timeframe,
                limit=batch_limit,
                start_time=start_time,
                end_time=current_end_time
            )
            
            if not batch:
                self.logger.warning(
                    f"[RestClient] Pagination stopped: no more candles available. "
                    f"Fetched {len(all_candles)}/{limit}"
                )
                break
            
            # Add batch to results (prepend for correct ordering when going backwards)
            if current_end_time:
                # Going backwards in time
                all_candles = batch + all_candles
            else:
                # Going forwards in time
                all_candles.extend(batch)
            
            remaining -= len(batch)
            
            # If we got fewer candles than requested, we've reached the limit
            if len(batch) < batch_limit:
                self.logger.info(
                    f"[RestClient] Reached end of available data. "
                    f"Fetched {len(all_candles)}/{limit} candles"
                )
                break
            
            # Update end_time for next batch (use oldest candle's open time)
            if current_end_time and batch:
                # Calculate the interval in milliseconds
                tf_seconds = self.app_config.get_timeframe_seconds(timeframe)
                tf_ms = tf_seconds * 1000
                
                # Set next end_time to just before the oldest candle in this batch
                oldest_candle_ts = batch[0]["ts"] * 1000  # Convert to ms
                current_end_time = oldest_candle_ts - tf_ms
                
                self.logger.debug(
                    f"[RestClient] Fetched {len(batch)} candles, "
                    f"{remaining} remaining, next end_time={current_end_time}"
                )
            
            # Small delay between requests to avoid rate limits
            if remaining > 0:
                time.sleep(0.1)
        
        self.logger.info(
            f"[RestClient] Pagination complete: fetched {len(all_candles)} candles "
            f"for {symbol} {timeframe}"
        )
        
        return all_candles

    def fetch_kline_exact(
        self,
        symbol: str,
        timeframe: str,
        timestamp: int
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single candle by exact close timestamp.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Interval (e.g., "1m", "5m", "15m", "1h")
            timestamp: Exact close timestamp in seconds

        Returns:
            Single normalized candle dict or None if not found
        """
        # Convert to milliseconds for Binance API
        close_time_ms = timestamp * 1000
        
        # Calculate the open time based on timeframe interval
        tf_seconds = self.app_config.get_timeframe_seconds(timeframe)
        open_time_ms = (timestamp - tf_seconds) * 1000

        # Fetch a small window around the target timestamp
        params = {
            "symbol": symbol.upper(),
            "interval": timeframe,
            "startTime": open_time_ms,
            "endTime": close_time_ms,
            "limit": 1
        }

        raw_klines = self._request_with_retry(
            params, 
            context=f"{symbol} {timeframe} ts={timestamp}"
        )

        if not raw_klines:
            return None

        candles = self._normalize_klines(raw_klines)
        
        # Find exact match by close timestamp
        for candle in candles:
            if candle["ts"] == timestamp:
                return candle

        # If no exact match, return the closest one (Binance might have slight variations)
        if candles:
            self.logger.warning(
                f"[RestClient] No exact match for ts={timestamp}, returning closest candle"
            )
            return candles[0]

        return None

    def fetch_klines_range(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch candles within a specific timestamp range.

        Args:
            symbol: Trading pair
            timeframe: Interval
            start_ts: Start timestamp in seconds
            end_ts: End timestamp in seconds

        Returns:
            List of normalized candles within the range
        """
        return self.fetch_klines(
            symbol=symbol,
            timeframe=timeframe,
            limit=1000,
            start_time=start_ts * 1000,
            end_time=end_ts * 1000
        )

    # -------------------------------------------------------------------------
    # INTERNAL REQUEST HANDLING WITH RETRY LOGIC
    # -------------------------------------------------------------------------

    def _request_with_retry(
        self,
        params: Dict[str, Any],
        context: str = ""
    ) -> Optional[List[List[Any]]]:
        """
        Execute REST request with retry logic and rate limit handling.

        Args:
            params: Query parameters for the API request
            context: Descriptive context for logging

        Returns:
            Raw Binance kline data or None on failure
        """
        attempt = 0
        last_error = None

        while attempt < self.max_retries:
            try:
                response = self.session.get(
                    self.rest_endpoint,
                    params=params,
                    timeout=self.request_timeout
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    wait_time = min(retry_after, self.retry_delay * (2 ** attempt))
                    
                    self.logger.warning(
                        f"[RestClient] Rate limit hit for {context}. "
                        f"Waiting {wait_time}s (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(wait_time)
                    attempt += 1
                    continue

                # Handle other HTTP errors
                if response.status_code == 418:
                    # IP banned
                    self.logger.error(
                        f"[RestClient] IP BANNED (418) for {context}. "
                        f"Check Binance API restrictions."
                    )
                    return None

                if response.status_code >= 500:
                    # Server error - retry
                    self.logger.warning(
                        f"[RestClient] Server error {response.status_code} for {context}. "
                        f"Retrying... (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(self.retry_delay * (2 ** attempt))
                    attempt += 1
                    continue

                # Raise for other bad status codes
                response.raise_for_status()

                # Success - return JSON data
                data = response.json()
                
                if not isinstance(data, list):
                    self.logger.error(
                        f"[RestClient] Unexpected response format for {context}: {type(data)}"
                    )
                    return None

                self.logger.debug(
                    f"[RestClient] Successfully fetched {len(data)} candles for {context}"
                )
                return data

            except requests.exceptions.Timeout:
                last_error = f"Request timeout after {self.request_timeout}s"
                self.logger.warning(
                    f"[RestClient] {last_error} for {context} "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                attempt += 1
                time.sleep(self.retry_delay * (2 ** attempt))

            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {e}"
                self.logger.warning(
                    f"[RestClient] {last_error} for {context} "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                attempt += 1
                time.sleep(self.retry_delay * (2 ** attempt))

            except requests.exceptions.RequestException as e:
                last_error = f"Request failed: {e}"
                self.logger.error(
                    f"[RestClient] {last_error} for {context} "
                    f"(attempt {attempt + 1}/{self.max_retries})"
                )
                attempt += 1
                time.sleep(self.retry_delay * (2 ** attempt))

            except Exception as e:
                last_error = f"Unexpected error: {e}"
                self.logger.exception(
                    f"[RestClient] {last_error} for {context}"
                )
                return None

        # All retries exhausted
        self.logger.error(
            f"[RestClient] FAILED after {self.max_retries} attempts for {context}. "
            f"Last error: {last_error}"
        )
        return None

    # -------------------------------------------------------------------------
    # NORMALIZATION
    # -------------------------------------------------------------------------

    def _normalize_klines(self, raw: List[List[Any]]) -> List[Dict[str, Any]]:
        """
        Convert Binance kline array format into internal candle dictionary format.

        Binance kline format:
        [
            [
                0: Open time (ms),
                1: Open,
                2: High,
                3: Low,
                4: Close,
                5: Volume,
                6: Close time (ms),
                7: Quote asset volume,
                8: Number of trades,
                9: Taker buy base asset volume,
                10: Taker buy quote asset volume,
                11: Ignore
            ]
        ]

        Internal format:
        {
            "ts": close_timestamp_seconds,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": float
        }

        Args:
            raw: Raw Binance kline data

        Returns:
            List of normalized candle dictionaries
        """
        candles = []
        
        for kline in raw:
            try:
                candles.append({
                    "ts": int(kline[6]) // 1000,  # Close time in seconds
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                })
            except (IndexError, ValueError, TypeError) as e:
                self.logger.error(
                    f"[RestClient] Failed to normalize kline: {e}. Skipping candle."
                )
                continue

        return candles

    def close(self):
        """Close the session and cleanup resources."""
        self.session.close()
        self.logger.debug("[RestClient] Session closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


__all__ = ["RestClient"]
