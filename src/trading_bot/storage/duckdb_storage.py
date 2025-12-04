"""
DuckDB Storage for Candle Data

Handles:
- Persistent candle storage with SQL queries
- Gap detection and backward filling
- Bulk insert optimization
- Single candle insert for real-time
- Sliding window data loading
"""

import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pandas as pd

from trading_bot.core.logger import get_logger


class DuckDBStorage:
    """
    DuckDB-based storage for market candles with gap detection and backward filling.

    Features:
    - SQL-based queries for analytics
    - Automatic gap detection and filling
    - Duplicate prevention via PRIMARY KEY
    - Efficient bulk inserts
    - Sliding window data loading
    """

    def __init__(
        self,
        db_path: str,
        app_config,
        fresh_start: bool = False,
    ):
        """
        Args:
            db_path: Path to DuckDB database file
            app_config: Application configuration object
            fresh_start: If True, clear all data on startup
        """
        self.logger = get_logger(__name__)
        self.db_path = Path(db_path)
        self.app_config = app_config
        self.fresh_start = fresh_start

        # Thread pool for async writes
        self.executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="duckdb_writer"
        )

        # Connection lock for thread safety
        self.conn_lock = Lock()

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize connection
        self.conn = duckdb.connect(str(self.db_path))

        self.logger.info(
            f"[DuckDBStorage] Initialized: path={self.db_path}, "
            f"fresh_start={fresh_start}"
        )

    # -------------------------------------------------------------------------
    # SCHEMA INITIALIZATION
    # -------------------------------------------------------------------------

    def initialize_schema(self) -> None:
        """Load and execute schema from schema/trading_schema.sql"""
        schema_path = Path("schema/trading_schema.sql")

        if not schema_path.exists():
            self.logger.warning(
                f"[DuckDBStorage] Schema file not found: {schema_path}, "
                f"skipping schema initialization"
            )
            return

        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Split by semicolon and execute each statement
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]

            with self.conn_lock:
                for stmt in statements:
                    try:
                        self.conn.execute(stmt)
                    except Exception as e:
                        # Skip if table already exists
                        if "already exists" not in str(e).lower():
                            self.logger.error(f"[DuckDBStorage] Schema error: {e}")

            self.logger.info("[DuckDBStorage] Schema initialized successfully")

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] Failed to load schema: {e}")
            raise

    # -------------------------------------------------------------------------
    # QUERY METHODS
    # -------------------------------------------------------------------------

    def get_last_candle(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the most recent candle for a symbol/timeframe.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Timeframe (e.g., "1m", "5m")

        Returns:
            Dictionary with candle data or None if no data exists
        """
        try:
            with self.conn_lock:
                result = self.conn.execute(
                    """
                    SELECT symbol, timeframe, open_ts, close_ts,
                           open, high, low, close, volume
                    FROM market.candles
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY open_ts DESC
                    LIMIT 1
                    """,
                    [symbol, timeframe]
                ).fetchone()

            if result:
                return {
                    'symbol': result[0],
                    'timeframe': result[1],
                    'ts': result[2],         # Use open_ts as ts
                    'open_ts': result[2],
                    'close_ts': result[3],
                    'open': result[4],
                    'high': result[5],
                    'low': result[6],
                    'close': result[7],
                    'volume': result[8],
                }
            return None

        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Failed to get last candle for "
                f"{symbol} {timeframe}: {e}"
            )
            return None

    def load_candles_for_window(
        self,
        symbol: str,
        timeframe: str,
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Load most recent N candles for sliding window.

        Args:
            symbol: Trading pair
            timeframe: Timeframe
            limit: Number of candles to load

        Returns:
            List of candle dictionaries, ordered by timestamp ascending
        """
        try:
            with self.conn_lock:
                results = self.conn.execute(
                    """
                    SELECT symbol, timeframe, open_ts, close_ts,
                           open, high, low, close, volume
                    FROM market.candles
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY open_ts DESC
                    LIMIT ?
                    """,
                    [symbol, timeframe, limit]
                ).fetchall()

            # Convert to dictionaries and reverse to get ascending order
            candles = []
            for row in reversed(results):
                candles.append({
                    'symbol': row[0],
                    'timeframe': row[1],
                    'ts': row[2],
                    'open_ts': row[2],
                    'close_ts': row[3],
                    'open': row[4],
                    'high': row[5],
                    'low': row[6],
                    'close': row[7],
                    'volume': row[8],
                })

            return candles

        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Failed to load candles for window "
                f"{symbol} {timeframe}: {e}"
            )
            return []

    # -------------------------------------------------------------------------
    # GAP DETECTION
    # -------------------------------------------------------------------------

    def calculate_gap(self, last_ts: int, timeframe_seconds: int) -> int:
        """
        Calculate number of missing candles between last timestamp and now.

        Args:
            last_ts: Last stored candle timestamp (seconds)
            timeframe_seconds: Candle interval in seconds

        Returns:
            Number of missing candles
        """
        now = int(time.time())
        gap_seconds = now - last_ts

        # Subtract one timeframe because last_ts candle already exists
        gap_candles = (gap_seconds // timeframe_seconds) - 1

        return max(0, gap_candles)

    def should_full_reload(self, gap_candles: int, max_candles: int) -> bool:
        """
        Determine if gap is too large and requires full reload.

        Args:
            gap_candles: Number of missing candles
            max_candles: Maximum candles configured for this timeframe

        Returns:
            True if full reload recommended, False for incremental fill
        """
        # Trigger full reload if gap is 50% larger than max configured
        threshold = max_candles * 1.5
        return gap_candles > threshold

    # -------------------------------------------------------------------------
    # DATA INSERTION
    # -------------------------------------------------------------------------

    def save_candle(
        self,
        symbol: str,
        timeframe: str,
        candle: Dict[str, Any]
    ) -> None:
        """
        Save a single validated candle (real-time).

        Args:
            symbol: Trading pair
            timeframe: Timeframe
            candle: Normalized candle dict
        """
        try:
            open_ts = int(candle.get('open_ts', candle['ts']))
            close_ts = int(candle.get('close_ts', open_ts))

            with self.conn_lock:
                self.conn.execute(
                    """
                    INSERT INTO market.candles
                    (symbol, timeframe, open_ts, close_ts,
                     open, high, low, close, volume, is_closed, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (symbol, timeframe, open_ts) DO NOTHING
                    """,
                    [
                        symbol,
                        timeframe,
                        open_ts,
                        close_ts,
                        float(candle['open']),
                        float(candle['high']),
                        float(candle['low']),
                        float(candle['close']),
                        float(candle['volume']),
                        True,  # is_closed
                        'ws'   # source
                    ]
                )

        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Failed to save candle {symbol} {timeframe}: {e}"
            )

    def save_candle_async(
        self,
        symbol: str,
        timeframe: str,
        candle: Dict[str, Any]
    ) -> None:
        """Submit candle save to thread pool (non-blocking)."""
        self.executor.submit(self.save_candle, symbol, timeframe, candle)

    def bulk_insert_candles(
        self,
        symbol: str,
        timeframe: str,
        candles: List[Dict[str, Any]],
        source: str = 'rest'
    ) -> int:
        """
        Bulk insert candles efficiently using pandas DataFrame.

        Args:
            symbol: Trading pair
            timeframe: Timeframe
            candles: List of candle dictionaries
            source: Data source ('rest', 'backfill', etc.)

        Returns:
            Number of candles inserted
        """
        if not candles:
            return 0

        try:
            # Prepare data for DataFrame
            rows = []
            for candle in candles:
                open_ts = int(candle.get('open_ts', candle['ts']))
                close_ts = int(candle.get('close_ts', open_ts))

                rows.append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'open_ts': open_ts,
                    'close_ts': close_ts,
                    'open': float(candle['open']),
                    'high': float(candle['high']),
                    'low': float(candle['low']),
                    'close': float(candle['close']),
                    'volume': float(candle['volume']),
                    'is_closed': True,
                    'source': source,
                })

            df = pd.DataFrame(rows)

            # Use DuckDB's efficient DataFrame insert
            with self.conn_lock:
                # Insert with conflict resolution
                self.conn.execute("""
                    INSERT INTO market.candles
                    SELECT * FROM df
                    ON CONFLICT (symbol, timeframe, open_ts) DO NOTHING
                """)

            self.logger.debug(
                f"[DuckDBStorage] Bulk inserted {len(candles)} candles for "
                f"{symbol} {timeframe}"
            )
            return len(candles)

        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Bulk insert failed for {symbol} {timeframe}: {e}"
            )
            return 0

    # -------------------------------------------------------------------------
    # BACKWARD FILLING
    # -------------------------------------------------------------------------

    def backward_fill_gap(
        self,
        symbol: str,
        timeframe: str,
        rest_client,
        count: int
    ) -> bool:
        """
        Fetch and insert missing candles backward from current time.

        Args:
            symbol: Trading pair
            timeframe: Timeframe
            rest_client: REST client for fetching candles
            count: Number of candles to fetch

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(
                f"[DuckDBStorage] Backward filling {count} candles for "
                f"{symbol} {timeframe}..."
            )

            # Fetch candles from REST API
            # We'll fetch in batches if count > 1000 (Binance limit)
            batch_size = 1000
            all_candles = []

            remaining = count
            while remaining > 0:
                fetch_count = min(remaining, batch_size)

                # Calculate end_time for this batch
                if all_candles:
                    # Use oldest candle's timestamp as end_time for next batch
                    end_time = all_candles[-1]['ts'] - 1
                else:
                    # First batch - use current time
                    end_time = None

                # Fetch batch
                batch = rest_client.fetch_klines(
                    symbol=symbol,
                    interval=timeframe,
                    limit=fetch_count,
                    end_time=end_time
                )

                if not batch:
                    self.logger.warning(
                        f"[DuckDBStorage] No more candles available for "
                        f"{symbol} {timeframe}"
                    )
                    break

                all_candles.extend(batch)
                remaining -= len(batch)

                # If we got less than requested, we've hit the data limit
                if len(batch) < fetch_count:
                    break

            if not all_candles:
                self.logger.error(
                    f"[DuckDBStorage] Failed to fetch candles for {symbol} {timeframe}"
                )
                return False

            # Insert into DuckDB
            inserted = self.bulk_insert_candles(
                symbol, timeframe, all_candles, source='backfill'
            )

            self.logger.info(
                f"[DuckDBStorage] Backward fill complete: inserted {inserted} candles "
                f"for {symbol} {timeframe}"
            )

            return True

        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Backward fill failed for {symbol} {timeframe}: {e}"
            )
            return False

    # -------------------------------------------------------------------------
    # DATA MANAGEMENT
    # -------------------------------------------------------------------------

    def clear_all_candles(self) -> None:
        """Delete all candles from market.candles table."""
        try:
            with self.conn_lock:
                self.conn.execute("DELETE FROM market.candles")
            self.logger.info("[DuckDBStorage] All candles cleared from database")
        except Exception as e:
            self.logger.error(f"[DuckDBStorage] Failed to clear candles: {e}")

    def delete_symbol_timeframe(self, symbol: str, timeframe: str) -> None:
        """Delete all candles for specific symbol/timeframe."""
        try:
            with self.conn_lock:
                self.conn.execute(
                    """
                    DELETE FROM market.candles
                    WHERE symbol = ? AND timeframe = ?
                    """,
                    [symbol, timeframe]
                )
            self.logger.info(
                f"[DuckDBStorage] Deleted all candles for {symbol} {timeframe}"
            )
        except Exception as e:
            self.logger.error(
                f"[DuckDBStorage] Failed to delete {symbol} {timeframe}: {e}"
            )

    def get_candle_count(self, symbol: str = None, timeframe: str = None) -> int:
        """Get total candle count, optionally filtered by symbol/timeframe."""
        try:
            with self.conn_lock:
                if symbol and timeframe:
                    result = self.conn.execute(
                        """
                        SELECT COUNT(*) FROM market.candles
                        WHERE symbol = ? AND timeframe = ?
                        """,
                        [symbol, timeframe]
                    ).fetchone()
                elif symbol:
                    result = self.conn.execute(
                        """
                        SELECT COUNT(*) FROM market.candles
                        WHERE symbol = ?
                        """,
                        [symbol]
                    ).fetchone()
                else:
                    result = self.conn.execute(
                        "SELECT COUNT(*) FROM market.candles"
                    ).fetchone()

            return result[0] if result else 0

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] Failed to get candle count: {e}")
            return 0

    # -------------------------------------------------------------------------
    # CLEANUP
    # -------------------------------------------------------------------------

    def shutdown(self) -> None:
        """Shutdown the thread pool executor and close connection."""
        if self.executor:
            self.executor.shutdown(wait=True)
            self.logger.debug("[DuckDBStorage] Executor shutdown complete")

        if self.conn:
            self.conn.close()
            self.logger.debug("[DuckDBStorage] Connection closed")


__all__ = ["DuckDBStorage"]
