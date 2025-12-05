"""
DuckDB Storage for Candle Data

SAFE CONCURRENT ACCESS:
✓ Single DuckDB connection (required by DuckDB)
✓ Write lock for all INSERT/UPDATE/DELETE
✓ No lock for reads (DuckDB MVCC ensures safety)
✓ Async writer using ThreadPoolExecutor
✓ Fully compatible with TradingBot pipelines
"""

import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pandas as pd

from trading_bot.core.logger import get_logger


class DuckDBStorage:
    """
    DuckDB storage with:
    - Single connection (DuckDB requirement)
    - Write lock for all writes (serialized, safe)
    - No locks on reads (MVCC handles concurrency)
    - Async write pool for non-blocking inserts
    """

    def __init__(self, db_path: str, app_config, fresh_start: bool = False):
        self.logger = get_logger(__name__)
        self.db_path = Path(db_path)
        self.app_config = app_config
        self.fresh_start = fresh_start

        # Global write lock (DuckDB = 1 writer max)
        self.write_lock = Lock()

        # Async writer pool
        self.executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="duckdb_writer"
        )

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Single global connection
        self.conn = duckdb.connect(str(self.db_path), read_only=False)

        self.logger.info(f"[DuckDBStorage] Connected: db={self.db_path}, fresh_start={fresh_start}")

    # ------------------------------------------------------------------
    # Schema 
    # ------------------------------------------------------------------
    def initialize_schema(self):
        schema_path = Path("schema/trading_schema.sql")
        if not schema_path.exists():
            self.logger.warning("[DuckDBStorage] Schema file missing — skipping")
            return

        try:
            sql = schema_path.read_text()
            stmts = [s.strip() for s in sql.split(";") if s.strip()]

            # Schema changes use write lock
            with self.write_lock:
                for s in stmts:
                    try:
                        self.conn.execute(s)
                    except Exception as e:
                        if "already exists" not in str(e).lower():
                            self.logger.error(f"[DuckDBStorage] Schema error: {e}")

            self.logger.info("[DuckDBStorage] Schema initialized")
            
            # Truncate tables if fresh_start is enabled
            if self.fresh_start:
                self._truncate_tables()

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] Schema load failed: {e}")
            raise

    def _truncate_tables(self):
        """Truncate all candle data for a fresh start."""
        with self.write_lock:
            self.conn.execute("DELETE FROM market.candles")
        self.logger.info("[DuckDBStorage] Tables truncated (fresh_start=true)")

    # ------------------------------------------------------------------
    # Helper
    # ------------------------------------------------------------------

    def get_last_timestamp(self, symbol: str, timeframe: str) -> Optional[int]:
        c = self.get_last_candle(symbol, timeframe)
        return c["ts"] if c else None

    # ------------------------------------------------------------------
    # Query: last candle (NO LOCK - DuckDB MVCC handles concurrency)
    # ------------------------------------------------------------------
    def get_last_candle(self, symbol: str, timeframe: str) -> Optional[Dict[str, Any]]:
        try:
            row = self.conn.execute(
                """
                SELECT symbol, timeframe, open_ts, close_ts,
                       open, high, low, close, volume, is_closed
                FROM market.candles
                WHERE symbol = ? AND timeframe = ?
                ORDER BY open_ts DESC
                LIMIT 1
                """,
                [symbol, timeframe]
            ).fetchone()

            if not row:
                return None

            return {
                "symbol": row[0],
                "timeframe": row[1],
                "ts": row[2],
                "open_ts": row[2],
                "close_ts": row[3],
                "open": row[4],
                "high": row[5],
                "low": row[6],
                "close": row[7],
                "volume": row[8],
                "is_closed": bool(row[9]),
            }

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] get_last_candle: {e}")
            return None

    # ------------------------------------------------------------------
    # Load full window (NO LOCK - DuckDB MVCC handles concurrency)
    # ------------------------------------------------------------------
    def load(self, symbol: str, timeframe: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load candles for symbol+timeframe in ascending order.
        
        Parameters:
        ----------
        limit : Optional[int]
            If specified, load only the most recent N candles.
            If None, load all candles.
        """
        try:
            # Build query with optional LIMIT
            query = """
                SELECT symbol, timeframe, open_ts, close_ts,
                       open, high, low, close, volume, is_closed
                FROM market.candles
                WHERE symbol = ? AND timeframe = ?
                ORDER BY open_ts DESC
            """
            
            if limit is not None:
                query += f" LIMIT {limit}"
            
            rows = self.conn.execute(query, [symbol, timeframe]).fetchall()
            
            # Reverse to get ascending order (oldest first)
            rows = list(reversed(rows))

            return [
                {
                    "symbol": r[0],
                    "timeframe": r[1],
                    "ts": r[2],
                    "open_ts": r[2],
                    "close_ts": r[3],
                    "open": r[4],
                    "high": r[5],
                    "low": r[6],
                    "close": r[7],
                    "volume": r[8],
                    "is_closed": bool(r[9]),
                }
                for r in rows
            ]

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] load: {e}")
            return []

    # ------------------------------------------------------------------
    # Save single real-time candle (WRITE - locked for safety)
    # ------------------------------------------------------------------
    def save_candle(self, symbol: str, timeframe: str, c: Dict[str, Any]):
        try:
            open_ts = int(c.get("open_ts", c["ts"]))
            close_ts = int(c.get("close_ts", open_ts))

            is_closed = bool(c.get("closed", True))

            with self.write_lock:
                self.conn.execute(
                    """
                    INSERT INTO market.candles
                    (symbol, timeframe, open_ts, close_ts,
                     open, high, low, close, volume,
                     is_closed, source, received_at, trading_day)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP,
                            CAST(to_timestamp(?) AS DATE))
                    ON CONFLICT(symbol, timeframe, open_ts) DO NOTHING
                    """,
                    [
                        symbol,
                        timeframe,
                        open_ts,
                        close_ts,
                        float(c["open"]),
                        float(c["high"]),
                        float(c["low"]),
                        float(c["close"]),
                        float(c["volume"]),
                        is_closed,
                        "ws",
                        open_ts,
                    ],
                )

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] save_candle: {e}")

    def save_candle_async(self, symbol: str, timeframe: str, c: Dict[str, Any]):
        self.executor.submit(self.save_candle, symbol, timeframe, c)

    # ------------------------------------------------------------------
    # Bulk insert (WRITE - locked for safety)
    # ------------------------------------------------------------------
    def bulk_insert_candles(self, symbol: str, timeframe: str, candles, source="rest"):
        if not candles:
            return 0

        try:
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "open_ts": int(c.get("open_ts", c["ts"])),
                        "close_ts": int(c.get("close_ts", c.get("open_ts", c["ts"]))),
                        "open": float(c["open"]),
                        "high": float(c["high"]),
                        "low": float(c["low"]),
                        "close": float(c["close"]),
                        "volume": float(c["volume"]),
                        "is_closed": bool(c.get("closed", True)),
                        "source": source,
                    }
                    for c in candles
                ]
            )

            with self.write_lock:
                self.conn.register("tmp_df", df)
                self.conn.execute(
                    """
                    INSERT INTO market.candles
                    (symbol, timeframe, open_ts, close_ts,
                     open, high, low, close, volume,
                     is_closed, source, received_at, trading_day)
                    SELECT symbol, timeframe, open_ts, close_ts,
                           open, high, low, close, volume,
                           is_closed, source, CURRENT_TIMESTAMP,
                           CAST(to_timestamp(open_ts) AS DATE)
                    FROM tmp_df
                    ON CONFLICT(symbol, timeframe, open_ts) DO NOTHING
                    """
                )

            return len(df)

        except Exception as e:
            self.logger.error(f"[DuckDBStorage] bulk_insert_candles: {e}")
            return 0

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def shutdown(self):
        if self.executor:
            self.executor.shutdown(wait=True)

        if self.conn:
            self.conn.close()
            self.logger.info("[DuckDBStorage] Shutdown complete")
