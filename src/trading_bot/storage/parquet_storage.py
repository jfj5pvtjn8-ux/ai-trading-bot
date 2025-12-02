"""
Parquet Storage for Candle Data

Handles:
- Real-time candle storage to daily parquet files
- Automatic daily rotation
- Auto-cleanup of files older than retention period
- Appends to existing files (preserves data on restart)
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List
from threading import Lock
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from trading_bot.core.logger import get_logger


class ParquetStorage:
    """
    Stores validated candles to parquet files with daily rotation.

    File structure:
        data/live/BTCUSDT_1m_2025-11-29.parquet
        data/live/BTCUSDT_5m_2025-11-29.parquet

    Features:
    - Append-only writes for each validated candle
    - Daily file rotation at midnight
    - Auto-cleanup of old files
    - Preserves existing data on restart (appends to today's files)
    """

    def __init__(
        self,
        base_path: str = "data/live",
        retention_days: int = 7,
        fresh_start: bool = True,
    ):
        """
        Args:
            base_path: Base directory for parquet files
            retention_days: Number of days to keep files (default 7)
            fresh_start: If True, delete today's files on startup (default True)
        """
        self.logger = get_logger(__name__)
        self.base_path = Path(base_path)
        self.archive_path = self.base_path.parent / "archive"
        self.retention_days = retention_days
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.date_rotated = False

        # Thread pool for async writes
        self.executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="parquet_writer"
        )

        # File locks per (symbol, timeframe, date) to prevent concurrent writes
        self.file_locks: Dict[tuple, Lock] = {}
        self.locks_lock = Lock()  # Lock for accessing file_locks dict

        # Create directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Optionally clean up today's files on startup
        if fresh_start:
            self._delete_todays_files()

        # Clean up old archive files only (live files get archived, not deleted)
        self._cleanup_old_archive_files()

        self.logger.info(
            f"[ParquetStorage] Initialized: path={self.base_path}, "
            f"retention={retention_days} days"
        )

    def _get_file_lock(self, symbol: str, timeframe: str, date: str) -> Lock:
        """Get or create a lock for a specific file."""
        key = (symbol, timeframe, date)
        with self.locks_lock:
            if key not in self.file_locks:
                self.file_locks[key] = Lock()
            return self.file_locks[key]

    # -------------------------------------------------------------------------
    # SINGLE CANDLE SAVE
    # -------------------------------------------------------------------------

    def save_candle(self, symbol: str, timeframe: str, candle: Dict[str, Any]):
        """
        Save a single validated candle to parquet file.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Timeframe (e.g., "1m", "5m")
            candle: Normalized candle dict with
                ts, open_ts, close_ts, open, high, low, close, volume
        """
        try:
            # Check if date has changed (midnight rollover)
            current_date = datetime.now().strftime("%Y-%m-%d")
            if current_date != self.current_date:
                self._rotate_to_new_date(current_date)

            # Get file lock to prevent concurrent writes
            file_lock = self._get_file_lock(symbol, timeframe, current_date)

            open_ts = int(candle.get("open_ts", candle["ts"]))
            close_ts = int(candle.get("close_ts", open_ts))

            with file_lock:
                filename = f"{symbol}_{timeframe}_{current_date}.parquet"
                filepath = self.base_path / filename

                dt_open = datetime.fromtimestamp(open_ts)

                # Prepare dataframe row
                df_new = pd.DataFrame(
                    [
                        {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            # master key and convenience columns
                            "timestamp": int(candle["ts"]),
                            "open_ts": open_ts,
                            "close_ts": close_ts,
                            "datetime": dt_open,
                            # OHLCV
                            "open": float(candle["open"]),
                            "high": float(candle["high"]),
                            "low": float(candle["low"]),
                            "close": float(candle["close"]),
                            "volume": float(candle["volume"]),
                            # meta
                            "received_at": int(datetime.now().timestamp()),
                        }
                    ]
                )

                # Append to existing file or create new
                if filepath.exists():
                    df_existing = pd.read_parquet(filepath)
                    df_combined = pd.concat(
                        [df_existing, df_new], ignore_index=True
                    )
                    df_combined.to_parquet(
                        filepath, index=False, compression="snappy"
                    )
                else:
                    df_new.to_parquet(
                        filepath, index=False, compression="snappy"
                    )
                    self.logger.info(
                        f"[ParquetStorage] Created new file: {filename}"
                    )

        except Exception as e:
            self.logger.error(f"[ParquetStorage] Failed to save candle: {e}")

    def save_candle_async(self, symbol: str, timeframe: str, candle: Dict[str, Any]):
        """Submit candle save to thread pool (non-blocking)."""
        self.executor.submit(self.save_candle, symbol, timeframe, candle)

    # -------------------------------------------------------------------------
    # BATCH SAVE (INITIAL LOAD)
    # -------------------------------------------------------------------------

    def save_candles_batch(
        self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]
    ) -> None:
        """Save multiple candles in a batch (used for initial load)."""
        if not candles:
            return

        try:
            # Check if date has changed (midnight rollover)
            current_date = datetime.now().strftime("%Y-%m-%d")
            if current_date != self.current_date:
                self._rotate_to_new_date(current_date)

            file_lock = self._get_file_lock(symbol, timeframe, current_date)

            rows = []
            now_ts = int(datetime.now().timestamp())

            for candle in candles:
                open_ts = int(candle.get("open_ts", candle["ts"]))
                close_ts = int(candle.get("close_ts", open_ts))
                dt_open = datetime.fromtimestamp(open_ts)

                rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp": int(candle["ts"]),
                        "open_ts": open_ts,
                        "close_ts": close_ts,
                        "datetime": dt_open,
                        "open": float(candle["open"]),
                        "high": float(candle["high"]),
                        "low": float(candle["low"]),
                        "close": float(candle["close"]),
                        "volume": float(candle["volume"]),
                        "received_at": now_ts,
                    }
                )

            with file_lock:
                filename = f"{symbol}_{timeframe}_{current_date}.parquet"
                filepath = self.base_path / filename

                df_new = pd.DataFrame(rows)

                # Append to existing file or create new
                if filepath.exists():
                    df_existing = pd.read_parquet(filepath)
                    df_combined = pd.concat(
                        [df_existing, df_new], ignore_index=True
                    )
                    df_combined.to_parquet(
                        filepath, index=False, compression="snappy"
                    )
                else:
                    df_new.to_parquet(
                        filepath, index=False, compression="snappy"
                    )
                    self.logger.info(
                        f"[ParquetStorage] Created new file: {filename}"
                    )

                self.logger.debug(
                    f"[ParquetStorage] Batch saved {len(candles)} candles to {filename}"
                )

        except Exception as e:
            self.logger.error(
                f"[ParquetStorage] Failed to batch save candles: {e}"
            )

    def save_candles_batch_async(
        self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]
    ):
        """Submit batch save to thread pool (non-blocking)."""
        self.executor.submit(
            self.save_candles_batch, symbol, timeframe, candles
        )

    # -------------------------------------------------------------------------
    # DATE ROTATION / CLEANUP
    # -------------------------------------------------------------------------

    def _rotate_to_new_date(self, new_date: str):
        """Archive yesterday's files and prepare for new date."""
        old_date = self.current_date

        try:
            # Archive all files from old date
            pattern = f"*_{old_date}.parquet"
            archived_count = 0

            for filepath in self.base_path.glob(pattern):
                # Create archive directory for old date
                archive_dir = self.archive_path / old_date
                archive_dir.mkdir(parents=True, exist_ok=True)

                # Move file to archive
                import shutil

                dest_path = archive_dir / filepath.name
                shutil.move(str(filepath), str(dest_path))
                archived_count += 1

            if archived_count > 0:
                self.logger.info(
                    f"[ParquetStorage] Date rotated: archived {archived_count} files "
                    f"from {old_date} to archive/{old_date}/"
                )

            # Cleanup old archives
            self._cleanup_old_archive_files()

            # Update current date
            self.current_date = new_date

        except Exception as e:
            self.logger.error(
                f"[ParquetStorage] Failed to rotate to new date: {e}"
            )

    def _delete_todays_files(self):
        """Delete today's parquet files on startup for fresh start."""
        today = datetime.now().strftime("%Y-%m-%d")
        pattern = f"*_{today}.parquet"

        deleted_count = 0
        for filepath in self.base_path.glob(pattern):
            try:
                filepath.unlink()
                deleted_count += 1
                self.logger.info(
                    f"[ParquetStorage] Deleted today's file: {filepath.name}"
                )
            except Exception as e:
                self.logger.error(
                    f"[ParquetStorage] Failed to delete {filepath.name}: {e}"
                )

        if deleted_count > 0:
            self.logger.info(
                f"[ParquetStorage] Fresh start: deleted {deleted_count} today's files"
            )

    def _cleanup_old_files(self):
        """Remove parquet files older than retention period."""
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        deleted_count = 0
        for filepath in self.base_path.glob("*.parquet"):
            try:
                # Extract date from filename (e.g., BTCUSDT_1m_2025-11-29.parquet)
                parts = filepath.stem.split("_")
                if len(parts) >= 3:
                    file_date = parts[-1]  # Last part is date

                    if file_date < cutoff_str:
                        filepath.unlink()
                        deleted_count += 1
                        self.logger.debug(
                            f"[ParquetStorage] Deleted old file: {filepath.name}"
                        )
            except Exception as e:
                self.logger.error(
                    f"[ParquetStorage] Failed to cleanup {filepath.name}: {e}"
                )

        if deleted_count > 0:
            self.logger.info(
                f"[ParquetStorage] Cleaned up {deleted_count} old files from live"
            )

    def _cleanup_old_archive_files(self):
        """Remove archive directories older than retention period."""
        if not self.archive_path.exists():
            return

        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        deleted_dirs = 0
        deleted_files = 0

        # Iterate through date directories in archive
        for date_dir in self.archive_path.iterdir():
            if not date_dir.is_dir():
                continue

            try:
                # Check if directory name is a date (YYYY-MM-DD format)
                dir_date = date_dir.name

                if dir_date < cutoff_str:
                    # Count files before deletion
                    file_count = len(list(date_dir.glob("*.parquet")))

                    # Delete the entire directory
                    import shutil

                    shutil.rmtree(date_dir)

                    deleted_dirs += 1
                    deleted_files += file_count
                    self.logger.debug(
                        f"[ParquetStorage] Deleted archive dir: {dir_date} "
                        f"({file_count} files)"
                    )
            except Exception as e:
                self.logger.error(
                    f"[ParquetStorage] Failed to cleanup archive {date_dir.name}: {e}"
                )

        if deleted_dirs > 0:
            self.logger.info(
                f"[ParquetStorage] Cleaned up {deleted_dirs} archive directories "
                f"({deleted_files} files) older than {self.retention_days} days"
            )

    # -------------------------------------------------------------------------
    # READBACK
    # -------------------------------------------------------------------------

    def get_todays_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """
        Retrieve today's candles from parquet file.

        Args:
            symbol: Trading pair
            timeframe: Timeframe

        Returns:
            DataFrame with today's candles or empty DataFrame
        """
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{symbol}_{timeframe}_{today}.parquet"
        filepath = self.base_path / filename

        if filepath.exists():
            return pd.read_parquet(filepath)
        return pd.DataFrame()

    # -------------------------------------------------------------------------

    def shutdown(self):
        """Shutdown the thread pool executor."""
        if self.executor:
            self.executor.shutdown(wait=True)
            self.logger.debug("[ParquetStorage] Executor shutdown complete")


__all__ = ["ParquetStorage"]
