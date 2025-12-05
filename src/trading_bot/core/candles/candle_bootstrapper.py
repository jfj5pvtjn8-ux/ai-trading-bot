"""
CandleBootstrapper: Initial Data Loading & Synchronization

Responsibilities:
-----------------
• Load historical candles into DuckDB on bot startup
• Handle fresh start mode (clear + reload)
• Handle incremental mode (gap detection + filling)
• Load candles from DuckDB into memory (CandleManager sliding windows)
• Progress tracking and error handling
• Validate data integrity before handing off to bot

This module separates data bootstrapping concerns from bot orchestration,
making the codebase more maintainable and testable.
"""

from __future__ import annotations
from typing import Dict, List, Any
from trading_bot.core.logger import get_logger
from trading_bot.validators.candles_validator import CandlesValidator


class CandleBootstrapper:
    def __init__(self, config, storage, rest_client, candle_managers):
        """
        Parameters:
        ----------
        config          → AppConfig
        storage         → DuckDBStorage
        rest_client     → RestClient
        candle_managers → Dict[(symbol, tf)] = CandleManager
        """
        self.config = config
        self.storage = storage
        self.rest = rest_client
        self.cm_map = candle_managers
        self.fresh_start = config.duckdb.fresh_start
        self.logger = get_logger(__name__)

    def run(self) -> bool:
        """
        Main entry point for bootstrapping.
        1. Fetch initial historical candles from exchange (if needed)
        2. Load candles from DuckDB into memory (CandleManager)
        
        Returns:
        -------
        bool: True if successful, False otherwise
        """
        try:
            # Step 1: Fetch initial historical data if database is empty or fresh_start
            if not self.fetch_initial_candles():
                return False
            
            # Step 2: Load from DB into memory
            return self.load_memory_windows()
        except Exception as e:
            self.logger.error(f"[Bootstrap] Failed: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------
    # Fetch initial historical candles
    # -------------------------------------------------------------
    def fetch_initial_candles(self) -> bool:
        """
        Fetch initial historical candles from exchange and store in DuckDB.
        Uses the initial_candles configuration from app.yml.
        
        Returns:
        -------
        bool: True if successful, False otherwise
        """
        self.logger.info("\n=== Fetching Initial Historical Candles ===")
        
        try:
            for key, cm in self.cm_map.items():
                symbol, tf = key
                
                # Check if we already have data for this symbol+timeframe
                last_candle = self.storage.get_last_candle(symbol, tf)
                
                if last_candle and not self.fresh_start:
                    self.logger.info(
                        f"  ✓ Skipping {symbol} {tf} - already has data "
                        f"(last: {last_candle['ts']})"
                    )
                    continue
                
                # Get the number of candles to fetch from config
                limit = self.config.duckdb.get_initial_candles(tf, default=1000)
                
                self.logger.info(f"  → Fetching {limit} candles for {symbol} {tf}...")
                
                # Fetch historical candles from exchange
                candles = self.rest.fetch_klines(symbol, tf, limit=limit)
                
                if not candles:
                    self.logger.warning(f"  ⚠ No candles received for {symbol} {tf}")
                    continue
                
                # Drop last candle (may be incomplete) before storing in database
                # Database should only contain closed candles
                if candles:
                    dropped = candles[-1]
                    candles = candles[:-1]
                    self.logger.info(
                        f"  → Dropped last REST candle {dropped['ts']} "
                        f"(may be incomplete, WebSocket will provide closed version)"
                    )
                
                # Save to database (only closed candles)
                self.storage.bulk_insert_candles(symbol, tf, candles, source="rest")
                
                self.logger.info(
                    f"  ✓ Fetched and stored {len(candles)} closed candles for {symbol} {tf}"
                )
            
            self.logger.info("\n✓ Initial candles fetch complete")
            
            # Validate data integrity (check for gaps)
            self._validate_fetched_data()
            
            return True
            
        except Exception as e:
            self.logger.error(f"[Initial Fetch] Failed: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------
    # Validate fetched data - COMPREHENSIVE CHECK
    # -------------------------------------------------------------
    def _validate_fetched_data(self):
        """
        Run COMPREHENSIVE validation on fetched candle data.
        Checks for: duplicates, gaps, misalignment, invalid data, NULL values.
        Auto-remediates critical issues (duplicates).
        Logs warnings for non-critical issues.
        """
        try:
            self.logger.info("\n=== Comprehensive Data Validation ===")
            
            # Get list of symbols and timeframes from candle managers
            symbols = list(set(key[0] for key in self.cm_map.keys()))
            timeframes = list(set(key[1] for key in self.cm_map.keys()))
            
            # Initialize validator
            validator = CandlesValidator(self.config.duckdb.database_path)
            
            # Run FULL validation (all checks) - pass storage connection to avoid conflicts
            report = validator.validate_all(
                symbols=symbols, 
                timeframes=timeframes,
                conn=self.storage.conn
            )
            
            # Check for duplicates (CRITICAL - auto-remediate)
            duplicates = report.get_issues_by_type('duplicate')
            if duplicates:
                self.logger.error(f"  ⚠ CRITICAL: Found {len(duplicates)} duplicate candles")
                self.logger.info("  → Auto-remediating: Removing duplicates...")
                removed = validator.remove_duplicates()
                self.logger.info(f"  ✓ Removed {removed} duplicate candles")
            
            # Check for invalid OHLCV data (CRITICAL)
            invalid_data = report.get_issues_by_type('invalid_data')
            if invalid_data:
                self.logger.error(
                    f"  ⚠ CRITICAL: Found {len(invalid_data)} candles with invalid OHLCV data"
                )
                for issue in invalid_data[:5]:
                    self.logger.error(f"    - {issue.symbol} {issue.timeframe}: {issue.description}")
            
            # Check for NULL values (CRITICAL)
            null_values = report.get_issues_by_type('null_value')
            if null_values:
                self.logger.error(
                    f"  ⚠ CRITICAL: Found {len(null_values)} candles with NULL values"
                )
            
            # Check for gaps (WARNING)
            gaps = report.get_issues_by_type('gap')
            if gaps:
                self.logger.warning(f"  ⚠ Found {len(gaps)} gap(s) in data:")
                critical_gaps = [g for g in gaps if g.severity == 'critical']
                
                for issue in gaps[:5]:  # Show first 5 gaps
                    missing = issue.details.get('missing_candles', 0)
                    gap_hours = issue.details.get('gap_duration_hours', 0)
                    self.logger.warning(
                        f"    - {issue.symbol} {issue.timeframe}: "
                        f"{missing} missing candle(s), {gap_hours}h gap"
                    )
                
                if len(gaps) > 5:
                    self.logger.warning(f"    ... and {len(gaps) - 5} more gaps")
                
                if critical_gaps:
                    self.logger.error(
                        f"  ⚠ {len(critical_gaps)} critical gap(s) detected (>5 missing candles)"
                    )
            
            # Check for misalignment (WARNING)
            misaligned = report.get_issues_by_type('misaligned')
            if misaligned:
                self.logger.warning(
                    f"  ⚠ Found {len(misaligned)} misaligned candles "
                    "(timestamps not on interval boundaries)"
                )
            
            # Check for invalid timestamps (WARNING/CRITICAL)
            invalid_ts = report.get_issues_by_type('invalid_timestamp')
            if invalid_ts:
                critical_ts = [t for t in invalid_ts if t.severity == 'critical']
                if critical_ts:
                    self.logger.error(
                        f"  ⚠ CRITICAL: Found {len(critical_ts)} candles with "
                        "close_ts <= open_ts"
                    )
            
            # Summary
            if report.has_critical_issues():
                self.logger.error(
                    f"\n  ⚠ VALIDATION SUMMARY: {len(report.issues)} total issues found "
                    f"({len([i for i in report.issues if i.severity == 'critical'])} critical)"
                )
            elif len(report.issues) > 0:
                self.logger.warning(
                    f"\n  ⚠ VALIDATION SUMMARY: {len(report.issues)} issues found (non-critical)"
                )
            else:
                self.logger.info("  ✓ No data integrity issues detected - All checks passed!")
            
        except Exception as e:
            self.logger.warning(f"[Validation] Failed to validate data: {e}", exc_info=True)
            # Don't fail bootstrap on validation errors

    # -------------------------------------------------------------
    # Load memory window for CandleManager
    # -------------------------------------------------------------
    def load_memory_windows(self) -> bool:
        """
        Loads all candles from DuckDB into CandleManager memory.

        Handles the critical "drop last candle" logic in incremental mode.
        """
        self.logger.info("\n=== Loading Sliding Windows into Memory ===")

        try:
            for key, cm in self.cm_map.items():
                symbol, tf = key
                
                # Load exactly the configured amount from database
                # Database only contains closed candles (last one was dropped before storing)
                limit = cm._candles.maxlen
                candles = self.storage.load(symbol, tf, limit=limit)

                cm.load_from_list(candles)
                
                # Get actual count in memory
                actual_loaded = cm.size()
                
                # Log loaded count (should match config, no truncation)
                self.logger.info(
                    f"  ✓ Loaded {actual_loaded} candles into memory for {symbol} {tf} "
                    f"(config limit: {cm._candles.maxlen})"
                )

            self.logger.info("\n✓ All candles loaded into memory successfully")
            return True

        except Exception as e:
            self.logger.error(f"[Memory Load] Failed: {e}")
            return False


__all__ = ["CandleBootstrapper"]
