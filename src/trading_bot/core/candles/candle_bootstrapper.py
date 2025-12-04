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

from typing import List, Optional, Dict, Tuple
from trading_bot.core.logger import get_logger
from trading_bot.config.symbols.models import SymbolsConfig, SymbolConfig
from trading_bot.config.app.models import AppConfig
from trading_bot.api.rest_client import RestClient
from trading_bot.storage.duckdb_storage import DuckDBStorage
from trading_bot.core.candles.candle_manager import CandleManager


class CandleBootstrapper:
    """
    Handles initial data loading for all symbols/timeframes.
    
    Supports two modes:
    - Fresh Start: Clear DB → Load initial_candles from config
    - Incremental: Check gaps → Fill or reload as needed
    """

    def __init__(self, app_config: AppConfig, rest_client: RestClient):
        """
        Initialize bootstrapper.
        
        Args:
            app_config: Application configuration
            rest_client: REST client for fetching historical data
        """
        self.logger = get_logger(__name__)
        self.app_config = app_config
        self.rest_client = rest_client

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def load_all_symbols(
        self,
        symbols_config: SymbolsConfig,
        duckdb_storage: DuckDBStorage,
        fresh_start: bool,
        max_gap_hours: float,
    ) -> bool:
        """
        Load initial candles for all enabled symbols into DuckDB.
        
        Args:
            symbols_config: Symbols configuration with enabled symbols
            duckdb_storage: DuckDB storage instance
            fresh_start: True = clear and reload, False = incremental
            max_gap_hours: Maximum gap (hours) before triggering full reload
            
        Returns:
            True if all symbols loaded successfully, False otherwise
        """
        enabled_symbols = [s for s in symbols_config.symbols if s.enabled]
        
        if not enabled_symbols:
            self.logger.error("[CandleBootstrapper] No enabled symbols found")
            return False

        # Calculate total tasks for progress tracking
        total_tasks = sum(len(s.timeframes) for s in enabled_symbols)
        
        self.logger.info(
            f"[CandleBootstrapper] Starting data load for {len(enabled_symbols)} symbols, "
            f"{total_tasks} symbol/timeframe pairs"
        )

        if fresh_start:
            return self._handle_fresh_start_mode(
                enabled_symbols, duckdb_storage, total_tasks
            )
        else:
            return self._handle_incremental_mode(
                enabled_symbols, duckdb_storage, max_gap_hours, total_tasks
            )

    # =========================================================================
    # FRESH START MODE
    # =========================================================================

    def _handle_fresh_start_mode(
        self,
        enabled_symbols: List[SymbolConfig],
        duckdb_storage: DuckDBStorage,
        total_tasks: int,
    ) -> bool:
        """
        Fresh start: Clear all data and load initial candles.
        
        Args:
            enabled_symbols: List of enabled symbol configurations
            duckdb_storage: DuckDB storage instance
            total_tasks: Total number of symbol/timeframe pairs
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("[CandleBootstrapper] Fresh start mode - clearing all data")
        duckdb_storage.clear_all_candles()
        
        current_task = 0

        for sym_cfg in enabled_symbols:
            symbol = sym_cfg.name
            
            for tf_cfg in sym_cfg.timeframes:
                tf = tf_cfg.tf
                current_task += 1
                
                self.logger.info(
                    f"\n[CandleBootstrapper] [{current_task}/{total_tasks}] "
                    f"Processing {symbol} {tf}..."
                )
                
                # Load initial candles from config
                count = self.app_config.duckdb.get_initial_candles(tf)
                self.logger.info(f"  → Loading {count} candles from API...")
                
                success = duckdb_storage.backward_fill_gap(
                    symbol, tf, self.rest_client, count
                )
                
                if not success:
                    self.logger.error(
                        f"  ✗ Failed to load candles for {symbol} {tf}"
                    )
                    return False
                
                # Show progress
                progress_pct = (current_task / total_tasks) * 100
                self.logger.info(f"  Progress: {progress_pct:.1f}% complete")

        return True

    # =========================================================================
    # INCREMENTAL MODE
    # =========================================================================

    def _handle_incremental_mode(
        self,
        enabled_symbols: List[SymbolConfig],
        duckdb_storage: DuckDBStorage,
        max_gap_hours: float,
        total_tasks: int,
    ) -> bool:
        """
        Incremental mode: Check existing data and fill gaps as needed.
        
        Args:
            enabled_symbols: List of enabled symbol configurations
            duckdb_storage: DuckDB storage instance
            max_gap_hours: Maximum gap (hours) before triggering full reload
            total_tasks: Total number of symbol/timeframe pairs
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("[CandleBootstrapper] Incremental mode - checking existing data")
        
        current_task = 0

        for sym_cfg in enabled_symbols:
            symbol = sym_cfg.name
            
            for tf_cfg in sym_cfg.timeframes:
                tf = tf_cfg.tf
                current_task += 1
                
                self.logger.info(
                    f"\n[CandleBootstrapper] [{current_task}/{total_tasks}] "
                    f"Processing {symbol} {tf}..."
                )
                
                # Check last candle in DuckDB
                last_candle = duckdb_storage.get_last_candle(symbol, tf)
                
                if not last_candle:
                    # No data - load initial candles
                    if not self._load_initial_candles(
                        symbol, tf, duckdb_storage
                    ):
                        return False
                else:
                    # Have data - check for gaps
                    if not self._handle_existing_data(
                        symbol, tf, last_candle, duckdb_storage, max_gap_hours
                    ):
                        return False
                
                # Show progress
                progress_pct = (current_task / total_tasks) * 100
                self.logger.info(f"  Progress: {progress_pct:.1f}% complete")

        return True

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _load_initial_candles(
        self,
        symbol: str,
        timeframe: str,
        duckdb_storage: DuckDBStorage,
    ) -> bool:
        """
        Load initial candles for a symbol/timeframe with no existing data.
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe
            duckdb_storage: DuckDB storage instance
            
        Returns:
            True if successful, False otherwise
        """
        count = self.app_config.duckdb.get_initial_candles(timeframe)
        self.logger.info(f"  → No data found, loading {count} candles...")
        
        success = duckdb_storage.backward_fill_gap(
            symbol, timeframe, self.rest_client, count
        )
        
        if not success:
            self.logger.error(f"  ✗ Failed to load initial candles")
            return False
        
        return True

    def _handle_existing_data(
        self,
        symbol: str,
        timeframe: str,
        last_candle: dict,
        duckdb_storage: DuckDBStorage,
        max_gap_hours: float,
    ) -> bool:
        """
        Handle existing data: check for gaps and fill or reload as needed.
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe
            last_candle: Last candle from DuckDB
            duckdb_storage: DuckDB storage instance
            max_gap_hours: Maximum gap (hours) before triggering full reload
            
        Returns:
            True if successful, False otherwise
        """
        last_ts = last_candle['ts']
        
        # Get timeframe seconds for gap calculation
        tf_seconds = self.app_config.get_timeframe_seconds(timeframe)
        gap_candles = duckdb_storage.calculate_gap(last_ts, tf_seconds)
        
        if gap_candles == 0:
            self.logger.info(f"  ✓ Data up-to-date (last: {last_ts})")
            return True
        
        # Calculate gap hours
        gap_hours = (gap_candles * tf_seconds) / 3600
        
        # Gap detected
        self.logger.info(
            f"  → Gap detected: {gap_candles} candles "
            f"({gap_hours:.1f} hours)"
        )
        
        if gap_hours <= max_gap_hours:
            # Fill gap incrementally
            return self._fill_gap(
                symbol, timeframe, gap_candles, duckdb_storage
            )
        else:
            # Gap too large - full reload
            return self._full_reload(
                symbol, timeframe, duckdb_storage, gap_hours
            )

    def _fill_gap(
        self,
        symbol: str,
        timeframe: str,
        missing_count: int,
        duckdb_storage: DuckDBStorage,
    ) -> bool:
        """
        Fill a gap by fetching missing candles.
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe
            missing_count: Number of missing candles
            duckdb_storage: DuckDB storage instance
            
        Returns:
            True if successful, False otherwise
        """
        success = duckdb_storage.backward_fill_gap(
            symbol, timeframe, self.rest_client, missing_count
        )
        
        if success:
            self.logger.info(f"  ✓ Gap filled successfully")
        else:
            self.logger.warning(f"  ⚠ Gap fill incomplete")
        
        return success

    def _full_reload(
        self,
        symbol: str,
        timeframe: str,
        duckdb_storage: DuckDBStorage,
        gap_hours: float,
    ) -> bool:
        """
        Perform a full reload when gap is too large.
        
        Args:
            symbol: Trading pair
            timeframe: Timeframe
            duckdb_storage: DuckDB storage instance
            gap_hours: Gap size in hours
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.warning(
            f"  → Gap too large ({gap_hours:.1f}h > "
            f"{self.app_config.duckdb.max_gap_hours}h), full reload..."
        )
        
        count = self.app_config.duckdb.get_initial_candles(timeframe)
        duckdb_storage.delete_symbol_timeframe(symbol, timeframe)
        
        success = duckdb_storage.backward_fill_gap(
            symbol, timeframe, self.rest_client, count
        )
        
        if not success:
            self.logger.error(f"  ✗ Failed to reload data")
            return False
        
        return True

    # =========================================================================
    # MEMORY LOADING
    # =========================================================================

    def load_into_memory(
        self,
        candle_managers: Dict[Tuple[str, str], CandleManager],
        duckdb_storage: DuckDBStorage,
        symbols_config: SymbolsConfig,
    ) -> bool:
        """
        Load candles from DuckDB into CandleManager sliding windows.
        
        This is the final step of bootstrapping: DuckDB → Memory.
        
        Args:
            candle_managers: Dict of (symbol, timeframe) → CandleManager
            duckdb_storage: DuckDB storage instance
            symbols_config: Symbols configuration with fetch sizes
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("\nLoading candles into memory (sliding windows)...")
        
        try:
            enabled_symbols = [s for s in symbols_config.symbols if s.enabled]
            
            for sym_cfg in enabled_symbols:
                symbol = sym_cfg.name
                
                for tf_cfg in sym_cfg.timeframes:
                    tf = tf_cfg.tf
                    key = (symbol, tf)
                    
                    # Fetch candles from DuckDB for sliding window
                    window_size = tf_cfg.fetch  # From symbols.yml
                    
                    self.logger.info(f"  → Loading {window_size} candles for {symbol} {tf}...")
                    candles = duckdb_storage.load_candles_for_window(
                        symbol, tf, limit=window_size
                    )
                    
                    if not candles:
                        self.logger.warning(f"  ⚠ No candles found in DuckDB for {symbol} {tf}")
                        continue
                    
                    # Load candles into CandleManager's sliding window
                    cm = candle_managers[key]
                    cm.load_initial(candles)
                    
                    self.logger.info(
                        f"  ✓ Loaded {len(candles)} candles into memory for {symbol} {tf}"
                    )
            
            self.logger.info("\n✓ All candles loaded into memory")
            return True
            
        except Exception as e:
            self.logger.error(f"[Memory Load] Failed: {e}")
            return False


__all__ = ["CandleBootstrapper"]
