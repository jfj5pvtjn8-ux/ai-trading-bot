"""
Multi-Timeframe Symbol Manager (MTF Symbol Manager)

Orchestrates all components for a single trading symbol across multiple timeframes:
- CandleManager per timeframe (storage)
- CandleSync per timeframe (validation & gap filling)
- LiquidityMap per timeframe (optional, for decision layer)
- Coordinates initial data loading
- Routes WebSocket candles to appropriate CandleSync with TF priority
- 1m is the master timeframe that drives the system
- Higher TF candles are processed before 1m when they close simultaneously
- Notifies decision layer when candles are ready
"""

import queue
import threading
from typing import Dict, Optional, Callable, Any, List, Tuple
from trading_bot.core.logger import get_symbol_logger
from trading_bot.config.symbols.models import SymbolConfig
from trading_bot.config.app.models import AppConfig
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync
from trading_bot.core.candles.initial_candles_loader import InitialCandlesLoader
from trading_bot.api.rest_client import RestClient
from trading_bot.decision_layer.liquidity_map import LiquidityMap
from trading_bot.decision_layer.trend_detector import TrendFusion
from trading_bot.decision_layer.tf_config import get_timeframe_config


class MultiTFSymbolManager:
    """
    Manages all timeframes for a single symbol with TF priority processing.
    
    Responsibilities:
    - Create and manage CandleManager for each timeframe
    - Create and manage CandleSync for each timeframe
    - Initialize with historical data via InitialCandlesLoader
    - Route incoming WebSocket candles with PRIORITY based on TF
    - Process higher TF before lower TF when candles close simultaneously
    - 1m is the master timeframe that drives the entire system
    - Track initialization status
    - Provide access to candle data for decision layer
    
    TF Priority (highest to lowest):
    1h → 15m → 5m → 1m (master)
    
    When multiple TFs close at the same time, they are queued and processed
    in priority order, with 1m always processed last.
    """

    def __init__(
        self,
        symbol_cfg: SymbolConfig,
        app_config: AppConfig,
        rest_client: RestClient,
        storage = None
    ):
        """
        Args:
            symbol_cfg: Symbol configuration with timeframes
            app_config: Application configuration
            rest_client: REST client for fetching historical data
            storage: ParquetStorage instance for saving candles (optional)
        """
        self.symbol = symbol_cfg.name
        self.logger = get_symbol_logger(self.symbol)  # Symbol-specific logger
        self.symbol_cfg = symbol_cfg
        self.app_config = app_config
        self.rest_client = rest_client
        self.storage = storage
        
        # Component storage
        self.candle_managers: Dict[str, CandleManager] = {}
        self.candle_syncs: Dict[str, CandleSync] = {}
        
        # Decision layer components (one per timeframe)
        self.liquidity_maps: Dict[str, LiquidityMap] = {}
        self.trend_fusion: Optional[TrendFusion] = None
        
        # State tracking
        self.is_initialized = False
        self.initialization_errors: Dict[str, str] = {}
        
        # Callbacks for decision layer
        self.on_candle_callback: Optional[Callable[[str, str, Dict], None]] = None
        
        # TF Priority Queue System
        # Higher timeframes have higher priority, 1m (master) has lowest
        self.tf_priority_map = self._build_tf_priority_map()
        self.candle_queue: queue.PriorityQueue = queue.PriorityQueue()
        self.processing_lock = threading.Lock()
        self.is_processing = False
        
        # Initialize components
        self._setup_components()
        
        # Initialize decision layer (professional-grade)
        self._setup_liquidity_maps()  # One map per timeframe
        self._setup_trend_fusion()

    # -------------------------------------------------------------------------
    # TIMEFRAME PRIORITY SYSTEM
    # -------------------------------------------------------------------------

    def _build_tf_priority_map(self) -> Dict[str, int]:
        """
        Build priority map for timeframes.
        
        Priority (lower number = higher priority):
        1h  → priority 1 (highest)
        15m → priority 2
        5m  → priority 3
        1m  → priority 4 (lowest/master - drives the system)
        
        Returns:
            Dictionary mapping timeframe to priority number
        """
        # Standard TF priority order
        priority_order = ["1h", "15m", "5m", "1m"]
        
        # Build map from symbol config
        tf_priority = {}
        
        for tf_cfg in self.symbol_cfg.timeframes:
            tf = tf_cfg.tf
            
            # Assign priority based on standard order
            if tf in priority_order:
                tf_priority[tf] = priority_order.index(tf) + 1
            else:
                # Unknown TF gets low priority (before 1m)
                tf_priority[tf] = 99
        
        self.logger.info(f"TF priority map: {tf_priority}")
        return tf_priority

    def _get_tf_priority(self, timeframe: str) -> int:
        """Get priority for a timeframe (lower = higher priority)."""
        return self.tf_priority_map.get(timeframe, 999)

    # -------------------------------------------------------------------------
    # INITIALIZATION
    # -------------------------------------------------------------------------

    def _setup_components(self):
        """Create CandleManager and CandleSync for each timeframe."""
        self.logger.info(f"Setting up with {len(self.symbol_cfg.timeframes)} timeframes")
        
        for tf_cfg in self.symbol_cfg.timeframes:
            tf = tf_cfg.tf
            
            try:
                # Create CandleManager for this timeframe with TF-specific max_size
                candle_manager = CandleManager(max_size=tf_cfg.fetch)
                self.candle_managers[tf] = candle_manager
                
                # Create CandleSync for this timeframe
                candle_sync = CandleSync(
                    rest_client=self.rest_client,
                    symbol=self.symbol,
                    timeframe=tf,
                    candle_manager=candle_manager,
                    app_config=self.app_config
                )
                
                # Set callback to notify when valid candle is received
                candle_sync.set_callback(self._on_validated_candle)
                
                self.candle_syncs[tf] = candle_sync
                
                self.logger.debug(
                    f"{tf}: components created "
                    f"(max_size={tf_cfg.fetch})"
                )
                
            except Exception as e:
                self.logger.error(f"Failed to setup {tf}: {e}")
                self.initialization_errors[tf] = str(e)
    
    def _setup_liquidity_maps(self):
        """Initialize one independent liquidity map per timeframe with adaptive configs."""
        try:
            for tf_cfg in self.symbol_cfg.timeframes:
                tf = tf_cfg.tf
                
                # Get timeframe-specific configuration
                tf_config = get_timeframe_config(tf)
                
                # Each timeframe gets its OWN independent liquidity map with optimized parameters
                self.liquidity_maps[tf] = LiquidityMap(
                    symbol=self.symbol,
                    timeframes=[tf],  # Single timeframe per map
                    timeframe_config=tf_config,  # Adaptive parameters
                )
                
                self.logger.info(
                    f"✓ {tf} LiquidityMap: {tf_config.description} "
                    f"(pivot={tf_config.pivot_left}/{tf_config.pivot_right}, "
                    f"lookback={tf_config.lookback_candles}, "
                    f"vol_spike={tf_config.volume_spike_multiplier}x, "
                    f"atr_range={tf_config.atr_min_multiplier}-{tf_config.atr_max_multiplier})"
                )
            
            self.logger.info(f"✓ {len(self.liquidity_maps)} adaptive liquidity maps initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to setup liquidity maps: {e}")
            self.liquidity_maps = {}
    
    def _setup_trend_fusion(self):
        """Initialize trend fusion engine for this symbol."""
        try:
            timeframes = [tf_cfg.tf for tf_cfg in self.symbol_cfg.timeframes]
            
            self.trend_fusion = TrendFusion(
                symbol=self.symbol,
                timeframes=timeframes,
                ema_fast=20,
                ema_slow=50,
                rsi_period=14,
                structure_lookback=20,
            )
            
            self.logger.info(f"✓ Trend fusion engine initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to setup trend fusion: {e}")
            self.trend_fusion = None

    def load_initial_data(self) -> bool:
        """
        Load historical candles for all timeframes.
        
        Returns:
            True if initialization successful, False otherwise
        """
        if self.is_initialized:
            self.logger.warning(f"Already initialized")
            return True
        
        self.logger.info(f"Loading initial data")
        
        try:
            # Create initial loader
            loader = InitialCandlesLoader(
                app_config=self.app_config,
                rest_client=self.rest_client
            )
            
            # Load data for all timeframes
            success = loader.load_initial_for_symbol(
                symbol_cfg=self.symbol_cfg,
                candle_managers=self.candle_managers,
                candle_syncs=self.candle_syncs,
                liquidity_maps=self.liquidity_maps  # Pass dict of maps
            )
            
            if success:
                self.is_initialized = True
                self.logger.info(f"Initialization complete")
                
                # Save initial historical candles to parquet (batch mode for speed)
                if self.storage:
                    self.logger.info(f"Saving initial candles to parquet")
                    for tf in self.candle_managers:
                        candles = self.candle_managers[tf].get_all()
                        # Use batch save for much faster writes
                        self.storage.save_candles_batch(self.symbol, tf, candles)
                    self.logger.info(f"✓ Initial candles saved to parquet")
                
                # Log summary
                for tf in self.candle_managers:
                    count = len(self.candle_managers[tf].get_all())
                    last_ts = self.candle_managers[tf].last_timestamp()
                    self.logger.info(
                        f"{tf}: {count} candles, last_ts={last_ts}"
                    )
            else:
                self.logger.error(f"Initialization failed")
            
            return success
            
        except Exception as e:
            self.logger.exception(f"Error loading initial data: {e}")
            return False

    # -------------------------------------------------------------------------
    # WEBSOCKET ROUTING WITH PRIORITY QUEUE
    # -------------------------------------------------------------------------

    def on_ws_candle(self, timeframe: str, candle: Dict[str, Any]):
        """
        Route incoming WebSocket candle to priority queue for ordered processing.
        
        Called by WebSocketClient when a closed candle is received.
        Uses priority queue to ensure higher TF candles are processed first.
        
        Processing Order (when candles close simultaneously):
        1h → 15m → 5m → 1m (master)
        
        Args:
            timeframe: Timeframe of the candle (e.g., "1m", "5m")
            candle: Normalized candle dictionary
        """
        if timeframe not in self.candle_syncs:
            self.logger.warning(
                f"Received candle for unknown timeframe {timeframe}"
            )
            return
        
        if not self.is_initialized:
            self.logger.warning(
                f"Received candle before initialization complete. Buffering..."
            )
            return
        
        # Get priority for this timeframe
        priority = self._get_tf_priority(timeframe)
        
        # Add to priority queue (lower priority number = processed first)
        self.candle_queue.put((priority, timeframe, candle))
        
        self.logger.debug(
            f"{timeframe}: queued with priority {priority} "
            f"(queue_size={self.candle_queue.qsize()})"
        )
        
        # Schedule processing with small delay to batch simultaneous candles
        # This ensures multiple TFs closing at same time are collected before processing
        threading.Timer(0.1, self._process_candle_queue).start()

    def _process_candle_queue(self):
        """
        Process all queued candles in priority order.
        
        Ensures that when multiple TFs close simultaneously:
        1. Higher TF candles are processed first (1h, 15m, 5m)
        2. 1m (master) is always processed last
        3. Decision layer is notified in correct order
        
        Thread-safe to prevent concurrent processing.
        """
        # Prevent concurrent processing
        if not self.processing_lock.acquire(blocking=False):
            return
        
        try:
            self.is_processing = True
            processed_count = 0
            
            # Process all queued candles in priority order
            while not self.candle_queue.empty():
                try:
                    # Get highest priority candle (lowest number)
                    priority, timeframe, candle = self.candle_queue.get_nowait()
                    
                    self.logger.info(
                        f"{timeframe}: processing "
                        f"(priority={priority}, ts={candle['ts']})"
                    )
                    
                    # Route to CandleSync for validation
                    candle_sync = self.candle_syncs[timeframe]
                    candle_sync.on_ws_closed_candle(candle)
                    
                    processed_count += 1
                    
                except queue.Empty:
                    break
                except Exception as e:
                    self.logger.error(
                        f"Error processing queued candle: {e}"
                    )
            
            if processed_count > 0:
                self.logger.info(
                    f"Processed {processed_count} candles in priority order"
                )
        
        finally:
            self.is_processing = False
            self.processing_lock.release()

    def _on_validated_candle(self, timeframe: str, candle: Dict[str, Any]):
        """
        Internal callback from CandleSync when a candle passes validation.
        
        Called AFTER the candle has been validated and added to CandleManager.
        This maintains the priority order established by the queue.
        
        CRITICAL: This is where liquidity map refresh happens!
        - Each TF candle close triggers refresh for THAT TF only
        - No unnecessary recalculations
        - 1m (master) triggers after all higher TFs
        
        Args:
            timeframe: Timeframe of the validated candle
            candle: Validated candle dictionary
        """
        priority = self._get_tf_priority(timeframe)
        
        self.logger.info(
            f"{timeframe}: validated candle "
            f"ts={candle['ts']}, priority={priority}, close=${candle['close']:.2f}"
        )
        
        # =====================================================================
        # LIQUIDITY MAP REFRESH (EVENT-DRIVEN)
        # =====================================================================
        # Refresh liquidity zones for THIS specific timeframe's independent map
        if timeframe in self.liquidity_maps:
            try:
                liq_map = self.liquidity_maps[timeframe]
                candles = self.candle_managers[timeframe].get_all()
                current_price = candle["close"]
                
                updated = liq_map.on_candle_close(
                    timeframe=timeframe,
                    candles=candles,
                    current_price=current_price
                )
                
                if updated:
                    self.logger.debug(
                        f"{timeframe}: liquidity map refreshed (independent)"
                    )
                    
                    # =========================================================
                    # DETAILED LIQUIDITY MAP OUTPUT LOGGING
                    # =========================================================
                    self._log_liquidity_map_output(timeframe, liq_map, current_price)
                    
            except Exception as e:
                self.logger.error(
                    f"Error refreshing liquidity map for {timeframe}: {e}"
                )
        
        # =====================================================================
        # SPECIAL HANDLING FOR 1m MASTER TIMEFRAME
        # =====================================================================
        if timeframe == "1m":
            self.logger.debug(
                f"1m (MASTER) candle processed - triggering fusion analysis"
            )
            
            # Collect signals from all timeframe liquidity maps for fusion
            if self.liquidity_maps:
                try:
                    current_price = candle["close"]
                    
                    # Log each timeframe's liquidity state
                    for tf in ["1h", "15m", "5m", "1m"]:
                        if tf in self.liquidity_maps:
                            lm = self.liquidity_maps[tf]
                            
                            zones = lm.get_zones_for_timeframe(tf)
                            active_zones = [z for z in zones if z.is_active]
                            fvgs = lm.get_fvgs(timeframe=tf, only_unfilled=True)
                            
                            # Count zone types
                            support_zones = len([z for z in active_zones if z.zone_type in ["support", "demand"]])
                            resistance_zones = len([z for z in active_zones if z.zone_type in ["resistance", "supply"]])
                            
                            nearest_support = lm.get_nearest_support(current_price)
                            nearest_resistance = lm.get_nearest_resistance(current_price)
                            
                            # Format with distance from current price
                            if nearest_support:
                                dist = current_price - nearest_support.price_high
                                dist_pct = (dist / current_price) * 100
                                support_str = f"${nearest_support.price_high:.2f} (-{dist_pct:.2f}%)"
                            else:
                                support_str = "None"
                            
                            if nearest_resistance:
                                dist = nearest_resistance.price_low - current_price
                                dist_pct = (dist / current_price) * 100
                                resistance_str = f"${nearest_resistance.price_low:.2f} (+{dist_pct:.2f}%)"
                            else:
                                resistance_str = "None (price discovery)"
                            
                            self.logger.info(
                                f"[{tf}] Active: {len(active_zones)} ({support_zones}S/{resistance_zones}R), "
                                f"FVGs: {len(fvgs)}, Support: {support_str}, Resistance: {resistance_str}"
                            )
                    
                    # TODO: Implement fusion layer to combine signals from all TF maps
                    # This will produce final trading decision for 1m execution
                    
                except Exception as e:
                    self.logger.error(f"Error in 1m fusion analysis: {e}")
        
        # =====================================================================
        # TREND FUSION ANALYSIS
        # =====================================================================
        # Update trend state for this timeframe
        if self.trend_fusion:
            try:
                candles = self.candle_managers[timeframe].get_all()
                current_price = candle["close"]
                
                # Get this timeframe's liquidity map
                liq_map = self.liquidity_maps.get(timeframe)
                
                updated = self.trend_fusion.on_candle_close(
                    timeframe=timeframe,
                    candles=candles,
                    liquidity_map=liq_map,
                    current_price=current_price
                )
                
                if updated:
                    self.logger.debug(
                        f"{timeframe}: trend state updated"
                    )
                
                # If 1m (master), check for fusion signals
                if timeframe == "1m":
                    signal = self.trend_fusion.get_fusion_signal()
                    
                    if signal:
                        self.logger.info(
                            f"🎯 TRADE SIGNAL: {signal.signal_type} "
                            f"(confidence={signal.confidence:.2%}, "
                            f"aligned_tfs={len(signal.aligned_timeframes)}, "
                            f"zone=${signal.key_zone_price:.2f})"
                        )
                        
                        if signal.entry_price:
                            self.logger.info(
                                f"   Entry: ${signal.entry_price:.2f}, "
                                f"SL: ${signal.stop_loss:.2f}, "
                                f"TP: ${signal.take_profit:.2f}"
                            )
                    
                    # Log trend alignment status
                    if self.trend_fusion.is_aligned():
                        direction = self.trend_fusion.get_dominant_direction()
                        self.logger.info(
                            f"✓ MTF Trend Aligned: {direction.value if direction else 'unknown'}"
                        )
                
            except Exception as e:
                self.logger.error(f"Error in trend fusion for {timeframe}: {e}")
        
        # =====================================================================
        # NOTIFY DECISION LAYER
        # =====================================================================
        # Notify external decision layer if callback is set
        if self.on_candle_callback:
            try:
                self.on_candle_callback(self.symbol, timeframe, candle)
            except Exception as e:
                self.logger.error(
                    f"Error in on_candle_callback for {timeframe}: {e}"
                )

    # -------------------------------------------------------------------------
    # CALLBACK REGISTRATION
    # -------------------------------------------------------------------------

    def set_on_candle_callback(self, callback: Callable[[str, str, Dict], None]):
        """
        Register callback to be notified when validated candles are received.
        
        Args:
            callback: Function with signature callback(symbol, timeframe, candle)
        """
        self.on_candle_callback = callback
        self.logger.info(f"on_candle callback registered")

    # -------------------------------------------------------------------------
    # DATA ACCESS
    # -------------------------------------------------------------------------

    def get_candle_manager(self, timeframe: str) -> Optional[CandleManager]:
        """
        Get CandleManager for a specific timeframe.
        
        Args:
            timeframe: Timeframe to get manager for
            
        Returns:
            CandleManager instance or None if not found
        """
        return self.candle_managers.get(timeframe)

    def get_all_candle_managers(self) -> Dict[str, CandleManager]:
        """Get all CandleManagers keyed by timeframe."""
        return self.candle_managers.copy()

    def get_candle_sync(self, timeframe: str) -> Optional[CandleSync]:
        """
        Get CandleSync for a specific timeframe.
        
        Args:
            timeframe: Timeframe to get sync for
            
        Returns:
            CandleSync instance or None if not found
        """
        return self.candle_syncs.get(timeframe)

    def get_latest_candle(self, timeframe: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest candle for a timeframe.
        
        Args:
            timeframe: Timeframe to get candle from
            
        Returns:
            Latest candle dict or None
        """
        manager = self.get_candle_manager(timeframe)
        if manager:
            return manager.get_latest_candle()
        return None

    def get_candles(self, timeframe: str, count: Optional[int] = None) -> list:
        """
        Get candles for a timeframe.
        
        Args:
            timeframe: Timeframe to get candles from
            count: Number of recent candles to get (None = all)
            
        Returns:
            List of candle dictionaries
        """
        manager = self.get_candle_manager(timeframe)
        if not manager:
            return []
        
        all_candles = manager.get_all()
        
        if count is None:
            return all_candles
        
        return all_candles[-count:] if count > 0 else []

    def set_liquidity_map(self, timeframe: str, liquidity_map: Any):
        """
        Register a LiquidityMap for a timeframe.
        
        Args:
            timeframe: Timeframe to register for
            liquidity_map: LiquidityMap instance
        """
        self.liquidity_maps[timeframe] = liquidity_map
        self.logger.info(f"{timeframe}: LiquidityMap registered")

    def get_liquidity_map(self, timeframe: str) -> Optional[Any]:
        """
        Get LiquidityMap for a timeframe.
        
        Args:
            timeframe: Timeframe to get map for
            
        Returns:
            LiquidityMap instance or None
        """
        return self.liquidity_maps.get(timeframe)

    # -------------------------------------------------------------------------
    # STATUS & DIAGNOSTICS
    # -------------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """
        Get status summary of this symbol manager.
        
        Returns:
            Dictionary with status information
        """
        timeframe_status = {}
        
        for tf in self.candle_managers:
            manager = self.candle_managers[tf]
            sync = self.candle_syncs[tf]
            
            timeframe_status[tf] = {
                "priority": self._get_tf_priority(tf),
                "candle_count": len(manager.get_all()),
                "last_timestamp": manager.last_timestamp(),
                "latest_close": manager.get_latest_close(),
                "sync_last_ts": sync.last_closed_ts,
                "has_liquidity_map": tf in self.liquidity_maps,
                "is_master": tf == "1m"
            }
        
        return {
            "symbol": self.symbol,
            "is_initialized": self.is_initialized,
            "timeframe_count": len(self.candle_managers),
            "queue_size": self.candle_queue.qsize(),
            "is_processing": self.is_processing,
            "timeframes": timeframe_status,
            "initialization_errors": self.initialization_errors,
            "tf_priority_map": self.tf_priority_map
        }

    def get_summary(self) -> str:
        """Get human-readable status summary."""
        status = self.get_status()
        
        lines = [
            f"Symbol: {status['symbol']}",
            f"Initialized: {status['is_initialized']}",
            f"Timeframes: {status['timeframe_count']}",
            f"Queue Size: {status['queue_size']}",
            f"Processing: {status['is_processing']}"
        ]
        
        # Sort timeframes by priority for display
        sorted_tfs = sorted(
            status['timeframes'].items(),
            key=lambda x: x[1]['priority']
        )
        
        lines.append("\nTimeframes (by priority):")
        for tf, tf_status in sorted_tfs:
            master_flag = " [MASTER]" if tf_status['is_master'] else ""
            close_val = tf_status['latest_close'] if tf_status['latest_close'] else 0
            lines.append(
                f"  [{tf_status['priority']}] {tf}{master_flag}: "
                f"{tf_status['candle_count']} candles, "
                f"last_ts={tf_status['last_timestamp']}, "
                f"close=${close_val:.2f}"
            )
        
        if status['initialization_errors']:
            lines.append("\nErrors:")
            for tf, error in status['initialization_errors'].items():
                lines.append(f"  {tf}: {error}")
        
        return "\n".join(lines)

    # -------------------------------------------------------------------------
    # CLEANUP
    # -------------------------------------------------------------------------

    def shutdown(self):
        """Clean up resources."""
        self.logger.info(f"Shutting down")
        
        # Clear queue
        while not self.candle_queue.empty():
            try:
                self.candle_queue.get_nowait()
            except queue.Empty:
                break
        
        # Clear callbacks
        self.on_candle_callback = None
        
        # Clear references
        self.candle_managers.clear()
        self.candle_syncs.clear()
        self.liquidity_maps.clear()
        self.tf_priority_map.clear()
        
        self.is_initialized = False

    def __repr__(self) -> str:
        return (
            f"MultiTFSymbolManager(symbol={self.symbol}, "
            f"timeframes={len(self.candle_managers)}, "
            f"initialized={self.is_initialized}, "
            f"queue_size={self.candle_queue.qsize()})"
        )
    
    # -------------------------------------------------------------------------
    # LIQUIDITY MAP OUTPUT LOGGING
    # -------------------------------------------------------------------------
    
    def _log_liquidity_map_output(
        self,
        timeframe: str,
        liq_map: LiquidityMap,
        current_price: float
    ):
        """
        Log comprehensive LiquidityMap output for this timeframe.
        
        Includes: zones, FVGs, displacements, PD analysis, confluence, etc.
        """
        try:
            self.logger.info(f"{'='*60}")
            self.logger.info(f"[{timeframe}] LIQUIDITY MAP OUTPUT @ ${current_price:.2f}")
            self.logger.info(f"{'='*60}")
            
            # -----------------------------------------------------------------
            # ZONES
            # -----------------------------------------------------------------
            zones = liq_map.get_zones_for_timeframe(timeframe)
            active_zones = [z for z in zones if z.is_active]
            
            support_zones = [z for z in active_zones if z.zone_type in ["support", "demand"]]
            resistance_zones = [z for z in active_zones if z.zone_type in ["resistance", "supply"]]
            
            self.logger.info(
                f"[{timeframe}] ZONES: {len(active_zones)} active ({len(support_zones)}S/{len(resistance_zones)}R), "
                f"{len(zones)} total"
            )
            
            # Nearest support/resistance
            nearest_support = liq_map.get_nearest_support(current_price)
            nearest_resistance = liq_map.get_nearest_resistance(current_price)
            
            if nearest_support:
                dist = current_price - nearest_support.price_high
                dist_pct = (dist / current_price) * 100
                self.logger.info(
                    f"[{timeframe}]   Nearest Support: ${nearest_support.price_low:.2f}-${nearest_support.price_high:.2f} "
                    f"(-{dist_pct:.2f}%), strength={nearest_support.strength}, touches={nearest_support.touch_count}, "
                    f"pd={nearest_support.pd_position}"
                )
            else:
                self.logger.info(f"[{timeframe}]   Nearest Support: None")
            
            if nearest_resistance:
                dist = nearest_resistance.price_low - current_price
                dist_pct = (dist / current_price) * 100
                self.logger.info(
                    f"[{timeframe}]   Nearest Resistance: ${nearest_resistance.price_low:.2f}-${nearest_resistance.price_high:.2f} "
                    f"(+{dist_pct:.2f}%), strength={nearest_resistance.strength}, touches={nearest_resistance.touch_count}, "
                    f"pd={nearest_resistance.pd_position}"
                )
            else:
                self.logger.info(f"[{timeframe}]   Nearest Resistance: None (price discovery)")
            
            # -----------------------------------------------------------------
            # PREMIUM/DISCOUNT ANALYSIS
            # -----------------------------------------------------------------
            premium_zones = [z for z in active_zones if z.is_premium()]
            discount_zones = [z for z in active_zones if z.is_discount()]
            equilibrium_zones = [z for z in active_zones if z.is_equilibrium()]
            
            # Determine current price position
            if nearest_support and nearest_resistance:
                range_size = nearest_resistance.price_low - nearest_support.price_high
                price_in_range = current_price - nearest_support.price_high
                position_pct = (price_in_range / range_size * 100) if range_size > 0 else 50
                
                if position_pct > 55:
                    current_pd = "PREMIUM"
                elif position_pct < 45:
                    current_pd = "DISCOUNT"
                else:
                    current_pd = "EQUILIBRIUM"
            else:
                current_pd = "UNKNOWN"
            
            self.logger.info(
                f"[{timeframe}] PD ANALYSIS: Current={current_pd}, "
                f"Premium zones={len(premium_zones)}, Discount zones={len(discount_zones)}, "
                f"Equilibrium zones={len(equilibrium_zones)}"
            )
            
            # -----------------------------------------------------------------
            # FAIR VALUE GAPS (FVGs)
            # -----------------------------------------------------------------
            fvgs_all = liq_map.get_fvgs(timeframe=timeframe, only_unfilled=False)
            fvgs_unfilled = liq_map.get_fvgs(timeframe=timeframe, only_unfilled=True)
            fvgs_bullish = liq_map.get_fvgs(timeframe=timeframe, only_unfilled=True, fvg_type="bullish")
            fvgs_bearish = liq_map.get_fvgs(timeframe=timeframe, only_unfilled=True, fvg_type="bearish")
            
            self.logger.info(
                f"[{timeframe}] FVGs: {len(fvgs_unfilled)} unfilled ({len(fvgs_bullish)}B/{len(fvgs_bearish)}B), "
                f"{len(fvgs_all)} total"
            )
            
            # Nearest FVGs
            fvg_above = liq_map.get_nearest_fvg(current_price, direction="above", only_unfilled=True)
            fvg_below = liq_map.get_nearest_fvg(current_price, direction="below", only_unfilled=True)
            
            if fvg_above:
                dist_pct = ((fvg_above.gap_low - current_price) / current_price) * 100
                self.logger.info(
                    f"[{timeframe}]   FVG Above: {fvg_above.fvg_type.upper()} ${fvg_above.gap_low:.2f}-${fvg_above.gap_high:.2f} "
                    f"(+{dist_pct:.2f}%), size=${fvg_above.gap_size:.2f}, fill={fvg_above.fill_percentage:.0f}%"
                )
            
            if fvg_below:
                dist_pct = ((current_price - fvg_below.gap_high) / current_price) * 100
                self.logger.info(
                    f"[{timeframe}]   FVG Below: {fvg_below.fvg_type.upper()} ${fvg_below.gap_low:.2f}-${fvg_below.gap_high:.2f} "
                    f"(-{dist_pct:.2f}%), size=${fvg_below.gap_size:.2f}, fill={fvg_below.fill_percentage:.0f}%"
                )
            
            if not fvg_above and not fvg_below:
                self.logger.info(f"[{timeframe}]   No nearby unfilled FVGs")
            
            # -----------------------------------------------------------------
            # DISPLACEMENTS
            # -----------------------------------------------------------------
            displacements = liq_map.get_displacements(timeframe=timeframe)
            recent_disp = liq_map.get_recent_displacements(timeframe=timeframe, lookback=1)
            strongest_disp = liq_map.get_strongest_displacement(timeframe=timeframe, metric="move_pct")
            
            bullish_disp = [d for d in displacements if d.is_bullish]
            bearish_disp = [d for d in displacements if d.is_bearish]
            
            self.logger.info(
                f"[{timeframe}] DISPLACEMENTS: {len(displacements)} total ({len(bullish_disp)}B/{len(bearish_disp)}B)"
            )
            
            if recent_disp:
                d = recent_disp[0]
                self.logger.info(
                    f"[{timeframe}]   Recent: {d.direction.upper()} {d.num_candles} candles, "
                    f"${d.start_price:.2f}→${d.end_price:.2f} ({d.move_pct:+.2f}%), "
                    f"vol_surge={d.volume_surge_ratio:.2f}x"
                )
            
            # Only log strongest if it's different from recent (compare by timestamp)
            if strongest_disp and (not recent_disp or strongest_disp.end_ts != recent_disp[0].end_ts):
                self.logger.info(
                    f"[{timeframe}]   Strongest: {strongest_disp.direction.upper()} "
                    f"({strongest_disp.move_pct:+.2f}%), vol={strongest_disp.volume_surge_ratio:.2f}x"
                )
            
            # -----------------------------------------------------------------
            # CONFLUENCE ZONES (if multi-TF)
            # -----------------------------------------------------------------
            if len(self.liquidity_maps) > 1:
                confluence_zones = liq_map.get_confluence_zones(min_timeframes=2, min_strength="medium")
                
                if confluence_zones:
                    self.logger.info(
                        f"[{timeframe}] CONFLUENCE: {len(confluence_zones)} zones with multi-TF agreement"
                    )
                    
                    for i, zone in enumerate(confluence_zones[:3], 1):  # Top 3
                        dist_pct = abs(zone.midpoint - current_price) / current_price * 100
                        direction = "above" if zone.midpoint > current_price else "below"
                        
                        self.logger.info(
                            f"[{timeframe}]   #{i} {zone.zone_type.upper()} @ ${zone.price_low:.2f}-${zone.price_high:.2f} "
                            f"({direction} {dist_pct:.2f}%), {zone.confluence_count} TFs, strength={zone.strength}"
                        )
            
            # -----------------------------------------------------------------
            # STATISTICS
            # -----------------------------------------------------------------
            stats = liq_map.get_statistics()
            
            self.logger.info(
                f"[{timeframe}] STATS: zones_created={stats['total_zones_created']}, "
                f"zones_broken={stats['zones_broken']}"
            )
            
            self.logger.info(f"{'='*60}")
            
        except Exception as e:
            self.logger.error(f"[{timeframe}] Error logging liquidity map output: {e}")


__all__ = ["MultiTFSymbolManager"]
