"""
Liquidity Sweep Plugin

Detects liquidity sweeps - when price takes out key levels to hunt
liquidity before reversing.

Architecture:
- Uses LiquiditySweep model from models/
- Implements detection and update logic
- Requires SSLBSLPlugin dependency
- Provides query interface for decision layer
"""

from typing import List, Dict, Any, Optional

from .base import LiquidityPlugin, PluginConfig
from ..models import LiquiditySweep


class LiquiditySweepPlugin(LiquidityPlugin):
    """
    Liquidity sweep detection plugin.
    
    Detection Logic:
    1. Monitor key liquidity levels (SSL/BSL, swing points, OBs)
    2. Detect when price breaks these levels
    3. Watch for reversal within N candles
    4. Confirm as sweep if reversal occurs
    
    Sweep Confirmation:
    - Price breaks level (close beyond, not just wick)
    - Reversal within 3-5 candles
    - Reversal is strong (large opposite candle)
    - Volume spike on reversal preferred
    
    Key Levels to Monitor:
    - SSL/BSL levels (equal highs/lows)
    - Swing highs and lows
    - Order blocks
    - Previous highs/lows
    
    Strength Factors:
    - Level strength (more touches = stronger)
    - Reversal decisiveness
    - Volume profile
    - Confluence with other patterns
    
    Integration:
    - Requires SSL/BSL plugin data
    - Can use BOS/CHOCH plugin for structure
    - Can use OrderBlock plugin for additional levels
    
    Usage:
        # Requires other plugin data
        ssl_plugin = SSLBSLPlugin(...)
        bos_plugin = BOSCHOCHPlugin(...)
        
        sweep_plugin = LiquiditySweepPlugin(
            symbol="BTCUSDT",
            timeframe="5m",
            ssl_plugin=ssl_plugin,
            bos_plugin=bos_plugin
        )
        
        sweep_plugin.on_candle_close(candles, current_price)
        
        # Query sweeps
        recent = sweep_plugin.get(confirmed_only=True)
        buy_side = sweep_plugin.get(sweep_type="buy_side")
        strong = sweep_plugin.get(min_strength="strong")
    
    TODO Implementation Steps:
    1. Collect key levels from other plugins
    2. Detect level breaks
    3. Monitor for reversal
    4. Confirm sweep
    5. Calculate strength
    6. Track volume spikes
    """
    
    def __init__(
        self,
        symbol: str,
        timeframe: str,
        config: Optional[PluginConfig] = None,
        ssl_plugin: Any = None,
        bos_plugin: Any = None,
        ob_plugin: Any = None,
        timeframe_config: Any = None  # ENHANCEMENT E: Per-TF sweep sensitivity
    ):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[LiquiditySweep] = []
        self.ssl_plugin = ssl_plugin  # SSL/BSL levels
        self.bos_plugin = bos_plugin  # Swing points
        self.ob_plugin = ob_plugin    # Order blocks
        self.reversal_window = 5      # Candles to confirm reversal
        
        # ENHANCEMENT E: Use timeframe-specific sweep sensitivity
        if timeframe_config:
            self.sweep_penetration_pct = timeframe_config.sweep_penetration_pct
            self.sweep_rejection_pct = timeframe_config.sweep_rejection_pct
        else:
            # Legacy defaults
            self.sweep_penetration_pct = 0.001  # 0.1% penetration
            self.sweep_rejection_pct = 0.0008   # 0.08% rejection
        
        self.min_reversal_percent = self.sweep_rejection_pct  # Use rejection % for reversal
    
    def detect(self, candles: List[Dict[str, Any]]) -> List[LiquiditySweep]:
        """
        Detect liquidity sweeps.
        
        Algorithm:
        1. Get SSL/BSL levels from SSL plugin
        2. Check which levels were swept recently
        3. Look for reversal after sweep
        4. Create LiquiditySweep patterns
        """
        if not candles or not self.ssl_plugin:
            return []
        
        sweeps = []
        
        # Get recently swept levels from SSL/BSL plugin
        recent_sweeps = self.ssl_plugin.get_recent_sweeps(lookback=20)
        
        for ssl_level in recent_sweeps:
            if not ssl_level.is_swept or not ssl_level.sweep_candle_idx:
                continue
            
            # Check if we already tracked this sweep
            existing_ids = {s.sweep_id for s in self._patterns}
            sweep_id = f"{self.symbol}_{self.timeframe}_sweep_{ssl_level.level_id}"
            
            if sweep_id in existing_ids:
                continue
            
            # Find the sweep candle
            sweep_idx = ssl_level.sweep_candle_idx
            if sweep_idx >= len(candles):
                continue
            
            sweep_candle = candles[sweep_idx]
            
            # Determine sweep type
            if ssl_level.level_type == "BSL":
                sweep_type = "buy_side"  # Swept resistance/high
                sweep_price = sweep_candle["high"]
            else:  # SSL
                sweep_type = "sell_side"  # Swept support/low
                sweep_price = sweep_candle["low"]
            
            # Look for reversal in next few candles
            reversal_detected = False
            reversal_price = None
            reversal_ts = None
            
            for i in range(sweep_idx + 1, min(sweep_idx + self.reversal_window + 1, len(candles))):
                rev_candle = candles[i]
                
                if sweep_type == "buy_side":
                    # Buy-side sweep should reverse down
                    move_pct = (ssl_level.price - rev_candle["low"]) / ssl_level.price
                    if move_pct >= self.min_reversal_percent / 100:
                        reversal_detected = True
                        reversal_price = rev_candle["low"]
                        reversal_ts = rev_candle["ts"]
                        break
                else:
                    # Sell-side sweep should reverse up
                    move_pct = (rev_candle["high"] - ssl_level.price) / ssl_level.price
                    if move_pct >= self.min_reversal_percent / 100:
                        reversal_detected = True
                        reversal_price = rev_candle["high"]
                        reversal_ts = rev_candle["ts"]
                        break
            
            # Create sweep pattern
            sweep = LiquiditySweep(
                sweep_id=sweep_id,
                timeframe=self.timeframe,
                sweep_type=sweep_type,
                level_price=ssl_level.price,
                sweep_price=sweep_price,
                sweep_candle_idx=sweep_idx,
                sweep_ts=ssl_level.sweep_timestamp,
                reversal_price=reversal_price,
                reversal_ts=reversal_ts,
                confirmed=reversal_detected,
                level_type="ssl_bsl",
                strength="strong" if reversal_detected else "medium"
            )
            sweeps.append(sweep)
        
        return sweeps
    
    def update(
        self,
        candles: List[Dict[str, Any]],
        current_price: float
    ) -> None:
        """
        Update sweeps: confirm reversals.
        
        Algorithm:
        1. Check unconfirmed sweeps for late reversals
        2. Update reversal data if found
        3. Mark as confirmed
        4. Upgrade strength
        """
        if not candles or not self._patterns:
            return
        
        recent = candles[-self.reversal_window * 2:] if len(candles) > self.reversal_window * 2 else candles
        
        for sweep in self._patterns:
            if sweep.confirmed:
                continue
            
            # Try to find reversal for unconfirmed sweeps
            for candle in recent:
                if sweep.sweep_type == "buy_side":
                    # Check for down reversal
                    move_pct = (sweep.level_price - candle["low"]) / sweep.level_price
                    if move_pct >= self.min_reversal_percent / 100:
                        sweep.confirmed = True
                        sweep.reversal_price = candle["low"]
                        sweep.reversal_ts = candle["ts"]
                        sweep.strength = "strong"
                        break
                else:
                    # Check for up reversal
                    move_pct = (candle["high"] - sweep.level_price) / sweep.level_price
                    if move_pct >= self.min_reversal_percent / 100:
                        sweep.confirmed = True
                        sweep.reversal_price = candle["high"]
                        sweep.reversal_ts = candle["ts"]
                        sweep.strength = "strong"
                        break
    
    def get(
        self,
        sweep_type: Optional[str] = None,
        confirmed_only: bool = True,
        min_strength: Optional[str] = None
    ) -> List[LiquiditySweep]:
        """
        Query liquidity sweeps.
        
        Args:
            sweep_type: "buy_side", "sell_side", or None
            confirmed_only: Only confirmed sweeps
            min_strength: "weak", "medium", or "strong"
        """
        sweeps = list(self._patterns)
        
        if confirmed_only:
            sweeps = [s for s in sweeps if s.confirmed]
        
        if sweep_type:
            sweeps = [s for s in sweeps if s.sweep_type == sweep_type]
        
        if min_strength:
            strength_order = {"weak": 0, "medium": 1, "strong": 2}
            min_val = strength_order.get(min_strength, 0)
            sweeps = [s for s in sweeps if strength_order[s.strength] >= min_val]
        
        return sweeps
    
    def get_recent_sweeps(self, lookback_minutes: int = 60) -> List[LiquiditySweep]:
        """Get sweeps within lookback period."""
        if not self._patterns:
            return []
        
        latest_ts = self._patterns[-1].sweep_ts
        cutoff_ts = latest_ts - (lookback_minutes * 60000)
        
        return [s for s in self._patterns if s.sweep_ts >= cutoff_ts]
    
    def _merge_patterns(self, new_patterns: List[LiquiditySweep]) -> None:
        """Merge new sweeps with existing."""
        existing_ids = {s.sweep_id for s in self._patterns}
        
        for new_sweep in new_patterns:
            if new_sweep.sweep_id not in existing_ids:
                self._patterns.append(new_sweep)
        
        # Keep last 100 sweeps
        self._patterns = self._patterns[-100:]
