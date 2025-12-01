# Liquidity Map Architecture

## Overview

The Liquidity Map system follows a clean **plugin architecture** with **complete separation of concerns** between data structures (models), business logic (plugins), and shared constants (enums).

```
decision_layer/
├── enums.py             → Centralized enumerations
├── models/              → Pure data structures (12 models)
├── plugins/             → Detection logic (6 plugins)
├── detectors/           → Helper algorithms (3 detectors)
├── liquidity_map.py     → Main orchestrator
├── trend_detector.py    → Multi-timeframe trend fusion
├── zone_classifier.py   → Advanced zone classification
└── ARCHITECTURE.md      → This documentation
```

---

## Architecture Principles

### 1. **Separation of Concerns**

**Models** (`models/`):
- Pure data structures using `@dataclass`
- No business logic, only properties and helper methods
- Can be serialized/deserialized easily
- Shared across multiple plugins
- Testable independently

**Plugins** (`plugins/`):
- Import models from `models/`
- Implement detection algorithms
- Update pattern states
- Provide query interface
- Follow `LiquidityPlugin` base class

**Detectors** (`detectors/`):
- Reusable algorithms (pivot detection, volume clustering)
- No state, pure functions
- Used by multiple plugins

### 2. **Plugin Pattern**

All plugins follow the same interface:

```python
class SomePlugin(LiquidityPlugin):
    def detect(self, candles) -> List[Pattern]:
        """Detect new patterns in candles."""
        
    def update(self, candles, current_price) -> None:
        """Update existing patterns."""
        
    def get(self, **filters) -> List[Pattern]:
        """Query patterns with filters."""
```

### 3. **Dependency Injection**

Plugins can depend on other plugins:

```python
# Breaker Block needs Order Block data
breaker_plugin = BreakerBlockPlugin(
    symbol="BTCUSDT",
    timeframe="5m",
    ob_plugin=order_block_plugin  # Dependency injection
)

# Liquidity Sweep needs SSL/BSL data
sweep_plugin = LiquiditySweepPlugin(
    symbol="BTCUSDT",
    timeframe="5m",
    ssl_plugin=ssl_bsl_plugin  # Dependency injection
)
```

---

## Models Reference

### Core Models

| Model | File | Purpose |
|-------|------|---------|
| `LiquidityZone` | `liquidity_zone.py` | Support/resistance zones from volume/pivots |
| `FairValueGap` | `fair_value_gap.py` | Price imbalance zones (FVG) |
| `VolumeProfile` | `volume_profile.py` | Volume distribution by price |
| `TimeframeZones` | `timeframe_zones.py` | Container for all patterns per TF |

### SMC Pattern Models

| Model | File | Purpose |
|-------|------|---------|
| `LiquidityLevel` | `liquidity_level.py` | SSL/BSL equal highs/lows |
| `OrderBlock` | `order_block.py` | Institutional order blocks |
| `StructureBreak` | `structure_break.py` | BOS/CHOCH market structure |
| `StructurePoint` | `structure_break.py` | Swing high/low points |
| `BreakerBlock` | `breaker_block.py` | Failed OBs that flipped |
| `LiquiditySweep` | `liquidity_sweep.py` | Liquidity hunt patterns |

### Trend Analysis Models

| Model | File | Purpose |
|-------|------|---------|
| `TrendState` | `trend_state.py` | Trend state for single timeframe |
| `TrendFusionSignal` | `trend_fusion_signal.py` | MTF trend signal with zone confluence |

### Zone Classification Models

| Model | File | Purpose |
|-------|------|---------|
| `ClassifiedZone` | `classified_zone.py` | Enhanced zone with quality scoring |

---

## Enumerations Reference

All enums are centralized in `enums.py`:

### Trend Enums

| Enum | Values | Purpose |
|------|--------|---------|
| `TrendDirection` | STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH | Trend direction classification |
| `TrendStrength` | VERY_WEAK, WEAK, MODERATE, STRONG, VERY_STRONG | Trend strength measurement |

### Zone Classification Enums

| Enum | Values | Purpose |
|------|--------|---------|
| `ZoneQuality` | EXCELLENT, GOOD, AVERAGE, POOR, INVALID | Overall zone quality |
| `TimeRelevance` | FRESH, RECENT, AGED, STALE | Zone age relevance |
| `ZoneReaction` | STRONG_BOUNCE, WEAK_BOUNCE, BREAKTHROUGH, PENDING | Price reaction to zone |

### Model Features

All models include:
- ✅ Clear documentation
- ✅ Type hints
- ✅ Properties for computed values
- ✅ Helper methods for common operations
- ✅ Meaningful `__repr__` for debugging

Example:

```python
@dataclass
class OrderBlock:
    ob_id: str
    timeframe: str
    ob_type: str  # "bullish" or "bearish"
    price_high: float
    price_low: float
    # ... more fields
    
    @property
    def midpoint(self) -> float:
        """Get the midpoint price."""
        return (self.price_high + self.price_low) / 2
    
    def contains_price(self, price: float) -> bool:
        """Check if price is within block."""
        return self.price_low <= price <= self.price_high
```

---

## Plugins Reference

### Plugin System

| Plugin | Model Used | Purpose |
|--------|------------|---------|
| `FVGPlugin` | `FairValueGap` | Detect price imbalances |
| `SSLBSLPlugin` | `LiquidityLevel` | Detect equal highs/lows |
| `OrderBlockPlugin` | `OrderBlock` | Detect institutional blocks |
| `BOSCHOCHPlugin` | `StructureBreak`, `StructurePoint` | Detect structure breaks |
| `BreakerBlockPlugin` | `BreakerBlock` | Detect failed OBs |
| `LiquiditySweepPlugin` | `LiquiditySweep` | Detect liquidity hunts |

### Plugin Dependencies

```
FVGPlugin              → (no dependencies)
SSLBSLPlugin           → (no dependencies)
OrderBlockPlugin       → (no dependencies)
BOSCHOCHPlugin         → (no dependencies)
BreakerBlockPlugin     → OrderBlockPlugin ✓
LiquiditySweepPlugin   → SSLBSLPlugin ✓
```

### Plugin Lifecycle

```python
# 1. Initialize with config
plugin = OrderBlockPlugin(
    symbol="BTCUSDT",
    timeframe="5m",
    config=PluginConfig(
        lookback_candles=100,
        min_strength=0.7
    )
)

# 2. On candle close event
plugin.on_candle_close(candles, current_price)
# Internally calls:
#   - detect(candles)      → Find new patterns
#   - update(candles, price) → Update existing
#   - _merge_patterns()    → Deduplicate

# 3. Query patterns
active_obs = plugin.get(only_unmitigated=True)
nearest = plugin.get_nearest(current_price, direction="above")
stats = plugin.get_statistics()
```

---

## Integration Example

### LiquidityMap Usage

```python
from decision_layer import LiquidityMap
from decision_layer.models import OrderBlock, LiquidityLevel, FairValueGap

# Initialize with all plugins
liq_map = LiquidityMap(
    symbol="BTCUSDT",
    timeframes=["1m", "5m", "15m", "1h"],
    enable_fvg=True,
    enable_ssl_bsl=True,
    enable_order_blocks=True,
    enable_bos_choch=True,
    enable_breaker_blocks=True,
    enable_liquidity_sweeps=True
)

# On candle close (called by MTFSymbolManager)
liq_map.on_candle_close(
    timeframe="5m",
    candles=candle_manager.get_all(),
    current_price=91500.0
)

# Query patterns
fvgs = liq_map.get_fvgs(timeframe="5m", only_unfilled=True)
order_blocks = liq_map.plugins["5m"]["order_block"].get(only_unmitigated=True)
ssl_levels = liq_map.plugins["5m"]["ssl_bsl"].get(level_type="SSL")

# Access models directly
for ob in order_blocks:
    print(f"OB: {ob.ob_type} @ ${ob.midpoint:.2f}")
    if ob.contains_price(current_price):
        print("  → Price inside OB!")
```

---

## Benefits of This Architecture

### ✅ **Modularity**
- Add new plugins without touching existing code
- Plugins can be enabled/disabled independently
- Easy to test individual components

### ✅ **Reusability**
- Models shared across plugins
- Detectors (pivot, volume) reused by multiple plugins
- Same plugin works across all timeframes

### ✅ **Maintainability**
- Clear separation: models vs logic
- Each plugin is self-contained
- Changes to one plugin don't affect others

### ✅ **Testability**
- Test models independently (no logic)
- Test plugins with mock data
- Test integration with real data

### ✅ **Extensibility**
- Add new SMC patterns easily
- Plugins can depend on other plugins
- Models can be extended with new properties

### ✅ **Type Safety**
- Full type hints throughout
- IDE autocomplete works perfectly
- Catch errors at development time

---

## Best Practices

### When Creating New Patterns

1. **Create model first** in `models/`:
```python
@dataclass
class NewPattern:
    pattern_id: str
    timeframe: str
    # ... fields
    
    @property
    def some_computed_value(self) -> float:
        """Computed property."""
        return ...
```

2. **Create plugin** in `plugins/`:
```python
class NewPatternPlugin(LiquidityPlugin):
    def __init__(self, symbol, timeframe, config=None):
        super().__init__(symbol, timeframe, config)
        self._patterns: List[NewPattern] = []
    
    def detect(self, candles):
        # Detection logic
        pass
```

3. **Export in `__init__.py`**:
```python
# models/__init__.py
from .new_pattern import NewPattern

# plugins/__init__.py
from .new_pattern_plugin import NewPatternPlugin
```

4. **Integrate in LiquidityMap**:
```python
# Add to __init__ parameters
enable_new_pattern: bool = True

# Initialize in _initialize_plugins
if enable_new_pattern:
    self.plugins[tf]["new_pattern"] = NewPatternPlugin(...)
```

### Code Style

- ✅ Use `@dataclass` for models
- ✅ Full type hints everywhere
- ✅ Docstrings for all public methods
- ✅ Properties for computed values
- ✅ Helper methods in models (not plugins)
- ✅ Meaningful variable names
- ✅ Clear `__repr__` for debugging

---

## Testing Strategy

### Unit Tests (Models)
```python
def test_order_block_midpoint():
    ob = OrderBlock(
        ob_id="test",
        timeframe="5m",
        ob_type="bullish",
        price_high=100,
        price_low=90,
        # ...
    )
    assert ob.midpoint == 95
    assert ob.size == 10
    assert ob.contains_price(95) == True
```

### Integration Tests (Plugins)
```python
def test_order_block_detection():
    plugin = OrderBlockPlugin("BTCUSDT", "5m")
    candles = generate_test_candles_with_impulse()
    
    plugin.on_candle_close(candles, current_price=100)
    obs = plugin.get()
    
    assert len(obs) > 0
    assert obs[0].ob_type in ["bullish", "bearish"]
```

### System Tests (Full Integration)
```python
def test_liquidity_map_integration():
    liq_map = LiquidityMap(
        symbol="BTCUSDT",
        timeframes=["1m", "5m"],
        enable_order_blocks=True
    )
    
    # Simulate candle closes
    for candle in test_candles:
        liq_map.on_candle_close("5m", candles, candle["close"])
    
    # Verify patterns detected
    obs = liq_map.plugins["5m"]["order_block"].get()
    assert len(obs) > 0
```

---

## Performance Considerations

- **Event-driven**: Updates only on candle close
- **Timeframe isolation**: Each TF independent
- **Efficient queries**: Pre-filtered by status
- **Pattern cleanup**: Remove old/invalid patterns
- **Lookback windows**: Configurable limits

---

## Future Enhancements

1. **Persistence**: Serialize models to database
2. **Backtesting**: Historical pattern analysis
3. **Visualization**: Chart overlays for patterns
4. **ML Integration**: Pattern strength prediction
5. **More patterns**: Wyckoff, Elliott Wave, etc.

---

## Summary

This architecture provides a **solid foundation** for institutional-grade trading systems:

- 🎯 **Clear structure**: Models, Plugins, Detectors, Orchestrator
- 🔌 **Pluggable**: Add/remove features easily
- 🧪 **Testable**: Unit, integration, system tests
- 📈 **Scalable**: Add symbols, timeframes, patterns
- 🛡️ **Type-safe**: Full type hints throughout
- 📚 **Documented**: Clear docs and examples

All SMC patterns follow the same consistent pattern, making the codebase easy to understand, maintain, and extend.
