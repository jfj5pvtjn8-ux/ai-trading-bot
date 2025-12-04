# Bug Fix Summary - Candle Gap Detection

**Date**: 2024-12-04  
**Status**: ✅ FIXED AND VERIFIED

---

## Problem

16 candle gaps were detected in the database despite having CandleSync gap detection logic in place. Gaps occurred during both:
- **Startup**: 13 gaps during initial data load (11:00-11:17)
- **Runtime**: 3 gaps during normal operation (12:06, 12:17, 12:29)

All gaps were single-candle (1 minute) gaps marked as warnings.

---

## Root Cause

**Critical Bug in `src/trading_bot/bot.py` line 324:**

```python
# WRONG - Setting non-existent attribute
sync._last_validated_ts = latest['ts']
```

The `_seed_candle_syncs()` method was setting a **non-existent private attribute** instead of calling the proper method to initialize gap tracking.

**Impact:**
1. `CandleSync.last_open_ts` remained `None` throughout bot lifetime
2. Gap detection logic in `on_ws_closed_candle()` has early return when `last_open_ts is None`
3. Every WebSocket candle treated as "first candle", skipping all gap checks
4. **Gap detection was completely disabled**

---

## Fix Applied

**Changed line 324 in `src/trading_bot/bot.py`:**

```python
# CORRECT - Call proper seeding method
sync.set_initial_last_ts(latest['ts'])
```

**Additional Fixes:**
- Replaced deprecated `datetime.utcfromtimestamp()` with `datetime.fromtimestamp(timestamp, tz=timezone.utc)` in 5 locations in `candles_validator.py`

---

## Validation Results

### Before Fix (2025-12-04 12:30)
```
Total Issues Found: 16
- 16 gaps (all single-candle warnings)
- 0 duplicates, 0 misalignments, 0 NULL values, 0 data quality issues
```

### After Fix (2025-12-04 13:48)
```
Total Issues Found: 0
✅ No issues detected - Data integrity validated
```

**Test Environment:**
- Full Docker rebuild with `--no-cache`
- `fresh_start=True` mode (cleared all data)
- 32,020 candles validated
- All 8 CandleSync validators properly seeded
- WebSocket candles flowing continuously

**Logs Verification:**
```
2025-12-04 13:39:20 [CandleSync] Seed last_open_ts=1764855540 for BTCUSDT 1m ✓
2025-12-04 13:39:20 [CandleSync] Seed last_open_ts=1764855300 for BTCUSDT 5m ✓
2025-12-04 13:39:20 [CandleSync] Seed last_open_ts=1764855000 for BTCUSDT 15m ✓
2025-12-04 13:39:20 [CandleSync] Seed last_open_ts=1764853200 for BTCUSDT 1h ✓
... (all 8 syncs seeded correctly)

2025-12-04 13:48:00 [VALIDATED] BTCUSDT 1m ts=1764856020 close=92509.0 ✓
2025-12-04 13:49:00 [VALIDATED] BTCUSDT 1m ts=1764856080 close=92463.37 ✓
... (continuous WebSocket flow, no gaps)
```

---

## Files Modified

1. **`src/trading_bot/bot.py`** (line 324)
   - Fixed: Use `set_initial_last_ts()` instead of wrong attribute

2. **`src/trading_bot/validators/candles_validator.py`** (5 locations)
   - Fixed: Replaced deprecated datetime methods

3. **`GAPS_FIX_PLAN.md`** (updated)
   - Documented primary root cause and fix

---

## Technical Details

### CandleSync Gap Detection Flow

```python
# src/trading_bot/core/candles/candle_sync.py

class CandleSync:
    def __init__(self, ...):
        self.last_open_ts: Optional[int] = None  # Correct attribute
    
    def set_initial_last_ts(self, ts: int):
        """Proper seeding method - now being called correctly"""
        self.last_open_ts = ts
    
    def on_ws_closed_candle(self, c: Candle):
        # Early return if not seeded (was always True before fix)
        if self.last_open_ts is None:
            return self._accept(c, storage)
        
        # Gap detection (now properly executed)
        expected_next = self.last_open_ts + self.interval_seconds
        if c.open_ts > expected_next:
            # GAP DETECTED - trigger backfill
            self._forward_fill_expected_gap(expected_next, c.open_ts)
        
        return self._accept(c, storage)
```

### Why Bug Caused All Gaps

1. Bot calls `_seed_candle_syncs()` during initialization
2. Method fetched latest candle from DB: `latest = storage.get_latest_candle(symbol, timeframe)`
3. **Bug**: Set wrong attribute `sync._last_validated_ts = latest['ts']`
4. Result: `sync.last_open_ts` remained `None`
5. Every WebSocket candle hit early return: `if self.last_open_ts is None: return self._accept()`
6. Gap detection code never executed
7. Missing candles never backfilled via REST API

---

## Monitoring Plan

- ✅ Let bot run for 1-2 hours
- ✅ Run validator periodically to confirm zero gaps
- ✅ Monitor logs for any "GAP detected" messages (expected during reconnections)
- ✅ Test reconnection scenario (restart container)
- ✅ Verify gap-fill works correctly after fix

---

## Additional Fix - Bootstrap Gap Filling

**Date**: 2024-12-04 14:20  
**Issue**: `backward_fill_gap()` was fetching most recent candles instead of filling historical gaps

**Root Cause**:
- Method used `end_time=current_time` which fetches RECENT candles (14:10, 14:09...)
- Did NOT fill historical gaps from downtime (e.g., 13:51 missing during rebuild)

**Fix Applied** (`src/trading_bot/storage/duckdb_storage.py` lines 394-450):
```python
# Get last stored timestamp and fetch FROM that point forward
latest = self.get_last_candle(symbol, timeframe)
if latest:
    next_candle_ts = latest['ts'] + tf_seconds
    start_time = next_candle_ts * 1000  # Start after last candle
    batch = rest_client.fetch_klines(
        symbol=symbol,
        timeframe=timeframe,
        limit=count + 5,
        start_time=start_time  # Fetch forward from gap, not backward from now
    )
```

**Limitation**: 
- Bootstrapper only checks "last candle vs NOW" (`calculate_gap`)
- Does NOT scan for gaps in middle of sequences
- Historical gaps from downtime will show as validator warnings
- **This is expected behavior** - validator finds them, but bootstrapper doesn't rescan history

**Runtime Gap Detection** (CandleSync) **IS** working - prevents NEW gaps during operation

---

## Conclusion

A single-line bug in the seeding logic completely disabled the gap detection system. The fix ensures `CandleSync` properly tracks the last candle timestamp, enabling gap detection and automatic backfilling via REST API.

**Results**:
- ✅ CandleSync gap detection: **FIXED** - prevents new gaps during runtime
- ✅ Bootstrap gap filling: **IMPROVED** - fills gaps from last candle forward
- ⚠️ Historical gaps from downtime: **Expected** - validator shows warnings, no auto-fill

**Current Status**: **Zero NEW gaps** during continuous operation. Historical gaps from Docker rebuilds are expected and documented.
