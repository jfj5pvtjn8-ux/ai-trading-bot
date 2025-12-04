#!/usr/bin/env python3
"""
Gap Filler Utility

Scans the entire candle database for gaps and fills them using the REST API.
Unlike the bootstrapper which only checks "last candle vs now", this scans
the complete sequence and fills ALL gaps found by the validator.

Usage:
    python src/trading_bot/validators/gap_filler.py
    python src/trading_bot/validators/gap_filler.py --dry-run
"""

import sys
import os
import duckdb
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.trading_bot.api.rest_client import RestClient
from src.trading_bot.config.app_config import AppConfig


class GapFiller:
    """Fill all gaps found in the candle database."""
    
    def __init__(self, db_path: str = "data/trading.duckdb", dry_run: bool = False):
        self.db_path = db_path
        self.dry_run = dry_run
        self.conn = duckdb.connect(db_path, read_only=dry_run)
        
        # Load config for REST client
        config_path = project_root / "config" / "app.yml"
        self.app_config = AppConfig.from_yaml(str(config_path))
        self.rest_client = RestClient(
            base_url=self.app_config.binance_api_url,
            timeout=self.app_config.api_timeout
        )
        
        # Timeframe mapping
        self.tf_seconds = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600
        }
    
    def find_all_gaps(self) -> List[Dict]:
        """
        Find all gaps in all symbol/timeframe combinations.
        
        Returns:
            List of gap dictionaries with symbol, timeframe, start_ts, end_ts, count
        """
        print("\n" + "="*80)
        print("SCANNING DATABASE FOR GAPS")
        print("="*80)
        
        # Get all unique symbol/timeframe combinations
        pairs = self.conn.execute("""
            SELECT DISTINCT symbol, timeframe
            FROM market.candles
            ORDER BY symbol, timeframe
        """).fetchall()
        
        all_gaps = []
        
        for symbol, timeframe in pairs:
            gaps = self._find_gaps_for_pair(symbol, timeframe)
            all_gaps.extend(gaps)
        
        return all_gaps
    
    def _find_gaps_for_pair(self, symbol: str, timeframe: str) -> List[Dict]:
        """Find all gaps for a specific symbol/timeframe pair."""
        tf_seconds = self.tf_seconds.get(timeframe, 60)
        
        # Query to find gaps using LAG window function
        query = f"""
            WITH sequenced AS (
                SELECT 
                    symbol,
                    timeframe,
                    open_ts,
                    LAG(open_ts) OVER (ORDER BY open_ts) as prev_ts
                FROM market.candles
                WHERE symbol = ? AND timeframe = ?
                ORDER BY open_ts
            )
            SELECT 
                prev_ts,
                open_ts,
                (open_ts - prev_ts) as gap_seconds
            FROM sequenced
            WHERE prev_ts IS NOT NULL 
            AND (open_ts - prev_ts) > {tf_seconds}
            ORDER BY open_ts
        """
        
        results = self.conn.execute(query, [symbol, timeframe]).fetchall()
        
        gaps = []
        for prev_ts, curr_ts, gap_seconds in results:
            missing_count = (gap_seconds // tf_seconds) - 1
            if missing_count > 0:
                gaps.append({
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'start_ts': prev_ts,
                    'end_ts': curr_ts,
                    'missing_count': missing_count,
                    'gap_seconds': gap_seconds
                })
        
        return gaps
    
    def fill_gap(self, gap: Dict) -> Tuple[bool, int]:
        """
        Fill a specific gap by fetching candles from REST API.
        
        Args:
            gap: Gap dictionary with symbol, timeframe, start_ts, end_ts, missing_count
            
        Returns:
            Tuple of (success, candles_inserted)
        """
        symbol = gap['symbol']
        timeframe = gap['timeframe']
        start_ts = gap['start_ts']
        missing_count = gap['missing_count']
        tf_seconds = self.tf_seconds[timeframe]
        
        # Calculate the first missing candle timestamp
        first_missing_ts = start_ts + tf_seconds
        
        # Convert to milliseconds for Binance API
        start_time_ms = first_missing_ts * 1000
        
        print(f"\n  Fetching {missing_count} candles from API...")
        print(f"  Start time: {datetime.fromtimestamp(first_missing_ts, tz=timezone.utc)}")
        
        if self.dry_run:
            print(f"  [DRY RUN] Would fetch and insert {missing_count} candles")
            return True, missing_count
        
        try:
            # Fetch candles from Binance
            # Add +2 to limit to ensure we get all missing candles
            candles = self.rest_client.fetch_klines(
                symbol=symbol,
                timeframe=timeframe,
                limit=missing_count + 2,
                start_time=start_time_ms
            )
            
            if not candles:
                print(f"  ❌ No candles returned from API")
                return False, 0
            
            # Filter to only the missing candles (between start_ts and end_ts)
            end_ts = gap['end_ts']
            filtered_candles = [
                c for c in candles 
                if start_ts < c['ts'] < end_ts
            ]
            
            if not filtered_candles:
                print(f"  ⚠️  API returned candles but none match the gap range")
                return False, 0
            
            # Insert candles into database
            inserted = self._insert_candles(symbol, timeframe, filtered_candles)
            
            print(f"  ✅ Inserted {inserted} candles")
            return True, inserted
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return False, 0
    
    def _insert_candles(self, symbol: str, timeframe: str, candles: List[Dict]) -> int:
        """Insert candles into database with conflict handling."""
        if not candles:
            return 0
        
        # Prepare data for bulk insert
        rows = []
        for c in candles:
            rows.append((
                symbol,
                timeframe,
                c['ts'],
                c['open_ts'],
                c['close_ts'],
                c['open'],
                c['high'],
                c['low'],
                c['close'],
                c['volume'],
                c['quote_volume'],
                c['trades'],
                'backfill'  # Mark as backfill source
            ))
        
        # Insert with ON CONFLICT DO NOTHING (don't overwrite existing candles)
        insert_query = """
            INSERT INTO market.candles (
                symbol, timeframe, ts, open_ts, close_ts,
                open, high, low, close, volume,
                quote_volume, trades, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        """
        
        inserted = 0
        for row in rows:
            try:
                self.conn.execute(insert_query, row)
                inserted += 1
            except Exception as e:
                print(f"    Warning: Could not insert candle at ts={row[2]}: {e}")
        
        return inserted
    
    def run(self) -> None:
        """Main execution: find and fill all gaps."""
        print("\n" + "="*80)
        print("GAP FILLER UTILITY")
        print("="*80)
        print(f"Database: {self.db_path}")
        print(f"Mode: {'DRY RUN (no changes)' if self.dry_run else 'LIVE (will fill gaps)'}")
        print("="*80)
        
        # Find all gaps
        gaps = self.find_all_gaps()
        
        if not gaps:
            print("\n✅ No gaps found! Database is complete.")
            return
        
        print(f"\n📊 Found {len(gaps)} gap(s):")
        print("-" * 80)
        
        total_missing = sum(g['missing_count'] for g in gaps)
        print(f"Total missing candles: {total_missing}")
        print("-" * 80)
        
        # Show each gap
        for i, gap in enumerate(gaps, 1):
            start_dt = datetime.fromtimestamp(gap['start_ts'], tz=timezone.utc)
            end_dt = datetime.fromtimestamp(gap['end_ts'], tz=timezone.utc)
            duration_hrs = gap['gap_seconds'] / 3600
            
            print(f"\n{i}. {gap['symbol']} {gap['timeframe']}:")
            print(f"   Missing: {gap['missing_count']} candles")
            print(f"   Duration: {duration_hrs:.2f} hours")
            print(f"   Gap: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} → {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        
        if self.dry_run:
            print("\n" + "="*80)
            print("DRY RUN COMPLETE - No changes made")
            print("Run without --dry-run to fill these gaps")
            print("="*80)
            return
        
        # Ask for confirmation
        print("\n" + "="*80)
        response = input(f"\nFill {len(gaps)} gap(s) ({total_missing} candles)? [y/N]: ")
        
        if response.lower() != 'y':
            print("\n❌ Cancelled by user")
            return
        
        # Fill all gaps
        print("\n" + "="*80)
        print("FILLING GAPS")
        print("="*80)
        
        success_count = 0
        total_inserted = 0
        
        for i, gap in enumerate(gaps, 1):
            start_dt = datetime.fromtimestamp(gap['start_ts'], tz=timezone.utc)
            end_dt = datetime.fromtimestamp(gap['end_ts'], tz=timezone.utc)
            
            print(f"\n[{i}/{len(gaps)}] {gap['symbol']} {gap['timeframe']}")
            print(f"  Gap: {start_dt.strftime('%H:%M:%S')} → {end_dt.strftime('%H:%M:%S')}")
            print(f"  Missing: {gap['missing_count']} candles")
            
            success, inserted = self.fill_gap(gap)
            
            if success:
                success_count += 1
                total_inserted += inserted
        
        # Summary
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Gaps processed: {len(gaps)}")
        print(f"Successfully filled: {success_count}")
        print(f"Failed: {len(gaps) - success_count}")
        print(f"Total candles inserted: {total_inserted}")
        print("="*80)
        
        if success_count == len(gaps):
            print("\n✅ All gaps filled successfully!")
        else:
            print(f"\n⚠️  {len(gaps) - success_count} gap(s) could not be filled")
        
        print("\nRun the validator to verify:")
        print("  python src/trading_bot/validators/candles_validator.py")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Fill all gaps in the candle database"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )
    parser.add_argument(
        '--db',
        default='data/trading.duckdb',
        help='Path to DuckDB database (default: data/trading.duckdb)'
    )
    
    args = parser.parse_args()
    
    filler = None
    try:
        filler = GapFiller(db_path=args.db, dry_run=args.dry_run)
        filler.run()
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if filler:
            filler.close()


if __name__ == "__main__":
    main()
