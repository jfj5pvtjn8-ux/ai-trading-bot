#!/usr/bin/env python3
"""
Comprehensive Parquet File Validator

This script validates all parquet files in the data/live directory for:
- File existence and readability
- Candle counts
- Sequential timestamps (no gaps)
- Duplicate timestamps
- Correct ordering
- Data integrity (valid OHLCV values)
- Time range coverage
- Missing candles compared to expected count

Usage:
    python validate_parquet.py
    python validate_parquet.py --dir data/live
    python validate_parquet.py --symbol BTCUSDT --date 2025-11-30
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
from tabulate import tabulate


class ParquetValidator:
    """Validates trading candle parquet files."""
    
    # Timeframe configurations
    TIMEFRAMES = {
        '1m': {'interval': 60, 'expected': 3000, 'desc': '1 minute'},
        '5m': {'interval': 300, 'expected': 1500, 'desc': '5 minutes'},
        '15m': {'interval': 900, 'expected': 800, 'desc': '15 minutes'},
        '1h': {'interval': 3600, 'expected': 400, 'desc': '1 hour'},
    }
    
    REQUIRED_COLUMNS = ['symbol', 'timeframe', 'timestamp', 'datetime', 
                        'open', 'high', 'low', 'close', 'volume', 'received_at']
    
    def __init__(self, data_dir: str = "data/live"):
        """Initialize validator with data directory."""
        self.data_dir = Path(data_dir)
        self.results = []
        self.errors = []
        self.warnings = []
        
    def find_parquet_files(self, symbol: Optional[str] = None, 
                          date_str: Optional[str] = None) -> List[Path]:
        """Find all parquet files matching criteria."""
        pattern = "*"
        if symbol:
            pattern = f"{symbol}_*"
        if date_str:
            pattern = f"{pattern}{date_str}.parquet"
        else:
            pattern = f"{pattern}*.parquet"
            
        files = list(self.data_dir.glob(pattern))
        return sorted(files)
    
    def validate_file_structure(self, file_path: Path) -> Dict:
        """Validate basic file structure and columns."""
        result = {
            'file': file_path.name,
            'exists': file_path.exists(),
            'readable': False,
            'size_mb': 0,
            'columns_ok': False,
            'row_count': 0,
        }
        
        try:
            df = pd.read_parquet(file_path)
            result['readable'] = True
            result['size_mb'] = round(file_path.stat().st_size / (1024 * 1024), 2)
            result['row_count'] = len(df)
            
            # Check columns
            missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
            if missing_cols:
                self.errors.append(
                    f"{file_path.name}: Missing columns: {missing_cols}"
                )
            else:
                result['columns_ok'] = True
                
        except Exception as e:
            self.errors.append(f"{file_path.name}: Failed to read - {e}")
            
        return result
    
    def check_gaps(self, df: pd.DataFrame, timeframe: str, 
                   file_name: str) -> Tuple[int, List[Dict]]:
        """Check for gaps in timestamps."""
        if timeframe not in self.TIMEFRAMES:
            return 0, []
            
        interval = self.TIMEFRAMES[timeframe]['interval']
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Calculate differences
        time_diffs = df['timestamp'].diff()[1:]
        gaps = time_diffs[time_diffs != interval]
        
        gap_details = []
        if len(gaps) > 0:
            for idx in gaps.index:
                prev_ts = df.loc[idx - 1, 'timestamp']
                curr_ts = df.loc[idx, 'timestamp']
                prev_dt = df.loc[idx - 1, 'datetime']
                curr_dt = df.loc[idx, 'datetime']
                
                missing_candles = int((curr_ts - prev_ts) / interval) - 1
                
                gap_details.append({
                    'prev_time': prev_dt,
                    'next_time': curr_dt,
                    'missing_seconds': curr_ts - prev_ts - interval,
                    'missing_candles': missing_candles,
                })
                
        return len(gaps), gap_details
    
    def check_duplicates(self, df: pd.DataFrame, file_name: str) -> int:
        """Check for duplicate timestamps."""
        duplicates = df.duplicated(subset=['timestamp'], keep=False)
        dup_count = duplicates.sum()
        
        if dup_count > 0:
            dup_timestamps = df[duplicates]['timestamp'].unique()
            self.errors.append(
                f"{file_name}: {dup_count} duplicate timestamps found: {dup_timestamps[:5]}"
            )
            
        return dup_count
    
    def check_ordering(self, df: pd.DataFrame, file_name: str) -> bool:
        """Check if timestamps are properly ordered."""
        is_sorted = df['timestamp'].is_monotonic_increasing
        
        if not is_sorted:
            self.errors.append(f"{file_name}: Timestamps not in ascending order")
            
        return is_sorted
    
    def check_ohlcv_integrity(self, df: pd.DataFrame, file_name: str) -> Dict:
        """Validate OHLCV data integrity."""
        issues = {
            'high_low_mismatch': 0,
            'open_close_range': 0,
            'negative_volume': 0,
            'zero_ohlc': 0,
        }
        
        # High should be >= Low
        issues['high_low_mismatch'] = (df['high'] < df['low']).sum()
        
        # Open and Close should be between Low and High
        open_out_of_range = (df['open'] < df['low']) | (df['open'] > df['high'])
        close_out_of_range = (df['close'] < df['low']) | (df['close'] > df['high'])
        issues['open_close_range'] = (open_out_of_range | close_out_of_range).sum()
        
        # Volume should be non-negative
        issues['negative_volume'] = (df['volume'] < 0).sum()
        
        # OHLC should not all be zero
        all_zero = (df['open'] == 0) & (df['high'] == 0) & (df['low'] == 0) & (df['close'] == 0)
        issues['zero_ohlc'] = all_zero.sum()
        
        # Report errors
        for issue_type, count in issues.items():
            if count > 0:
                self.errors.append(f"{file_name}: {count} {issue_type} violations")
                
        return issues
    
    def check_time_range(self, df: pd.DataFrame, timeframe: str) -> Dict:
        """Check time range and coverage."""
        df_sorted = df.sort_values('timestamp')
        
        first_time = pd.to_datetime(df_sorted.iloc[0]['datetime'])
        last_time = pd.to_datetime(df_sorted.iloc[-1]['datetime'])
        duration = last_time - first_time
        
        expected_count = self.TIMEFRAMES[timeframe]['expected']
        actual_count = len(df)
        coverage_pct = (actual_count / expected_count) * 100 if expected_count > 0 else 0
        
        return {
            'first_candle': first_time.strftime('%Y-%m-%d %H:%M:%S'),
            'last_candle': last_time.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_hours': round(duration.total_seconds() / 3600, 2),
            'expected_count': expected_count,
            'actual_count': actual_count,
            'coverage_pct': round(coverage_pct, 1),
        }
    
    def validate_file(self, file_path: Path) -> Dict:
        """Comprehensive validation of a single parquet file."""
        print(f"\n{'='*80}")
        print(f"Validating: {file_path.name}")
        print(f"{'='*80}")
        
        # Parse filename (e.g., BTCUSDT_1m_2025-11-30.parquet)
        parts = file_path.stem.split('_')
        if len(parts) < 3:
            self.errors.append(f"{file_path.name}: Invalid filename format")
            return {}
            
        symbol = parts[0]
        timeframe = parts[1]
        date = parts[2]
        
        # Basic structure validation
        structure = self.validate_file_structure(file_path)
        print(f"\n📄 File Structure:")
        print(f"   Size: {structure['size_mb']} MB")
        print(f"   Rows: {structure['row_count']:,}")
        print(f"   Columns OK: {'✓' if structure['columns_ok'] else '✗'}")
        
        if not structure['readable'] or not structure['columns_ok']:
            return structure
            
        # Load data
        df = pd.read_parquet(file_path)
        
        # Check ordering
        print(f"\n🔢 Timestamp Ordering:")
        is_sorted = self.check_ordering(df, file_path.name)
        print(f"   Ascending order: {'✓' if is_sorted else '✗ FAILED'}")
        
        # Check duplicates
        print(f"\n🔍 Duplicate Check:")
        dup_count = self.check_duplicates(df, file_path.name)
        if dup_count == 0:
            print(f"   No duplicates: ✓")
        else:
            print(f"   Duplicates found: ✗ {dup_count}")
        
        # Check gaps
        print(f"\n⛓️  Gap Analysis:")
        gap_count, gap_details = self.check_gaps(df, timeframe, file_path.name)
        if gap_count == 0:
            print(f"   No gaps: ✓")
        else:
            print(f"   Gaps found: ✗ {gap_count}")
            for i, gap in enumerate(gap_details[:5], 1):  # Show first 5
                print(f"   Gap {i}: {gap['prev_time']} -> {gap['next_time']}")
                print(f"          Missing {gap['missing_candles']} candles ({gap['missing_seconds']}s)")
            if len(gap_details) > 5:
                print(f"   ... and {len(gap_details) - 5} more gaps")
        
        # OHLCV integrity
        print(f"\n💹 OHLCV Integrity:")
        ohlcv_issues = self.check_ohlcv_integrity(df, file_path.name)
        all_good = all(count == 0 for count in ohlcv_issues.values())
        if all_good:
            print(f"   All checks passed: ✓")
        else:
            for issue, count in ohlcv_issues.items():
                if count > 0:
                    print(f"   {issue}: ✗ {count} violations")
        
        # Time range
        print(f"\n⏰ Time Range:")
        time_info = self.check_time_range(df, timeframe)
        print(f"   First candle: {time_info['first_candle']}")
        print(f"   Last candle:  {time_info['last_candle']}")
        print(f"   Duration:     {time_info['duration_hours']} hours")
        print(f"   Expected:     {time_info['expected_count']:,} candles")
        print(f"   Actual:       {time_info['actual_count']:,} candles")
        print(f"   Coverage:     {time_info['coverage_pct']}%")
        
        # Summary
        result = {
            'file': file_path.name,
            'symbol': symbol,
            'timeframe': timeframe,
            'date': date,
            'size_mb': structure['size_mb'],
            'candles': structure['row_count'],
            'expected': time_info['expected_count'],
            'coverage': time_info['coverage_pct'],
            'gaps': gap_count,
            'duplicates': dup_count,
            'ordered': is_sorted,
            'ohlcv_ok': all_good,
            'first_candle': time_info['first_candle'],
            'last_candle': time_info['last_candle'],
        }
        
        return result
    
    def validate_all(self, symbol: Optional[str] = None, 
                     date_str: Optional[str] = None) -> None:
        """Validate all matching parquet files."""
        files = self.find_parquet_files(symbol, date_str)
        
        if not files:
            print(f"❌ No parquet files found in {self.data_dir}")
            if symbol:
                print(f"   Symbol filter: {symbol}")
            if date_str:
                print(f"   Date filter: {date_str}")
            return
        
        print(f"\n{'='*80}")
        print(f"PARQUET FILE VALIDATION REPORT")
        print(f"{'='*80}")
        print(f"Directory: {self.data_dir.absolute()}")
        print(f"Files found: {len(files)}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Validate each file
        for file_path in files:
            result = self.validate_file(file_path)
            if result:
                self.results.append(result)
        
        # Print summary table
        if self.results:
            print(f"\n{'='*80}")
            print("SUMMARY TABLE")
            print(f"{'='*80}\n")
            
            table_data = []
            for r in self.results:
                status = '✓' if (r['gaps'] == 0 and r['duplicates'] == 0 and 
                                r['ordered'] and r['ohlcv_ok']) else '✗'
                table_data.append([
                    status,
                    r['symbol'],
                    r['timeframe'],
                    f"{r['candles']:,}",
                    f"{r['expected']:,}",
                    f"{r['coverage']:.1f}%",
                    r['gaps'],
                    r['duplicates'],
                    '✓' if r['ordered'] else '✗',
                    '✓' if r['ohlcv_ok'] else '✗',
                ])
            
            headers = ['OK', 'Symbol', 'TF', 'Candles', 'Expected', 'Coverage', 
                      'Gaps', 'Dups', 'Ordered', 'OHLCV']
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        
        # Print errors and warnings
        if self.errors:
            print(f"\n{'='*80}")
            print(f"❌ ERRORS ({len(self.errors)})")
            print(f"{'='*80}")
            for error in self.errors:
                print(f"  • {error}")
        
        if self.warnings:
            print(f"\n{'='*80}")
            print(f"⚠️  WARNINGS ({len(self.warnings)})")
            print(f"{'='*80}")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        # Final verdict
        print(f"\n{'='*80}")
        if not self.errors and not self.warnings:
            print("✅ ALL VALIDATIONS PASSED!")
        elif not self.errors:
            print("⚠️  VALIDATION COMPLETED WITH WARNINGS")
        else:
            print("❌ VALIDATION FAILED - ISSUES FOUND")
        print(f"{'='*80}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Validate trading bot parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_parquet.py
  python validate_parquet.py --dir data/live
  python validate_parquet.py --symbol BTCUSDT
  python validate_parquet.py --symbol BTCUSDT --date 2025-11-30
        """
    )
    
    parser.add_argument(
        '--dir',
        default='data/live',
        help='Data directory containing parquet files (default: data/live)'
    )
    parser.add_argument(
        '--symbol',
        help='Filter by symbol (e.g., BTCUSDT)'
    )
    parser.add_argument(
        '--date',
        help='Filter by date (e.g., 2025-11-30)'
    )
    
    args = parser.parse_args()
    
    # Create validator and run
    validator = ParquetValidator(args.dir)
    validator.validate_all(args.symbol, args.date)
    
    # Exit with error code if issues found
    sys.exit(1 if validator.errors else 0)


if __name__ == '__main__':
    main()
