"""
Candles Validator - Validates candle data integrity in DuckDB

This module provides comprehensive validation for stored candle data:
- Duplicate detection (same symbol/timeframe/timestamp)
- Misalignment detection (candles not on expected interval boundaries)
- Gap detection (missing candles in sequence)
- Timestamp validation (open_ts < close_ts)
- Data quality checks (null values, invalid prices/volumes)
"""

import duckdb
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class ValidationIssue:
    """Represents a single validation issue found in candle data"""
    issue_type: str  # 'duplicate', 'misaligned', 'gap', 'invalid_timestamp', 'null_value', 'invalid_data'
    severity: str  # 'critical', 'warning', 'info'
    symbol: str
    timeframe: str
    timestamp: Optional[int] = None
    description: str = ""
    details: Optional[Dict] = None


@dataclass
class ValidationReport:
    """Comprehensive validation report"""
    total_candles: int
    symbols_checked: List[str]
    timeframes_checked: List[str]
    issues: List[ValidationIssue]
    summary: Dict[str, int]
    
    def has_critical_issues(self) -> bool:
        """Check if there are any critical issues"""
        return any(issue.severity == 'critical' for issue in self.issues)
    
    def get_issues_by_type(self, issue_type: str) -> List[ValidationIssue]:
        """Get all issues of a specific type"""
        return [issue for issue in self.issues if issue.issue_type == issue_type]
    
    def print_summary(self):
        """Print a formatted summary of the validation report"""
        print("\n" + "="*100)
        print("CANDLE DATA VALIDATION REPORT")
        print("="*100)
        print(f"Total Candles Validated: {self.total_candles:,}")
        print(f"Symbols Checked: {', '.join(self.symbols_checked)}")
        print(f"Timeframes Checked: {', '.join(self.timeframes_checked)}")
        print(f"Total Issues Found: {len(self.issues)}")
        print("\n" + "-"*100)
        print("ISSUE BREAKDOWN:")
        print("-"*100)
        
        for issue_type, count in self.summary.items():
            if count > 0:
                severity_counts = {}
                for issue in self.issues:
                    if issue.issue_type == issue_type:
                        severity_counts[issue.severity] = severity_counts.get(issue.severity, 0) + 1
                
                severity_str = ", ".join([f"{sev}: {cnt}" for sev, cnt in severity_counts.items()])
                print(f"  {issue_type.upper()}: {count} ({severity_str})")
        
        print("="*100)
        
        if self.has_critical_issues():
            print("⚠️  CRITICAL ISSUES DETECTED - Immediate action required!")
        elif len(self.issues) > 0:
            print("⚠️  Issues found - Review recommended")
        else:
            print("✅ No issues detected - Data integrity validated")
        print("="*100 + "\n")


class CandlesValidator:
    """
    Validates candle data stored in DuckDB for integrity issues
    
    Performs the following validations:
    1. Duplicate Detection - Finds candles with same symbol/timeframe/timestamp
    2. Misalignment Detection - Identifies candles not aligned to interval boundaries
    3. Gap Detection - Finds missing candles in sequences
    4. Timestamp Validation - Ensures open_ts < close_ts
    5. Null Value Detection - Checks for NULL values in critical fields
    6. Data Quality - Validates price/volume ranges
    """
    
    def __init__(self, db_path: str):
        """
        Initialize the validator
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.timeframe_seconds = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
    
    def _get_connection(self):
        """Get a DuckDB connection (context manager)"""
        return duckdb.connect(self.db_path, read_only=True)
    
    def remove_duplicates(self) -> int:
        """
        Remove duplicate candles from the database.
        Keeps the first occurrence, deletes the rest.
        
        Returns:
            Number of duplicate candles removed
        """
        removed_count = 0
        
        with duckdb.connect(self.db_path, read_only=False) as conn:
            # Find and remove duplicates, keeping first occurrence
            result = conn.execute("""
                WITH ranked AS (
                    SELECT 
                        symbol, timeframe, open_ts,
                        ROW_NUMBER() OVER (PARTITION BY symbol, timeframe, open_ts ORDER BY received_at) as rn
                    FROM market.candles
                )
                DELETE FROM market.candles
                WHERE (symbol, timeframe, open_ts) IN (
                    SELECT symbol, timeframe, open_ts 
                    FROM ranked 
                    WHERE rn > 1
                )
            """).fetchall()
            
            # Get count of removed rows
            removed_count = conn.execute("""
                SELECT changes()
            """).fetchone()[0]
        
        return removed_count
    
    def validate_all(self, 
                     symbols: Optional[List[str]] = None,
                     timeframes: Optional[List[str]] = None,
                     conn: Optional[duckdb.DuckDBPyConnection] = None) -> ValidationReport:
        """
        Run all validations and generate comprehensive report
        
        Args:
            symbols: List of symbols to validate (None = all symbols)
            timeframes: List of timeframes to validate (None = all timeframes)
            conn: Optional existing connection to use (avoids connection conflicts)
            
        Returns:
            ValidationReport with all issues found
        """
        issues = []
        
        # Use provided connection or create new one
        if conn is not None:
            _conn = conn
            should_close = False
        else:
            _conn = duckdb.connect(self.db_path, read_only=True)
            should_close = True
        
        try:
            # Get symbols and timeframes if not specified
            if symbols is None or timeframes is None:
                result = _conn.execute("""
                    SELECT DISTINCT symbol, timeframe 
                    FROM market.candles 
                    ORDER BY symbol, timeframe
                """).fetchall()
                
                if symbols is None:
                    symbols = sorted(list(set(row[0] for row in result)))
                if timeframes is None:
                    timeframes = sorted(list(set(row[1] for row in result)))
            
            # Get total candle count
            total_candles = _conn.execute("SELECT COUNT(*) FROM market.candles").fetchone()[0]
            
            # Run all validation checks
            issues.extend(self._check_duplicates(_conn, symbols, timeframes))
            issues.extend(self._check_misalignment(_conn, symbols, timeframes))
            issues.extend(self._check_gaps(_conn, symbols, timeframes))
            issues.extend(self._check_timestamp_validity(_conn, symbols, timeframes))
            issues.extend(self._check_null_values(_conn, symbols, timeframes))
            issues.extend(self._check_data_quality(_conn, symbols, timeframes))
        finally:
            # Only close if we created the connection
            if should_close:
                _conn.close()
        
        # Create summary
        summary = {
            'duplicates': len([i for i in issues if i.issue_type == 'duplicate']),
            'misaligned': len([i for i in issues if i.issue_type == 'misaligned']),
            'gaps': len([i for i in issues if i.issue_type == 'gap']),
            'invalid_timestamps': len([i for i in issues if i.issue_type == 'invalid_timestamp']),
            'null_values': len([i for i in issues if i.issue_type == 'null_value']),
            'invalid_data': len([i for i in issues if i.issue_type == 'invalid_data'])
        }
        
        return ValidationReport(
            total_candles=total_candles,
            symbols_checked=symbols,
            timeframes_checked=timeframes,
            issues=issues,
            summary=summary
        )
    
    def _check_duplicates(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check for duplicate candles (same symbol, timeframe, open_ts)"""
        issues = []
        
        for symbol in symbols:
            for timeframe in timeframes:
                result = conn.execute("""
                    SELECT open_ts, COUNT(*) as count
                    FROM market.candles
                    WHERE symbol = ? AND timeframe = ?
                    GROUP BY open_ts
                    HAVING COUNT(*) > 1
                    ORDER BY open_ts
                """, [symbol, timeframe]).fetchall()
                
                for row in result:
                    timestamp, count = row
                    issues.append(ValidationIssue(
                        issue_type='duplicate',
                        severity='critical',
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp,
                        description=f"Duplicate candle found",
                        details={'duplicate_count': count}
                    ))
        
        return issues
    
    def _check_misalignment(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check for candles not aligned to interval boundaries"""
        issues = []
        
        for symbol in symbols:
            for timeframe in timeframes:
                if timeframe not in self.timeframe_seconds:
                    continue
                
                interval_seconds = self.timeframe_seconds[timeframe]
                
                # Find candles where open_ts is not divisible by interval
                result = conn.execute("""
                    SELECT open_ts, close_ts
                    FROM market.candles
                    WHERE symbol = ? 
                      AND timeframe = ?
                      AND (open_ts % ?) != 0
                    ORDER BY open_ts
                    LIMIT 100
                """, [symbol, timeframe, interval_seconds]).fetchall()
                
                for row in result:
                    open_ts, close_ts = row
                    expected_open = (open_ts // interval_seconds) * interval_seconds
                    issues.append(ValidationIssue(
                        issue_type='misaligned',
                        severity='warning',
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=open_ts,
                        description=f"Candle timestamp not aligned to {timeframe} interval",
                        details={
                            'actual_open_ts': open_ts,
                            'expected_open_ts': expected_open,
                            'offset_seconds': open_ts - expected_open
                        }
                    ))
        
        return issues
    
    def _check_gaps(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check for missing candles in sequences"""
        issues = []
        
        for symbol in symbols:
            for timeframe in timeframes:
                if timeframe not in self.timeframe_seconds:
                    continue
                
                interval_seconds = self.timeframe_seconds[timeframe]
                
                # Get all candles ordered by timestamp
                result = conn.execute("""
                    SELECT open_ts
                    FROM market.candles
                    WHERE symbol = ? AND timeframe = ?
                    ORDER BY open_ts
                """, [symbol, timeframe]).fetchall()
                
                if len(result) < 2:
                    continue
                
                timestamps = [row[0] for row in result]
                
                # Check for gaps between consecutive candles
                for i in range(len(timestamps) - 1):
                    current_ts = timestamps[i]
                    next_ts = timestamps[i + 1]
                    expected_next = current_ts + interval_seconds
                    
                    if next_ts > expected_next:
                        # Gap detected
                        missing_count = (next_ts - expected_next) // interval_seconds
                        gap_hours = (next_ts - expected_next) / 3600
                        
                        issues.append(ValidationIssue(
                            issue_type='gap',
                            severity='warning' if missing_count <= 5 else 'critical',
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=current_ts,
                            description=f"Gap of {missing_count} candle(s) detected",
                            details={
                                'gap_start': current_ts,
                                'gap_end': next_ts,
                                'missing_candles': missing_count,
                                'gap_duration_hours': round(gap_hours, 2)
                            }
                        ))
        
        return issues
    
    def _check_timestamp_validity(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check that close_ts > open_ts and timestamps are valid"""
        issues = []
        
        for symbol in symbols:
            for timeframe in timeframes:
                if timeframe not in self.timeframe_seconds:
                    continue
                
                interval_seconds = self.timeframe_seconds[timeframe]
                expected_duration = interval_seconds - 1  # Binance inclusive close
                
                # Find invalid timestamp relationships
                result = conn.execute("""
                    SELECT open_ts, close_ts
                    FROM market.candles
                    WHERE symbol = ? 
                      AND timeframe = ?
                      AND (close_ts <= open_ts OR (close_ts - open_ts) != ?)
                    LIMIT 100
                """, [symbol, timeframe, expected_duration]).fetchall()
                
                for row in result:
                    open_ts, close_ts = row
                    actual_duration = close_ts - open_ts
                    
                    if close_ts <= open_ts:
                        severity = 'critical'
                        desc = "Close timestamp <= Open timestamp"
                    else:
                        severity = 'warning'
                        desc = f"Unexpected duration: {actual_duration}s (expected {expected_duration}s)"
                    
                    issues.append(ValidationIssue(
                        issue_type='invalid_timestamp',
                        severity=severity,
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=open_ts,
                        description=desc,
                        details={
                            'open_ts': open_ts,
                            'close_ts': close_ts,
                            'actual_duration': actual_duration,
                            'expected_duration': expected_duration
                        }
                    ))
        
        return issues
    
    def _check_null_values(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check for NULL values in critical fields"""
        issues = []
        
        critical_fields = ['open', 'high', 'low', 'close', 'volume']
        
        for symbol in symbols:
            for timeframe in timeframes:
                for field in critical_fields:
                    result = conn.execute(f"""
                        SELECT open_ts
                        FROM market.candles
                        WHERE symbol = ? 
                          AND timeframe = ?
                          AND {field} IS NULL
                        LIMIT 100
                    """, [symbol, timeframe]).fetchall()
                    
                    for row in result:
                        issues.append(ValidationIssue(
                            issue_type='null_value',
                            severity='critical',
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=row[0],
                            description=f"NULL value in '{field}' field",
                            details={'field': field}
                        ))
        
        return issues
    
    def _check_data_quality(self, conn, symbols: List[str], timeframes: List[str]) -> List[ValidationIssue]:
        """Check data quality (OHLC relationships, negative values, etc.)"""
        issues = []
        
        for symbol in symbols:
            for timeframe in timeframes:
                # Check OHLC relationships: high >= low, high >= open, high >= close, low <= open, low <= close
                result = conn.execute("""
                    SELECT open_ts, open, high, low, close, volume
                    FROM market.candles
                    WHERE symbol = ? 
                      AND timeframe = ?
                      AND (
                          high < low OR
                          high < open OR
                          high < close OR
                          low > open OR
                          low > close OR
                          open <= 0 OR
                          high <= 0 OR
                          low <= 0 OR
                          close <= 0 OR
                          volume < 0
                      )
                    LIMIT 100
                """, [symbol, timeframe]).fetchall()
                
                for row in result:
                    open_ts, open_price, high, low, close, volume = row
                    problems = []
                    
                    if high < low:
                        problems.append(f"high ({high}) < low ({low})")
                    if high < open_price:
                        problems.append(f"high ({high}) < open ({open_price})")
                    if high < close:
                        problems.append(f"high ({high}) < close ({close})")
                    if low > open_price:
                        problems.append(f"low ({low}) > open ({open_price})")
                    if low > close:
                        problems.append(f"low ({low}) > close ({close})")
                    if open_price <= 0:
                        problems.append(f"open <= 0 ({open_price})")
                    if high <= 0:
                        problems.append(f"high <= 0 ({high})")
                    if low <= 0:
                        problems.append(f"low <= 0 ({low})")
                    if close <= 0:
                        problems.append(f"close <= 0 ({close})")
                    if volume < 0:
                        problems.append(f"volume < 0 ({volume})")
                    
                    issues.append(ValidationIssue(
                        issue_type='invalid_data',
                        severity='critical',
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=open_ts,
                        description="Invalid OHLCV data: " + "; ".join(problems),
                        details={
                            'open': open_price,
                            'high': high,
                            'low': low,
                            'close': close,
                            'volume': volume,
                            'problems': problems
                        }
                    ))
        
        return issues
    
    def get_data_quality_score(self, report: ValidationReport) -> Dict[str, float]:
        """
        Calculate data quality score (0-100) based on validation results.
        
        Returns:
            Dictionary with overall score and per-category scores
        """
        if report.total_candles == 0:
            return {'overall': 100.0}
        
        # Weight different issue types
        weights = {
            'duplicate': 10.0,      # Very serious
            'invalid_data': 10.0,   # Very serious
            'null_value': 10.0,     # Very serious
            'invalid_timestamp': 5.0,
            'gap': 2.0,
            'misaligned': 1.0
        }
        
        # Calculate penalty points
        total_penalty = 0.0
        category_scores = {}
        
        for issue_type, weight in weights.items():
            count = report.summary.get(issue_type, 0)
            if count > 0:
                # Penalty = (count / total_candles) * weight * severity_multiplier
                critical_count = len([i for i in report.issues if i.issue_type == issue_type and i.severity == 'critical'])
                severity_multiplier = 2.0 if critical_count > count / 2 else 1.0
                penalty = (count / report.total_candles) * weight * severity_multiplier * 100
                total_penalty += min(penalty, 20.0)  # Cap per-type penalty at 20
                
                # Calculate category score
                category_scores[issue_type] = max(0.0, 100.0 - penalty)
            else:
                category_scores[issue_type] = 100.0
        
        # Overall score
        overall_score = max(0.0, 100.0 - total_penalty)
        
        return {
            'overall': round(overall_score, 2),
            'categories': category_scores,
            'grade': self._get_grade(overall_score)
        }
    
    def _get_grade(self, score: float) -> str:
        """Get letter grade for data quality score"""
        if score >= 95:
            return 'A+'
        elif score >= 90:
            return 'A'
        elif score >= 85:
            return 'B+'
        elif score >= 80:
            return 'B'
        elif score >= 75:
            return 'C+'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def get_detailed_report(self, report: ValidationReport) -> str:
        """
        Generate a detailed text report with all issues
        
        Args:
            report: ValidationReport to format
            
        Returns:
            Formatted string with detailed report
        """
        lines = []
        lines.append("\n" + "="*100)
        lines.append("DETAILED CANDLE VALIDATION REPORT")
        lines.append("="*100)
        lines.append(f"Database: {self.db_path}")
        lines.append(f"Validation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total Candles: {report.total_candles:,}")
        lines.append(f"Symbols: {', '.join(report.symbols_checked)}")
        lines.append(f"Timeframes: {', '.join(report.timeframes_checked)}")
        lines.append(f"Total Issues: {len(report.issues)}")
        lines.append("="*100)
        
        if len(report.issues) == 0:
            lines.append("\n✅ NO ISSUES FOUND - Data integrity validated successfully!")
            lines.append("="*100)
            return "\n".join(lines)
        
        # Group issues by type
        issue_types = ['duplicate', 'misaligned', 'gap', 'invalid_timestamp', 'null_value', 'invalid_data']
        
        for issue_type in issue_types:
            type_issues = report.get_issues_by_type(issue_type)
            if not type_issues:
                continue
            
            lines.append(f"\n{issue_type.upper().replace('_', ' ')} ({len(type_issues)} issues)")
            lines.append("-"*100)
            
            for i, issue in enumerate(type_issues[:50], 1):  # Limit to first 50 per type
                ts_str = datetime.fromtimestamp(issue.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if issue.timestamp else 'N/A'
                lines.append(f"{i}. [{issue.severity.upper()}] {issue.symbol} {issue.timeframe}")
                lines.append(f"   Timestamp: {issue.timestamp} ({ts_str})")
                lines.append(f"   {issue.description}")
                if issue.details:
                    lines.append(f"   Details: {issue.details}")
                lines.append("")
            
            if len(type_issues) > 50:
                lines.append(f"   ... and {len(type_issues) - 50} more issues of this type")
                lines.append("")
        
        lines.append("="*100)
        
        return "\n".join(lines)


# CLI execution
if __name__ == '__main__':
    import sys
    
    def main():
        # Default database path
        db_path = 'data/trading.duckdb'
        
        # Check for command line argument
        if len(sys.argv) > 1:
            db_path = sys.argv[1]
        
        print('\n' + '='*100)
        print('🔍 CANDLE DATA VALIDATION')
        print('='*100)
        print(f'Database: {db_path}')
        print(f'Validation Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print('='*100)
        print('\nRunning comprehensive validation checks...')
        print('  ✓ Checking for duplicate candles')
        print('  ✓ Checking for misaligned timestamps')
        print('  ✓ Checking for gaps in sequences')
        print('  ✓ Validating timestamp relationships')
        print('  ✓ Checking for NULL values')
        print('  ✓ Validating OHLCV data quality')
        print('\nThis may take a minute...\n')
        
        # Create validator
        validator = CandlesValidator(db_path)
        
        # Run all validations
        report = validator.validate_all()
        
        # Calculate and display data quality score
        quality_score = validator.get_data_quality_score(report)
        print('\n' + '='*100)
        print('DATA QUALITY SCORE')
        print('='*100)
        print(f"Overall Score: {quality_score['overall']}/100 (Grade: {quality_score['grade']})")
        print('\nCategory Scores:')
        for category, score in quality_score.get('categories', {}).items():
            status = '✅' if score >= 90 else '⚠️' if score >= 70 else '❌'
            print(f"  {status} {category.replace('_', ' ').title()}: {score:.1f}/100")
        print('='*100)
        
        # Print summary
        report.print_summary()
        
        # Show critical issues detail
        critical_issues = [issue for issue in report.issues if issue.severity == 'critical']
        if critical_issues:
            print('\n' + '='*100)
            print(f'CRITICAL ISSUES DETAIL (showing first 20 of {len(critical_issues)}):')
            print('='*100)
            for i, issue in enumerate(critical_issues[:20], 1):
                ts_str = datetime.fromtimestamp(issue.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') if issue.timestamp else 'N/A'
                print(f'\n{i}. [{issue.issue_type.upper()}] {issue.symbol} {issue.timeframe}')
                print(f'   Timestamp: {issue.timestamp} ({ts_str})')
                print(f'   {issue.description}')
                if issue.details:
                    details_str = ', '.join([f'{k}={v}' for k, v in list(issue.details.items())[:5]])
                    print(f'   Details: {details_str}')
            
            if len(critical_issues) > 20:
                print(f'\n... and {len(critical_issues) - 20} more critical issues')
        
        # Show gap summary
        gaps = report.get_issues_by_type('gap')
        if gaps:
            print('\n' + '='*100)
            print(f'GAP SUMMARY ({len(gaps)} gaps found):')
            print('='*100)
            for i, gap in enumerate(gaps[:10], 1):
                start_time = datetime.fromtimestamp(gap.details['gap_start'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                end_time = datetime.fromtimestamp(gap.details['gap_end'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                print(f'\n{i}. {gap.symbol} {gap.timeframe}:')
                print(f'   Missing: {gap.details["missing_candles"]} candles')
                print(f'   Duration: {gap.details["gap_duration_hours"]} hours')
                print(f'   Gap Period: {start_time} → {end_time}')
            
            if len(gaps) > 10:
                print(f'\n... and {len(gaps) - 10} more gaps')
        
        # Show misalignment summary
        misaligned = report.get_issues_by_type('misaligned')
        if misaligned:
            print('\n' + '='*100)
            print(f'MISALIGNMENT SUMMARY ({len(misaligned)} misaligned candles found):')
            print('='*100)
            for i, issue in enumerate(misaligned[:10], 1):
                ts_str = datetime.fromtimestamp(issue.timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                print(f'{i}. {issue.symbol} {issue.timeframe} at {ts_str}')
                print(f'   Offset: {issue.details["offset_seconds"]} seconds from expected boundary')
            
            if len(misaligned) > 10:
                print(f'\n... and {len(misaligned) - 10} more misaligned candles')
        
        print('\n' + '='*100)
        print('✅ Validation Complete!')
        print('='*100)
        
        # Return exit code
        if report.has_critical_issues():
            print('\n⚠️  WARNING: Critical issues detected. Please review and fix.')
            return 1
        else:
            print('\n✅ No critical issues found. Data integrity validated.')
            return 0
    
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f'\n❌ Error running validator: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
