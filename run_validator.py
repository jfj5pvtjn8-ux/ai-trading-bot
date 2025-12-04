"""
Script to run the candles validator
"""

import sys
sys.path.insert(0, 'src')

from trading_bot.validators.candles_validator import CandlesValidator
from datetime import datetime

def main():
    # Create validator
    validator = CandlesValidator('data/trading.duckdb')
    
    print('\n' + '='*100)
    print('🔍 CANDLE DATA VALIDATION')
    print('='*100)
    print(f'Database: data/trading.duckdb')
    print(f'Validation Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('='*100)
    print('\nRunning comprehensive validation checks...')
    print('  - Checking for duplicate candles')
    print('  - Checking for misaligned timestamps')
    print('  - Checking for gaps in sequences')
    print('  - Validating timestamp relationships')
    print('  - Checking for NULL values')
    print('  - Validating OHLCV data quality')
    print('\nThis may take a minute...\n')
    
    # Run all validations
    report = validator.validate_all(
        symbols=['BTCUSDT', 'XRPUSDT'],
        timeframes=['1m', '5m', '15m', '1h']
    )
    
    # Print summary
    report.print_summary()
    
    # Show detailed report for critical issues
    critical_issues = [issue for issue in report.issues if issue.severity == 'critical']
    if critical_issues:
        print('\n' + '='*100)
        print(f'CRITICAL ISSUES DETAIL (showing first 20 of {len(critical_issues)}):')
        print('='*100)
        for i, issue in enumerate(critical_issues[:20], 1):
            ts_str = datetime.utcfromtimestamp(issue.timestamp).strftime('%Y-%m-%d %H:%M:%S') if issue.timestamp else 'N/A'
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
            start_time = datetime.utcfromtimestamp(gap.details['gap_start']).strftime('%Y-%m-%d %H:%M:%S')
            end_time = datetime.utcfromtimestamp(gap.details['gap_end']).strftime('%Y-%m-%d %H:%M:%S')
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
            ts_str = datetime.utcfromtimestamp(issue.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            print(f'{i}. {issue.symbol} {issue.timeframe} at {ts_str}')
            print(f'   Offset: {issue.details["offset_seconds"]} seconds from expected boundary')
        
        if len(misaligned) > 10:
            print(f'\n... and {len(misaligned) - 10} more misaligned candles')
    
    print('\n' + '='*100)
    print('✅ Validation Complete!')
    print('='*100)
    
    # Return exit code based on critical issues
    if report.has_critical_issues():
        print('\n⚠️  WARNING: Critical issues detected. Please review and fix.')
        return 1
    else:
        print('\n✅ No critical issues found. Data integrity validated.')
        return 0

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f'\n❌ Error running validator: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
