"""
Test script for RestClient + InitialCandlesLoader + CandleManager + CandleSync integration
"""

from trading_bot.config.app import load_app_config
from trading_bot.config.symbols import TimeframeConfig, SymbolConfig
from trading_bot.api.rest_client import RestClient
from trading_bot.core.candles.initial_candles_loader import InitialCandlesLoader
from trading_bot.core.candles.candle_manager import CandleManager
from trading_bot.core.candles.candle_sync import CandleSync

print('Testing InitialCandlesLoader + CandleManager + CandleSync Integration\n')
print('=' * 70)

# 1. Load configurations
app_config = load_app_config('config/app.yml')
print(f'\n✓ App config loaded')

# 2. Create REST client
rest_client = RestClient(app_config)
print(f'✓ REST client created')

# 3. Create test symbol config
symbol_cfg = SymbolConfig(
    name='BTCUSDT',
    enabled=True,
    timeframes=[
        TimeframeConfig(tf='1m', fetch=10),
        TimeframeConfig(tf='5m', fetch=5)
    ]
)
print(f'✓ Symbol config created: {symbol_cfg.name}')

# 4. Create CandleManagers and CandleSyncs for each timeframe
candle_managers = {}
candle_syncs = {}
liquidity_maps = {}

for tf_cfg in symbol_cfg.timeframes:
    tf = tf_cfg.tf
    
    # Create CandleManager with TF-specific max_size
    candle_managers[tf] = CandleManager(max_size=tf_cfg.fetch)
    
    # Create CandleSync (needs RestClient!)
    candle_syncs[tf] = CandleSync(
        rest_client=rest_client,
        symbol=symbol_cfg.name,
        timeframe=tf,
        candle_manager=candle_managers[tf],
        app_config=app_config
    )
    
    # Liquidity map placeholder (None for now)
    liquidity_maps[tf] = None
    
    print(f'✓ Created CandleManager and CandleSync for {tf}')

# 5. Create InitialCandlesLoader
loader = InitialCandlesLoader(app_config, rest_client)
print(f'✓ InitialCandlesLoader created')

# 6. Load initial candles
print(f'\n📊 Loading initial candles...\n')
success = loader.load_initial_for_symbol(
    symbol_cfg=symbol_cfg,
    candle_managers=candle_managers,
    candle_syncs=candle_syncs,
    liquidity_maps=liquidity_maps
)

if success:
    print(f'\n✅ Initial load completed successfully!\n')
    
    # 7. Verify data
    for tf in ['1m', '5m']:
        cm = candle_managers[tf]
        sync = candle_syncs[tf]
        
        candles = cm.get_all()
        last_ts = cm.last_timestamp()
        latest = cm.get_latest_candle()
        
        print(f'{tf} Timeframe:')
        print(f'  Candles loaded: {len(candles)}')
        print(f'  Last timestamp: {last_ts}')
        print(f'  CandleSync last_ts: {sync.last_closed_ts}')
        print(f'  Latest close: {latest["close"] if latest else "None"}')
        print()
else:
    print(f'\n❌ Initial load failed')

rest_client.close()
print('✓ Test completed!')
