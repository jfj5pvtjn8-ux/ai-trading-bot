# AI Trading Bot

A production-ready cryptocurrency trading bot with real-time candle processing, multi-timeframe analysis, and automated data management.

## 🚀 Quick Start

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Run the bot
python main.py
```

The bot will:
- Load configs from `config/` directory
- Fetch historical candles for BTCUSDT (1m, 5m, 15m, 1h)
- Start WebSocket for real-time updates
- Health endpoint: `http://localhost:8080/health`

## ✅ Features

- **Real-time WebSocket Streaming** - Live candle data from Binance  
- **Multi-Timeframe Support** - Simultaneous tracking of 1m, 5m, 15m, 1h  
- **Automatic Gap Filling** - Missing candles fetched via REST API  
- **Parquet Storage** - Efficient data persistence with 7-day retention  
- **Health Monitoring** - HTTP endpoints for status and statistics  
- **Closed Candles Only** - Processes only completed candles (x=true)  
- **Graceful Shutdown** - Clean resource cleanup on exit

## 📊 How It Works

### Data Flow
```
1. InitialCandlesLoader → Fetch historical candles (REST API)
2. CandleManager → Store in memory + Parquet files  
3. WebSocket → Stream real-time closed candles
4. CandleSync → Validate + fill gaps
5. Ready for trading logic
```

### Multi-Timeframe Subscriptions
The bot subscribes to **separate WebSocket streams** per timeframe:
- `btcusdt@kline_1m` → 1-minute candles
- `btcusdt@kline_5m` → 5-minute candles
- `btcusdt@kline_15m` → 15-minute candles
- `btcusdt@kline_1h` → 1-hour candles

Each timeframe has **independent** callbacks and storage.

### Closed Candles Only
```python
# WebSocket only processes closed candles
if not kline.get("x", False):
    return  # Skip if candle not closed
```

## 📁 Project Structure

```
trading-bot/
├── main.py                # Entry point - RUN THIS
├── src/trading_bot/
│   ├── bot.py            # Main orchestrator
│   ├── api/
│   │   ├── rest_client.py   # Historical data
│   │   └── ws_client.py     # Real-time stream
│   ├── core/
│   │   └── candles/
│   │       ├── candle_manager.py         # Storage
│   │       ├── candle_sync.py            # Validation
│   │       └── initial_candles_loader.py # Historical loader
│   ├── config/            # Config loaders
│   └── storage/
│       └── parquet_storage.py  # Persistence
├── config/
│   ├── app.yml           # Exchange endpoints
│   └── symbols.yml       # Trading symbols & timeframes
├── data/live/            # Parquet files (7-day retention)
└── .env                  # Environment variables
```

## ⚙️ Configuration

### `config/symbols.yml`
```yaml
symbols:
  - name: "BTCUSDT"
    enabled: true
    timeframes:
      - tf: "1m"
        fetch: 3000  # ~2 days of 1m candles
      - tf: "5m"
        fetch: 1500  # ~5 days
      - tf: "15m"
        fetch: 800   # ~8 days
      - tf: "1h"
        fetch: 400   # ~17 days
```

### `.env`
```bash
REST_ENDPOINT=https://api.binance.com
WS_ENDPOINT=wss://stream.binance.com:9443
DATA_DIR=data/live
PARQUET_RETENTION_DAYS=7
HEALTH_PORT=8080
LOG_LEVEL=INFO
```

## 🔍 API Endpoints

### Health Check
```bash
curl http://localhost:8080/health
```
Returns candle counts and connection status per symbol/timeframe.

### Statistics
```bash
curl http://localhost:8080/stats
```
Returns detailed statistics including last candle data.

## 📝 Logs

Logs written to:
- **Console**: Colorized real-time output
- **File**: `logs/trading_bot_{DATE}.log`

## 🧪 Testing

```bash
# Validate Parquet files
python src/trading_bot/utils/validate_parquet.py

# Check WebSocket connection
curl http://localhost:8080/health | jq '.websocket'
```

## 🛠️ Development

### Dependencies
```bash
pip install -r requirements.txt
```

Core libraries:
- `pandas` - Data manipulation
- `pyarrow` - Parquet storage
- `websocket-client` - Real-time streaming
- `requests` - REST API
- `flask` - Health endpoints
- `pyyaml` - Config parsing

### Code Structure
- **No trading logic yet** - Clean foundation for strategy development
- **Modular design** - Easy to extend with decision layers
- **Fully tested imports** - No compilation errors

## 🎯 Next Steps

Ready to add:
1. **Trading strategies** (moving averages, RSI, etc.)
2. **Order execution** (buy/sell logic)
3. **Risk management** (position sizing, stop loss)
4. **More symbols** (ETH, SOL, etc.)

## 📜 License

MIT

## 🤝 Contributing

Issues and PRs welcome!
