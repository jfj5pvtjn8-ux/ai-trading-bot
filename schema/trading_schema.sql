-- ===========================
-- TRADING BOT DATABASE SCHEMA
-- DuckDB Schema for Market Data & Signals
-- ===========================

-- ===========================
-- SCHEMAS
-- ===========================
CREATE SCHEMA IF NOT EXISTS market;
CREATE SCHEMA IF NOT EXISTS signals;

-- ===========================
-- MARKET.CANDLES
-- Large historical OHLCV series
-- ===========================
CREATE TABLE IF NOT EXISTS market.candles (
    -- Identity
    symbol         TEXT        NOT NULL,       -- e.g. 'BTCUSDT'
    timeframe      TEXT        NOT NULL,       -- e.g. '1m', '5m', '15m', '1h'

    -- Time
    open_ts        BIGINT      NOT NULL,       -- candle OPEN timestamp (seconds, aligned to TF)
    close_ts       BIGINT      NOT NULL,       -- candle CLOSE timestamp (seconds)

    -- OHLCV
    open           DOUBLE      NOT NULL,
    high           DOUBLE      NOT NULL,
    low            DOUBLE      NOT NULL,
    close          DOUBLE      NOT NULL,
    volume         DOUBLE      NOT NULL,
    quote_volume   DOUBLE      DEFAULT 0,      -- optional, from exchange if available
    trade_count    BIGINT      DEFAULT 0,

    -- Metadata
    is_closed      BOOLEAN     NOT NULL DEFAULT TRUE,
    source         TEXT        DEFAULT 'rest', -- 'rest' | 'ws' | 'backfill'
    received_at    TIMESTAMP   DEFAULT now(),

    -- Optional partition helper
    trading_day    DATE        DEFAULT date(from_unixtime(open_ts)),

    -- Constraints (DuckDB will enforce UNIQUE)
    CONSTRAINT candles_pk PRIMARY KEY (symbol, timeframe, open_ts)
);

-- Helpful projection / sort order for large scans
CREATE INDEX IF NOT EXISTS idx_candles_sym_tf_day
ON market.candles (symbol, timeframe, trading_day);


-- ===========================
-- MARKET.TRADES
-- Individual trades (optional, for micro-structure)
-- ===========================
CREATE TABLE IF NOT EXISTS market.trades (
    symbol         TEXT        NOT NULL,
    trade_id       BIGINT      NOT NULL,          -- exchange trade id
    ts             BIGINT      NOT NULL,          -- trade timestamp (seconds or ms truncated)
    price          DOUBLE      NOT NULL,
    qty            DOUBLE      NOT NULL,
    side           TEXT,                           -- 'buy' | 'sell' if available
    is_maker       BOOLEAN,                        -- maker / taker flag if available
    received_at    TIMESTAMP   DEFAULT now(),

    CONSTRAINT trades_pk PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_sym_ts
ON market.trades (symbol, ts);


-- ===========================
-- MARKET.ORDERBOOK
-- You can store *snapshots* or *events*.
-- Here: top-N snapshot in a compact form.
-- ===========================
CREATE TABLE IF NOT EXISTS market.orderbook_snapshots (
    symbol         TEXT        NOT NULL,
    ts             BIGINT      NOT NULL,          -- snapshot timestamp (seconds or ms truncated)

    -- Best bid/ask
    best_bid_price DOUBLE,
    best_bid_qty   DOUBLE,
    best_ask_price DOUBLE,
    best_ask_qty   DOUBLE,

    -- Optional top-of-book depth as lists
    bid_prices     DOUBLE[],                      -- DuckDB LIST<DOUBLE>
    bid_qtys       DOUBLE[],
    ask_prices     DOUBLE[],
    ask_qtys       DOUBLE[],

    received_at    TIMESTAMP DEFAULT now(),

    CONSTRAINT ob_snap_pk PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_ob_sym_ts
ON market.orderbook_snapshots (symbol, ts);


-- ===========================
-- MARKET.FEATURES
-- Derived indicators for each candle (ATR, EMA, FVG, zones, etc.)
-- One row per (symbol, timeframe, open_ts).
-- ===========================
CREATE TABLE IF NOT EXISTS market.features (
    symbol         TEXT        NOT NULL,
    timeframe      TEXT        NOT NULL,
    open_ts        BIGINT      NOT NULL,      -- matches market.candles.open_ts

    -- Volatility / trend
    atr_14         DOUBLE,
    ema_20         DOUBLE,
    ema_50         DOUBLE,
    ema_200        DOUBLE,
    rsi_14         DOUBLE,

    -- Structure / zones (IDs refer to external zone tables if you add them later)
    swing_high     BOOLEAN     DEFAULT FALSE,
    swing_low      BOOLEAN     DEFAULT FALSE,
    fvg_up         BOOLEAN     DEFAULT FALSE,
    fvg_down       BOOLEAN     DEFAULT FALSE,
    order_block_id TEXT,
    breaker_block_id TEXT,
    liquidity_zone_id TEXT,

    -- Volume profile / micro-structure
    volume_delta   DOUBLE,                    -- buy - sell, if you derive it
    session_tag    TEXT,                      -- e.g. 'LO', 'NY', 'ASIA'

    -- Metadata
    computed_at    TIMESTAMP   DEFAULT now(),

    CONSTRAINT features_pk PRIMARY KEY (symbol, timeframe, open_ts)
);

CREATE INDEX IF NOT EXISTS idx_features_sym_tf_ts
ON market.features (symbol, timeframe, open_ts);


-- ===========================
-- SIGNALS.MODEL_INPUTS
-- What you fed into the model (for audit / training / replay)
-- ===========================
CREATE TABLE IF NOT EXISTS signals.model_inputs (
    symbol          TEXT        NOT NULL,
    timeframe       TEXT        NOT NULL,
    open_ts         BIGINT      NOT NULL,     -- candle open_ts
    model_name      TEXT        NOT NULL,     -- e.g. 'lm_v1', 'xgboost_v2'
    feature_version TEXT        NOT NULL,     -- to track feature-engineering version

    -- Raw / serialized input vector
    features_json   JSON,                    -- or VARCHAR if you want raw string
    -- optional: flattened columns if you want direct querying

    created_at      TIMESTAMP   DEFAULT now(),

    CONSTRAINT model_inputs_pk PRIMARY KEY (symbol, timeframe, open_ts, model_name, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_inputs_model_ts
ON signals.model_inputs (model_name, timeframe, open_ts);


-- ===========================
-- SIGNALS.MODEL_OUTPUTS
-- Model predictions + decisions + execution state
-- ===========================
CREATE TABLE IF NOT EXISTS signals.model_outputs (
    symbol          TEXT        NOT NULL,
    timeframe       TEXT        NOT NULL,
    open_ts         BIGINT      NOT NULL,       -- the candle the prediction is for
    model_name      TEXT        NOT NULL,
    feature_version TEXT        NOT NULL,

    -- Prediction fields
    direction       TEXT,                       -- 'long' | 'short' | 'flat'
    score_long      DOUBLE,
    score_short     DOUBLE,
    score_neutral   DOUBLE,
    raw_output      JSON,                       -- raw logits/probs if you want

    -- Decision / execution tracking
    strategy_id     TEXT,                       -- your strategy/router id
    decided_action  TEXT,                       -- 'enter_long', 'close_short', etc.
    order_id        TEXT,                       -- linked to execution layer, if any
    executed        BOOLEAN     DEFAULT FALSE,
    pnl_realized    DOUBLE      DEFAULT 0,
    pnl_unrealized  DOUBLE      DEFAULT 0,

    created_at      TIMESTAMP   DEFAULT now(),

    CONSTRAINT model_outputs_pk PRIMARY KEY (symbol, timeframe, open_ts, model_name, feature_version)
);

CREATE INDEX IF NOT EXISTS idx_outputs_model_ts
ON signals.model_outputs (model_name, timeframe, open_ts);
