"""
WebSocket Client: Real-time closed candle stream from Binance WebSocket API.

Improvements:
- Uses open_ts as canonical timestamp (aligned with REST)
- Full normalization (open_ts, close_ts, volume, trades, taker volumes, quote volume)
- Symbol + timeframe included
- Duplicate candle prevention supported in callback layer
- Alignment validation for each timeframe
- Only CLOSED candles ("x": true)
- Safe reconnection with jitter
"""

import json
import time
import random
import threading
from typing import Dict, Callable, Optional, List, Any
from websocket import WebSocketApp

from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class WebSocketClient:
    """
    Binance WebSocket client for real-time kline streaming.
    
    Features:
    - Multi-symbol multi-timeframe support
    - Automatic reconnection with backoff + jitter
    - Correct normalization for LM compatibility
    - Closed candle-only routing
    - Thread-safe subscriptions
    """

    def __init__(self, app_config: AppConfig):
        self.logger = get_logger(__name__)
        self.app_config = app_config

        self.ws_endpoint = app_config.exchange.ws_endpoint

        # WebSocket connection state
        self.ws: Optional[WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.is_connected = False

        # Reconnect parameters
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 1  # seconds base value

        # Subscriptions
        self.subscriptions: List[str] = []  # ["btcusdt@kline_1m"]
        self.callbacks: Dict[str, Callable] = {}  # {stream: callback}

        self.lock = threading.Lock()

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def subscribe(
        self,
        symbol: str,
        timeframe: str,
        callback: Callable[[str, str, Dict[str, Any]], None]
    ):
        """
        Subscribe to a Binance kline stream.
        
        callback receives: (symbol, timeframe, normalized_candle)
        """
        stream_name = f"{symbol.lower()}@kline_{timeframe}"

        with self.lock:
            if stream_name not in self.subscriptions:
                self.subscriptions.append(stream_name)
                self.callbacks[stream_name] = callback
                self.logger.info(f"[WebSocket] Subscribed to {stream_name}")
            else:
                self.logger.warning(f"[WebSocket] Already subscribed: {stream_name}")

    def start(self):
        """Start the WebSocket connection in a new thread."""
        if self.is_running:
            self.logger.warning("[WebSocket] Already running")
            return

        if not self.subscriptions:
            self.logger.error("[WebSocket] No subscriptions found. Use subscribe().")
            return

        self.is_running = True
        self.reconnect_attempts = 0

        streams = "/".join(self.subscriptions)
        ws_url = f"{self.ws_endpoint}/stream?streams={streams}"

        self.logger.info(f"[WebSocket] Connecting to {len(self.subscriptions)} streams")
        self.logger.debug(f"[WebSocket] URL: {ws_url}")

        self.ws = WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
        self.ws_thread.start()

        self.logger.info("[WebSocket] Started in background thread")

    def stop(self):
        """Gracefully stop the WebSocket connection."""
        if not self.is_running:
            self.logger.warning("[WebSocket] Not running")
            return

        self.logger.info("[WebSocket] Stopping...")
        self.is_running = False

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5)

        self.is_connected = False
        self.logger.info("[WebSocket] Stopped")

    def is_alive(self) -> bool:
        return self.is_running and self.is_connected

    # =========================================================================
    # INTERNAL RUN LOOP + CALLBACKS
    # =========================================================================

    def _run_forever(self):
        """Main WebSocket event loop with reconnection logic."""
        while self.is_running:
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10)

                if self.is_running:
                    self._handle_reconnect()

            except Exception as e:
                self.logger.error(f"[WebSocket] Exception in run_forever: {e}")
                if self.is_running:
                    self._handle_reconnect()

    def _on_open(self, ws):
        self.is_connected = True
        self.reconnect_attempts = 0
        self.logger.info(f"[WebSocket] Connected ({len(self.subscriptions)} streams)")

    def _on_message(self, ws, message: str):
        try:
            msg = json.loads(message)

            # Validate message container
            if "stream" not in msg or "data" not in msg:
                return

            stream_name = msg["stream"]
            data = msg["data"]

            if data.get("e") != "kline":
                return

            k = data.get("k", {})

            # Only closed candles
            if not k.get("x", False):
                return

            # Normalize candle
            candle = self._normalize_kline(k)

            # Route to callback
            callback = self.callbacks.get(stream_name)
            if callback:
                try:
                    callback(candle["symbol"], candle["timeframe"], candle)
                except Exception as e:
                    self.logger.error(f"[WebSocket] Callback error for {stream_name}: {e}")
            else:
                self.logger.warning(f"[WebSocket] No callback registered for {stream_name}")

        except Exception as e:
            self.logger.exception(f"[WebSocket] Error processing message: {e}")

    def _on_error(self, ws, error):
        self.logger.error(f"[WebSocket] Error: {error}")

    def _on_close(self, ws, code, msg):
        self.is_connected = False
        self.logger.warning(f"[WebSocket] Closed: code={code}, msg={msg}")

    # =========================================================================
    # RECONNECTION
    # =========================================================================

    def _handle_reconnect(self):
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error("[WebSocket] Max reconnection attempts reached.")
            self.is_running = False
            return

        self.reconnect_attempts += 1

        wait = min(
            self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)),
            60
        )

        # Add jitter to avoid retry storms
        wait += random.uniform(0, 0.3)

        self.logger.info(
            f"[WebSocket] Reconnecting in {wait:.2f}s "
            f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
        )

        time.sleep(wait)

    # =========================================================================
    # NORMALIZATION — LM-READY
    # =========================================================================

    def _normalize_kline(self, k: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize Binance WebSocket kline into internal LM-compatible candle.
        Uses open_ts as canonical timestamp, NOT close_ts.
        """
        try:
            open_ts = int(k["t"]) // 1000
            close_ts = int(k["T"]) // 1000

            symbol = k["s"]
            timeframe = k["i"]

            # Alignment checker
            tf_sec = self.app_config.get_timeframe_seconds(timeframe)
            if open_ts % tf_sec != 0:
                self.logger.warning(
                    f"[WebSocket] Misaligned candle: {symbol} {timeframe} ts={open_ts}"
                )

            return {
                "symbol": symbol,
                "timeframe": timeframe,

                "open_ts": open_ts,
                "close_ts": close_ts,
                "ts": open_ts,  # canonical index

                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),

                "quote_volume": float(k["q"]),
                "trades": int(k["n"]),
                "taker_buy_base": float(k["V"]),
                "taker_buy_quote": float(k["Q"]),
            }

        except Exception as e:
            self.logger.error(f"[WebSocket] Normalize error: {e}")
            raise

    # =========================================================================

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


__all__ = ["WebSocketClient"]
