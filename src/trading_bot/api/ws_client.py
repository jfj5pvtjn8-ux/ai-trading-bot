"""
High-Reliability Binance WebSocket Client
-----------------------------------------

Features:
- Multi-symbol, multi-timeframe streaming
- Guaranteed callback signature: callback(candle_dict)
- Closed-candle filtering
- Automatic reconnection with exponential backoff
- Stream-name normalization (fixes missing-callback issues)
- Thread-safe subscription registry
- Heartbeat logging for monitoring
"""

import json
import time
import threading
from typing import Dict, Callable, Optional, Any, List
from websocket import WebSocketApp

from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class WebSocketClient:
    def __init__(self, app_config: AppConfig):
        self.logger = get_logger(__name__)
        self.app_config = app_config

        self.ws_endpoint = app_config.exchange.ws_endpoint

        # Runtime state
        self.ws: Optional[WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.is_connected = False

        # Subscription registry
        self.subscriptions: List[str] = []
        self.callbacks: Dict[str, Callable[[Dict[str, Any]], None]] = {}

        # Reconnection
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 15
        self.reconnect_delay = 1  # seconds

        # Thread-safety
        self.lock = threading.Lock()

        # Heartbeat monitoring
        self.last_message_ts = 0
        self.heartbeat_interval = 30  # warning if no messages for 30s

    # -------------------------------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------------------------------

    def subscribe(self, symbol: str, timeframe: str,
                  callback: Callable[[Dict[str, Any]], None]):
        """
        Subscribe to a stream such as btcusdt@kline_1m.
        Callback ALWAYS receives exactly: callback(candle)
        """
        stream = f"{symbol.lower()}@kline_{timeframe}"

        with self.lock:
            if stream in self.subscriptions:
                self.logger.warning(f"[WebSocket] Duplicate subscription: {stream}")
                return

            self.subscriptions.append(stream)
            self.callbacks[stream] = callback

        self.logger.info(f"[WebSocket] Subscribed → {stream}")

    def start(self):
        """Open WebSocket connection."""
        if self.is_running:
            self.logger.warning("[WebSocket] Already running")
            return

        if not self.subscriptions:
            self.logger.error("[WebSocket] No subscriptions to start WebSocket")
            return

        stream_query = "/".join(self.subscriptions)
        url = f"{self.ws_endpoint}/stream?streams={stream_query}"

        self.logger.info(f"[WebSocket] Connecting to {len(self.subscriptions)} streams")
        self.logger.debug(f"[WebSocket] URL: {url}")

        self.is_running = True

        self.ws = WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message_wrapper,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
        self.ws_thread.start()

        # Start heartbeat monitor
        threading.Thread(target=self._heartbeat_monitor, daemon=True).start()

        self.logger.info("[WebSocket] Started.")

    def stop(self):
        """Gracefully stop the WebSocket client."""
        self.logger.info("[WebSocket] Stopping...")
        self.is_running = False
        self.is_connected = False

        if self.ws:
            try:
                self.ws.close()
            except:
                pass

        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5)

        self.logger.info("[WebSocket] Stopped.")

    def is_alive(self):
        return self.is_running and self.is_connected

    # -------------------------------------------------------------------------
    # INTERNAL WRAPPER (Fixes callback argument mismatch)
    # -------------------------------------------------------------------------

    def _on_message_wrapper(self, ws, message):
        """WebSocketApp passes (ws, message). We ignore ws."""
        try:
            self._on_message(message)
        except Exception as e:
            self.logger.error(f"[WebSocket] Wrapper error: {e}")

    # -------------------------------------------------------------------------
    # LOW-LEVEL CALLBACKS
    # -------------------------------------------------------------------------

    def _run_forever(self):
        """Handles reconnection logic."""
        while self.is_running:
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
                if self.is_running:
                    self._handle_reconnect()
            except Exception as e:
                self.logger.error(f"[WebSocket] run_forever exception: {e}")
                time.sleep(2)

    def _on_open(self, ws):
        self.is_connected = True
        self.reconnect_attempts = 0
        self.last_message_ts = time.time()
        self.logger.info("[WebSocket] Connected.")

    def _on_close(self, ws, code, reason):
        self.is_connected = False
        self.logger.warning(f"[WebSocket] Closed → code={code}, reason={reason}")

    def _on_error(self, ws, error):
        self.logger.error(f"[WebSocket] Error: {error}")

    # -------------------------------------------------------------------------
    # MESSAGE PROCESSING
    # -------------------------------------------------------------------------

    def _on_message(self, message: str):
        """Process Binance combined stream messages."""
        self.last_message_ts = time.time()

        try:
            msg = json.loads(message)

            # Combined streams always include "stream" & "data"
            if "stream" not in msg or "data" not in msg:
                return

            stream = msg["stream"].lower()  # Normalized for dictionary lookup
            data = msg["data"]

            if data.get("e") != "kline":
                return

            k = data.get("k")
            if not k:
                return

            # Only closed candles
            if not k.get("x", False):
                return

            candle = self._normalize_kline(k)

            cb = self.callbacks.get(stream)
            if cb:
                try:
                    cb(candle)
                except Exception as e:
                    self.logger.error(f"[WebSocket] Callback error ({stream}): {e}")

        except Exception as e:
            self.logger.error(f"[WebSocket] on_message error: {e}")

    # -------------------------------------------------------------------------
    # NORMALIZATION
    # -------------------------------------------------------------------------

    def _normalize_kline(self, k: Dict[str, Any]) -> Dict[str, Any]:
        """Convert raw kline into internal normalized format."""
        try:
            ts = k.get("T")
            if ts is None:
                raise ValueError("Missing close timestamp T")

            return {
                "ts": int(ts) // 1000,
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
            }

        except Exception as e:
            self.logger.error(f"[WebSocket] Failed to normalize kline: {e}")
            return {}

    # -------------------------------------------------------------------------
    # RECONNECTION LOGIC
    # -------------------------------------------------------------------------

    def _handle_reconnect(self):
        if not self.is_running:
            return

        self.is_connected = False

        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error("[WebSocket] Max reconnect attempts reached.")
            self.is_running = False
            return

        self.reconnect_attempts += 1
        wait = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 60)

        self.logger.warning(f"[WebSocket] Reconnecting in {wait}s...")
        time.sleep(wait)

    # -------------------------------------------------------------------------
    # HEARTBEAT MONITOR
    # -------------------------------------------------------------------------

    def _heartbeat_monitor(self):
        """Warns if no WebSocket messages received for heartbeat_interval seconds."""
        while self.is_running:
            time.sleep(self.heartbeat_interval)
            if self.is_connected:
                delta = time.time() - self.last_message_ts
                if delta > self.heartbeat_interval:
                    self.logger.warning(
                        f"[WebSocket] Heartbeat stalled ({int(delta)}s without data)"
                    )
