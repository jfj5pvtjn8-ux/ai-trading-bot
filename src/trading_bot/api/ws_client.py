"""
WebSocket Client: Real-time candle stream from Binance WebSocket API.

Handles:
- Multi-symbol, multi-timeframe subscriptions
- Automatic reconnection on disconnect
- Proper error handling and rate limiting
- Routing closed candles to appropriate handlers
"""

import json
import time
import threading
from typing import Dict, Callable, Optional, List, Any
from websocket import WebSocketApp
from trading_bot.core.logger import get_logger
from trading_bot.config.app.models import AppConfig


class WebSocketClient:
    """
    Binance WebSocket client for real-time kline (candle) streaming.
    
    Features:
    - Subscribe to multiple symbol@kline_timeframe streams
    - Automatic reconnection with exponential backoff
    - Thread-safe event handling
    - Routes closed candles to registered callbacks
    """

    def __init__(self, app_config: AppConfig):
        """
        Args:
            app_config: AppConfig instance with ws_endpoint
        """
        self.logger = get_logger(__name__)
        self.app_config = app_config
        self.ws_endpoint = app_config.exchange.ws_endpoint
        
        # WebSocket connection
        self.ws: Optional[WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        
        # Connection state
        self.is_running = False
        self.is_connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 1  # seconds
        
        # Subscription management
        self.subscriptions: List[str] = []  # ["btcusdt@kline_1m", ...]
        self.callbacks: Dict[str, Callable] = {}  # {stream_name: callback_fn}
        
        # Threading lock for thread-safe operations
        self.lock = threading.Lock()

    # -------------------------------------------------------------------------
    # PUBLIC API
    # -------------------------------------------------------------------------

    def subscribe(
        self,
        symbol: str,
        timeframe: str,
        callback: Callable[[Dict[str, Any]], None]
    ):
        """
        Subscribe to a symbol@kline_timeframe stream.
        
        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            timeframe: Interval (e.g., "1m", "5m", "15m", "1h")
            callback: Function to call when closed candle received
                      callback(candle: Dict[str, Any])
        """
        stream_name = f"{symbol.lower()}@kline_{timeframe}"
        
        with self.lock:
            if stream_name not in self.subscriptions:
                self.subscriptions.append(stream_name)
                self.callbacks[stream_name] = callback
                self.logger.info(f"[WebSocket] Subscribed to {stream_name}")
            else:
                self.logger.warning(f"[WebSocket] Already subscribed to {stream_name}")

    def start(self):
        """Start the WebSocket connection in a separate thread."""
        if self.is_running:
            self.logger.warning("[WebSocket] Already running")
            return
        
        if not self.subscriptions:
            self.logger.error("[WebSocket] No subscriptions configured. Call subscribe() first.")
            return
        
        self.is_running = True
        self.reconnect_attempts = 0
        
        # Build WebSocket URL with combined streams
        streams = "/".join(self.subscriptions)
        ws_url = f"{self.ws_endpoint}/stream?streams={streams}"
        
        self.logger.info(f"[WebSocket] Starting connection to {len(self.subscriptions)} streams")
        self.logger.debug(f"[WebSocket] URL: {ws_url}")
        
        # Create WebSocket app
        self.ws = WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # Run in separate thread
        self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
        self.ws_thread.start()
        
        self.logger.info("[WebSocket] Started in background thread")

    def stop(self):
        """Stop the WebSocket connection gracefully."""
        if not self.is_running:
            self.logger.warning("[WebSocket] Not running")
            return
        
        self.logger.info("[WebSocket] Stopping connection...")
        self.is_running = False
        
        if self.ws:
            self.ws.close()
        
        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5)
        
        self.is_connected = False
        self.logger.info("[WebSocket] Stopped")

    def is_alive(self) -> bool:
        """Check if WebSocket is connected and running."""
        return self.is_running and self.is_connected

    # -------------------------------------------------------------------------
    # INTERNAL WEBSOCKET CALLBACKS
    # -------------------------------------------------------------------------

    def _run_forever(self):
        """Run WebSocket connection with automatic reconnection."""
        while self.is_running:
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
                
                # If we exit normally and should still be running, reconnect
                if self.is_running:
                    self._handle_reconnect()
                    
            except Exception as e:
                self.logger.error(f"[WebSocket] Exception in run_forever: {e}")
                if self.is_running:
                    self._handle_reconnect()

    def _on_open(self, ws):
        """Called when WebSocket connection is established."""
        self.is_connected = True
        self.reconnect_attempts = 0
        self.logger.info(f"[WebSocket] Connected to {len(self.subscriptions)} streams")

    def _on_message(self, ws, message: str):
        """
        Called when a message is received from WebSocket.
        
        Binance stream message format:
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1638747660000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1638747600000,  // Kline start time
                    "T": 1638747659999,  // Kline close time
                    "s": "BTCUSDT",
                    "i": "1m",
                    "f": 100,
                    "L": 200,
                    "o": "50000.00",
                    "c": "50100.00",
                    "h": "50200.00",
                    "l": "49900.00",
                    "v": "10.5",
                    "n": 100,
                    "x": true,  // Is this kline closed?
                    "q": "500000.00",
                    "V": "5.5",
                    "Q": "250000.00",
                    "B": "0"
                }
            }
        }
        """
        try:
            msg = json.loads(message)
            
            # Validate message structure
            if "stream" not in msg or "data" not in msg:
                self.logger.debug(f"[WebSocket] Ignoring non-stream message")
                return
            
            stream_name = msg["stream"]
            data = msg["data"]
            
            # Only process kline events
            if data.get("e") != "kline":
                return
            
            kline = data.get("k", {})
            
            # Only process CLOSED candles
            if not kline.get("x", False):
                return
            
            # Normalize to internal candle format
            candle = self._normalize_kline(kline)
            
            # Route to appropriate callback
            if stream_name in self.callbacks:
                try:
                    self.callbacks[stream_name](candle)
                except Exception as e:
                    self.logger.error(
                        f"[WebSocket] Error in callback for {stream_name}: {e}"
                    )
            else:
                self.logger.warning(
                    f"[WebSocket] No callback registered for {stream_name}"
                )
                
        except json.JSONDecodeError as e:
            self.logger.error(f"[WebSocket] Invalid JSON: {e}")
        except Exception as e:
            self.logger.exception(f"[WebSocket] Error processing message: {e}")

    def _on_error(self, ws, error):
        """Called when WebSocket encounters an error."""
        self.logger.error(f"[WebSocket] Error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        """Called when WebSocket connection is closed."""
        self.is_connected = False
        self.logger.warning(
            f"[WebSocket] Connection closed: "
            f"code={close_status_code}, msg={close_msg}"
        )

    # -------------------------------------------------------------------------
    # RECONNECTION LOGIC
    # -------------------------------------------------------------------------

    def _handle_reconnect(self):
        """Handle reconnection with exponential backoff."""
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error(
                f"[WebSocket] Max reconnection attempts ({self.max_reconnect_attempts}) reached. "
                f"Giving up."
            )
            self.is_running = False
            return
        
        self.reconnect_attempts += 1
        wait_time = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 60)
        
        self.logger.info(
            f"[WebSocket] Reconnecting in {wait_time}s "
            f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})"
        )
        
        time.sleep(wait_time)

    # -------------------------------------------------------------------------
    # NORMALIZATION
    # -------------------------------------------------------------------------

    def _normalize_kline(self, kline: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert Binance WebSocket kline format to internal candle format.
        
        Args:
            kline: Binance kline object from WebSocket
            
        Returns:
            Normalized candle dictionary
        """
        try:
            return {
                "ts": int(kline["T"]) // 1000,  # Close time in seconds
                "open": float(kline["o"]),
                "high": float(kline["h"]),
                "low": float(kline["l"]),
                "close": float(kline["c"]),
                "volume": float(kline["v"]),
            }
        except (KeyError, ValueError, TypeError) as e:
            self.logger.error(f"[WebSocket] Failed to normalize kline: {e}")
            raise

    # -------------------------------------------------------------------------
    # CONTEXT MANAGER SUPPORT
    # -------------------------------------------------------------------------

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


__all__ = ["WebSocketClient"]
