"""Validators for app configuration."""
from typing import Any, Dict, Optional
from ..base import BaseValidator, ConfigError


class AppConfigValidator(BaseValidator):
    """Validator for app configuration objects."""

    @staticmethod
    def validate_app_info(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "app", path)
        
        name = obj.get("name")
        BaseValidator.validate_string(name, "app.name", path=path)
        
        log_level = obj.get("log_level", "INFO")
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if log_level not in valid_levels:
            ctx = f"{path}: " if path else ""
            raise ConfigError(f"{ctx}app.log_level must be one of {valid_levels}")

    @staticmethod
    def validate_candle_settings(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "candles", path)
        
        max_size = obj.get("max_size", 600)
        BaseValidator.validate_int(max_size, "candles.max_size", min_value=1, path=path)
        
        initial_retries = obj.get("initial_retries", 3)
        BaseValidator.validate_int(initial_retries, "candles.initial_retries", min_value=0, path=path)
        
        retry_delay = obj.get("retry_delay", 0.5)
        BaseValidator.validate_float(retry_delay, "candles.retry_delay", min_value=0, path=path)

    @staticmethod
    def validate_duckdb_settings(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "duckdb", path)
        
        # Validate enabled flag
        enabled = obj.get("enabled", True)
        if not isinstance(enabled, bool):
            ctx = f"{path}: " if path else ""
            raise ConfigError(f"{ctx}duckdb.enabled must be a boolean")
        
        # Validate database_path
        database_path = obj.get("database_path", "data/trading.duckdb")
        BaseValidator.validate_string(database_path, "duckdb.database_path", path=path)
        
        # Validate fresh_start flag
        fresh_start = obj.get("fresh_start", False)
        if not isinstance(fresh_start, bool):
            ctx = f"{path}: " if path else ""
            raise ConfigError(f"{ctx}duckdb.fresh_start must be a boolean")
        
        # Validate max_gap_hours
        max_gap_hours = obj.get("max_gap_hours", 168)
        BaseValidator.validate_int(max_gap_hours, "duckdb.max_gap_hours", min_value=1, path=path)
        
        # Validate initial_candles
        initial_candles = obj.get("initial_candles", {})
        BaseValidator.validate_dict(initial_candles, "duckdb.initial_candles", path)
        
        # Validate each timeframe has positive integer value
        for tf, count in initial_candles.items():
            BaseValidator.validate_string(tf, "duckdb.initial_candles key", path=path)
            BaseValidator.validate_int(count, f"duckdb.initial_candles[{tf}]", min_value=1, path=path)

    @staticmethod
    def validate_exchange_settings(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "exchange", path)
        
        name = obj.get("name")
        BaseValidator.validate_string(name, "exchange.name", path=path)
        
        rest_endpoint = obj.get("rest_endpoint")
        BaseValidator.validate_url(rest_endpoint, "exchange.rest_endpoint", schemes=("http", "https"), path=path)
        
        ws_endpoint = obj.get("ws_endpoint")
        BaseValidator.validate_url(ws_endpoint, "exchange.ws_endpoint", schemes=("ws", "wss"), path=path)
        
        request_timeout = obj.get("request_timeout", 5)
        BaseValidator.validate_int(request_timeout, "exchange.request_timeout", min_value=1, path=path)

    @staticmethod
    def validate_app_config(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        base = path if path else ""
        ctx = f"{base}: " if base else ""
        
        BaseValidator.validate_dict(obj, "config", path)
        
        # Validate app section
        app = obj.get("app")
        if not app:
            raise ConfigError(f"{ctx}missing required 'app' section")
        AppConfigValidator.validate_app_info(app, path=f"{base}.app" if base else "app")
        
        # Validate timeframe_intervals
        timeframe_intervals = obj.get("timeframe_intervals", {})
        BaseValidator.validate_dict(timeframe_intervals, "timeframe_intervals", path)
        
        for tf, seconds in timeframe_intervals.items():
            BaseValidator.validate_string(tf, f"timeframe_intervals key", path=path)
            BaseValidator.validate_int(seconds, f"timeframe_intervals[{tf}]", min_value=1, path=path)
        
        # Validate candles section
        candles = obj.get("candles", {})
        AppConfigValidator.validate_candle_settings(
            candles, path=f"{base}.candles" if base else "candles"
        )
        
        # Validate duckdb section (optional)
        duckdb = obj.get("duckdb", {})
        if duckdb:
            AppConfigValidator.validate_duckdb_settings(
                duckdb, path=f"{base}.duckdb" if base else "duckdb"
            )
        
        # Validate exchange section
        exchange = obj.get("exchange", {})
        AppConfigValidator.validate_exchange_settings(
            exchange, path=f"{base}.exchange" if base else "exchange"
        )


__all__ = ["AppConfigValidator", "ConfigError"]
