"""Dataclass models for app configuration."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any

from .validators import AppConfigValidator


@dataclass(frozen=True)
class AppInfo:
    """Application metadata."""
    name: str
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "AppInfo":
        AppConfigValidator.validate_app_info(obj)
        return cls(
            name=obj.get("name"),
            log_level=obj.get("log_level", "INFO")
        )

    def __repr__(self) -> str:
        return f"AppInfo(name={self.name!r}, log_level={self.log_level!r})"


@dataclass(frozen=True)
class CandleSettings:
    """Candle manager and loader settings."""
    max_size: int = 600
    initial_retries: int = 3
    retry_delay: float = 0.5

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "CandleSettings":
        AppConfigValidator.validate_candle_settings(obj)
        return cls(
            max_size=obj.get("max_size", 600),
            initial_retries=obj.get("initial_retries", 3),
            retry_delay=obj.get("retry_delay", 0.5)
        )

    def __repr__(self) -> str:
        return f"CandleSettings(max_size={self.max_size}, initial_retries={self.initial_retries}, retry_delay={self.retry_delay})"


@dataclass(frozen=True)
class DuckDBSettings:
    """DuckDB initial candles configuration."""
    initial_candles: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "DuckDBSettings":
        AppConfigValidator.validate_duckdb_settings(obj)
        return cls(
            initial_candles=obj.get("initial_candles", {})
        )

    def get_initial_candles(self, timeframe: str, default: int = 1000) -> int:
        """Get number of initial candles to load for a timeframe."""
        return self.initial_candles.get(timeframe, default)

    def __repr__(self) -> str:
        return f"DuckDBSettings(initial_candles={self.initial_candles})"


@dataclass(frozen=True)
class ExchangeSettings:
    """Exchange API settings."""
    name: str
    rest_endpoint: str
    ws_endpoint: str
    request_timeout: int = 5

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "ExchangeSettings":
        AppConfigValidator.validate_exchange_settings(obj)
        return cls(
            name=obj.get("name"),
            rest_endpoint=obj.get("rest_endpoint"),
            ws_endpoint=obj.get("ws_endpoint"),
            request_timeout=obj.get("request_timeout", 5)
        )

    def __repr__(self) -> str:
        return f"ExchangeSettings(name={self.name!r}, rest_endpoint={self.rest_endpoint!r}, ws_endpoint={self.ws_endpoint!r}, request_timeout={self.request_timeout})"


@dataclass(frozen=True)
class AppConfig:
    """Root application configuration."""
    app: AppInfo
    timeframe_intervals: Dict[str, int] = field(default_factory=dict)
    candles: CandleSettings = field(default_factory=CandleSettings)
    duckdb: DuckDBSettings = field(default_factory=DuckDBSettings)
    exchange: ExchangeSettings = field(default_factory=ExchangeSettings)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "AppConfig":
        AppConfigValidator.validate_app_config(obj)
        
        app_info = AppInfo.from_dict(obj.get("app", {}))
        timeframe_intervals = obj.get("timeframe_intervals", {})
        candles = CandleSettings.from_dict(obj.get("candles", {}))
        duckdb = DuckDBSettings.from_dict(obj.get("duckdb", {}))
        exchange = ExchangeSettings.from_dict(obj.get("exchange", {}))
        
        return cls(
            app=app_info,
            timeframe_intervals=timeframe_intervals,
            candles=candles,
            duckdb=duckdb,
            exchange=exchange
        )

    def get_timeframe_seconds(self, timeframe: str) -> int:
        """Get interval in seconds for a timeframe."""
        if timeframe not in self.timeframe_intervals:
            raise ValueError(f"Unknown timeframe: {timeframe}")
        return self.timeframe_intervals[timeframe]

    def __repr__(self) -> str:
        return f"AppConfig(app={self.app!r}, timeframes={len(self.timeframe_intervals)}, candles={self.candles!r}, duckdb={self.duckdb!r}, exchange={self.exchange!r})"


__all__ = ["AppConfig", "AppInfo", "CandleSettings", "DuckDBSettings", "ExchangeSettings"]
