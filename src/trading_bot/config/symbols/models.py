"""Dataclass models for symbols configuration (symbols package).

This mirrors `core.config.models` but lives inside the `core.config.symbols`
package to scope symbols-specific configuration code.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Any, Dict

from .validators import ConfigValidator


@dataclass(frozen=True)
class TimeframeConfig:
    """Configuration for a single timeframe."""
    tf: str
    fetch: int

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "TimeframeConfig":
        """Create from a dict with validation."""
        ConfigValidator.validate_timeframe(obj)
        tf = obj.get("tf")
        fetch = obj.get("fetch")
        return cls(tf=tf, fetch=fetch)

    def __repr__(self) -> str:
        return f"TimeframeConfig(tf={self.tf!r}, fetch={self.fetch})"


@dataclass(frozen=True)
class SymbolConfig:
    """Configuration for a single symbol."""
    name: str
    enabled: bool = True
    timeframes: List[TimeframeConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "SymbolConfig":
        """Create from a dict with validation."""
        ConfigValidator.validate_symbol(obj)
        name = obj.get("name")
        enabled = obj.get("enabled", True)
        tfs = obj.get("timeframes", [])
        timeframes = [TimeframeConfig.from_dict(t) for t in tfs]
        return cls(name=name, enabled=enabled, timeframes=timeframes)

    def __repr__(self) -> str:
        return f"SymbolConfig(name={self.name!r}, enabled={self.enabled}, timeframes={len(self.timeframes)})"


@dataclass(frozen=True)
class SymbolsConfig:
    """Root configuration containing all symbols."""
    symbols: List[SymbolConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "SymbolsConfig":
        """Create from a dict with validation."""
        ConfigValidator.validate_symbols_config(obj)
        symbols = obj.get("symbols", [])
        symbol_objs = [SymbolConfig.from_dict(s) for s in symbols]
        return cls(symbols=symbol_objs)

    def __repr__(self) -> str:
        return f"SymbolsConfig(symbols={len(self.symbols)})"


__all__ = ["TimeframeConfig", "SymbolConfig", "SymbolsConfig"]
