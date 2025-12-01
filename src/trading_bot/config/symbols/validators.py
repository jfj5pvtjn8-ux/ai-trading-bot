"""Configuration validator for symbols config (symbols package)."""
from typing import Any, Dict, Optional
import re
from ..base import BaseValidator, ConfigError


class ConfigValidator(BaseValidator):
    """Dedicated validator for config objects for the symbols config.

    Validators accept an optional `path` parameter that is prefixed to
    error messages to help locate the failing item in a nested config.
    """

    @staticmethod
    def validate_timeframe(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "timeframe", path)
        
        tf = obj.get("tf")
        BaseValidator.validate_string(tf, "timeframe.tf", path=path)
        
        fetch = obj.get("fetch")
        BaseValidator.validate_int(fetch, "timeframe.fetch", min_value=1, path=path)
        
        if not re.match(r"^\d+[mhd]$", tf):
            # allow common forms but don't strictly reject unfamiliar ones
            pass

    @staticmethod
    def validate_symbol(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        BaseValidator.validate_dict(obj, "symbol", path)
        
        name = obj.get("name")
        BaseValidator.validate_string(name, "symbol.name", path=path)
        
        enabled = obj.get("enabled", True)
        BaseValidator.validate_bool(enabled, "symbol.enabled", path=path)
        
        tfs = obj.get("timeframes", [])
        BaseValidator.validate_list(tfs, "symbol.timeframes", path=path)
        
        for i, t in enumerate(tfs):
            tf_path = f"{path}.timeframes[{i}]" if path else f"timeframes[{i}]"
            ConfigValidator.validate_timeframe(t, path=tf_path)

    @staticmethod
    def validate_symbols_config(obj: Dict[str, Any], path: Optional[str] = None) -> None:
        base = path if path else ""
        ctx = f"{base}: " if base else ""
        
        BaseValidator.validate_dict(obj, "config", path)
        
        symbols = obj.get("symbols", [])
        BaseValidator.validate_list(symbols, "root 'symbols'", path)
        
        for i, s in enumerate(symbols):
            symbol_path = f"{base}.symbols[{i}]" if base else f"symbols[{i}]"
            ConfigValidator.validate_symbol(s, path=symbol_path)


__all__ = ["ConfigValidator", "ConfigError"]
