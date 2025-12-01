"""Symbols config loader inside `core.config.symbols` package."""
from __future__ import annotations

from typing import Any, Dict
from pathlib import Path

from .models import SymbolsConfig
from .validators import ConfigValidator, ConfigError


class SymbolsConfigLoader:
    """Loader for YAML configuration files for the symbols config."""

    @staticmethod
    def _read_file(path: Path | str) -> Dict[str, Any]:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")

        try:
            import yaml
        except ImportError as e:
            raise RuntimeError(
                "PyYAML is required to load YAML config files. Install with: pip install pyyaml"
            ) from e

        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @staticmethod
    def load(path: Path | str = Path("config/symbols.yml")) -> SymbolsConfig:
        data = SymbolsConfigLoader._read_file(path)
        if data is None:
            raise ValueError(f"Config file {path} is empty or invalid")

        try:
            ConfigValidator.validate_symbols_config(data, path=str(path))
        except ConfigError as e:
            raise RuntimeError(f"Invalid config in {path}: {e}") from e

        return SymbolsConfig.from_dict(data)


# Backwards compatibility alias
ConfigLoader = SymbolsConfigLoader


def load_symbols_config(path: Path | str = Path("config/symbols.yml")) -> SymbolsConfig:
    return SymbolsConfigLoader.load(path)


__all__ = ["SymbolsConfigLoader", "ConfigLoader", "load_symbols_config"]
