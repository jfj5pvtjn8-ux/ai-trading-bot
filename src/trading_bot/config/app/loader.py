"""App config loader."""
from __future__ import annotations

from typing import Any, Dict
from pathlib import Path

from .models import AppConfig
from .validators import AppConfigValidator
from ..base import ConfigError


class AppConfigLoader:
    """Loader for YAML app configuration."""

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
    def load(path: Path | str = Path("config/app.yml")) -> AppConfig:
        """Load and validate the app configuration file.

        Args:
            path: Path to the YAML config file

        Returns:
            AppConfig instance with parsed and validated config

        Raises:
            FileNotFoundError: if config file does not exist
            RuntimeError: if PyYAML is not installed or config is invalid
        """
        data = AppConfigLoader._read_file(path)
        if data is None:
            raise ValueError(f"Config file {path} is empty or invalid")

        try:
            AppConfigValidator.validate_app_config(data, path=str(path))
        except ConfigError as e:
            raise RuntimeError(f"Invalid config in {path}: {e}") from e

        return AppConfig.from_dict(data)


def load_app_config(path: Path | str = Path("config/app.yml")) -> AppConfig:
    """Convenience function to load app config."""
    return AppConfigLoader.load(path)


__all__ = ["AppConfigLoader", "load_app_config"]
