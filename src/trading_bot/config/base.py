"""Base classes for configuration system."""
from typing import Any, Dict, Optional


class ConfigError(Exception):
    """Base error for all config validation errors."""


class BaseValidator:
    """Base validator with common validation patterns."""

    @staticmethod
    def validate_dict(obj: Any, name: str, path: Optional[str] = None) -> None:
        """Validate that object is a dictionary.
        
        Args:
            obj: Object to validate
            name: Name of the field being validated
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a dict
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, dict):
            raise ConfigError(f"{ctx}{name} must be a dict")

    @staticmethod
    def validate_string(
        obj: Any, 
        field_name: str, 
        allow_empty: bool = False,
        path: Optional[str] = None
    ) -> None:
        """Validate that object is a string.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            allow_empty: Whether empty strings are allowed
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a valid string
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, str):
            raise ConfigError(f"{ctx}{field_name} must be a string")
        if not allow_empty and not obj:
            raise ConfigError(f"{ctx}{field_name} must be a non-empty string")

    @staticmethod
    def validate_int(
        obj: Any,
        field_name: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        path: Optional[str] = None
    ) -> None:
        """Validate that object is an integer within optional bounds.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            min_value: Optional minimum value (inclusive)
            max_value: Optional maximum value (inclusive)
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a valid integer or out of bounds
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, int):
            raise ConfigError(f"{ctx}{field_name} must be an integer")
        if min_value is not None and obj < min_value:
            raise ConfigError(f"{ctx}{field_name} must be >= {min_value}, got {obj}")
        if max_value is not None and obj > max_value:
            raise ConfigError(f"{ctx}{field_name} must be <= {max_value}, got {obj}")

    @staticmethod
    def validate_float(
        obj: Any,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        path: Optional[str] = None
    ) -> None:
        """Validate that object is a float within optional bounds.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            min_value: Optional minimum value (inclusive)
            max_value: Optional maximum value (inclusive)
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a valid number or out of bounds
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, (int, float)):
            raise ConfigError(f"{ctx}{field_name} must be a number")
        if min_value is not None and obj < min_value:
            raise ConfigError(f"{ctx}{field_name} must be >= {min_value}, got {obj}")
        if max_value is not None and obj > max_value:
            raise ConfigError(f"{ctx}{field_name} must be <= {max_value}, got {obj}")

    @staticmethod
    def validate_bool(obj: Any, field_name: str, path: Optional[str] = None) -> None:
        """Validate that object is a boolean.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a boolean
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, bool):
            raise ConfigError(f"{ctx}{field_name} must be a boolean")

    @staticmethod
    def validate_list(obj: Any, field_name: str, path: Optional[str] = None) -> None:
        """Validate that object is a list.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a list
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, list):
            raise ConfigError(f"{ctx}{field_name} must be a list")

    @staticmethod
    def validate_url(
        obj: Any,
        field_name: str,
        schemes: tuple = ("http", "https"),
        path: Optional[str] = None
    ) -> None:
        """Validate that object is a URL with allowed schemes.
        
        Args:
            obj: Object to validate
            field_name: Name of the field being validated
            schemes: Tuple of allowed URL schemes
            path: Optional path context for error messages
            
        Raises:
            ConfigError: If obj is not a valid URL
        """
        ctx = f"{path}: " if path else ""
        if not isinstance(obj, str):
            raise ConfigError(f"{ctx}{field_name} must be a string")
        if not any(obj.startswith(f"{scheme}://") or obj.startswith(f"{scheme}:") for scheme in schemes):
            schemes_str = ", ".join(schemes)
            raise ConfigError(f"{ctx}{field_name} must start with one of: {schemes_str}")


__all__ = ["ConfigError", "BaseValidator"]
