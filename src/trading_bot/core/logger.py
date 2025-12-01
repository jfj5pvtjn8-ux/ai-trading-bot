"""Project logging setup.

Provides a single `get_logger(name=None)` factory that returns a configured
logger. Configuration uses the standard library only: a console `StreamHandler`
and a custom date-based rotating handler writing to `logs/app_YYYY-MM-DD.log`.

Behavior:
- Log level is taken from the environment variable `LOG_LEVEL` (default INFO).
- Creates daily log files in `logs/` directory.
- Automatically rotates at midnight (creates new file, archives old one).
- Archives old logs to `logs/archive/YYYY-MM-DD/` on rotation.
- Deletes logs older than 7 days from archive.
"""
import logging
import logging.handlers
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


class DailyRotatingFileHandler(logging.Handler):
	"""Custom handler that rotates log files at midnight and archives old files."""
	
	def __init__(self, logs_dir: str, retention_days: int = 7, prefix: str = "app"):
		super().__init__()
		self.logs_dir = Path(logs_dir)
		self.retention_days = retention_days
		self.prefix = prefix  # e.g., "app", "BTCUSDT", "XRPUSDT"
		self.current_date = datetime.now().strftime("%Y-%m-%d")
		self.current_handler = None
		self._setup_handler()
	
	def _setup_handler(self):
		"""Create or recreate the file handler for current date."""
		# Close existing handler if any
		if self.current_handler:
			self.current_handler.close()
		
		# Create log file for current date with prefix
		log_file = self.logs_dir / f"{self.prefix}_{self.current_date}.log"
		self.current_handler = logging.FileHandler(str(log_file), encoding="utf-8")
		
		# Copy formatter from parent handler
		if self.formatter:
			self.current_handler.setFormatter(self.formatter)
	
	def emit(self, record):
		"""Emit a record, rotating the file if date has changed."""
		try:
			# Check if date has changed (midnight rollover)
			current_date = datetime.now().strftime("%Y-%m-%d")
			
			if current_date != self.current_date:
				# Archive the old log file
				old_date = self.current_date
				old_log_file = self.logs_dir / f"{self.prefix}_{old_date}.log"
				
				# Close current handler before archiving
				if self.current_handler:
					self.current_handler.close()
				
				# Archive old file
				if old_log_file.exists():
					archive_dir = self.logs_dir / "archive" / old_date
					archive_dir.mkdir(parents=True, exist_ok=True)
					shutil.move(str(old_log_file), str(archive_dir / old_log_file.name))
					# Using print here as logger may not be fully initialized during rotation
					print(f"[Logger] Rotated log: archived {old_log_file.name} to archive/{old_date}/")
				
				# Cleanup old archives
				_cleanup_old_archive_logs(self.logs_dir, self.retention_days)
				
				# Update current date and setup new handler
				self.current_date = current_date
				self._setup_handler()
			
			# Emit the record to current handler
			if self.current_handler:
				self.current_handler.emit(record)
				
		except Exception:
			self.handleError(record)
	
	def close(self):
		"""Close the handler."""
		if self.current_handler:
			self.current_handler.close()
		super().close()


def _ensure_logs_dir(path: str) -> None:
	try:
		os.makedirs(path, exist_ok=True)
	except Exception:
		# If we cannot create the logs directory, continue silently and let
		# logging fall back to console only.
		pass


def _archive_old_logs(logs_dir: Path, today: str) -> None:
	"""Move old log files (not today) to archive directory."""
	try:
		archive_dir = logs_dir / "archive"
		moved_count = 0
		
		for log_file in logs_dir.glob("app_*.log"):
			# Extract date from filename (app_YYYY-MM-DD.log)
			filename = log_file.name
			if filename.startswith("app_") and filename.endswith(".log"):
				file_date = filename[4:-4]  # Extract YYYY-MM-DD
				
				if file_date != today:
					# Create date subdirectory in archive
					date_archive_dir = archive_dir / file_date
					date_archive_dir.mkdir(parents=True, exist_ok=True)
					
					# Move file to archive
					dest_path = date_archive_dir / log_file.name
					shutil.move(str(log_file), str(dest_path))
					moved_count += 1
		
		if moved_count > 0:
			print(f"[Logger] Archived {moved_count} old log files")
	except Exception as e:
		print(f"[Logger] Warning: Failed to archive logs: {e}")


def _cleanup_old_archive_logs(logs_dir: Path, retention_days: Optional[int] = None) -> None:
	"""Delete archive log directories older than retention period."""
	try:
		if retention_days is None:
			retention_days = int(os.getenv("LOG_RETENTION_DAYS", "7"))
		
		archive_dir = logs_dir / "archive"
		if not archive_dir.exists():
			return
		
		cutoff_date = datetime.now() - timedelta(days=retention_days)
		cutoff_str = cutoff_date.strftime("%Y-%m-%d")
		
		deleted_dirs = 0
		for date_dir in archive_dir.iterdir():
			if not date_dir.is_dir():
				continue
			
			dir_date = date_dir.name
			if dir_date < cutoff_str:
				shutil.rmtree(date_dir)
				deleted_dirs += 1
		
		if deleted_dirs > 0:
			print(f"[Logger] Cleaned up {deleted_dirs} archive log directories older than {retention_days} days")
	except Exception as e:
		print(f"[Logger] Warning: Failed to cleanup archive logs: {e}")


def _configure_root_logger(log_level: int, logs_dir: Optional[str]) -> None:
	root = logging.getLogger()
	if root.handlers:
		# already configured
		return

	root.setLevel(log_level)

	# Console handler
	console_h = logging.StreamHandler()
	console_fmt = logging.Formatter(
		"%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S",
	)
	console_h.setFormatter(console_fmt)
	root.addHandler(console_h)

	# File handler (date-based with automatic midnight rotation)
	if logs_dir:
		_ensure_logs_dir(logs_dir)
		try:
			# Archive old logs and cleanup on startup
			today = datetime.now().strftime("%Y-%m-%d")
			base_dir = Path(logs_dir)
			_archive_old_logs(base_dir, today)
			_cleanup_old_archive_logs(base_dir, retention_days=7)
			
			# Create custom rotating file handler
			file_h = DailyRotatingFileHandler(logs_dir, retention_days=7)
			file_fmt = logging.Formatter(
				"%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
				datefmt="%Y-%m-%d %H:%M:%S",
			)
			file_h.setFormatter(file_fmt)
			root.addHandler(file_h)
		except Exception:
			# If file handler can't be created, fall back to console only.
			pass


def get_logger(name: Optional[str] = None) -> logging.Logger:
	"""Return a configured logger for `name`.

	Example:
		from trading_bot.core.logger import get_logger
		log = get_logger(__name__)
		log.info("starting app")

	The top-level configuration runs once on the first call.
	"""
	# Read configuration from environment; defaults are safe for production.
	level_name = os.getenv("LOG_LEVEL", "INFO").upper()
	try:
		level = getattr(logging, level_name)
	except AttributeError:
		level = logging.INFO

	# Get logs directory from environment or use default
	logs_dir = os.getenv("LOGS_DIR")
	if not logs_dir:
		# Go up from src/trading_bot/core/logger.py to project root
		project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
		logs_dir = os.path.join(project_root, "logs")

	_configure_root_logger(level, logs_dir)

	return logging.getLogger(name if name else "trading-bot")


def get_symbol_logger(symbol: str) -> logging.Logger:
	"""Return a symbol-specific logger that writes to separate file.
	
	Creates a logger that writes to:
	- Console (shared with main logger)
	- Symbol-specific file: logs/SYMBOL_YYYY-MM-DD.log
	
	Example:
		from trading_bot.core.logger import get_symbol_logger
		log = get_symbol_logger("BTCUSDT")
		log.info("Processing candle")  # Goes to logs/BTCUSDT_2025-11-30.log
	
	Args:
		symbol: Trading symbol name (e.g., "BTCUSDT", "XRPUSDT")
	
	Returns:
		Configured logger instance with symbol-specific file handler
	"""
	# Read configuration from environment
	level_name = os.getenv("LOG_LEVEL", "INFO").upper()
	try:
		level = getattr(logging, level_name)
	except AttributeError:
		level = logging.INFO
	
	# Get logs directory from environment or use default
	logs_dir = os.getenv("LOGS_DIR")
	if not logs_dir:
		project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
		logs_dir = os.path.join(project_root, "logs")
	_ensure_logs_dir(logs_dir)
	
	# Create logger with symbol-specific name
	logger_name = f"trading-bot.{symbol}"
	logger = logging.getLogger(logger_name)
	
	# Prevent propagation to root logger to avoid duplicate console output
	logger.propagate = False
	logger.setLevel(level)
	
	# Check if already configured (avoid duplicate handlers)
	if logger.handlers:
		return logger
	
	# Add console handler (shared output)
	console_h = logging.StreamHandler()
	console_fmt = logging.Formatter(
		"%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S",
	)
	console_h.setFormatter(console_fmt)
	logger.addHandler(console_h)
	
	# Add symbol-specific file handler with daily rotation
	try:
		retention_days = int(os.getenv("LOG_RETENTION_DAYS", "7"))
		file_h = DailyRotatingFileHandler(logs_dir, retention_days=retention_days, prefix=symbol)
		file_fmt = logging.Formatter(
			"%(asctime)s %(levelname)-8s %(message)s",
			datefmt="%Y-%m-%d %H:%M:%S",
		)
		file_h.setFormatter(file_fmt)
		logger.addHandler(file_h)
	except Exception as e:
		print(f"[Logger] Warning: Failed to create file handler for {symbol}: {e}")
	
	return logger


__all__ = ["get_logger", "get_symbol_logger"]
