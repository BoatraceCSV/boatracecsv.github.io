"""Structured JSON logging for boatrace data automation."""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class StructuredLogger:
    """Logger that outputs structured JSON to stdout and optional file."""

    def __init__(
        self,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
    ):
        """Initialize logger.

        Args:
            log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Optional log file path with {DATE} placeholder
        """
        self.log_level = log_level.upper()
        self.log_file = log_file
        self.level_values = {
            "DEBUG": 10,
            "INFO": 20,
            "WARNING": 30,
            "ERROR": 40,
            "CRITICAL": 50,
        }

    def _should_log(self, level: str) -> bool:
        """Check if message should be logged based on level."""
        return self.level_values.get(level.upper(), 20) >= self.level_values.get(
            self.log_level, 20
        )

    def _format_log(self, level: str, event: str, **context: Any) -> str:
        """Format log message as JSON."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level.upper(),
            "event": event,
            **context,
        }
        return json.dumps(log_entry, ensure_ascii=False, default=str)

    def _write_log(self, log_json: str) -> None:
        """Write log to stdout and optional file."""
        print(log_json)

        if self.log_file:
            log_path = Path(self.log_file.replace("{DATE}", datetime.now().strftime("%Y-%m-%d")))
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(log_json + "\n")

    def debug(self, event: str, **context: Any) -> None:
        """Log debug message."""
        if self._should_log("DEBUG"):
            log_json = self._format_log("DEBUG", event, **context)
            self._write_log(log_json)

    def info(self, event: str, **context: Any) -> None:
        """Log info message."""
        if self._should_log("INFO"):
            log_json = self._format_log("INFO", event, **context)
            self._write_log(log_json)

    def warning(self, event: str, **context: Any) -> None:
        """Log warning message."""
        if self._should_log("WARNING"):
            log_json = self._format_log("WARNING", event, **context)
            self._write_log(log_json)

    def error(self, event: str, **context: Any) -> None:
        """Log error message."""
        if self._should_log("ERROR"):
            log_json = self._format_log("ERROR", event, **context)
            self._write_log(log_json)

    def critical(self, event: str, **context: Any) -> None:
        """Log critical message."""
        if self._should_log("CRITICAL"):
            log_json = self._format_log("CRITICAL", event, **context)
            self._write_log(log_json)


# Global logger instance
_logger: Optional[StructuredLogger] = None


def initialize_logger(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
) -> StructuredLogger:
    """Initialize and return global logger."""
    global _logger
    _logger = StructuredLogger(log_level=log_level, log_file=log_file)
    return _logger


def get_logger() -> StructuredLogger:
    """Get global logger instance."""
    global _logger
    if _logger is None:
        _logger = StructuredLogger()
    return _logger


def debug(event: str, **context: Any) -> None:
    """Log debug message."""
    get_logger().debug(event, **context)


def info(event: str, **context: Any) -> None:
    """Log info message."""
    get_logger().info(event, **context)


def warning(event: str, **context: Any) -> None:
    """Log warning message."""
    get_logger().warning(event, **context)


def error(event: str, **context: Any) -> None:
    """Log error message."""
    get_logger().error(event, **context)


def critical(event: str, **context: Any) -> None:
    """Log critical message."""
    get_logger().critical(event, **context)
