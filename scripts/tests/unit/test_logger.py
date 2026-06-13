"""Unit tests for logger module."""

import pytest
import json
from pathlib import Path
from boatrace import logger as logging_module


def test_logger_initialization():
    """Test logger initialization."""
    logger = logging_module.initialize_logger(log_level="INFO")
    assert logger is not None
    assert logger.log_level == "INFO"


def test_logger_log_levels(capsys):
    """Test different log levels."""
    logger = logging_module.StructuredLogger(log_level="INFO")

    logger.info("test_event", key="value")
    captured = capsys.readouterr()

    # Should be valid JSON
    log_entry = json.loads(captured.out.strip())
    assert log_entry["level"] == "INFO"
    assert log_entry["event"] == "test_event"
    assert log_entry["key"] == "value"


def test_logger_debug_below_threshold(capsys):
    """Test debug logging below threshold."""
    logger = logging_module.StructuredLogger(log_level="INFO")

    logger.debug("debug_event")
    captured = capsys.readouterr()

    # DEBUG should not be logged at INFO level
    assert captured.out == ""


def test_logger_json_format(capsys):
    """Test JSON format of logs."""
    logger = logging_module.StructuredLogger(log_level="INFO")

    logger.info("test_event", count=5, name="test")
    captured = capsys.readouterr()

    log_entry = json.loads(captured.out.strip())
    assert "timestamp" in log_entry
    assert "level" in log_entry
    assert "event" in log_entry
    assert log_entry["count"] == 5
    assert log_entry["name"] == "test"


def test_logger_context_context_fields(capsys):
    """Test context fields in logs."""
    logger = logging_module.StructuredLogger(log_level="DEBUG")

    logger.info(
        "complex_event",
        error="something failed",
        duration_ms=1234,
        success=False,
    )
    captured = capsys.readouterr()

    log_entry = json.loads(captured.out.strip())
    assert log_entry["error"] == "something failed"
    assert log_entry["duration_ms"] == 1234
    assert log_entry["success"] is False


def test_get_logger_singleton():
    """Test logger singleton."""
    logger1 = logging_module.get_logger()
    logger2 = logging_module.get_logger()

    assert logger1 is logger2


def test_module_level_logging(capsys):
    """Test module-level logging functions."""
    logging_module.initialize_logger(log_level="INFO")

    logging_module.info("test_event", key="value")
    captured = capsys.readouterr()

    log_entry = json.loads(captured.out.strip())
    assert log_entry["event"] == "test_event"
