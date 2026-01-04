"""Pytest configuration and fixtures for boatrace tests."""

import json
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock


@pytest.fixture
def sample_k_file_content():
    """Provide sample K-file (results) content for testing."""
    # This is a minimal example - in real testing, use actual file samples
    return "K-file content here (fixed-width format)"


@pytest.fixture
def sample_b_file_content():
    """Provide sample B-file (program) content for testing."""
    # This is a minimal example - in real testing, use actual file samples
    return "B-file content here (fixed-width format)"


@pytest.fixture
def sample_race_result():
    """Provide sample RaceResult object for testing."""
    return {
        "date": "2025-12-01",
        "stadium": "唐津",
        "race_round": "01R",
        "title": "第１０回ｏｄｄｓｏｎ杯",
        "racers": [
            {"number": 1, "name": "太郎", "result": 1},
            {"number": 2, "name": "次郎", "result": 2},
            {"number": 3, "name": "三郎", "result": 3},
            {"number": 4, "name": "四郎", "result": 4},
            {"number": 5, "name": "五郎", "result": 5},
            {"number": 6, "name": "六郎", "result": 6},
        ],
    }


@pytest.fixture
def sample_race_program():
    """Provide sample RaceProgram object for testing."""
    return {
        "date": "2025-12-01",
        "stadium": "唐津",
        "race_round": "01R",
        "title": "第１０回ｏｄｄｓｏｎ杯",
        "racers_frames": [
            [{"weight": 58.5, "adjustment": 2.5} for _ in range(35)],
            [{"weight": 59.0, "adjustment": 3.0} for _ in range(35)],
        ],
    }


@pytest.fixture
def mock_logger(monkeypatch):
    """Provide mock logger for testing."""
    logger = MagicMock()
    return logger


@pytest.fixture
def temp_csv_directory(tmp_path):
    """Provide temporary directory for CSV file output during tests."""
    results_dir = tmp_path / "data" / "results" / "2025" / "12"
    programs_dir = tmp_path / "data" / "programs" / "2025" / "12"
    results_dir.mkdir(parents=True, exist_ok=True)
    programs_dir.mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest.fixture
def mock_git_operations(monkeypatch):
    """Provide mock git operations for testing."""
    git_ops = MagicMock()
    git_ops.commit_and_push = MagicMock(return_value=True)
    return git_ops


@pytest.fixture
def sample_config():
    """Provide sample configuration for testing."""
    return {
        "rate_limit_interval_seconds": 3,
        "max_retries": 3,
        "initial_backoff_seconds": 5,
        "max_backoff_seconds": 30,
        "request_timeout_seconds": 30,
        "log_level": "INFO",
        "log_file": "logs/boatrace-{DATE}.json",
    }
