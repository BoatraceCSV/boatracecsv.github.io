"""Unit tests for data models."""

import pytest
from boatrace.models import (
    RaceResult,
    RaceProgram,
    RacerResult,
    RacerFrame,
    ConversionSession,
    ConversionError,
)


def test_racer_result_creation():
    """Test RacerResult creation."""
    racer = RacerResult(
        number=1,
        name="太郎",
        weight=58.5,
        result=1,
    )

    assert racer.number == 1
    assert racer.name == "太郎"
    assert racer.weight == 58.5
    assert racer.result == 1


def test_race_result_valid():
    """Test RaceResult validation."""
    race = RaceResult(
        date="2025-12-01",
        stadium="唐津",
        race_round="01R",
        title="第１０回ｏｄｄｓｏｎ杯",
        racers=[
            RacerResult(number=i, name=f"racer{i}", weight=58.0, result=i)
            for i in range(1, 7)
        ],
    )

    assert race.is_valid()


def test_race_result_invalid_result_count():
    """Test RaceResult with wrong racer count."""
    race = RaceResult(
        date="2025-12-01",
        stadium="唐津",
        race_round="01R",
        title="Test",
        racers=[
            RacerResult(number=i, name=f"racer{i}", weight=58.0, result=i)
            for i in range(1, 5)  # Only 4 racers
        ],
    )

    assert not race.is_valid()


def test_racer_frame_creation():
    """Test RacerFrame creation."""
    frame = RacerFrame(
        entry_number=1,
        registration_number="123456",
        racer_name="太郎",
        age=30,
        win_rate=0.45,
        place_rate=0.65,
        average_score=5.5,
        motor_number="01",
        motor_wins=10,
        motor_2nd=5,
        boat_number="02",
        boat_wins=8,
        boat_2nd=6,
        weight=58.5,
        adjustment=2.5,
    )

    assert frame.entry_number == 1
    assert frame.racer_name == "太郎"
    assert frame.weight == 58.5


def test_conversion_session_creation():
    """Test ConversionSession creation."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-03",
    )

    assert session.start_date == "2025-12-01"
    assert session.end_date == "2025-12-03"
    assert session.mode == "daily"
    assert session.dry_run is False


def test_conversion_session_add_error():
    """Test adding error to session."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-01",
    )

    session.add_error(
        date="2025-12-01",
        error_type="download_failed",
        message="Connection timeout",
    )

    assert len(session.errors) == 1
    assert session.errors[0].error_type == "download_failed"


def test_conversion_session_exit_code_success():
    """Test exit code for successful session."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-01",
    )

    assert session.exit_code() == 0


def test_conversion_session_exit_code_partial_failure():
    """Test exit code for partial failure."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-01",
    )

    session.add_error(
        date="2025-12-01",
        error_type="parse_error",
        message="Invalid format",
    )

    assert session.exit_code() == 1


def test_conversion_session_exit_code_critical():
    """Test exit code for critical error."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-01",
    )

    session.csv_files_created = 5
    session.git_push_success = False

    assert session.exit_code() == 2


def test_conversion_session_summary():
    """Test session summary generation."""
    session = ConversionSession(
        start_date="2025-12-01",
        end_date="2025-12-01",
    )

    session.dates_processed = 1
    session.files_downloaded = 2
    session.csv_files_created = 2

    summary = session.summary()

    assert "Dates processed: 1" in summary
    assert "Files downloaded: 2" in summary
    assert "CSV files created: 2" in summary
