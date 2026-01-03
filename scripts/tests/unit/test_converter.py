"""Unit tests for converter module."""

import pytest
from boatrace.models import RaceResult, RaceProgram, RacerResult, RacerFrame
from boatrace import converter


def test_races_to_csv_basic():
    """Test converting races to CSV."""
    races = [
        RaceResult(
            date="2025-12-01",
            stadium="唐津",
            race_round="01R",
            title="Test Race",
            race_code="1201",
            tansho="100",
            fukusho="50,40",
            racers=[
                RacerResult(number=i, name=f"racer{i}", weight=58.0, result=i)
                for i in range(1, 7)
            ],
        )
    ]

    csv_content = converter.races_to_csv(races)

    assert csv_content
    lines = csv_content.split("\n")
    # Header + 1 data row + empty line
    assert len(lines) >= 2
    # Header should have expected fields (Japanese headers)
    header = lines[0].split(",")
    assert "レース日" in header
    assert "レース場" in header
    assert "レース回" in header


def test_races_to_csv_empty():
    """Test converting empty race list."""
    races = []

    csv_content = converter.races_to_csv(races)

    assert csv_content
    lines = csv_content.split("\n")
    # Should only have header
    assert len(lines) >= 1
    assert "レース日" in lines[0]


def test_race_result_to_row():
    """Test converting single race to row."""
    race = RaceResult(
        date="2025-12-01",
        stadium="唐津",
        race_round="01R",
        title="Test Race",
        race_code="1201",
        racers=[
            RacerResult(number=i, name=f"racer{i}", weight=58.0, result=i)
            for i in range(1, 7)
        ],
    )

    row = converter.race_result_to_row(race)

    # New header structure: レースコード(0), タイトル(1), 日次(2), レース日(3), レース場(4), レース回(5)...
    assert len(row) >= 92  # Japanese header format with 92 base columns + racer data
    assert row[0] == "1201"  # レースコード
    assert row[1] == "Test Race"  # タイトル
    assert row[3] == "2025-12-01"  # レース日
    assert row[4] == "唐津"  # レース場
    assert row[5] == "01R"  # レース回


def test_programs_to_csv_basic():
    """Test converting programs to CSV."""
    programs = [
        RaceProgram(
            date="2025-12-01",
            stadium="唐津",
            race_round="01R",
            title="Test Program",
            race_code="1201",
            weather="晴",
            racer_frames=[
                RacerFrame(
                    entry_number=i,
                    registration_number=f"123{i:03d}",
                    racer_name=f"racer{i}",
                    age=30,
                    win_rate=0.5,
                    place_rate=0.6,
                    average_score=5.0,
                    motor_number="01",
                    motor_wins=10,
                    motor_2nd=5,
                    boat_number="02",
                    boat_wins=8,
                    boat_2nd=6,
                    weight=58.0,
                    adjustment=2.0,
                )
                for i in range(1, 7)
            ],
        )
    ]

    csv_content = converter.programs_to_csv(programs)

    assert csv_content
    lines = csv_content.split("\n")
    # Header + 1 data row
    assert len(lines) >= 2


def test_race_program_to_row():
    """Test converting single program to row."""
    program = RaceProgram(
        date="2025-12-01",
        stadium="唐津",
        race_round="01R",
        title="Test",
        weather="晴",
        racer_frames=[
            RacerFrame(
                entry_number=i,
                registration_number=f"123{i:03d}",
                racer_name=f"racer{i}",
                age=30,
                win_rate=0.5,
                place_rate=0.6,
                average_score=5.0,
                motor_number="01",
                motor_wins=10,
                motor_2nd=5,
                boat_number="02",
                boat_wins=8,
                boat_2nd=6,
                weight=58.0,
                adjustment=2.0,
            )
            for i in range(1, 7)
        ],
    )

    row = converter.race_program_to_row(program)

    # Should have header fields + 6 frames × 35 fields each
    assert len(row) >= 213  # 13 base + (6 * 35)
