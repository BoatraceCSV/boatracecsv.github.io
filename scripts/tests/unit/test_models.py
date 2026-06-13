"""Unit tests for data models."""

import pytest
from boatrace.models import (
    RaceProgram,
    RacerFrame,
)


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
