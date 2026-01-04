"""Integration tests for Phase 2 parser enhancements (new attributes)."""

import pytest
from boatrace.models import RaceResult, RacerResult
from boatrace.parser import parse_result_file, parse_racer_result_line, _extract_race_details


# Sample K-file content with new attributes
SAMPLE_K_FILE = """
01R  予選             H1800m  晴　  風  南西　 3m  波　  2cm
     逃げ
着 艇 登番                選手名   体重  モ  展示　進 ス　　     時間
01  1 4443 津田　　　裕絵 52   24  6.91   1    0.08     1.49.7
02  2 4659 木下　　　翔太 55   48  6.63   3    0.15     1.51.2
03  3 4337 平本　　　真之 55   68  6.77   2    0.12     1.52.5
04  4 4794 和田　　　拓也 54   57  6.65   4    0.14     1.53.1
05  5 4483 北野　　　輝季 53   60  6.70   5    0.11     1.54.6
06  6 3909 佐々木　康　幸 54   20  6.71   6    0.20     1.55.8

02R  予選             H1800m  曇り  風  北　　　 2m  波　  1cm
     差し
着 艇 登番                選手名   体重  モ  展示　進 ス　　     時間
01  1 4794 和田　　　拓也 54   57  6.65   1    0.12     1.48.9
02  2 5026 安河内　　　健 55   29  6.74   2    0.18     1.49.8
03  3 4446 和田　　　兼輔 52   61  6.65   3    0.08     1.51.3
04  4 4659 木下　　　翔太 55   48  6.63   4    0.20     1.52.6
05  5 3995 重野　　　哲之 54   47  6.72   5    0.16     1.54.2
06  6 4573 佐藤　　　　翼 53   70  6.71   6    0.12     1.55.7
"""


def test_parse_result_file_extracts_race_details():
    """Test that parse_result_file extracts race details (distance, weather, wind, etc.)."""
    races = parse_result_file(SAMPLE_K_FILE, date="2025-12-01")

    assert len(races) == 2, "Should parse 2 races"

    # Check first race
    race1 = races[0]
    assert race1.date == "2025-12-01"
    assert race1.race_round == "01R"
    assert race1.race_name == "予選", f"Expected race_name '予選', got '{race1.race_name}'"
    assert race1.distance == "1800", f"Expected distance '1800', got '{race1.distance}'"
    assert race1.weather == "晴", f"Expected weather '晴', got '{race1.weather}'"
    assert race1.wind_direction == "南西", f"Expected wind_direction '南西', got '{race1.wind_direction}'"
    assert race1.wind_speed == "3", f"Expected wind_speed '3', got '{race1.wind_speed}'"
    assert race1.wave_height == "2", f"Expected wave_height '2', got '{race1.wave_height}'"
    assert race1.winning_technique == "逃げ", f"Expected winning_technique '逃げ', got '{race1.winning_technique}'"

    # Check second race
    race2 = races[1]
    assert race2.race_round == "02R"
    assert race2.race_name == "予選", f"Expected race_name '予選', got '{race2.race_name}'"
    assert race2.distance == "1800"
    assert race2.weather == "曇り"
    assert race2.wind_direction == "北"
    assert race2.wind_speed == "2"
    assert race2.wave_height == "1"
    assert race2.winning_technique == "差し"


def test_parse_result_file_extracts_racer_attributes():
    """Test that parse_result_file extracts racer-level attributes."""
    races = parse_result_file(SAMPLE_K_FILE, date="2025-12-01")

    assert len(races) == 2
    race = races[0]

    assert len(race.racers) == 6, "Should parse 6 racers"

    # Check first racer
    racer1 = race.racers[0]
    assert racer1.number == 1
    assert racer1.name == "津田 裕絵", f"Expected name '津田 裕絵', got '{racer1.name}'"
    assert racer1.result == 1
    assert racer1.registration_number == "4443", f"Expected registration '4443', got '{racer1.registration_number}'"
    assert racer1.motor_number == "52", f"Expected motor_number '52', got '{racer1.motor_number}'"
    assert racer1.boat_number == "24", f"Expected boat_number '24', got '{racer1.boat_number}'"
    assert racer1.showcase_time == 6.91, f"Expected showcase_time 6.91, got {racer1.showcase_time}"
    assert racer1.entrance_course == 1, f"Expected entrance_course 1, got {racer1.entrance_course}"
    assert racer1.start_timing == 0.08, f"Expected start_timing 0.08, got {racer1.start_timing}"
    assert racer1.time is not None, "Expected race_time to be extracted"

    # Check second racer
    racer2 = race.racers[1]
    assert racer2.number == 2
    assert racer2.result == 2


def test_parse_racer_result_line_extracts_attributes():
    """Test parse_racer_result_line extracts all racer attributes."""
    # Sample line from the K-file
    line = "01  1 4443 津田　　　裕絵 52   24  6.91   1    0.08     1.49.7"

    racer = parse_racer_result_line(line)

    assert racer is not None
    assert racer.number == 1
    assert racer.result == 1
    assert racer.name == "津田 裕絵", f"Expected '津田 裕絵', got '{racer.name}'"
    assert racer.motor_number == "52", f"Expected motor_number '52', got '{racer.motor_number}'"
    assert racer.boat_number == "24", f"Expected boat_number '24', got '{racer.boat_number}'"
    assert racer.showcase_time == 6.91, f"Expected showcase_time 6.91, got {racer.showcase_time}"
    assert racer.entrance_course == 1, f"Expected entrance_course 1, got {racer.entrance_course}"
    assert racer.start_timing == 0.08, f"Expected start_timing 0.08, got {racer.start_timing}"
    assert racer.time is not None, "Expected race_time to be set"


def test_extract_race_details_function():
    """Test _extract_race_details function directly."""
    line = "01R  予選             H1800m  晴　  風  南西　 3m  波　  2cm"
    next_line = "     逃げ　"
    all_lines = [line, next_line]

    race = RaceResult(
        date="2025-12-01",
        stadium="大村",
        race_round="01R",
        title="Test Race",
    )

    _extract_race_details(race, line, all_lines, 0)

    assert race.race_name is not None, "race_name should be extracted"
    assert race.distance == "1800", f"Expected distance '1800', got '{race.distance}'"
    assert race.weather == "晴", f"Expected weather '晴', got '{race.weather}'"
    assert race.wind_direction == "南西", f"Expected wind_direction '南西', got '{race.wind_direction}'"
    assert race.wind_speed == "3", f"Expected wind_speed '3', got '{race.wind_speed}'"
    assert race.wave_height == "2", f"Expected wave_height '2', got '{race.wave_height}'"
    assert race.winning_technique == "逃げ", f"Expected winning_technique '逃げ', got '{race.winning_technique}'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
