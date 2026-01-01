"""Convert parsed race data to CSV format."""

import csv
from io import StringIO
from typing import List
from .models import RaceResult, RaceProgram
from . import logger as logging_module


# CSV Headers for results (91 columns)
RESULTS_HEADERS = [
    "date", "stadium", "race_round", "title", "race_code",
    "tansho", "fukusho", "wakren", "fuku2",
    "santan", "sanfuku", "santan_yosoku", "sanfuku_yosoku",
    "rentan", "renfuku", "rentan_yosoku", "renfuku_yosoku",
    "wide", "wide_yosoku", "trio", "trio_yosoku", "tiomate",
    # Racer 1
    "r1_number", "r1_name", "r1_weight", "r1_result", "r1_time", "r1_difference",
    # Racer 2
    "r2_number", "r2_name", "r2_weight", "r2_result", "r2_time", "r2_difference",
    # Racer 3
    "r3_number", "r3_name", "r3_weight", "r3_result", "r3_time", "r3_difference",
    # Racer 4
    "r4_number", "r4_name", "r4_weight", "r4_result", "r4_time", "r4_difference",
    # Racer 5
    "r5_number", "r5_name", "r5_weight", "r5_result", "r5_time", "r5_difference",
    # Racer 6
    "r6_number", "r6_name", "r6_weight", "r6_result", "r6_time", "r6_difference",
]

# CSV Headers for programs (218 columns)
PROGRAMS_HEADERS = [
    "date", "stadium", "race_round", "title", "race_code",
    "race_class", "race_type", "course_condition",
    "weather", "wind_direction", "wind_speed", "water_temperature", "water_level",
]

# Add racer frame headers (6 racers × 35 fields each = 210 fields)
for racer_num in range(1, 7):
    for field_num in range(1, 36):
        PROGRAMS_HEADERS.append(f"r{racer_num}_frame_field_{field_num:02d}")


def race_result_to_row(race: RaceResult) -> List[str]:
    """Convert RaceResult to CSV row.

    Args:
        race: RaceResult object

    Returns:
        List of CSV field values
    """
    row = [
        race.date,
        race.stadium,
        race.race_round,
        race.title,
        race.race_code or "",
        race.tansho or "",
        race.fukusho or "",
        race.wakren or "",
        race.fuku2 or "",
        race.santan or "",
        race.sanfuku or "",
        race.santan_yosoku or "",
        race.sanfuku_yosoku or "",
        race.rentan or "",
        race.renfuku or "",
        race.rentan_yosoku or "",
        race.renfuku_yosoku or "",
        race.wide or "",
        race.wide_yosoku or "",
        race.trio or "",
        race.trio_yosoku or "",
        race.tiomate or "",
    ]

    # Add racer data (pad with empty racers if fewer than 6)
    racers = race.racers + [None] * (6 - len(race.racers))

    for i, racer in enumerate(racers[:6]):
        if racer:
            row.extend([
                str(racer.number),
                racer.name,
                str(racer.weight),
                str(racer.result),
                str(racer.time) if racer.time is not None else "",
                str(racer.difference) if racer.difference is not None else "",
            ])
        else:
            row.extend(["", "", "", "", "", ""])

    return row


def race_program_to_row(program: RaceProgram) -> List[str]:
    """Convert RaceProgram to CSV row.

    Args:
        program: RaceProgram object

    Returns:
        List of CSV field values
    """
    row = [
        program.date,
        program.stadium,
        program.race_round,
        program.title,
        program.race_code or "",
        program.race_class or "",
        program.race_type or "",
        program.course_condition or "",
        program.weather or "",
        program.wind_direction or "",
        str(program.wind_speed) if program.wind_speed is not None else "",
        str(program.water_temperature) if program.water_temperature is not None else "",
        program.water_level or "",
    ]

    # Add racer frame data (pad with empty frames if fewer than 6)
    frames = program.racer_frames + [None] * (6 - len(program.racer_frames))

    for frame in frames[:6]:
        if frame:
            # Add 35 fields per frame (or whatever is available)
            row.extend([
                str(frame.entry_number),
                frame.registration_number,
                frame.racer_name,
                str(frame.age),
                str(frame.win_rate),
                str(frame.place_rate),
                str(frame.average_score),
                frame.motor_number,
                str(frame.motor_wins),
                str(frame.motor_2nd),
                frame.boat_number,
                str(frame.boat_wins),
                str(frame.boat_2nd),
                str(frame.weight),
                str(frame.adjustment),
            ])
            # Add remaining frame fields (20 more to make 35 total)
            for i in range(20):
                row.append(getattr(frame, f"field_{i+1}", "") or "")
        else:
            row.extend([""] * 35)

    return row


def races_to_csv(races: List[RaceResult]) -> str:
    """Convert list of RaceResult objects to CSV format.

    Args:
        races: List of RaceResult objects

    Returns:
        CSV content as string
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")

        # Write header
        writer.writerow(RESULTS_HEADERS)

        # Write data rows
        for race in races:
            row = race_result_to_row(race)
            writer.writerow(row)

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="results",
            rows=len(races) + 1,  # +1 for header
            size_bytes=len(csv_content.encode("utf-8")),
        )

        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="results",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""


def programs_to_csv(programs: List[RaceProgram]) -> str:
    """Convert list of RaceProgram objects to CSV format.

    Args:
        programs: List of RaceProgram objects

    Returns:
        CSV content as string
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")

        # Write header
        writer.writerow(PROGRAMS_HEADERS)

        # Write data rows
        for program in programs:
            row = race_program_to_row(program)
            writer.writerow(row)

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="programs",
            rows=len(programs) + 1,  # +1 for header
            size_bytes=len(csv_content.encode("utf-8")),
        )

        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="programs",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""
