"""Parse fixed-width text format files from boatrace."""

from typing import List, Optional
from .models import RaceResult, RaceProgram, RacerResult, RacerFrame
from . import logger as logging_module


class ParserError(Exception):
    """Parsing failed."""

    pass


# Stadium code mappings
STADIUM_NAMES = {
    "01": "桜花", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡", "08": "常滑",
    "09": "日本", "10": "三国", "11": "琵琶湖", "12": "唐津",
    "13": "大村", "14": "鳴門", "15": "丸亀", "16": "児島",
    "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大津",
}


def parse_result_file(content: str, date: str = "") -> List[RaceResult]:
    """Parse K-file (results) from fixed-width text format.

    Args:
        content: File content as string
        date: Date in YYYY-MM-DD format (optional, extracted from file if not provided)

    Returns:
        List of RaceResult objects
    """
    races = []

    try:
        lines = content.strip().split("\n")

        if not lines:
            logging_module.warning("parse_empty_file", file_type="K")
            return races

        current_race: Optional[RaceResult] = None
        race_count = 0

        for line_num, line in enumerate(lines, 1):
            # Remove trailing whitespace but preserve internal spacing
            line = line.rstrip()

            # Skip empty lines
            if not line:
                continue

            # Detect race header (starts with race code/stadium info)
            # Format varies; basic detection: line contains stadium code and race number
            try:
                if len(line) >= 20:
                    # Try to extract stadium code (first 2 chars might be numeric)
                    stadium_code = line[0:2].strip()
                    race_num = line[2:4].strip()

                    if (
                        stadium_code.isdigit() and
                        1 <= int(stadium_code) <= 24 and
                        race_num.isdigit() and
                        1 <= int(race_num) <= 12
                    ):
                        # This appears to be a new race
                        if current_race and len(current_race.racers) == 6:
                            races.append(current_race)

                        stadium_name = STADIUM_NAMES.get(stadium_code, "Unknown")
                        current_race = RaceResult(
                            date=date,
                            stadium=stadium_name,
                            race_round=f"{race_num.zfill(2)}R",
                            title="",  # Typically follows in next lines
                            race_code=f"{stadium_code}{race_num}",
                        )
                        race_count += 1

                        logging_module.debug(
                            "race_detected",
                            race_count=race_count,
                            stadium=stadium_name,
                            race_round=f"{race_num.zfill(2)}R",
                        )
                        continue

                    # If current race exists, try to parse racer data
                    if current_race and len(current_race.racers) < 6:
                        racer = parse_racer_result_line(line)
                        if racer:
                            current_race.racers.append(racer)
                            continue

            except (ValueError, IndexError):
                pass

        # Add final race if valid
        if current_race and len(current_race.racers) == 6:
            races.append(current_race)

        logging_module.info(
            "file_parsed",
            file_type="K",
            races_count=len(races),
        )
        return races

    except Exception as e:
        logging_module.error(
            "parse_error",
            file_type="K",
            error=str(e),
            error_type=type(e).__name__,
        )
        return races


def parse_racer_result_line(line: str) -> Optional[RacerResult]:
    """Parse a single racer result line.

    Args:
        line: Fixed-width format line

    Returns:
        RacerResult object or None if parsing fails
    """
    try:
        # Expected format (approximate field positions):
        # Positions vary by file format version
        # Basic parsing: racer number, name, weight, result

        # This is simplified - actual implementation would need exact positions
        parts = line.split()
        if len(parts) < 4:
            return None

        racer_num = int(parts[0])
        name = parts[1] if len(parts) > 1 else ""
        weight = float(parts[2]) if len(parts) > 2 else 0.0
        result = int(parts[3]) if len(parts) > 3 else 0

        if 1 <= racer_num <= 6 and 1 <= result <= 6:
            return RacerResult(
                number=racer_num,
                name=name,
                weight=weight,
                result=result,
            )

        return None

    except (ValueError, IndexError):
        return None


def parse_program_file(content: str, date: str = "") -> List[RaceProgram]:
    """Parse B-file (program) from fixed-width text format.

    Args:
        content: File content as string
        date: Date in YYYY-MM-DD format (optional)

    Returns:
        List of RaceProgram objects
    """
    programs = []

    try:
        lines = content.strip().split("\n")

        if not lines:
            logging_module.warning("parse_empty_file", file_type="B")
            return programs

        current_program: Optional[RaceProgram] = None
        program_count = 0

        for line_num, line in enumerate(lines, 1):
            line = line.rstrip()

            if not line:
                continue

            try:
                if len(line) >= 20:
                    # Detect program header (race number and stadium)
                    stadium_code = line[0:2].strip()
                    race_num = line[2:4].strip()

                    if (
                        stadium_code.isdigit() and
                        1 <= int(stadium_code) <= 24 and
                        race_num.isdigit() and
                        1 <= int(race_num) <= 12
                    ):
                        # New program
                        if current_program and len(current_program.racer_frames) == 6:
                            programs.append(current_program)

                        stadium_name = STADIUM_NAMES.get(stadium_code, "Unknown")
                        current_program = RaceProgram(
                            date=date,
                            stadium=stadium_name,
                            race_round=f"{race_num.zfill(2)}R",
                            title="",
                            race_code=f"{stadium_code}{race_num}",
                        )
                        program_count += 1

                        logging_module.debug(
                            "program_detected",
                            program_count=program_count,
                            stadium=stadium_name,
                            race_round=f"{race_num.zfill(2)}R",
                        )
                        continue

                    # Parse racer frame data
                    if current_program and len(current_program.racer_frames) < 6:
                        frame = parse_racer_frame_line(line)
                        if frame:
                            current_program.racer_frames.append(frame)
                            continue

            except (ValueError, IndexError):
                pass

        # Add final program
        if current_program and len(current_program.racer_frames) == 6:
            programs.append(current_program)

        logging_module.info(
            "file_parsed",
            file_type="B",
            programs_count=len(programs),
        )
        return programs

    except Exception as e:
        logging_module.error(
            "parse_error",
            file_type="B",
            error=str(e),
            error_type=type(e).__name__,
        )
        return programs


def parse_racer_frame_line(line: str) -> Optional[RacerFrame]:
    """Parse a single racer frame line from program file.

    Args:
        line: Fixed-width format line

    Returns:
        RacerFrame object or None if parsing fails
    """
    try:
        # Simplified parsing of racer frame data
        # In production, would use exact column positions
        parts = line.split()
        if len(parts) < 10:
            return None

        entry_num = int(parts[0])
        name = parts[2] if len(parts) > 2 else ""
        age = int(parts[3]) if len(parts) > 3 else 0
        weight = float(parts[-2]) if len(parts) > 2 else 0.0
        adjustment = float(parts[-1]) if len(parts) > 1 else 0.0

        if 1 <= entry_num <= 6:
            return RacerFrame(
                entry_number=entry_num,
                registration_number="",
                racer_name=name,
                age=age,
                win_rate=0.0,
                place_rate=0.0,
                average_score=0.0,
                motor_number="",
                motor_wins=0,
                motor_2nd=0,
                boat_number="",
                boat_wins=0,
                boat_2nd=0,
                weight=weight,
                adjustment=adjustment,
            )

        return None

    except (ValueError, IndexError):
        return None
