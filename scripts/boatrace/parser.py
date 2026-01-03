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

        # Pre-scan to find valid race header line indices
        # A valid race header has:
        # 1. Line matching pattern "nR" (n = 1-12) with non-digit after nR
        # 2. Followed (within next 3 lines) by "着 艇 登番" header
        valid_race_headers = set()
        
        for i, line in enumerate(lines):
            stripped = line.lstrip()
            
            # Check for race header pattern
            is_potential_header = False
            if stripped:
                if len(stripped) >= 2:
                    if stripped[0].isdigit() and stripped[1] == "R":
                        race_num = int(stripped[0])
                        if 1 <= race_num <= 9:
                            after_r = stripped[2:].lstrip()
                            if after_r and not after_r[0].isdigit():
                                is_potential_header = True
                    elif len(stripped) >= 3 and stripped[0:2].isdigit() and stripped[2] == "R":
                        race_num = int(stripped[0:2])
                        if 10 <= race_num <= 12:
                            after_r = stripped[3:].lstrip()
                            if after_r and not after_r[0].isdigit():
                                is_potential_header = True
            
            # If potential header, check if it's followed by "着 艇 登番" within next 3 lines
            if is_potential_header:
                for j in range(i + 1, min(i + 4, len(lines))):
                    if "着 艇 登番" in lines[j] or "着　艇　登番" in lines[j]:
                        valid_race_headers.add(i)
                        break

        # Parse races using valid race header indices
        current_race: Optional[RaceResult] = None
        race_count = 0
        in_race_detail = False

        for line_num, line in enumerate(lines, 1):
            # Remove trailing whitespace and handle \r (carriage return)
            line = line.rstrip()

            # Skip empty lines
            if not line:
                continue

            # Check if this line is a valid race header
            if (line_num - 1) in valid_race_headers:
                # Save previous race if it has racers
                if current_race and len(current_race.racers) > 0:
                    races.append(current_race)

                stripped = line.lstrip()
                
                # Extract race number
                race_num_str = ""
                if stripped[0].isdigit() and stripped[1] == "R":
                    race_num_str = stripped[0]
                elif stripped[0:2].isdigit() and stripped[2] == "R":
                    race_num_str = stripped[0:2]

                # Start new race
                current_race = RaceResult(
                    date=date,
                    stadium="大村",  # Default - could be improved
                    race_round=f"{race_num_str.zfill(2)}R",
                    title=stripped[2:].strip() if len(stripped) > 2 else "",
                    race_code=f"13{race_num_str}",
                )
                race_count += 1
                in_race_detail = False

                logging_module.debug(
                    "race_detected",
                    race_count=race_count,
                    race_round=f"{race_num_str.zfill(2)}R",
                )
                continue

            # Detect start of race detail section
            if "着 艇 登番" in line or "着　艇　登番" in line:
                in_race_detail = True
                continue

            # Detect racer result lines (contain race times and results)
            # Only parse if we're in the race detail section and have a current race
            if current_race and in_race_detail:
                # Try to parse racer data
                if len(line.strip()) > 10 and not line.startswith(" " * 20):
                    racer = parse_racer_result_line(line)
                    if racer and len(current_race.racers) < 6:
                        current_race.racers.append(racer)

        # Add final race if it has racers
        if current_race and len(current_race.racers) > 0:
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
        # Actual file format analysis:
        # Line 32: '  01  1 4443 津田...裕絵 52   24  6.91   1    0.08     1.49.7\r'
        # Parts[0]: 着順 (01, 02, 03, ...) ← Result
        # Parts[1]: 艇番 (1, 2, 3, ...) ← Racer/Boat number
        # Parts[2]: 登番 (registration)
        # Parts[3..N-6]: 選手名 (name - multiple parts)
        # Parts[-6]: 体重 (weight in kg)
        # Parts[-5]: モーター or 展示 (motor/display field)
        # Parts[-4]: 進入 (entrance number)
        # Parts[-3]: スタートタイム start time
        # Parts[-2]: 調整 (adjustment/time data)
        # Parts[-1]: レースタイム (race time) or '.'

        parts = line.split()
        if len(parts) < 10:
            return None

        # Extract result (着順) from parts[0]
        try:
            result = int(parts[0])
        except ValueError:
            return None
        
        # Extract racer number (艇番) from parts[1]
        try:
            racer_num = int(parts[1])
        except ValueError:
            return None
        
        # Extract weight (体重) from parts[-6]
        weight = 0.0
        try:
            weight = float(parts[-6])
        except (ValueError, IndexError):
            weight = 0.0

        # Extract name from parts[3] onwards (up to parts[-6])
        # Join the name parts with spaces
        name_parts = []
        if len(parts) > 3:
            # Name ends 6 positions from the end
            name_end_idx = len(parts) - 6
            if name_end_idx > 3:
                name_parts = parts[3:name_end_idx]
            elif name_end_idx == 3:
                # Only one name part
                name_parts = [parts[3]]
        
        name = " ".join(name_parts) if name_parts else ""

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
