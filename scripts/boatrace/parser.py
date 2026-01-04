"""Parse fixed-width text format files from boatrace."""

from typing import List, Optional
from .models import RaceResult, RaceProgram, RacerResult, RacerFrame
from . import logger as logging_module


class ParserError(Exception):
    """Parsing failed."""

    pass


# Stadium name to code mapping (for race code generation)
STADIUM_CODE_MAP = {
    "桐生": "01", "戸田": "02", "江戸川": "03", "平和島": "04",
    "多摩川": "05", "浜名湖": "06", "蒲郡": "07", "常滑": "08",
    "津": "09", "三国": "10", "びわこ": "11", "住之江": "12",
    "尼崎": "13", "鳴門": "14", "丸亀": "15", "児島": "16",
    "宮島": "17", "徳山": "18", "下関": "19", "若松": "20",
    "芦屋": "21", "福岡": "22", "唐津": "23", "大村": "24",
}

# Stadium code to name mapping (for reference)
STADIUM_NAMES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡", "08": "常滑",
    "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
    "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島",
    "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村",
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

        # Variables to hold extracted header information
        # These are updated each time a new "競走成績" section is found
        header_title = ""
        header_date = ""
        header_stadium = ""
        header_day = ""
        header_day_of_session = ""  # 日次（第2日など）

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
                # Try 2-digit race number first (01-12)
                if len(stripped) >= 3 and stripped[0:2].isdigit() and stripped[2] == "R":
                    race_num = int(stripped[0:2])
                    if 1 <= race_num <= 12:
                        after_r = stripped[3:].lstrip()
                        if after_r and not after_r[0].isdigit():
                            is_potential_header = True
                # Then try single-digit race number (1-9)
                elif len(stripped) >= 2 and stripped[0].isdigit() and stripped[1] == "R":
                    race_num = int(stripped[0])
                    if 1 <= race_num <= 9:
                        after_r = stripped[2:].lstrip()
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
        last_racer_line_index = 0  # Track where racer section ends for betting results

        for line_num, line in enumerate(lines, 1):
            # Remove trailing whitespace and handle \r (carriage return)
            line = line.rstrip()

            # Skip empty lines
            if not line:
                continue

            # Check for header section (競走成績) - update header info when found
            if "競走成績" in line:
                # Extract header information from the following lines
                try:
                    # Skip next line (i + 1)
                    if line_num < len(lines):
                        pass
                    # Get title from next line (i + 2)
                    if line_num + 1 < len(lines):
                        header_title = lines[line_num + 1].strip()
                    # Skip another line (i + 3), then extract day/date/stadium (i + 4)
                    if line_num + 3 < len(lines):
                        header_line = lines[line_num + 3]
                        
                        # Extract day_of_session: "第 7日" or "第7日" pattern
                        # Look for "第" and extract until "日"
                        if "第" in header_line and "日" in header_line:
                            start_idx = header_line.find("第")
                            end_idx = header_line.find("日", start_idx)
                            if start_idx >= 0 and end_idx >= 0:
                                # Extract "第 7日" and remove internal spaces
                                day_of_session_raw = header_line[start_idx:end_idx+1]
                                # Remove all spaces within the day_of_session
                                header_day_of_session = day_of_session_raw.replace(" ", "").replace("　", "")
                        
                        header_date = header_line[17:27].replace(' ', '0')
                        header_stadium = header_line[62:65].replace('　', '')
                except (IndexError, Exception):
                    # If header extraction fails, continue with current values
                    pass

            # Check if this line is a valid race header
            if (line_num - 1) in valid_race_headers:
                # Save previous race if it has racers
                if current_race and len(current_race.racers) > 0:
                    # Extract betting results starting from after the last racer line (convert to 0-indexed)
                    _extract_betting_results(current_race, lines, last_racer_line_index - 1)
                    races.append(current_race)

                stripped = line.lstrip()

                # Extract race number (same logic as header detection)
                race_num_str = ""
                title_offset = 0
                if len(stripped) >= 3 and stripped[0:2].isdigit() and stripped[2] == "R":
                    race_num_str = stripped[0:2]
                    title_offset = 3
                elif len(stripped) >= 2 and stripped[0].isdigit() and stripped[1] == "R":
                    race_num_str = stripped[0]
                    title_offset = 2

                # Start new race with current header information
                title_str = stripped[title_offset:].strip() if len(stripped) > title_offset else ""

                # Generate race code: YYYYMMDDRRNN
                # YYYY: year, MM: month, DD: day
                # RR: stadium code (01-24)
                # NN: race number (01-12)
                stadium_code = STADIUM_CODE_MAP.get(header_stadium, "24")  # Default to 24 (大村) if not found
                # Remove both '-' and '/' from date
                date_clean = header_date.replace('-', '').replace('/', '')
                race_code = f"{date_clean}{stadium_code}{race_num_str.zfill(2)}"

                current_race = RaceResult(
                    date=header_date if header_date else date,
                    stadium=header_stadium if header_stadium else "大村",
                    race_round=f"{race_num_str.zfill(2)}R",
                    title=header_title if header_title else title_str,
                    race_code=race_code,
                    day_of_session=header_day_of_session if header_day_of_session else None,
                )

                # Extract race details from the race header line (contains "R" and "H")
                if "H" in stripped:
                    _extract_race_details(current_race, stripped, lines, line_num - 1)

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

            # Extract race details (distance, weather, wind, wave height, winning technique)
            # Pattern: line contains "R" and "H" for race round and distance
            if current_race and in_race_detail and "R" in line and "H" in line:
                _extract_race_details(current_race, line, lines, line_num - 1)
                continue

            # Detect racer result lines (contain race times and results)
            # Only parse if we're in the race detail section and have a current race
            if current_race and in_race_detail:
                # Try to parse racer data
                if len(line.strip()) > 10 and not line.startswith(" " * 20):
                    racer = parse_racer_result_line(line)
                    if racer and len(current_race.racers) < 6:
                        current_race.racers.append(racer)
                        last_racer_line_index = line_num  # Track last racer line (1-indexed)

        # Add final race if it has racers
        if current_race and len(current_race.racers) > 0:
            # Extract betting results for final race starting from after last racer (convert to 0-indexed)
            _extract_betting_results(current_race, lines, last_racer_line_index - 1)
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
        # Parse using split() to extract clean parts
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

        # Extract registration number (登番) from parts[2]
        registration_number = parts[2] if len(parts) > 2 else ""

        # Find where name ends: look for the first numeric value after parts[3]
        # Expected format:
        # parts[0]: result (着順)
        # parts[1]: racer_num (艇番)
        # parts[2]: registration (登番)
        # parts[3:N]: name parts (選手名) - variable length
        # parts[N]: motor_number (モーター号)
        # parts[N+1]: boat_number (ボート号)
        # parts[N+2]: showcase_time (展示タイム)
        # parts[N+3]: entrance_course (進入コース)
        # parts[N+4]: start_timing (スタートタイミング)
        # parts[N+5]: race_time (レースタイム)
        
        name_parts = []
        name_end_idx = 3
        for i in range(3, len(parts)):
            part = parts[i]
            # Check if this is numeric
            try:
                float(part)
                # This is a numeric value, so name ends before this
                name_end_idx = i
                break
            except ValueError:
                # Not numeric, so this is part of the name
                name_parts.append(part)

        name = " ".join(name_parts) if name_parts else ""

        # Now extract numeric fields based on name_end_idx
        # All numeric fields follow immediately after the name
        motor_number = ""
        boat_number = ""
        showcase_time = None
        entrance_course = None
        start_timing = None
        race_time = None

        # Extract motor_number from parts[name_end_idx]
        try:
            if name_end_idx < len(parts):
                motor_number = parts[name_end_idx]
        except IndexError:
            pass

        # Extract boat_number from parts[name_end_idx + 1]
        try:
            if name_end_idx + 1 < len(parts):
                boat_number = parts[name_end_idx + 1]
        except IndexError:
            pass

        # Extract showcase_time from parts[name_end_idx + 2]
        try:
            if name_end_idx + 2 < len(parts):
                showcase_str = parts[name_end_idx + 2]
                if showcase_str and showcase_str != ".":
                    showcase_time = float(showcase_str)
        except (ValueError, IndexError):
            pass

        # Extract entrance_course from parts[name_end_idx + 3]
        try:
            if name_end_idx + 3 < len(parts):
                entrance_str = parts[name_end_idx + 3]
                if entrance_str and entrance_str.isdigit():
                    entrance_course = int(entrance_str)
        except (ValueError, IndexError):
            pass

        # Extract start_timing from parts[name_end_idx + 4]
        try:
            if name_end_idx + 4 < len(parts):
                start_str = parts[name_end_idx + 4]
                if start_str and start_str != ".":
                    start_timing = float(start_str)
        except (ValueError, IndexError):
            pass

        # Extract race_time from parts[name_end_idx + 5]
        # Time format can be "1.49.7" (1:49.7) or "108.9" (seconds)
        try:
            if name_end_idx + 5 < len(parts):
                race_str = parts[name_end_idx + 5]
                if race_str and race_str != ".":
                    # Check if it's in time format (minute.second.centisecond format)
                    if race_str.count(".") == 2:
                        # Format like "1.49.7" means 1:49.7
                        time_parts = race_str.split(".")
                        if len(time_parts) == 3:
                            try:
                                minutes = int(time_parts[0])
                                seconds = int(time_parts[1])
                                centiseconds = int(time_parts[2])
                                race_time = minutes * 60 + seconds + centiseconds / 10.0
                            except (ValueError, IndexError):
                                race_time = float(race_str)
                    else:
                        race_time = float(race_str)
        except (ValueError, IndexError):
            pass

        if 1 <= racer_num <= 6 and 1 <= result <= 6:
            return RacerResult(
                number=racer_num,
                name=name,
                weight=None,
                result=result,
                registration_number=registration_number if registration_number else None,
                motor_number=motor_number if motor_number else None,
                boat_number=boat_number if boat_number else None,
                showcase_time=showcase_time,
                entrance_course=entrance_course,
                start_timing=start_timing,
                time=race_time,
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

        # Full-width character mappings for race numbers
        # Used in B-files (programs) which use full-width characters
        fullwidth_numbers = {
            "１": "1", "２": "2", "３": "3", "４": "4", "５": "5",
            "６": "6", "７": "7", "８": "8", "９": "9", "０": "0",
            "１０": "10", "１１": "11", "１２": "12",
        }

        current_program: Optional[RaceProgram] = None
        program_count = 0
        in_program_detail = False

        for line_num, line in enumerate(lines, 1):
            line = line.rstrip()

            if not line:
                continue

            # Detect race header pattern: "　ＮＲ  race_title..."
            # Example: "　１Ｒ  シリーズ戦予          Ｈ１８００ｍ  電話投票締切予定１４：５７"
            # These use full-width characters
            is_race_header = False
            race_num_str = ""
            
            stripped = line.lstrip()
            
            # Check for full-width race number patterns (１Ｒ, ２Ｒ, ..., １０Ｒ, １１Ｒ, １２Ｒ)
            if "Ｒ" in stripped:
                # Try to extract full-width race number
                if stripped.startswith("１０Ｒ"):
                    is_race_header = True
                    race_num_str = "10"
                elif stripped.startswith("１１Ｒ"):
                    is_race_header = True
                    race_num_str = "11"
                elif stripped.startswith("１２Ｒ"):
                    is_race_header = True
                    race_num_str = "12"
                elif len(stripped) >= 2 and stripped[0] in fullwidth_numbers and stripped[1] == "Ｒ":
                    race_num_str = fullwidth_numbers[stripped[0]]
                    is_race_header = True
            
            if is_race_header:
                # Save previous program if it has racer frames
                if current_program and len(current_program.racer_frames) > 0:
                    programs.append(current_program)

                # Default to Omura (stadium 13)
                current_program = RaceProgram(
                    date=date,
                    stadium="大村",  # Default - could be improved
                    race_round=f"{race_num_str.zfill(2)}R",
                    title=stripped[2:].strip() if len(stripped) > 2 else "",
                    race_code=f"13{race_num_str}",
                )
                program_count += 1
                in_program_detail = False

                logging_module.debug(
                    "program_detected",
                    program_count=program_count,
                    race_round=f"{race_num_str.zfill(2)}R",
                )
                continue

            # Detect start of program detail section (header line with boat numbers)
            # Pattern: "艇 選手 選手  年 支 体級..." or similar
            if "艇" in line and "選手" in line:
                in_program_detail = True
                continue

            # Parse racer frame data
            if current_program and in_program_detail:
                # Try to parse racer frame
                if len(line.strip()) > 10 and not line.startswith(" " * 20):
                    frame = parse_racer_frame_line(line)
                    if frame and len(current_program.racer_frames) < 6:
                        current_program.racer_frames.append(frame)

        # Add final program
        if current_program and len(current_program.racer_frames) > 0:
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
        # B-file racer line format (from fixed-width file):
        # Position 0: 艇番 (1-6)
        # Position 1: Space
        # Position 2-5: 登番 (4-digit registration number)
        # Position 6-9+: 選手名 (racer name, variable length)
        # Position 10-11: 年齢 (age, 2 digits)
        # Position 12+: 支部 (prefecture, variable length)
        # Position 14-15: 体重 (weight, 2 digits)
        # Position 16-17: クラス (class, 2 characters like B1, A2)
        # Position 18+: Various statistics (勝率, etc.)
        
        stripped = line.strip()
        if not stripped or len(stripped) < 18:
            return None

        try:
            # Extract boat number (艇番) from first character
            entry_num = int(stripped[0])
        except (ValueError, IndexError):
            return None
        
        # Validate boat number
        if entry_num < 1 or entry_num > 6:
            return None

        # Extract registration number (登番) from positions 2-5
        try:
            registration_number = stripped[2:6].strip()
        except IndexError:
            registration_number = ""

        # Extract racer name from positions 6-9
        # The name can be variable length, typically 2-4 characters
        try:
            name = ""
            # Collect characters from position 6 until we hit a digit (which indicates age)
            for i in range(6, min(len(stripped), 15)):
                char = stripped[i]
                # Stop at digits (which are part of age)
                if char.isdigit():
                    break
                name += char
            
            racer_name = name.replace("\u3000", " ").strip()  # Replace full-width space
        except IndexError:
            racer_name = ""

        # Extract age (should be 2 digits, typically after name)
        age = 0
        try:
            # Look for 2-digit age starting from position 10
            age_str = ""
            for i in range(10, min(len(stripped), 13)):
                if stripped[i].isdigit():
                    age_str += stripped[i]
                elif age_str:
                    break
            
            if age_str and len(age_str) <= 2:
                age = int(age_str)
        except (ValueError, IndexError):
            age = 0

        # Extract weight (should be 2 digits, typically around position 14-15)
        weight = 0.0
        try:
            weight_str = ""
            for i in range(14, min(len(stripped), 17)):
                if stripped[i].isdigit():
                    weight_str += stripped[i]
                elif weight_str:
                    break
            
            if weight_str:
                weight = float(weight_str)
        except (ValueError, IndexError):
            weight = 0.0

        return RacerFrame(
            entry_number=entry_num,
            registration_number=registration_number,
            racer_name=racer_name,
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
            adjustment=0.0,
        )

    except (ValueError, IndexError):
        return None


def _extract_betting_results(race: RaceResult, lines: List[str], start_line_index: int) -> None:
    """Extract betting results from K-file lines.

    Args:
        race: RaceResult object to update
        lines: All lines from the file
        start_line_index: Index of the first result line to process
    """
    try:
        # Process lines starting from start_line_index to find betting results
        # Betting results section follows the racer results
        in_betting_section = False
        last_bet_type = ""  # Track current betting type for continuation lines

        for i in range(start_line_index, len(lines)):
            line = lines[i].rstrip()

            # Skip empty lines while looking for betting section
            if not line:
                # If we've already found betting results, stop at the next empty line
                if in_betting_section:
                    break
                # Otherwise, continue looking
                continue

            # Check for betting keywords first
            has_bet_keyword = any(keyword in line for keyword in [
                "単勝", "複勝", "２連単", "2連単", "２連複", "2連複",
                "拡連複", "３連単", "3連単", "３連複", "3連複", "不成立"
            ])

            # Parse based on Japanese keywords
            if "単勝" in line:
                in_betting_section = True
                # Win bet: Extract boat number and payout
                # Format: "単勝   1  　　３２０" -> parts = ["単勝", "1", "３２０"]
                parts = line.split()
                if len(parts) >= 3:
                    # parts[0] = "単勝", parts[1] = boat number, parts[2] = payout
                    boat_num = parts[1].strip()
                    payout = parts[2].strip()
                    if boat_num and payout:
                        race.tansho = f"{boat_num},{payout}"
                elif len(parts) >= 2:
                    # Fallback: boat number but no payout found yet
                    boat_num = parts[1].strip()
                    if boat_num:
                        race.tansho = f"{boat_num},"

            elif "複勝" in line:
                in_betting_section = True
                last_bet_type = "fukusho"  # Track for continuation lines
                # Place bet (1 or 2 winners)
                # Format: "複勝   1  　　１３０" (single) or "複勝     1    160  3    320" (double) or continuation lines
                if "複勝" in line and line.strip().startswith("複"):
                    # Main fukusho line with 1 or 2 winners
                    parts = line.split()
                    # Check if both winners are on the same line: ["複勝", "1", "160", "3", "320"]
                    if len(parts) >= 5:
                        # Two winners on same line
                        boat1 = parts[1].strip()
                        payout1 = parts[2].strip()
                        boat2 = parts[3].strip()
                        payout2 = parts[4].strip()
                        if boat1 and payout1:
                            race.fukusho = f"{boat1},{payout1},{boat2},{payout2}"
                    elif len(parts) >= 3:
                        # Single winner or first of multi-line winners
                        boat_num = parts[1].strip()
                        payout = parts[2].strip()
                        if boat_num and payout:
                            race.fukusho = f"{boat_num},{payout},"
                    elif len(parts) >= 2:
                        boat_num = parts[1].strip()
                        if boat_num:
                            race.fukusho = f"{boat_num},"

            elif "２連単" in line or "2連単" in line:
                in_betting_section = True
                last_bet_type = "santan"  # Track for continuation lines
                # Exacta: "2連単   1-2　　１２３０" or "２連単   1-3        360  人気     2"
                parts = line.split()
                if len(parts) >= 3:
                    combo = parts[1].strip()
                    payout = parts[2].strip()
                    # Check if popularity (人気) is on the same line
                    popularity = ""
                    if "人気" in line:
                        # Find the 人気 keyword and extract the value after it
                        for j in range(len(parts)):
                            if parts[j] == "人気" and j + 1 < len(parts):
                                popularity = parts[j + 1].strip()
                                break
                    if combo and payout:
                        race.santan = f"{combo},{payout},{popularity}"
                elif len(parts) >= 2:
                    combo = parts[1].strip()
                    if combo:
                        race.santan = f"{combo},,"

            elif "２連複" in line or "2連複" in line:
                in_betting_section = True
                last_bet_type = "renfuku"  # Track for continuation lines
                # Quinella: "2連複   1-2　　　９８０" or "２連複   1-3        330  人気     2"
                parts = line.split()
                if len(parts) >= 3:
                    combo = parts[1].strip()
                    payout = parts[2].strip()
                    # Check if popularity (人気) is on the same line
                    popularity = ""
                    if "人気" in line:
                        # Find the 人気 keyword and extract the value after it
                        for j in range(len(parts)):
                            if parts[j] == "人気" and j + 1 < len(parts):
                                popularity = parts[j + 1].strip()
                                break
                    if combo and payout:
                        race.renfuku = f"{combo},{payout},{popularity}"
                elif len(parts) >= 2:
                    combo = parts[1].strip()
                    if combo:
                        race.renfuku = f"{combo},,"

            elif "拡連複" in line:
                in_betting_section = True
                # Wide (3 combinations across multiple lines)
                if i + 2 < len(lines):
                    l1 = line
                    l2 = lines[i + 1].rstrip() if i + 1 < len(lines) else ""
                    l3 = lines[i + 2].rstrip() if i + 2 < len(lines) else ""

                    c1 = l1[14:17].strip() if 14 < len(l1) else ""
                    p1 = l1[21:28].strip() if 21 < len(l1) else ""
                    pop1 = l1[36:38].strip() if 36 < len(l1) else ""

                    c2 = l2[17:20].strip() if 17 < len(l2) else ""
                    p2 = l2[24:31].strip() if 24 < len(l2) else ""
                    pop2 = l2[39:41].strip() if 39 < len(l2) else ""

                    c3 = l3[17:20].strip() if 17 < len(l3) else ""
                    p3 = l3[24:31].strip() if 24 < len(l3) else ""
                    pop3 = l3[39:41].strip() if 39 < len(l3) else ""

                    race.wide = f"{c1},{p1},{pop1},{c2},{p2},{pop2},{c3},{p3},{pop3}"

            elif "３連単" in line or "3連単" in line:
                in_betting_section = True
                # Trifecta
                combo = line[14:19].strip() if 14 < len(line) else ""
                payout = line[21:28].strip() if 21 < len(line) else ""
                popularity = line[35:38].strip() if 35 < len(line) else ""
                if combo and payout:
                    race.santan_yosoku = f"{combo},{payout},{popularity}"

            elif "３連複" in line or "3連複" in line:
                in_betting_section = True
                # Trio
                combo = line[14:19].strip() if 14 < len(line) else ""
                payout = line[21:28].strip() if 21 < len(line) else ""
                popularity = line[35:38].strip() if 35 < len(line) else ""
                if combo and payout:
                    race.trio = f"{combo},{payout},{popularity}"

            elif "不成立" in line:
                # Race invalidated
                logging_module.debug("race_invalidated", race_round=race.race_round)
                break

            # Handle continuation lines for betting results (lines without betting keywords but with boat numbers)
            elif in_betting_section and not has_bet_keyword:
                # This could be a continuation line for fukusho or other multi-line betting results
                # Format: "　　　　 2  　　１１０" -> parts = ["2", "１１０"]
                parts = line.split()
                if len(parts) >= 2 and parts[0].isdigit():
                    boat_num = parts[0]
                    payout = parts[1]
                    # Add to fukusho (最後に処理された複勝に追加)
                    if last_bet_type == "fukusho":
                        if race.fukusho:
                            race.fukusho = race.fukusho.rstrip(',') + f",{boat_num},{payout}"
                        else:
                            race.fukusho = f"{boat_num},{payout}"

    except (IndexError, ValueError):
        logging_module.debug("betting_results_extraction_failed", race_round=race.race_round)


def _extract_race_details(race: RaceResult, line: str, all_lines: List[str], line_index: int) -> None:
    """Extract race details (distance, weather, wind, etc.) from fixed-width line.

    Args:
        race: RaceResult object to update
        line: The race details line containing "R" and "H"
        all_lines: All lines from the file (for accessing next line for winning technique)
        line_index: Index of the current line in all_lines
    """
    try:
        # Handle "進入固定" special case
        if "進入固定" in line:
            line = line.replace('進入固定 H', '進入固定 H')

        # Extract race_name: text before "H" (which marks distance)
        h_index = line.find('H')
        if h_index > 0:
            # Extract from start of race name (after "R" marker) to before "H"
            # Find the "R" marker first
            r_index = line.find('R')
            if r_index >= 0:
                # Race name starts after "R" and any whitespace
                start_index = r_index + 1
                while start_index < len(line) and line[start_index] in ' 　':
                    start_index += 1
                race_name_str = line[start_index:h_index].replace('　', '').strip()
                if race_name_str:
                    race.race_name = race_name_str

        # Extract distance: find "H" followed by digits and "m"
        if h_index >= 0:
            # Distance format: "H1800m" or similar
            # Extract from H to next space
            distance_end = h_index + 1
            while distance_end < len(line) and line[distance_end] not in ' 　':
                distance_end += 1
            distance_str = line[h_index:distance_end].strip()
            
            # Extract only the numeric part (1800) from "H1800m"
            if distance_str and 'm' in distance_str:
                # Remove 'H' and 'm', keep just the number
                distance_num = distance_str.replace('H', '').replace('m', '').strip()
                if distance_num:
                    race.distance = distance_num

        # Extract weather: look for "曇り", "晴", "雨", "曇" (longer strings first)
        weather_chars = ['曇り', '晴', '雨', '曇']
        for char in weather_chars:
            weather_idx = line.find(char)
            if weather_idx >= 0:
                race.weather = char
                break

        # Extract wind direction: look for combinations first (北西, 南西, etc.), then single chars
        wind_directions = ['北西', '北東', '南西', '南東', '北', '南', '東', '西']
        for direction in wind_directions:
            wind_idx = line.find(direction)
            if wind_idx >= 0:
                race.wind_direction = direction
                break

        # Extract wind speed: find "m" preceded by a digit, starting after distance
        # We need to skip the "H1800m" distance first
        search_start = h_index + 1 if h_index >= 0 else 0
        for i in range(search_start, len(line)):
            if line[i] == 'm' and i > 0:
                # Look backwards for the digit, skip whitespace
                j = i - 1
                while j >= 0 and line[j] in ' 　':
                    j -= 1
                # Now collect digits backwards
                end_j = j + 1
                while j >= 0 and (line[j].isdigit() or line[j] == '.'):
                    j -= 1
                wind_speed_str = line[j+1:end_j].strip()
                if wind_speed_str and wind_speed_str not in ['800', '1800', '1200', '1600']:
                    race.wind_speed = wind_speed_str
                    break

        # Extract wave height: find "cm" preceded by digits
        cm_index = line.find('cm')
        if cm_index > 0:
            # Look backwards for the digit
            j = cm_index - 1
            while j >= 0 and line[j] in ' 　':
                j -= 1
            # Now collect digits backwards
            end_j = j + 1
            while j >= 0 and (line[j].isdigit() or line[j] == '.'):
                j -= 1
            wave_height_str = line[j+1:end_j].strip()
            if wave_height_str:
                race.wave_height = wave_height_str

        # Extract winning technique from next line
        if line_index + 1 < len(all_lines):
            next_line = all_lines[line_index + 1]
            # Look for common winning techniques
            techniques = ['逃げ', '差し', 'まくり', 'まくり差し']
            for technique in techniques:
                if technique in next_line:
                    race.winning_technique = technique
                    break

    except (IndexError, ValueError):
        # If extraction fails, just log and continue
        logging_module.debug("race_details_extraction_failed", race_round=race.race_round)
