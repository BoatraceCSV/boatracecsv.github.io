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
    """Parse B-file (program) from actual boat racing format.

    Handles the actual B-file format from boatrace.or.jp:
    - STARTB header
    - Multiple venues with "番組表" sections
    - Venue info and title extraction per section
    - Race details with "電話投票締切予定" keyword
    - Fixed-width racer data format

    Args:
        content: File content as string (Shift-JIS encoded)
        date: Date in YYYY-MM-DD format (optional)

    Returns:
        List of RaceProgram objects
    """
    programs = []

    try:
        # Clean CRLF line endings
        lines = [line.rstrip('\r') for line in content.strip().split("\n")]

        if not lines:
            logging_module.warning("parse_empty_file", file_type="B")
            return programs

        # Character conversion table (full-width to ASCII)
        trans_asc = str.maketrans('１２３４５６７８９０Ｒ：　', '1234567890R: ')

        # State variables
        current_program: Optional[RaceProgram] = None
        title = ""
        day_of_session = ""
        stadium = ""
        program_count = 0

        i = 0
        while i < len(lines):
            line = lines[i]
            i += 1

            if not line.strip():
                continue

            # Detect venue/title header (contains stadium name and date)
            # Example: ボートレース福　岡   １２月　９日  スポーツ報知杯　　　  第　５日
            # This line appears BEFORE "番組表", so reset venue/title for each venue
            if "ボートレース" in line and ("月" in line or "年" in line):
                try:
                    # Extract venue name (starts with ボートレース)
                    start_idx = line.find("ボートレース")
                    venue_part = line[start_idx:start_idx+15].replace("　", "")
                    stadium = venue_part if venue_part.startswith("ボートレース") else ""
                except:
                    pass

            # Detect program table header with "番組表" - THIS RESETS TITLE FOR NEW VENUE
            if "番組表" in line:
                # The title will be extracted from the next few lines
                title = ""  # Reset title for new venue section
                continue

            # Try to extract title from line after 番組表
            # Example: "          スポーツ報知杯" or "          若松夜王Ｓ３戦ソフトバンクホークス杯福岡県内選手権"
            if not title and line.strip():
                # Check if this looks like a title line (no leading markers, has Japanese)
                stripped = line.strip()
                # Skip lines that look like structural markers
                if not stripped.startswith("−") and not stripped.startswith("第") and "月" not in stripped and "日" not in stripped:
                    # This might be a title line
                    if any(ord(c) >= 0x4E00 for c in stripped):  # Has CJK characters
                        title = stripped.replace("　", "")
                        logging_module.debug(
                            "title_extracted",
                            title=title,
                            stadium=stadium,
                        )

            # Parse detailed date line
            # Example: 第　５日          ２０２５年１２月　９日                  ボートレース若　松
            if "年" in line and "月" in line and "日" in line:
                try:
                    # Extract day_of_session
                    if "第" in line:
                        day_match = line[line.find("第"):line.find("第")+4]
                        day_of_session = day_match.translate(trans_asc).replace(' ', '')
                    
                    # Try to extract stadium again from this line
                    if "ボートレース" in line:
                        start_idx = line.rfind("ボートレース")
                        stadium_match = line[start_idx:start_idx+15].replace("　", "")
                        if stadium_match.startswith("ボートレース"):
                            stadium = stadium_match
                except:
                    pass

            # Detect race detail line with "電話投票締切予定"
            if "電話投票締切予定" in line:
                # Save previous program if valid
                if current_program and len(current_program.racer_frames) == 6:
                    programs.append(current_program)

                # Parse race details from this line
                # Format: "　１Ｒ  カタメン１予          Ｈ１８００ｍ  電話投票締切予定１１：０３"
                try:
                    # Extract race round (positions 1-3)
                    race_round_raw = line[1:3].translate(trans_asc).replace(' ', '')
                    
                    # Extract race name (positions 5-21)
                    race_name = line[5:21].replace('　', '').strip()
                    
                    # Extract distance (Ｈ１８００ｍ -> 1800)
                    distance_part = line[22:26].translate(trans_asc).replace(' ', '')
                    distance = distance_part if distance_part else ""
                    
                    # Extract post time (電話投票締切予定 の後)
                    deadline_idx = line.find("電話投票締切予定")
                    if deadline_idx >= 0:
                        post_time_raw = line[deadline_idx+8:deadline_idx+13].translate(trans_asc)
                        post_time = post_time_raw.replace(' ', '')
                    else:
                        post_time = ""

                    # Create new program
                    current_program = RaceProgram(
                        date=date,
                        stadium=stadium or "福岡",
                        race_round=race_round_raw,
                        title=title,
                        day_of_session=day_of_session,
                        race_name=race_name,
                        distance=distance,
                        post_time=post_time,
                    )
                    program_count += 1

                    logging_module.debug(
                        "program_detected",
                        program_count=program_count,
                        race_round=race_round_raw,
                        stadium=stadium,
                        title=title,
                    )
                except Exception as e:
                    logging_module.debug("race_header_parse_failed", error=str(e))

                # Skip next 3 lines (header lines)
                for _ in range(3):
                    if i < len(lines):
                        i += 1
                continue

            # Detect start of racer data section
            if "艇" in line and "選手" in line and "番" in line:
                # Skip separator line if present
                if "---" in line or "━" in line:
                    continue
                if i < len(lines) and ("---" in lines[i] or "━" in lines[i]):
                    i += 1
                continue

            # Parse racer frame data
            if current_program and len(current_program.racer_frames) < 6:
                # Check if this looks like a racer data line
                if line.strip() and not "---" in line and "電話投票" not in line:
                    # Try to parse as racer frame
                    try:
                        frame = parse_racer_frame_line(line)
                        if frame:
                            current_program.racer_frames.append(frame)
                            continue
                    except:
                        pass

                # Empty line might indicate end of racer section
                if not line.strip():
                    # Don't set in_program_detail to False yet, wait for next race
                    pass

        # Add final program if valid
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
    """Parse a single racer frame line from actual B-file format.

    Format example (mixed fixed-width and space-separated):
    1 4488小山　勉39埼玉53A1 6.08 41.67 6.58 66.67 13 33.97170 29.94 63331 35    10

    After space-separated values, session results are in special format:
      First part: consecutive digits (days 1-3 results, with day3-2R potentially empty)
      Gap/space
      Second part: remaining days results
      Last part: 早見 (early indicator)

    Args:
        line: B-file racer data line

    Returns:
        RacerFrame object or None if parsing fails
    """
    try:
        stripped = line.strip()
        if not stripped or len(stripped) < 15:
            return None

        # Entry number (first character should be 1-6)
        try:
            entry_num = int(stripped[0])
            if entry_num < 1 or entry_num > 6:
                return None
        except (ValueError, IndexError):
            return None

        # Position 1: space
        if len(stripped) < 6 or stripped[1] != ' ':
            return None

        # Position 2-5: registration number
        registration_number = stripped[2:6]
        
        # Position 6+: extract name until we find age digits
        rest = stripped[6:]
        
        # Find where name ends and age begins (look for 2 consecutive digits)
        name_end = 0
        for i in range(len(rest)):
            if i + 1 < len(rest) and rest[i].isdigit() and rest[i+1].isdigit():
                # Found 2 digits - this is age
                # Verify next character is prefecture (Japanese)
                if i + 2 < len(rest) and ord(rest[i+2]) >= 0x4E00:
                    name_end = i
                    break
        
        if name_end == 0:
            return None
        
        racer_name = rest[:name_end].replace("　", " ").strip()
        
        # After name: age(2) + prefecture + weight(2) + class(2) + space + space-separated values
        remaining = rest[name_end:]
        
        try:
            # Age: first 2 characters
            age_str = remaining[:2]
            age = int(age_str) if age_str.isdigit() else 0
            
            # Prefecture: next part until we find digits (weight)
            i = 2
            prefecture_start = i
            while i < len(remaining) and not remaining[i].isdigit():
                i += 1
            prefecture = remaining[prefecture_start:i]
            
            # Weight: next 2 digits
            weight_str = remaining[i:i+2]
            weight = float(weight_str) if weight_str.isdigit() else 0.0
            
            # Class: next 2 characters
            i += 2
            class_grade = remaining[i:i+2].strip()
            
            # Rest: space-separated values
            # Find the space after class
            i += 2
            while i < len(remaining) and remaining[i] == ' ':
                i += 1
            
            rest_text = remaining[i:].strip()
            parts = rest_text.split()
            
            if len(parts) < 8:
                # Need at least: win, place, local_win, local_place, motor#, motor_rate_boat, boat_rate, results
                return None
            
            idx = 0
            
            # Indices 0-1: 全国勝率, 全国2連対率
            win_rate = float(parts[idx]) if idx < len(parts) else 0.0
            idx += 1
            place_rate = float(parts[idx]) if idx < len(parts) else 0.0
            idx += 1
            
            # Indices 2-3: 当地勝率, 当地2連対率
            local_win_rate = float(parts[idx]) if idx < len(parts) else 0.0
            idx += 1
            local_place_rate = float(parts[idx]) if idx < len(parts) else 0.0
            idx += 1
            
            # Index 4: モーター号
            motor_number = parts[idx] if idx < len(parts) else ""
            idx += 1
            
            # Index 5: モーター2連対率 + ボート号 (concatenated, e.g., "33.97170")
            motor_2nd_rate = 0.0
            boat_number = ""
            
            if idx < len(parts):
                curr_part = parts[idx]
                # This part contains motor_2nd_rate + boat_number
                if '.' in curr_part:
                    dot_idx = curr_part.find('.')
                    motor_2nd_rate = float(curr_part[:dot_idx+3]) if dot_idx + 3 <= len(curr_part) else float(curr_part)
                    boat_number = curr_part[dot_idx+3:] if dot_idx + 3 < len(curr_part) else ""
                else:
                    boat_number = curr_part
                
                idx += 1
            
            # Index 6: ボート2連対率
            boat_2nd_rate = 0.0
            if idx < len(parts):
                boat_2nd_rate = float(parts[idx]) if parts[idx] else 0.0
                idx += 1
            
            # Initialize all result fields as empty
            results = {
                'day1_race1': "",
                'day1_race2': "",
                'day2_race1': "",
                'day2_race2': "",
                'day3_race1': "",
                'day3_race2': "",
                'day4_race1': "",
                'day4_race2': "",
                'day5_race1': "",
                'day5_race2': "",
                'day6_race1': "",
                'day6_race2': "",
            }
            
            hayami = ""
            
            # Parse remaining parts: results and 早見
            # Format example: ["63331", "35", "10"]
            # or: ["63331", "35", "", "", "10"]
            
            if idx < len(parts):
                # First results part: typically concatenated digits like "63331"
                # This represents days 1-3 results
                results_part1 = parts[idx]
                idx += 1
                
                # Extract individual digits from first part
                digit_list = []
                for char in results_part1:
                    if char.isdigit():
                        digit_list.append(char)
                
                # Assign to days
                if len(digit_list) >= 1:
                    results['day1_race1'] = digit_list[0]
                if len(digit_list) >= 2:
                    results['day1_race2'] = digit_list[1]
                if len(digit_list) >= 3:
                    results['day2_race1'] = digit_list[2]
                if len(digit_list) >= 4:
                    results['day2_race2'] = digit_list[3]
                if len(digit_list) >= 5:
                    results['day3_race1'] = digit_list[4]
                # day3_race2 is implicitly empty when we move to next part
            
            # Second results part: continuation like "35" or other format
            if idx < len(parts):
                results_part2 = parts[idx]
                idx += 1
                
                # Extract digits from second part
                digit_list = []
                for char in results_part2:
                    if char.isdigit():
                        digit_list.append(char)
                
                # Based on user expectation: 日3_2R=3, 日4_1R=5
                # But user also expects 日3_2R=(empty)
                # This suggests: day3_race2 is from first part (empty), day4_race1/race2 are from second part
                # Actually looking closer at user data: "3, 5" should be day4_1R, day4_2R only if day3_2R is empty
                # So if results_part1 has only 5 digits (days 1-3 with day3_2R empty), then part2 starts with day4
                
                # Re-interpret: if part1 is "63331" (5 digits), that's days 1-2-3-1, then day3_2 is empty
                # Then part2 "35" would be day4_1R=3, day4_2R=5
                if len(digit_list) >= 1:
                    results['day3_race2'] = digit_list[0]  # Actually day4_1R if day3_2 is empty
                if len(digit_list) >= 2:
                    results['day4_race1'] = digit_list[1]  # Actually day4_2R if day3_2 is empty
                
                # Hmm, this is getting confusing. Let me use user's explicit expectation
                # User says: day3_2R=(empty), day4_1R=3, day4_2R=5
                # Raw: "63331 35"
                # So: part1 "63331" = day1_1R, day1_2R, day2_1R, day2_2R, day3_1R (with day3_2R empty)
                # Then: part2 "35" = day4_1R (=3), day4_2R (=5)
                
                # Re-assign
                results['day3_race2'] = ""  # Keep empty as per user expectation
                results['day4_race1'] = digit_list[0] if len(digit_list) >= 1 else ""
                results['day4_race2'] = digit_list[1] if len(digit_list) >= 2 else ""
                results['day5_race1'] = digit_list[2] if len(digit_list) >= 3 else ""
                results['day5_race2'] = digit_list[3] if len(digit_list) >= 4 else ""
                results['day6_race1'] = digit_list[4] if len(digit_list) >= 5 else ""
                results['day6_race2'] = digit_list[5] if len(digit_list) >= 6 else ""
            
            # Get 早見 (should be last part)
            if idx < len(parts):
                hayami = parts[idx]
        
        except (ValueError, IndexError):
            return None

        return RacerFrame(
            entry_number=entry_num,
            registration_number=registration_number,
            racer_name=racer_name,
            age=age,
            prefecture=prefecture,
            class_grade=class_grade,
            win_rate=win_rate,
            place_rate=place_rate,
            average_score=local_win_rate,
            local_win_rate=local_win_rate,
            local_place_rate=local_place_rate,
            motor_number=motor_number,
            motor_wins=int(motor_2nd_rate) if motor_2nd_rate else 0,
            motor_2nd=0,
            motor_2nd_rate=motor_2nd_rate,
            boat_number=boat_number,
            boat_wins=int(boat_2nd_rate) if boat_2nd_rate else 0,
            boat_2nd=0,
            boat_2nd_rate=boat_2nd_rate,
            weight=weight,
            adjustment=0.0,
            results_day1_race1=results['day1_race1'],
            results_day1_race2=results['day1_race2'],
            results_day2_race1=results['day2_race1'],
            results_day2_race2=results['day2_race2'],
            results_day3_race1=results['day3_race1'],
            results_day3_race2=results['day3_race2'],
            results_day4_race1=results['day4_race1'],
            results_day4_race2=results['day4_race2'],
            results_day5_race1=results['day5_race1'],
            results_day5_race2=results['day5_race2'],
            results_day6_race1=results['day6_race1'],
            results_day6_race2=results['day6_race2'],
            hayami=hayami,
        )

    except Exception:
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
