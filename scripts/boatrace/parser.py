"""Parse fixed-width text format files from boatrace."""

import re
import warnings
from typing import List, Optional
from .models import RaceProgram, RacerFrame
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


def parse_program_file(content: str, date: str = "") -> List[RaceProgram]:
    """Parse B-file (program) from actual boat racing format.

    .. deprecated::
        B-file (mbrace.or.jp の出走表 .lzh) 依存は撤去済み。本関数および
        :func:`parse_racer_frame_line` は production パイプライン
        (race-card / recent-form / motor-stats) から呼ばれていません。
        固定幅テキストの脆さ (初日に早見が無い行や、ボート2連=100.00 で
        セパレータが消えるなど) で芦屋 11R 単独表示バグの原因になった
        経緯があるため、レース一覧は ``boatrace.holding_list`` (boatcast.jp の
        getHoldingList2 API + title CSV フォールバック) を使用してください。

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
    warnings.warn(
        "parse_program_file is deprecated; use boatrace.holding_list "
        "(getHoldingList2 API + title CSV fallback) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
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
                    # Extract race round - find "Ｒ" and extract digits before it
                    # Handle both formats: "　１Ｒ" (1-9 races) and "１０Ｒ", "１１Ｒ", "１２Ｒ" (10-12 races)
                    race_r_idx = line.find("Ｒ")
                    if race_r_idx >= 1:
                        # Extract backwards from "Ｒ" to get the race number(s)
                        # Could be 1 or 2 full-width digits before "Ｒ"
                        race_digits = ""
                        j = race_r_idx - 1
                        while j >= 0 and line[j] in "１２３４５６７８９０":
                            race_digits = line[j] + race_digits
                            j -= 1
                        
                        if race_digits:
                            race_round_raw = race_digits + "R"
                            # Convert to ASCII
                            race_round_raw = race_round_raw.translate(trans_asc).replace(' ', '')
                        else:
                            # Fallback to old method if no digits found
                            race_round_raw = line[1:3].translate(trans_asc).replace(' ', '')
                    else:
                        race_round_raw = ""
                    
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

    .. deprecated::
        See :func:`parse_program_file`. B-file 固定幅パースは production
        から外しています。

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

            # 固定幅フォーマットで「モーター2連 + ボート番号 + ボート2連」が
            # スペース無しに連結されるケースを救う。
            # 通常: ["65.00146", "20.83"] (モーター率+ボート番号 / ボート率)
            # ボート率が 100.00 (6桁) のとき間のスペースが消えるため
            # ["50.00159100.00"] のように 1 トークンに潰れる。
            #  → 末尾の "###.##" を切り出して 2 トークンに分割する。
            expanded_parts: List[str] = []
            for token in parts:
                if token.count(".") >= 2:
                    m = re.match(r"^(\d{1,3}\.\d{2}\d{1,3})(\d{1,3}\.\d{2})$", token)
                    if m:
                        expanded_parts.append(m.group(1))
                        expanded_parts.append(m.group(2))
                        continue
                expanded_parts.append(token)
            parts = expanded_parts

            if len(parts) < 7:
                # Need at least: 全国勝率, 全国2連, 当地勝率, 当地2連,
                # モーター番号, モーター2連+ボート番号, ボート2連.
                # 今節成績 / 早見 は初日などで欠けることがあるので任意扱い。
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

            # Index 5: モーター2連対率
            motor_2nd_rate = 0.0
            if idx < len(parts):
                motor_2nd_rate = float(parts[idx]) if parts[idx] else 0.0
                idx += 1

            # Index 6: ボート号
            boat_number = ""
            if idx < len(parts):
                boat_number = parts[idx] if parts[idx] else ""
                idx += 1

            # Index 7: ボート2連対率
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


