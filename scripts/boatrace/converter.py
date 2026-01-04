"""Convert parsed race data to CSV format."""

import csv
from io import StringIO
from typing import List
from .models import RaceResult, RaceProgram
from . import logger as logging_module


# CSV Headers for results
RESULTS_HEADERS = [
    "レースコード", "タイトル", "日次", "レース日", "レース場", "レース回", "レース名",
    "距離(m)", "天候", "風向", "風速(m)", "波の高さ(cm)", "決まり手",
    "単勝_艇番", "単勝_払戻金",
    "複勝_1着_艇番", "複勝_1着_払戻金", "複勝_2着_艇番", "複勝_2着_払戻金",
    "2連単_組番", "2連単_払戻金", "2連単_人気",
    "2連複_組番", "2連複_払戻金", "2連複_人気",
    "拡連複_1-2着_組番", "拡連複_1-2着_払戻金", "拡連複_1-2着_人気",
    "拡連複_1-3着_組番", "拡連複_1-3着_払戻金", "拡連複_1-3着_人気",
    "拡連複_2-3着_組番", "拡連複_2-3着_払戻金", "拡連複_2-3着_人気",
    "3連単_組番", "3連単_払戻金", "3連単_人気",
    "3連複_組番", "3連複_払戻金", "3連複_人気",
    # Racer 1
    "1着_着順", "1着_艇番", "1着_登録番号", "1着_選手名", "1着_モーター番号", "1着_ボート番号",
    "1着_展示タイム", "1着_進入コース", "1着_スタートタイミング", "1着_レースタイム",
    # Racer 2
    "2着_着順", "2着_艇番", "2着_登録番号", "2着_選手名", "2着_モーター番号", "2着_ボート番号",
    "2着_展示タイム", "2着_進入コース", "2着_スタートタイミング", "2着_レースタイム",
    # Racer 3
    "3着_着順", "3着_艇番", "3着_登録番号", "3着_選手名", "3着_モーター番号", "3着_ボート番号",
    "3着_展示タイム", "3着_進入コース", "3着_スタートタイミング", "3着_レースタイム",
    # Racer 4
    "4着_着順", "4着_艇番", "4着_登録番号", "4着_選手名", "4着_モーター番号", "4着_ボート番号",
    "4着_展示タイム", "4着_進入コース", "4着_スタートタイミング", "4着_レースタイム",
    # Racer 5
    "5着_着順", "5着_艇番", "5着_登録番号", "5着_選手名", "5着_モーター番号", "5着_ボート番号",
    "5着_展示タイム", "5着_進入コース", "5着_スタートタイミング", "5着_レースタイム",
    # Racer 6
    "6着_着順", "6着_艇番", "6着_登録番号", "6着_選手名", "6着_モーター番号", "6着_ボート番号",
    "6着_展示タイム", "6着_進入コース", "6着_スタートタイミング", "6着_レースタイム",
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


def _parse_betting_result(result: str, field_count: int) -> List[str]:
    """Parse betting result string into CSV fields.

    Args:
        result: Betting result string (e.g., "1,1230")
        field_count: Number of fields to return (2 or 3 or more for wide)

    Returns:
        List of field values padded to field_count
    """
    if not result:
        return [""] * field_count

    parts = result.split(",")
    # Pad with empty strings to match field_count
    return (parts + [""] * field_count)[:field_count]


def race_result_to_row(race: RaceResult) -> List[str]:
    """Convert RaceResult to CSV row.

    Args:
        race: RaceResult object

    Returns:
        List of CSV field values
    """
    # Extract race information from available data
    race_code = race.race_code or ""

    row = [
        race_code,                  # レースコード
        race.title,                 # タイトル
        race.day_of_session or "",  # 日次
        race.date,                  # レース日
        race.stadium,               # レース場
        race.race_round,            # レース回
        race.race_name or "",       # レース名
        race.distance or "",        # 距離(m)
        race.weather or "",         # 天候
        race.wind_direction or "",  # 風向
        race.wind_speed or "",      # 風速(m)
        race.wave_height or "",     # 波の高さ(cm)
        race.winning_technique or "", # 決まり手
    ]

    # Add betting results
    # 単勝 (win): boat, payout
    tansho_fields = _parse_betting_result(race.tansho, 2)
    row.extend(tansho_fields)

    # 複勝 (place): boat1, payout1, boat2, payout2
    fukusho_fields = _parse_betting_result(race.fukusho, 4)
    row.extend(fukusho_fields)

    # 2連単 (exacta): combo, payout, popularity
    santan_fields = _parse_betting_result(race.santan, 3)
    row.extend(santan_fields)

    # 2連複 (quinella): combo, payout, popularity
    renfuku_fields = _parse_betting_result(race.renfuku, 3)
    row.extend(renfuku_fields)

    # 拡連複 (wide): 3 combinations, each with combo, payout, popularity
    wide_fields = _parse_betting_result(race.wide, 9)
    row.extend(wide_fields)

    # 3連単 (trifecta): combo, payout, popularity
    santan_yosoku_fields = _parse_betting_result(race.santan_yosoku, 3)
    row.extend(santan_yosoku_fields)

    # 3連複 (trio): combo, payout, popularity
    trio_fields = _parse_betting_result(race.trio, 3)
    row.extend(trio_fields)

    # Add racer data (pad with empty racers if fewer than 6)
    # Sort racers by result (1st place first)
    sorted_racers = sorted(race.racers, key=lambda r: r.result)
    sorted_racers = sorted_racers + [None] * (6 - len(sorted_racers))

    for i, racer in enumerate(sorted_racers[:6]):
        if racer:
            row.extend([
                str(racer.result),                                      # 着順
                str(racer.number),                                      # 艇番
                racer.registration_number or "",                        # 登録番号
                racer.name,                                             # 選手名
                racer.motor_number or "",                               # モーター番号
                racer.boat_number or "",                                # ボート番号
                str(racer.showcase_time) if racer.showcase_time else "", # 展示タイム
                str(racer.entrance_course) if racer.entrance_course else "", # 進入コース
                str(racer.start_timing) if racer.start_timing else "",  # スタートタイミング
                str(racer.time) if racer.time else "",                  # レースタイム
            ])
        else:
            row.extend(["", "", "", "", "", "", "", "", "", ""])

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
