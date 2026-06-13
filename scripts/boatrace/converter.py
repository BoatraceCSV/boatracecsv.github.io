"""Convert parsed race data to CSV format."""

import csv
from io import StringIO
from typing import List, Optional
from .models import (
    MotorStat,
    OriginalExhibitionData,
    RaceCard,
    RaceCardBoat,
    RaceCardSession,
    RacePreview,
    RecentForm,
    RecentFormBoat,
    RecentFormSession,
)
from . import logger as logging_module


# Venue code mapping for race code generation (YYYYMMDDCCNN format)
# Must match STADIUM_CODE_MAP in parser.py
VENUE_CODES = {
    # Names with "ボートレース" prefix (used in programs CSV)
    "ボートレース桐生": "01",
    "ボートレース戸田": "02",
    "ボートレース江戸川": "03",
    "ボートレース平和島": "04",
    "ボートレース多摩川": "05",
    "ボートレース浜名湖": "06",
    "ボートレース蒲郡": "07",
    "ボートレース常滑": "08",
    "ボートレース津": "09",
    "ボートレース三国": "10",
    "ボートレースびわこ": "11",
    "ボートレース琵琶湖": "11",  # Alternative name for びわこ
    "ボートレース住之江": "12",
    "ボートレース尼崎": "13",
    "ボートレース鳴門": "14",
    "ボートレース丸亀": "15",
    "ボートレース児島": "16",
    "ボートレース宮島": "17",
    "ボートレース徳山": "18",
    "ボートレース下関": "19",
    "ボートレース若松": "20",
    "ボートレース芦屋": "21",
    "ボートレース福岡": "22",
    "ボートレース唐津": "23",
    "ボートレース大村": "24",
    # Fallback names without prefix (for results CSV compatibility)
    "桐生": "01",
    "戸田": "02",
    "江戸川": "03",
    "平和島": "04",
    "多摩川": "05",
    "浜名湖": "06",
    "蒲郡": "07",
    "常滑": "08",
    "津": "09",
    "三国": "10",
    "びわこ": "11",
    "琵琶湖": "11",  # Alternative name for びわこ
    "住之江": "12",
    "尼崎": "13",
    "鳴門": "14",
    "丸亀": "15",
    "児島": "16",
    "宮島": "17",
    "徳山": "18",
    "下関": "19",
    "若松": "20",
    "芦屋": "21",
    "福岡": "22",
    "唐津": "23",
    "大村": "24",
}


# CSV Headers for previews (直前情報)
PREVIEWS_HEADERS = [
    "レースコード", "タイトル", "レース日", "レース場", "レース回",
    "風速(m)", "風向", "波の高さ(cm)", "天候", "気温(℃)", "水温(℃)",
]

# Add boat headers (6 boats, 7 fields each)
for boat_num in range(1, 7):
    PREVIEWS_HEADERS.extend([
        f"艇{boat_num}_艇番",
        f"艇{boat_num}_コース",
        f"艇{boat_num}_体重(kg)",
        f"艇{boat_num}_体重調整(kg)",
        f"艇{boat_num}_展示タイム",
        f"艇{boat_num}_チルト調整",
        f"艇{boat_num}_スタート展示",
    ])


def race_preview_to_row(preview: RacePreview) -> List[str]:
    """Convert RacePreview to CSV row.

    Args:
        preview: RacePreview object

    Returns:
        List of CSV field values
    """
    race_code = preview.race_code or ""

    row = [
        race_code,                              # レースコード
        preview.title or "",                    # タイトル
        preview.date,                           # レース日
        preview.stadium,                        # レース場
        preview.race_round,                     # レース回
        str(preview.wind_speed) if preview.wind_speed is not None else "",  # 風速(m)
        str(preview.wind_direction) if preview.wind_direction is not None else "",  # 風向
        str(preview.wave_height) if preview.wave_height is not None else "",  # 波の高さ(cm)
        str(preview.weather) if preview.weather is not None else "",  # 天候
        str(preview.air_temperature) if preview.air_temperature is not None else "",  # 気温(℃)
        str(preview.water_temperature) if preview.water_temperature is not None else "",  # 水温(℃)
    ]

    # Add boat data (pad with empty boats if fewer than 6)
    boats = preview.boats + [None] * (6 - len(preview.boats))

    for boat in boats[:6]:
        if boat:
            row.extend([
                str(boat.boat_number) if boat.boat_number else "",  # 艇番
                str(boat.course_number) if boat.course_number is not None else "",  # コース
                str(boat.weight) if boat.weight is not None else "",  # 体重(kg)
                str(boat.weight_adjustment) if boat.weight_adjustment is not None else "",  # 体重調整(kg)
                str(boat.exhibition_time) if boat.exhibition_time is not None else "",  # 展示タイム
                str(boat.tilt_adjustment) if boat.tilt_adjustment is not None else "",  # チルト調整
                str(boat.start_timing) if boat.start_timing is not None else "",  # スタート展示
            ])
        else:
            row.extend([""] * 7)

    return row


def previews_to_csv(previews: List[RacePreview]) -> str:
    """Convert list of RacePreview objects to CSV format.

    Args:
        previews: List of RacePreview objects

    Returns:
        CSV content as string
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")

        # Write header
        writer.writerow(PREVIEWS_HEADERS)

        # Write data rows
        for preview in previews:
            row = race_preview_to_row(preview)
            writer.writerow(row)

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="previews",
            rows=len(previews) + 1,  # +1 for header
            size_bytes=len(csv_content.encode("utf-8")),
        )

        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="previews",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""


# ---------------------------------------------------------------------------
# Original exhibition data (オリジナル展示データ) from race.boatcast.jp
# ---------------------------------------------------------------------------

# One CSV row per race.
# Column layout (fixed width; stadiums with only 2 measurements leave the
# third measurement label and each 艇N_値3 column blank):
#   レースコード, レース日, レース場, レース回, ステータス, 計測数,
#   計測項目1, 計測項目2, 計測項目3,
#   艇1_選手名, 艇1_値1, 艇1_値2, 艇1_値3,
#   ...
#   艇6_選手名, 艇6_値1, 艇6_値2, 艇6_値3
ORIGINAL_EXHIBITION_HEADERS: List[str] = [
    "レースコード",
    "レース日",
    "レース場",
    "レース回",
    "ステータス",
    "計測数",
    "計測項目1",
    "計測項目2",
    "計測項目3",
]
for _boat_num in range(1, 7):
    ORIGINAL_EXHIBITION_HEADERS.extend(
        [
            f"艇{_boat_num}_選手名",
            f"艇{_boat_num}_値1",
            f"艇{_boat_num}_値2",
            f"艇{_boat_num}_値3",
        ]
    )


def _fmt_optional(value) -> str:
    """Format an optional value for CSV: None -> ''."""
    if value is None:
        return ""
    return str(value)


def original_exhibition_to_row(data: OriginalExhibitionData) -> List[str]:
    """Convert a single OriginalExhibitionData to a CSV row.

    The ``レース場`` column is emitted as a 2-digit zero-padded code
    (``"01"`` - ``"24"``) for consistency with race_cards / recent_form
    CSVs whose ``レース場コード`` column uses the same format. This
    enables straight string equality joins across files.
    """
    labels = list(data.measure_labels) + [""] * 3
    row: List[str] = [
        data.race_code,
        data.date,
        f"{data.stadium_number:02d}",
        f"{data.race_number:02d}R",
        _fmt_optional(data.status),
        _fmt_optional(data.measure_count),
        labels[0],
        labels[1],
        labels[2],
    ]

    # Index boats by number for stable ordering.
    boats_by_number = {b.boat_number: b for b in data.boats}
    for boat_num in range(1, 7):
        boat = boats_by_number.get(boat_num)
        if boat is None:
            row.extend(["", "", "", ""])
            continue
        row.extend(
            [
                boat.racer_name or "",
                _fmt_optional(boat.value1),
                _fmt_optional(boat.value2),
                _fmt_optional(boat.value3),
            ]
        )

    return row


def original_exhibition_to_csv(
    items: List[OriginalExhibitionData],
) -> str:
    """Convert a list of OriginalExhibitionData to CSV content.

    Args:
        items: List of OriginalExhibitionData objects.

    Returns:
        CSV content as string (header + rows). Returns "" on failure.
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")

        writer.writerow(ORIGINAL_EXHIBITION_HEADERS)

        # Stable sort: by stadium then race number.
        ordered = sorted(
            items, key=lambda d: (d.stadium_number, d.race_number)
        )
        for item in ordered:
            writer.writerow(original_exhibition_to_row(item))

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="original_exhibition",
            rows=len(ordered) + 1,
            size_bytes=len(csv_content.encode("utf-8")),
        )

        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="original_exhibition",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""


# ---------------------------------------------------------------------------
# Race cards (出走表詳細) from race.boatcast.jp's bc_j_str3
# ---------------------------------------------------------------------------

# One CSV row per race. Schema: 4 race-meta columns + 6 boats x 96 columns
# (26 boat profile + 14 session slots x 5 sub-columns) = 580 columns total.
# The session slots are emitted in source order (col[25]..col[38] of bc_j_str3),
# i.e. day1-race1, day1-race2, day2-race1, ..., day7-race2.

RACE_CARD_HEADERS: List[str] = [
    "レースコード",
    "レース日",
    "レース場コード",
    "レース回",
]

# Per-boat profile columns. Order matches RaceCardBoat field declaration.
_RACE_CARD_BOAT_PROFILE_FIELDS: List[str] = [
    "登録番号",
    "選手名",
    "期別",
    "支部",
    "出身地",
    "年齢",
    "級別",
    "賞除",
    "F本数",
    "L本数",
    "全国平均ST",
    "全国勝率",
    "全国2連対率",
    "全国3連対率",
    "当地勝率",
    "当地2連対率",
    "当地3連対率",
    "モーターフラグ",
    "モーター番号",
    "モーター2連対率",
    "モーター3連対率",
    "ボートフラグ",
    "ボート番号",
    "ボート2連対率",
    "ボート3連対率",
    "早見",
]

# Per-session sub-columns.
_RACE_CARD_SESSION_FIELDS: List[str] = [
    "R番号",
    "進入",
    "枠",
    "ST",
    "着順",
]

for _boat_num in range(1, 7):
    for _profile_field in _RACE_CARD_BOAT_PROFILE_FIELDS:
        RACE_CARD_HEADERS.append(f"艇{_boat_num}_{_profile_field}")
    # 14 slots: indexed by (day, race_in_day). Column naming follows
    # ``艇N_節D{D}走{S}_*`` so day=1..7, race_in_day=1..2.
    for _day in range(1, 8):
        for _race_in_day in range(1, 3):
            for _session_field in _RACE_CARD_SESSION_FIELDS:
                RACE_CARD_HEADERS.append(
                    f"艇{_boat_num}_節D{_day}走{_race_in_day}_{_session_field}"
                )


def _race_card_session_cells(session: Optional[RaceCardSession]) -> List[str]:
    """Render a single session quintuple as 5 CSV cells. Empty session -> 5 blanks."""
    if session is None:
        return ["", "", "", "", ""]
    return [
        _fmt_optional(session.race_number),
        _fmt_optional(session.entry_course),
        _fmt_optional(session.waku),
        _fmt_optional(session.start_timing),
        _fmt_optional(session.finish_position),
    ]


def _race_card_boat_cells(boat: Optional[RaceCardBoat]) -> List[str]:
    """Render a single boat (profile + 14 sessions) as 96 CSV cells.

    Missing boat (欠場 / not held) -> 96 blanks.
    """
    if boat is None:
        return [""] * (len(_RACE_CARD_BOAT_PROFILE_FIELDS) + 14 * len(_RACE_CARD_SESSION_FIELDS))

    cells: List[str] = [
        _fmt_optional(boat.registration_number),
        boat.racer_name or "",
        boat.period or "",
        boat.branch or "",
        boat.birthplace or "",
        _fmt_optional(boat.age),
        boat.grade or "",
        boat.prize_excluded or "",
        _fmt_optional(boat.f_count),
        _fmt_optional(boat.l_count),
        _fmt_optional(boat.national_avg_st),
        _fmt_optional(boat.national_win_rate),
        _fmt_optional(boat.national_double_rate),
        _fmt_optional(boat.national_triple_rate),
        _fmt_optional(boat.local_win_rate),
        _fmt_optional(boat.local_double_rate),
        _fmt_optional(boat.local_triple_rate),
        _fmt_optional(boat.motor_flag),
        _fmt_optional(boat.motor_number),
        _fmt_optional(boat.motor_double_rate),
        _fmt_optional(boat.motor_triple_rate),
        _fmt_optional(boat.boat_flag),
        _fmt_optional(boat.boat_id),
        _fmt_optional(boat.boat_double_rate),
        _fmt_optional(boat.boat_triple_rate),
        _fmt_optional(boat.hayami),
    ]

    # Always emit 14 session slots; pad with empty sessions if the source
    # gave fewer (defensive — RaceCardScraper always produces 14).
    sessions = list(boat.sessions) + [None] * (14 - len(boat.sessions))
    for slot in sessions[:14]:
        cells.extend(_race_card_session_cells(slot))

    return cells


def race_card_to_row(card: RaceCard) -> List[str]:
    """Convert a single :class:`RaceCard` to a CSV row (580 cells)."""
    row: List[str] = [
        card.race_code,
        card.date,
        f"{card.stadium_number:02d}",
        f"{card.race_number:02d}R",
    ]

    boats_by_number = {b.boat_number: b for b in card.boats}
    for boat_num in range(1, 7):
        row.extend(_race_card_boat_cells(boats_by_number.get(boat_num)))

    return row


def race_cards_to_csv(items: List[RaceCard]) -> str:
    """Serialise a list of :class:`RaceCard` to CSV content (header + rows).

    Returns ``""`` on failure.
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(RACE_CARD_HEADERS)

        ordered = sorted(items, key=lambda c: (c.stadium_number, c.race_number))
        for item in ordered:
            writer.writerow(race_card_to_row(item))

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="race_cards",
            rows=len(ordered) + 1,
            size_bytes=len(csv_content.encode("utf-8")),
        )
        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="race_cards",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""


# ---------------------------------------------------------------------------
# Recent form (近況成績) — bc_zensou (national) / bc_zensou_touchi (local)
# ---------------------------------------------------------------------------

# One CSV row per race. Schema: 4 race-meta columns + 6 boats x 32 columns
# (2 identity + 5 sessions x 6 sub-columns) = 196 columns total.
# The same shape is used for both the national and the local variants —
# only the underlying data source differs, so a single converter serves
# both files.

RECENT_FORM_HEADERS: List[str] = [
    "レースコード",
    "レース日",
    "レース場コード",
    "レース回",
]

_RECENT_FORM_IDENTITY_FIELDS: List[str] = ["登録番号", "選手名"]

# Per-session sub-columns (in source order).
_RECENT_FORM_SESSION_FIELDS: List[str] = [
    "開始日",
    "終了日",
    "場コード",
    "場名",
    "グレード",
    "着順列",
]

for _boat_num in range(1, 7):
    for _identity_field in _RECENT_FORM_IDENTITY_FIELDS:
        RECENT_FORM_HEADERS.append(f"艇{_boat_num}_{_identity_field}")
    for _session_idx in range(1, 6):  # 前1節..前5節
        for _session_field in _RECENT_FORM_SESSION_FIELDS:
            RECENT_FORM_HEADERS.append(
                f"艇{_boat_num}_前{_session_idx}節_{_session_field}"
            )


def _recent_form_session_cells(session: Optional[RecentFormSession]) -> List[str]:
    """Render one session as 6 CSV cells. Empty session -> 6 blanks."""
    if session is None:
        return ["", "", "", "", "", ""]
    return [
        session.start_date or "",
        session.end_date or "",
        session.stadium_code or "",
        session.stadium_name or "",
        session.grade or "",
        session.finish_sequence or "",
    ]


def _recent_form_boat_cells(boat: Optional[RecentFormBoat]) -> List[str]:
    """Render one boat (identity + 5 sessions) as 32 CSV cells.

    Missing boat -> 32 blanks. Missing trailing sessions are padded.
    """
    if boat is None:
        return [""] * (
            len(_RECENT_FORM_IDENTITY_FIELDS) + 5 * len(_RECENT_FORM_SESSION_FIELDS)
        )

    cells: List[str] = [
        _fmt_optional(boat.registration_number),
        boat.racer_name or "",
    ]
    sessions = list(boat.sessions) + [None] * (5 - len(boat.sessions))
    for slot in sessions[:5]:
        cells.extend(_recent_form_session_cells(slot))
    return cells


def recent_form_to_row(form: RecentForm) -> List[str]:
    """Convert a single :class:`RecentForm` to a CSV row (196 cells)."""
    row: List[str] = [
        form.race_code,
        form.date,
        f"{form.stadium_number:02d}",
        f"{form.race_number:02d}R",
    ]
    boats_by_number = {b.boat_number: b for b in form.boats}
    for boat_num in range(1, 7):
        row.extend(_recent_form_boat_cells(boats_by_number.get(boat_num)))
    return row


def recent_forms_to_csv(items: List[RecentForm], variant: str = "national") -> str:
    """Serialise a list of :class:`RecentForm` to CSV content.

    ``variant`` is used only for log labelling — it does not change the CSV
    schema (national and local share the exact same column layout).
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(RECENT_FORM_HEADERS)

        ordered = sorted(items, key=lambda f: (f.stadium_number, f.race_number))
        for item in ordered:
            writer.writerow(recent_form_to_row(item))

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type=f"recent_form_{variant}",
            rows=len(ordered) + 1,
            size_bytes=len(csv_content.encode("utf-8")),
        )
        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type=f"recent_form_{variant}",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""


# ---------------------------------------------------------------------------
# Motor stats (モーター期成績) — bc_mst + bc_mdc
# ---------------------------------------------------------------------------

# One CSV row per motor (per stadium per record_date). 34 columns total —
# named columns for confidence ★★★/★★ fields, ``raw_col_NN`` columns for
# confidence ★ fields whose meaning is still unconfirmed.

MOTOR_STATS_HEADERS: List[str] = [
    "記録日",
    "モーター期起算日",
    "場コード",
    "モーター番号",
    "勝率",
    "勝率順位",
    "2連対率",
    "2連対率順位",
    "3連対率",
    "3連対率順位",
    "1着回数",
    "1着順位",
    "2着回数",
    "2着順位",
    "3着回数",
    "3着順位",
    "連対外回数",
    "出走数",
    "優勝回数",
    "優勝順位",
    "優出回数",
    "優出順位",
    "raw_col_21",
    "raw_col_22",
    "平均ラップ秒",
    "平均ラップ順位",
    "期内初使用日",
    "整備種別1回数",
    "整備種別2回数",
    "整備種別3回数",
    "整備種別4回数",
    "整備種別5回数",
    "整備種別6回数",
    "直近メンテ日",
]


def motor_stat_to_row(stat: MotorStat) -> List[str]:
    """Convert a single :class:`MotorStat` to a CSV row (34 cells)."""
    return [
        stat.record_date,
        _fmt_optional(stat.motor_period_start),
        _fmt_optional(stat.stadium_code),
        _fmt_optional(stat.motor_number),
        _fmt_optional(stat.win_rate),
        _fmt_optional(stat.win_rate_rank),
        _fmt_optional(stat.double_rate),
        _fmt_optional(stat.double_rate_rank),
        _fmt_optional(stat.triple_rate),
        _fmt_optional(stat.triple_rate_rank),
        _fmt_optional(stat.first_count),
        _fmt_optional(stat.first_rank),
        _fmt_optional(stat.second_count),
        _fmt_optional(stat.second_rank),
        _fmt_optional(stat.third_count),
        _fmt_optional(stat.third_rank),
        _fmt_optional(stat.out_of_place_count),
        _fmt_optional(stat.start_count),
        _fmt_optional(stat.championship_count),
        _fmt_optional(stat.championship_rank),
        _fmt_optional(stat.final_count),
        _fmt_optional(stat.final_rank),
        _fmt_optional(stat.raw_col_21),
        _fmt_optional(stat.raw_col_22),
        _fmt_optional(stat.avg_lap_seconds),
        _fmt_optional(stat.avg_lap_rank),
        _fmt_optional(stat.first_use_date),
        _fmt_optional(stat.maintenance_type1_count),
        _fmt_optional(stat.maintenance_type2_count),
        _fmt_optional(stat.maintenance_type3_count),
        _fmt_optional(stat.maintenance_type4_count),
        _fmt_optional(stat.maintenance_type5_count),
        _fmt_optional(stat.maintenance_type6_count),
        _fmt_optional(stat.last_maintenance_date),
    ]


def motor_stats_to_csv(items: List[MotorStat]) -> str:
    """Serialise a list of :class:`MotorStat` to CSV content (header + rows).

    Rows are ordered by ``(stadium_code, motor_number)``. Returns ``""`` on
    failure.
    """
    try:
        output = StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(MOTOR_STATS_HEADERS)

        ordered = sorted(
            items,
            key=lambda s: (
                s.stadium_code or "",
                s.motor_number if s.motor_number is not None else 0,
            ),
        )
        for item in ordered:
            writer.writerow(motor_stat_to_row(item))

        csv_content = output.getvalue()
        output.close()

        logging_module.info(
            "csv_generated",
            file_type="motor_stats",
            rows=len(ordered) + 1,
            size_bytes=len(csv_content.encode("utf-8")),
        )
        return csv_content

    except Exception as e:
        logging_module.error(
            "csv_generation_failed",
            file_type="motor_stats",
            error=str(e),
            error_type=type(e).__name__,
        )
        return ""
