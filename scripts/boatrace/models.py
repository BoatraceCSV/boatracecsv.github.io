"""Data models for boatrace entities."""

from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, date as date_type


@dataclass
class RacerFrame:
    """Racer frame data from program file."""

    entry_number: int
    registration_number: str
    racer_name: str
    age: int
    win_rate: float
    place_rate: float
    average_score: float
    motor_number: str
    motor_wins: int
    motor_2nd: int
    boat_number: str
    boat_wins: int
    boat_2nd: int
    weight: float
    adjustment: float

    # Racer profile information
    prefecture: Optional[str] = None  # 支部 (e.g., "福岡")
    class_grade: Optional[str] = None  # 級別 (e.g., "A1", "B1")

    # Local and boat/motor statistics
    local_win_rate: Optional[float] = None  # 当地勝率
    local_place_rate: Optional[float] = None  # 当地2連対率
    motor_2nd_rate: Optional[float] = None  # モーター2連対率
    boat_2nd_rate: Optional[float] = None  # ボート2連対率

    # Today's (session's) results - 6 days × 2 halves = 12 values
    # Format: 日1_1R, 日1_2R, 日2_1R, 日2_2R, 日3_1R, 日3_2R, 日4_1R, 日4_2R, 日5_1R, 日5_2R, 日6_1R, 日6_2R
    results_day1_race1: Optional[str] = None  # 1日目1R (今節成績_1-1)
    results_day1_race2: Optional[str] = None  # 1日目2R (今節成績_1-2)
    results_day2_race1: Optional[str] = None  # 2日目1R (今節成績_2-1)
    results_day2_race2: Optional[str] = None  # 2日目2R (今節成績_2-2)
    results_day3_race1: Optional[str] = None  # 3日目1R (今節成績_3-1)
    results_day3_race2: Optional[str] = None  # 3日目2R (今節成績_3-2)
    results_day4_race1: Optional[str] = None  # 4日目1R (今節成績_4-1)
    results_day4_race2: Optional[str] = None  # 4日目2R (今節成績_4-2)
    results_day5_race1: Optional[str] = None  # 5日目1R (今節成績_5-1)
    results_day5_race2: Optional[str] = None  # 5日目2R (今節成績_5-2)
    results_day6_race1: Optional[str] = None  # 6日目1R (今節成績_6-1)
    results_day6_race2: Optional[str] = None  # 6日目2R (今節成績_6-2)

    # Early indicator
    hayami: Optional[str] = None  # 早見

    # Additional legacy fields
    field_1: Optional[str] = None
    field_2: Optional[str] = None
    field_3: Optional[str] = None
    field_4: Optional[str] = None
    field_5: Optional[str] = None
    field_6: Optional[str] = None
    field_7: Optional[str] = None
    field_8: Optional[str] = None


@dataclass
class RacerResult:
    """Racer result data from result file."""

    number: int
    name: str
    weight: float
    result: int  # Finishing position (1-6)
    time: Optional[float] = None
    difference: Optional[float] = None
    disqualified: bool = False
    registration_number: Optional[str] = None
    motor_number: Optional[str] = None
    boat_number: Optional[str] = None
    showcase_time: Optional[float] = None
    entrance_course: Optional[int] = None
    start_timing: Optional[float] = None


@dataclass
class RaceResult:
    """Complete race result with all racer data."""

    date: str  # YYYY-MM-DD
    stadium: str
    race_round: str  # e.g., "01R"
    title: str
    race_code: Optional[str] = None

    # Race information (extracted from title or K-file)
    race_name: Optional[str] = None
    distance: Optional[str] = None
    day_of_session: Optional[str] = None
    weather: Optional[str] = None
    wind_direction: Optional[str] = None
    wind_speed: Optional[str] = None
    wave_height: Optional[str] = None
    winning_technique: Optional[str] = None

    # Betting results
    tansho: Optional[str] = None
    fukusho: Optional[str] = None
    wakren: Optional[str] = None
    fuku2: Optional[str] = None
    santan: Optional[str] = None
    sanfuku: Optional[str] = None
    santan_yosoku: Optional[str] = None
    sanfuku_yosoku: Optional[str] = None
    rentan: Optional[str] = None
    renfuku: Optional[str] = None
    rentan_yosoku: Optional[str] = None
    renfuku_yosoku: Optional[str] = None
    wide: Optional[str] = None
    wide_yosoku: Optional[str] = None
    trio: Optional[str] = None
    trio_yosoku: Optional[str] = None
    tiomate: Optional[str] = None

    # Racers (always 6)
    racers: List[RacerResult] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Check if race result is valid."""
        return (
            len(self.racers) == 6
            and all(1 <= r.result <= 6 for r in self.racers)
            and len(set(r.result for r in self.racers)) == 6  # All results unique
        )


@dataclass
class RaceProgram:
    """Race program with racer frame data."""

    date: str  # YYYY-MM-DD
    stadium: str
    race_round: str  # e.g., "01R"
    title: str
    day_of_session: Optional[str] = None  # e.g., "第1日"
    race_name: Optional[str] = None  # e.g., "予選"
    distance: Optional[str] = None  # e.g., "1800"
    post_time: Optional[str] = None  # e.g., "10:40"
    race_code: Optional[str] = None
    race_class: Optional[str] = None
    race_type: Optional[str] = None
    course_condition: Optional[str] = None
    weather: Optional[str] = None
    wind_direction: Optional[str] = None
    wind_speed: Optional[float] = None
    water_temperature: Optional[float] = None
    water_level: Optional[str] = None

    # Racer frames (always 6)
    racer_frames: List[RacerFrame] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Check if race program is valid."""
        return (
            len(self.racer_frames) == 6
            and all(frame.entry_number > 0 for frame in self.racer_frames)
        )


@dataclass
class PreviewBoatInfo:
    """Preview data for a single boat."""

    boat_number: int
    course_number: Optional[int] = None
    weight: Optional[float] = None
    weight_adjustment: Optional[float] = None
    exhibition_time: Optional[float] = None
    tilt_adjustment: Optional[float] = None
    start_timing: Optional[float] = None


@dataclass
class RacePreview:
    """Preview data for a single race (直前情報)."""

    date: str  # YYYY-MM-DD
    stadium: str
    race_round: str  # e.g., "01R"
    title: Optional[str] = None
    race_code: Optional[str] = None
    stadium_number: Optional[int] = None

    # Weather information
    wind_speed: Optional[float] = None
    wind_direction: Optional[int] = None
    wave_height: Optional[float] = None
    weather: Optional[int] = None
    air_temperature: Optional[float] = None
    water_temperature: Optional[float] = None

    # Boats (always 6)
    boats: List[PreviewBoatInfo] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Check if race preview is valid."""
        return len(self.boats) == 6


@dataclass
class OriginalExhibitionBoat:
    """Original exhibition data for a single boat (race.boatcast.jp)."""

    boat_number: int
    racer_name: Optional[str] = None
    # Up to 3 measurement values. The meaning of each column depends on
    # the stadium (see OriginalExhibitionData.measure_labels).
    value1: Optional[float] = None
    value2: Optional[float] = None
    value3: Optional[float] = None


@dataclass
class OriginalExhibitionData:
    """Original exhibition data (オリジナル展示データ) for a single race.

    Source: https://race.boatcast.jp/txt/{jo}/bc_oriten_{YYYYMMDD}_{jo}_{race}.txt
    """

    date: str  # YYYY-MM-DD
    stadium_number: int  # 1..24
    race_number: int  # 1..12
    race_code: str  # YYYYMMDDCCNN

    # Status field from line 2, column 1 of source TSV.
    # "1" = normal / measured, "2" = could not be measured,
    # "0" = measuring (previous race not finished), None = no data yet.
    status: Optional[str] = None

    # Number of measurement columns (from line 2, column 2 of source TSV).
    # 2 or 3 in practice.
    measure_count: Optional[int] = None

    # Measurement column labels (e.g., "一周", "まわり足", "直線").
    measure_labels: List[str] = field(default_factory=list)

    # Boats (always 6 when valid).
    boats: List[OriginalExhibitionBoat] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Check if the race data has all 6 boats."""
        return len(self.boats) == 6

    def is_measurable(self) -> bool:
        """Return False when the stadium could not measure this race."""
        return self.status not in ("2",)


@dataclass
class RaceCardSession:
    """One slot of 節間成績 (in-series race-by-race breakdown).

    A racer can race up to twice per day. ``bc_j_str3`` carries 7 day x 2 slot =
    14 quintuples (col[25]..col[38]). Each quintuple is encoded as
    ``"{R番号},{進入},{枠},{ST},{着順}"`` where:

    - Empty placeholder rows look like ``"-,-,-,-,-"`` (no race in that slot).
    - ``ST`` may have a leading dot (``".10"`` -> ``0.10``).
    - ``着順`` is a single-character token: full-width digit ``"１"-"６"`` for
      finishing position, or one of the special tokens ``"F"`` フライング /
      ``"L"`` 出遅れ / ``"欠"`` 欠場 / ``"転"`` 転覆 / ``"妨"`` 妨害失格 /
      ``"落"`` 落水. Stored as half-width string so consumers can compare with
      ``"1"-"6"`` directly.
    """

    race_number: Optional[int] = None  # R番号 (1..12)
    entry_course: Optional[int] = None  # 進入 (1..6)
    waku: Optional[int] = None  # 枠 (1..6)
    start_timing: Optional[float] = None  # ST (.10 -> 0.10; F flag is captured in finish_position)
    finish_position: Optional[str] = None  # 着順 ("1"-"6" / "F" / "L" / "欠" / "転" / "妨" / "落")


@dataclass
class RaceCardBoat:
    """One boat's row in bc_j_str3 (出走表詳細, parallel to programs)."""

    boat_number: int  # 1..6 (= line number after "data=" / header)

    # Identity
    registration_number: Optional[str] = None  # 登録番号
    racer_name: Optional[str] = None  # 選手名 (full-width spaces collapsed to single half-width)
    period: Optional[str] = None  # 期別 (e.g. "81期")
    branch: Optional[str] = None  # 支部 (e.g. "愛知")
    birthplace: Optional[str] = None  # 出身地
    age: Optional[int] = None
    grade: Optional[str] = None  # 級別 ("A1" / "A2" / "B1" / "B2")

    # Penalty / late counts
    f_count: Optional[int] = None  # F本数
    l_count: Optional[int] = None  # L本数

    # National stats (past 6 months excl. current series)
    national_avg_st: Optional[float] = None  # 全国平均ST
    national_win_rate: Optional[float] = None  # 全国勝率
    national_double_rate: Optional[float] = None  # 全国2連対率 (%)
    national_triple_rate: Optional[float] = None  # 全国3連対率 (%)

    # Local stats (past 3 years at this stadium)
    local_win_rate: Optional[float] = None  # 当地勝率
    local_double_rate: Optional[float] = None  # 当地2連対率
    local_triple_rate: Optional[float] = None  # 当地3連対率

    # Motor / boat
    motor_flag: Optional[int] = None  # モーターフラグ ("1" = special state)
    motor_number: Optional[int] = None  # 物理モーター番号 (from col[17])
    motor_double_rate: Optional[float] = None
    motor_triple_rate: Optional[float] = None
    boat_flag: Optional[int] = None
    # 物理ボート番号 (from col[21]). Renamed to avoid collision with the
    # ``boat_number`` slot/lane field above (1..6).
    boat_id: Optional[int] = None
    boat_double_rate: Optional[float] = None
    boat_triple_rate: Optional[float] = None

    # 早見 (other race number same day; blank when only one race)
    hayami: Optional[int] = None

    # 14 session slots (index 0 = day1 race1, 1 = day1 race2, ..., 13 = day7 race2)
    sessions: List[RaceCardSession] = field(default_factory=list)


@dataclass
class RaceCard:
    """Race card detail (出走表詳細) for one race, sourced from bc_j_str3."""

    date: str  # YYYY-MM-DD
    stadium_number: int  # 1..24
    race_number: int  # 1..12
    race_code: str  # YYYYMMDDCCNN

    # Header fields from line 2 of TSV: "{status}\t{ncols}".
    # status "1" = normal, "2" = race could not be held / data unavailable.
    status: Optional[str] = None
    ncols: Optional[int] = None  # second meta column (typically "6" = number of boats)

    # Always 6 boats when valid.
    boats: List[RaceCardBoat] = field(default_factory=list)

    def is_valid(self) -> bool:
        return len(self.boats) == 6


@dataclass
class RecentFormSession:
    """One ``節`` (race series) record of recent results.

    Used for both the national variant (``bc_zensou``) and the local variant
    (``bc_zensou_touchi``). Layout matches a contiguous 6-column block in the
    source TSV: ``開始日 / 終了日 / 場コード / 場名 / グレード / 着順列``.
    """

    start_date: Optional[str] = None  # 開始日 (YYYY-MM-DD)
    end_date: Optional[str] = None  # 終了日 (YYYY-MM-DD)
    stadium_code: Optional[str] = None  # 場コード (zero-padded "01"-"24")
    stadium_name: Optional[str] = None  # 場名 (full-width spaces collapsed)
    grade: Optional[str] = None  # グレード ("一般" / "ＧⅢ" / "ＧⅡ" / "ＧⅠ" / "ＳＧ" / etc.)
    # Raw 着順列 string. Tokens (single full-width char each):
    #   "１"-"６" 着順 / "F" フライング / "L" 出遅れ / "欠" 欠場 /
    #   "転" 転覆 / "妨" 妨害失格 / "落" 落水 / "[N]" 優勝戦N着 /
    #   "　" (full-width space) 日区切り
    # Trailing padding spaces are stripped.
    finish_sequence: Optional[str] = None


@dataclass
class RecentFormBoat:
    """One boat's recent-form data within a race.

    Identity fields (registration_number / racer_name) are filled from the
    matching ``bc_zensou`` (or ``bc_zensou_touchi``) row by registration
    number. Sessions are exactly 5 entries: index 0 = most recent (前1節),
    index 4 = oldest in the file (前5節). When the source has no row for the
    racer, identity fields and all sessions remain ``None``.
    """

    boat_number: int  # 1..6 (slot)
    registration_number: Optional[str] = None
    racer_name: Optional[str] = None
    sessions: List[RecentFormSession] = field(default_factory=list)


@dataclass
class RecentForm:
    """Recent-form data for one race (5 most recent 節, per boat).

    Two parallel files share this dataclass — the difference between the
    "national" and "local" variants is only in the underlying TSV
    (``bc_zensou`` vs ``bc_zensou_touchi``) and the meaning of the figures
    inside ``finish_sequence``. Aside from where the data was sourced, the
    schema is identical, so the same converter/serialiser can render both.
    """

    date: str  # YYYY-MM-DD
    stadium_number: int  # 1..24
    race_number: int  # 1..12
    race_code: str  # YYYYMMDDCCNN

    # Always 6 boats when valid.
    boats: List[RecentFormBoat] = field(default_factory=list)

    def is_valid(self) -> bool:
        return len(self.boats) == 6


@dataclass
class ConversionError:
    """Error during conversion process."""

    date: str
    error_type: str  # e.g., "download_failed", "parse_error", "write_error"
    message: str
    details: Optional[str] = None
    file_type: Optional[str] = None  # "K" or "B"
    timestamp: Optional[str] = None


@dataclass
class ConversionSession:
    """Session state tracking for fetch-and-convert operations."""

    start_date: str
    end_date: str
    mode: str = "daily"  # "daily" or "backfill"
    dry_run: bool = False
    force_overwrite: bool = False

    # Counters
    dates_processed: int = 0
    files_downloaded: int = 0
    files_decompressed: int = 0
    files_parsed: int = 0
    files_converted: int = 0
    csv_files_created: int = 0
    csv_files_skipped: int = 0

    # Preview counters
    previews_scraped: int = 0
    previews_failed: int = 0

    # Error tracking
    errors: List[ConversionError] = field(default_factory=list)

    # Git operations
    git_commit: Optional[str] = None
    git_push_success: bool = False

    # Timing
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def add_error(
        self,
        date: str,
        error_type: str,
        message: str,
        details: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> None:
        """Add error to session."""
        self.errors.append(
            ConversionError(
                date=date,
                error_type=error_type,
                message=message,
                details=details,
                file_type=file_type,
                timestamp=datetime.utcnow().isoformat() + "Z",
            )
        )

    def get_duration_seconds(self) -> Optional[float]:
        """Get session duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def exit_code(self) -> int:
        """Determine exit code based on session state.

        0 - Success: All files processed without critical errors
        1 - Partial failure: Some files succeeded, some failed
        2 - Critical error: Git operations failed
        3 - Configuration error: Invalid arguments
        """
        if self.git_push_success is False and self.csv_files_created > 0:
            return 2  # Git push failed after files were created

        if self.errors:
            # Check if any critical errors (git-related)
            critical_errors = [e for e in self.errors if e.error_type == "git_error"]
            if critical_errors:
                return 2

            # Otherwise partial failure
            return 1

        return 0  # Success

    def summary(self) -> str:
        """Generate summary of session."""
        lines = [
            f"✓ COMPLETED {'(dry-run - no files written)' if self.dry_run else 'SUCCESSFULLY'}",
            f"  - Dates processed: {self.dates_processed}",
            f"  - Files downloaded: {self.files_downloaded}",
            f"  - Files decompressed: {self.files_decompressed}",
            f"  - Files parsed: {self.files_parsed}",
            f"  - Files converted: {self.files_converted}",
            f"  - CSV files created: {self.csv_files_created}",
            f"  - CSV files skipped: {self.csv_files_skipped}",
        ]

        if self.git_commit:
            lines.append(f"  - Git commit: {self.git_commit}")

        if self.git_push_success:
            lines.append("  - Push status: SUCCESS")
        elif self.csv_files_created > 0:
            lines.append("  - Push status: FAILED")

        if self.errors:
            lines.append(f"  - Errors: {len(self.errors)}")

        duration = self.get_duration_seconds()
        if duration:
            lines.append(f"  - Duration: {duration:.1f}s")

        return "\n".join(lines)
