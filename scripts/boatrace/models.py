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
class TokutenHayamiRacer:
    """One racer's row of 得点率早見 (bc_j_tokuten_hayami)."""

    boat_number: int  # 1..6
    class_grade: Optional[str] = None  # A1 / A2 / B1 / B2
    registration_number: Optional[str] = None
    racer_name: Optional[str] = None

    # Border flag from col[4] ("00" / "01"). The SPA highlights 得点率 / 順位
    # when the last digit is "1", i.e. the racer is at or above the border
    # rank (see ``TokutenHayamiData.border_rank``).
    border_status: Optional[str] = None

    # col[5]. Normally a numeric score rate ("6.33"), but the source puts a
    # status word here for racers with no score: 賞除 / 欠場 / 帰郷 / 追配.
    score_rate: Optional[str] = None

    # col[6]. Rank within the series (1 = best).
    rank: Optional[str] = None

    # col[20]. The racer's *other* race number today ("7" = 7R), blank when
    # they only race once. Same semantics as 早見 in bc_j_str3.
    other_race_number: Optional[str] = None

    # col[7..18] as 6 pairs, index 0 = 1着 .. index 5 = 6着.
    # ``if_rank_score_rates[k]``: the score rate this racer would end up with
    # if they finish (k+1)th in this race.
    # ``if_rank_statuses[k]``: the source's colour code for that cell.
    # Observed bit semantics (from the SPA's class mapping):
    #   1 = 得点率がボーダー以上 / 2 = 次レースの結果次第でボーダー以上の可能性
    #   4 = 当レース終了時点でボーダー以上
    if_rank_score_rates: List[Optional[str]] = field(default_factory=list)
    if_rank_statuses: List[Optional[str]] = field(default_factory=list)


@dataclass
class TokutenHayamiData:
    """得点率早見 (score-rate quick reference) for a single race.

    Source: https://race.boatcast.jp/hp_txt/{jo}/bc_j_tokuten_hayami_{YYYYMMDD}_{jo}_{race}.txt
    """

    date: str  # YYYY-MM-DD
    stadium_number: int  # 1..24
    race_number: int  # 1..12
    race_code: str  # YYYYMMDDCCNN

    # Line 2 of the source. "1" = ready. Anything else means the table is not
    # published for this race yet (the SPA shows a "please wait" notice).
    status: Optional[str] = None

    # Placement points for *this* race, index 0 = 1着 .. index 5 = 6着.
    # 予選 is 10/8/6/4/2/1; 準優・特別レース shift up by 1 (11/9/7/5/3/2).
    rank_points: List[Optional[str]] = field(default_factory=list)

    # Border rank of the series (e.g. 18 = top 18 advance to 準優勝戦).
    # Taken from the trailing line of the source; cross-checked against
    # ``TokutenHayamiRacer.border_status``.
    border_rank: Optional[str] = None

    racers: List[TokutenHayamiRacer] = field(default_factory=list)

    def is_valid(self) -> bool:
        """Check if the race data has all 6 racers."""
        return len(self.racers) == 6

    def is_ready(self) -> bool:
        """Return True when the source says the table is published."""
        return self.status == "1"


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

    # 賞除 (prize-money excluded flag from col[6]). Raw source value
    # (typically '賞除' when set, blank otherwise). Rare: ~0.8% of boats.
    prize_excluded: Optional[str] = None

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
class Waku10Run:
    """One past race in the 枠番別過去10走 (bc_j_waku10) breakdown.

    Each run is a triplet in the source TSV: ``着順 / 進入コース / グレード``.
    進入コース is only present when the racer entered from a course other
    than the 枠 (the SPA legend: 「コース番号(番号非表示は枠通り)」) —
    an empty value therefore means 枠なり進入.
    """

    finish_position: Optional[str] = None  # 着順 ("1"-"6" / "F" / "L" / "欠" / "落" / "沈" / "転" / "不" / "エ" / "失" / "妨")
    entry_course: Optional[int] = None  # 進入コース (None = 枠なり)
    grade: Optional[str] = None  # グレード ("IP" = 一般 / "G1" / "G2" / "G3" / "SG")


@dataclass
class Waku10Boat:
    """One boat's row in bc_j_waku10 (枠番別過去10走).

    The three summary figures are 枠番別 (per-waku) aggregates as rendered
    by the SPA's 枠番別データ block: 勝率 / 平均ST / スタート順.
    ``runs`` is exactly 10 entries when valid, index 0 = 前走 (most
    recent), index 9 = 10走前 — the source TSV is newest-first.
    """

    boat_number: int  # 1..6 (= line number after the meta lines)
    racer_name: Optional[str] = None  # 選手名 (full-width spaces collapsed)
    win_rate: Optional[float] = None  # 枠番別勝率
    avg_st: Optional[float] = None  # 枠番別平均ST
    avg_start_order: Optional[float] = None  # 枠番別平均スタート順
    runs: List[Waku10Run] = field(default_factory=list)


@dataclass
class Waku10Card:
    """枠番別過去10走 data for one race, sourced from bc_j_waku10."""

    date: str  # YYYY-MM-DD
    stadium_number: int  # 1..24
    race_number: int  # 1..12
    race_code: str  # YYYYMMDDCCNN

    # Meta line: "{status}\t{ncols}" (same shape as bc_j_str3).
    status: Optional[str] = None

    # Always 6 boats when valid.
    boats: List[Waku10Boat] = field(default_factory=list)

    def is_valid(self) -> bool:
        return len(self.boats) == 6


@dataclass
class MotorHistoryEntry:
    """One (motor, 節) usage record from ``bc_mrireki`` (モーター履歴).

    A ``bc_mrireki_{節終了日}_{jo}`` file is a static snapshot taken after
    the 節 ending on ``session_end_key``: for every motor at the stadium it
    lists the last ~3 節 with the racer who used the motor and the raw
    finish sequence (same token conventions as
    :class:`RecentFormSession.finish_sequence`).
    """

    stadium_code: str  # "01".."24" (zero-padded)
    session_end_key: str  # 基準節終了日 (YYYY-MM-DD) — the file's key
    motor_number: Optional[int] = None  # モーター番号
    start_date: Optional[str] = None  # 使用節の開始日 (YYYY-MM-DD)
    end_date: Optional[str] = None  # 使用節の終了日 (YYYY-MM-DD)
    grade: Optional[str] = None  # グレード ("一般" / "ＧⅢ" / "ＧⅡ" / "ＧⅠ" / "ＳＧ")
    title: Optional[str] = None  # 開催タイトル
    racer_name: Optional[str] = None  # 使用者 (full-width spaces collapsed)
    # Raw 着順列 (trailing full-width padding stripped). Tokens: "１"-"６"
    # 着順 / "Ｆ" / "Ｌ" / "欠" / "転" / "妨" / "落" / "[N]" 優勝戦N着 /
    # "　" 日区切り。
    finish_sequence: Optional[str] = None


@dataclass
class ScheduleEntry:
    """One 節 (race series) in a stadium's monthly schedule (bc_mon_2)."""

    stadium_code: str  # "01".."24" (zero-padded)
    start_date: Optional[str] = None  # 節開始日 (YYYY-MM-DD)
    end_date: Optional[str] = None  # 節終了日 (YYYY-MM-DD)
    grade: Optional[str] = None  # グレード ("IP" = 一般 / "G1" / "G2" / "G3" / "SG")
    title: Optional[str] = None  # 開催タイトル
    races: Optional[str] = None  # 1日のレース数 (e.g. "12R")


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
class MotorStat:
    """Per-motor period statistics from race.boatcast.jp's ``bc_mdc``.

    Schema reflects the empirical decoding of the 33-column TSV row:

    * Confidence ★★★ (verified by JS or distribution): col[0,1,2,17,19] →
      named fields. Plus the rate/rank pairs col[3..14] and col[18,20]
      verified via "rank-1 motor has max value" check across 5 stadiums.
    * Confidence ★★★ (verified by mathematical identity across 2,772 rows):
      col[15] = ``連対外回数`` (non-place finishes), col[16] = ``出走数``
      (total starts). Identity ``col[16] == col[9] + col[11] + col[13] +
      col[15]`` holds in 100% of observed rows.
    * Confidence ★★ (strong situational evidence): col[23,24] (avg lap),
      col[25,32] (dates), col[26..31] (maintenance category counts).
    * Confidence ★ (hypothesis only): col[21,22] kept as raw fields
      without semantic naming.

    See README's *Motor Stats* section for the full provenance table.
    """

    record_date: str  # YYYY-MM-DD (snapshot date = B-file fetch date)
    motor_period_start: Optional[str] = None  # YYYY-MM-DD (col[0])
    stadium_code: Optional[str] = None  # "01"-"24" (col[1])
    motor_number: Optional[int] = None  # col[2]

    # Rate / rank pairs (col[3..8]).
    win_rate: Optional[float] = None  # col[3] / 100
    win_rate_rank: Optional[int] = None  # col[4]
    double_rate: Optional[float] = None  # col[5] / 100, percent
    double_rate_rank: Optional[int] = None  # col[6]
    triple_rate: Optional[float] = None  # col[7] / 100, percent
    triple_rate_rank: Optional[int] = None  # col[8]

    # Finish counts + ranks (col[9..14]).
    first_count: Optional[int] = None  # col[9]
    first_rank: Optional[int] = None  # col[10]
    second_count: Optional[int] = None  # col[11]
    second_rank: Optional[int] = None  # col[12]
    third_count: Optional[int] = None  # col[13]
    third_rank: Optional[int] = None  # col[14]

    # Confidence ★★★ — verified via identity ``out_of_place_count + 1着 +
    # 2着 + 3着 == start_count`` across all 2,772 historical rows.
    out_of_place_count: Optional[int] = None  # col[15] — 連対外回数 (4着以下+DNF合計)
    start_count: Optional[int] = None  # col[16] — 出走数

    # 優勝・優出 (col[17..20]).
    championship_count: Optional[int] = None  # col[17]
    championship_rank: Optional[int] = None  # col[18]
    final_count: Optional[int] = None  # col[19] - 優出 (made grand final)
    final_rank: Optional[int] = None  # col[20]

    # Confidence ★ — kept raw (col[21,22]).
    raw_col_21: Optional[int] = None
    raw_col_22: Optional[int] = None

    # 平均ラップ + rank (col[23,24]).
    avg_lap_seconds: Optional[float] = None  # col[23] / 100
    avg_lap_rank: Optional[int] = None  # col[24]

    # Dates (col[25] / col[32]).
    first_use_date: Optional[str] = None  # YYYY-MM-DD
    last_maintenance_date: Optional[str] = None  # YYYY-MM-DD

    # Maintenance counts by category (col[26..31]). Category names are
    # currently unknown — typical boatrace categories are piston / ring /
    # cylinder / lower / carburetor / other, but we have not confirmed
    # the mapping for boatcast's specific encoding.
    maintenance_type1_count: Optional[int] = None  # col[26]
    maintenance_type2_count: Optional[int] = None  # col[27]
    maintenance_type3_count: Optional[int] = None  # col[28]
    maintenance_type4_count: Optional[int] = None  # col[29]
    maintenance_type5_count: Optional[int] = None  # col[30]
    maintenance_type6_count: Optional[int] = None  # col[31]
