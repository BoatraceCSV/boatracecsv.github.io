# Data Model Design

**Phase**: 1 (Design & Contracts)
**Date**: 2026-01-01

## Entity: RaceResult

**Source**: K-file (results) from official boatrace server
**Output**: CSV with 91 columns

### Core Fields

```python
@dataclass
class RaceResult:
    # Race Identifiers (13 fields)
    race_code: str              # YYYYMMDDJJRR (race code for lookup)
    title: str                  # e.g., "全日本王者決定戦"
    day: str                    # e.g., "第1日" (ordinal day)
    race_date: str              # YYYY/MM/DD (Japanese format)
    stadium: str                # e.g., "唐津" (location)
    race_round: str             # "01R" - "12R" (race number)
    race_name: str              # e.g., "予選" (race type)
    distance_m: int             # 1800, 1650, etc.
    weather: str                # "晴", "曇り", "雨"
    wind_direction: str         # "南", "北", "北東", etc.
    wind_speed_ms: str          # e.g., "3" (m/s)
    wave_height_cm: str         # e.g., "3" (cm)
    winning_technique: str      # e.g., "逃げ", "差し", "まくり"

    # Betting Results (26 fields) - optional (may be missing for invalid races)
    win_boat_num: Optional[str]         # 単勝_艇番
    win_payout: Optional[str]           # 単勝_払戻金
    place_1st_boat: Optional[str]       # 複勝_1着_艇番
    place_1st_payout: Optional[str]     # 複勝_1着_払戻金
    place_2nd_boat: Optional[str]       # 複勝_2着_艇番
    place_2nd_payout: Optional[str]     # 複勝_2着_払戻金
    exacta_combo: Optional[str]         # 2連単_組番
    exacta_payout: Optional[str]        # 2連単_払戻金
    exacta_popularity: Optional[str]    # 2連単_人気
    quinella_combo: Optional[str]       # 2連複_組番
    quinella_payout: Optional[str]      # 2連複_払戻金
    quinella_popularity: Optional[str]  # 2連複_人気
    quinella_place_1_2: Optional[str]   # 拡連複_1-2着
    quinella_place_1_3: Optional[str]   # 拡連複_1-3着
    quinella_place_2_3: Optional[str]   # 拡連複_2-3着
    trifecta_combo: Optional[str]       # 3連単_組番
    trifecta_payout: Optional[str]      # 3連単_払戻金
    trifecta_popularity: Optional[str]  # 3連単_人気
    trio_combo: Optional[str]           # 3連複_組番
    trio_payout: Optional[str]          # 3連複_払戻金
    trio_popularity: Optional[str]      # 3連複_人気

    # Racer Results (52 fields) - 6 boats × 8 fields + boat numbers
    racers: List[RacerResult]   # See RacerResult entity below

    # Validation
    is_invalid: bool = False    # Flag if race marked "不成立" (no results)

@dataclass
class RacerResult:
    finish_position: str        # "1着", "2着", ..., "6着"
    boat_number: str            # 1-6
    registration_number: str    # e.g., "5191"
    name: str                   # Racer name
    motor_number: str           # Motor assignment
    boat_number_assign: str     # Boat assignment
    display_time: str           # Exhibition time (展示タイム)
    entrance_course: str        # Lane assignment (進入コース)
    start_timing: str           # Start timing (スタートタイミング)
    race_time: str              # Final race time
```

### Validation Rules

- `race_date` must be in format YYYY/MM/DD
- `race_round` must be 01R-12R (2 digits + R)
- `distance_m` typically 1650, 1800, 1950, 2000
- `weather` one of: 晴, 曇り, 雨, 雷, 雪, etc.
- At least one racer must have finish_position (otherwise mark is_invalid=true)
- All 6 racers must be present in output (some may have empty fields)

---

## Entity: RaceProgram

**Source**: B-file (program/schedule) from official boatrace server
**Output**: CSV with 218 columns

### Core Fields

```python
@dataclass
class RaceProgram:
    # Race Info (8 fields)
    title: str                  # e.g., "全日本王者決定戦"
    day: str                    # e.g., "第1日"
    race_date: str              # YYYY年MM月DD日 (Japanese format)
    stadium: str                # e.g., "唐津"
    race_round: str             # "01R" - "12R"
    race_name: str              # e.g., "予選"
    distance_m: int             # 1800, 1650, etc.
    voting_deadline: str        # Time in HH:MM format

    # Racer Frames (35 fields each, 6 frames total)
    frames: List[RacerFrame]    # 6 entries, one per boat position
```

### RacerFrame Entity

```python
@dataclass
class RacerFrame:
    frame_number: int           # 1-6 (entry position)
    boat_number: str            # 1-6
    registration_number: str    # e.g., "5191"
    name: str                   # Racer name
    age: str                    # Age (年齢)
    branch: str                 # Branch (支部)
    weight: str                 # Weight (体重, kg)
    rank: str                   # Rank (級別): A1, A2, B1
    national_win_rate: str      # 全国勝率
    national_place_rate: str    # 全国2連対率
    local_win_rate: str         # 当地勝率
    local_place_rate: str       # 当地2連対率
    motor_number: str           # Motor assignment
    motor_place_rate: str       # モーター2連対率
    boat_number_assign: str     # Boat assignment
    boat_place_rate: str        # ボート2連対率

    # Race Record This Tournament (12 fields)
    # Format: race#_position format (e.g., "3" = 3rd, blank = not run)
    race_1_1: str               # Race 1, attempt 1 position (or blank)
    race_1_2: str               # Race 1, attempt 2 position (or blank)
    race_2_1: str               # Race 2, attempt 1 position
    race_2_2: str               # Race 2, attempt 2 position
    race_3_1: str               # Race 3, attempt 1 position
    race_3_2: str               # Race 3, attempt 2 position
    race_4_1: str               # Race 4, attempt 1 position
    race_4_2: str               # Race 4, attempt 2 position
    race_5_1: str               # Race 5, attempt 1 position
    race_5_2: str               # Race 5, attempt 2 position
    race_6_1: str               # Race 6, attempt 1 position
    race_6_2: str               # Race 6, attempt 2 position

    # Quick Reference
    early_prediction: str       # 早見 (prediction/odds indicator)
```

### Validation Rules

- `race_date` must be in format YYYY年MM月DD日
- All 6 frames must be present in output
- `rank` must be one of: A1, A2, B1, B2 (or empty if data missing)
- Win/place rates are decimals (e.g., "6.89", blank if new racer)
- Race record positions are single digits 1-6 or blank

---

## Entity: ConversionSession

**Purpose**: Track state during a conversion operation
**Scope**: Ephemeral (created for each workflow run)

```python
@dataclass
class ConversionSession:
    session_id: str             # UUID for this execution
    start_date: date            # Start of date range
    end_date: date              # End of date range
    mode: str                   # "daily" or "backfill"

    # Statistics
    total_dates: int = 0
    files_downloaded: int = 0
    files_decompressed: int = 0
    files_parsed: int = 0
    files_converted: int = 0
    files_skipped: int = 0      # Already exist
    files_failed: int = 0       # Parse/conversion error

    # Error tracking
    errors: List[ConversionError] = field(default_factory=list)

    # Git operations
    csv_files_created: List[str] = field(default_factory=list)
    git_commit_hash: Optional[str] = None
    git_push_status: str = "pending"  # pending, success, failed

@dataclass
class ConversionError:
    date: date
    file_type: str              # "K" or "B"
    stage: str                  # "download", "decompress", "parse", "convert", "write"
    error_message: str
    timestamp: datetime
    retry_count: int = 0
```

---

## Relationships

```
ConversionSession
├── RaceResult (multiple per date if multiple stadiums)
│   └── RacerResult (6 per race)
├── RaceProgram (multiple per date if multiple stadiums)
│   └── RacerFrame (6 per program)
└── ConversionError (0 or more)
```

---

## Storage Strategy

**In-Memory**: RaceResult, RaceProgram, RacerResult, RacerFrame (no persistence needed - generated per run)

**File-Based**:
- Results CSV: `data/results/YYYY/MM/DD.csv`
- Program CSV: `data/programs/YYYY/MM/DD.csv`
- Error logs: `logs/boatrace-YYYY-MM-DD.json`

**Git**:
- Committed CSV files serve as permanent archive
- Repository history is the historical record

---

## State Transitions

### RaceResult Lifecycle
```
Pending → Downloaded → Decompressed → Parsed → Converted → Written → Committed
                                                              ↓
                                                          (skip if exists)
```

### RaceProgram Lifecycle
```
Pending → Downloaded → Decompressed → Parsed → Converted → Written → Committed
                                                              ↓
                                                          (skip if exists)
```

### ConversionSession Lifecycle
```
Created → Running → Completed (with summary)
                         ↓
                    Git Operations
                         ↓
                    Pushed/Failed
```
