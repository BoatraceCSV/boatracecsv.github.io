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

    # Additional frame fields (35 total per racer)
    field_1: Optional[str] = None
    field_2: Optional[str] = None
    field_3: Optional[str] = None
    field_4: Optional[str] = None
    field_5: Optional[str] = None
    field_6: Optional[str] = None
    field_7: Optional[str] = None
    field_8: Optional[str] = None
    field_9: Optional[str] = None
    field_10: Optional[str] = None
    field_11: Optional[str] = None
    field_12: Optional[str] = None
    field_13: Optional[str] = None
    field_14: Optional[str] = None
    field_15: Optional[str] = None
    field_16: Optional[str] = None
    field_17: Optional[str] = None
    field_18: Optional[str] = None
    field_19: Optional[str] = None
    field_20: Optional[str] = None


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


@dataclass
class RaceResult:
    """Complete race result with all racer data."""

    date: str  # YYYY-MM-DD
    stadium: str
    race_round: str  # e.g., "01R"
    title: str
    race_code: Optional[str] = None

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
