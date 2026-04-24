"""Unit tests for backfill-original-exhibition orchestration.

These tests mock out the network-dependent pieces (B-file download, parser,
scraper) and exercise the orchestration logic in `_process_one_date` and
`_iter_dates` directly.

The backfill script lives at scripts/backfill-original-exhibition.py. Python
module names cannot contain hyphens, so we load it via importlib.
"""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from boatrace.models import (
    OriginalExhibitionBoat,
    OriginalExhibitionData,
    RaceProgram,
)


def _load_backfill_module():
    """Load scripts/backfill-original-exhibition.py as a module."""
    script_path = (
        Path(__file__).parent.parent.parent / "backfill-original-exhibition.py"
    )
    spec = importlib.util.spec_from_file_location(
        "backfill_original_exhibition", script_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def bf():
    return _load_backfill_module()


# ---------------------------------------------------------------------------
# _iter_dates
# ---------------------------------------------------------------------------


def test_iter_dates_single_day(bf):
    assert list(bf._iter_dates("2024-03-11", "2024-03-11")) == ["2024-03-11"]


def test_iter_dates_inclusive_range(bf):
    dates = list(bf._iter_dates("2024-03-10", "2024-03-12"))
    assert dates == ["2024-03-10", "2024-03-11", "2024-03-12"]


def test_iter_dates_end_before_start(bf):
    assert list(bf._iter_dates("2024-03-12", "2024-03-10")) == []


def test_validate_date_accepts_valid(bf):
    assert bf._validate_date("2024-03-11") is True


def test_validate_date_rejects_invalid(bf):
    assert bf._validate_date("2024-13-45") is False
    assert bf._validate_date("not-a-date") is False


# ---------------------------------------------------------------------------
# CSV path resolution
# ---------------------------------------------------------------------------


def test_csv_path_for_maps_date_correctly(bf, tmp_path):
    path = bf._csv_path_for(tmp_path, "2024-03-11")
    assert path == tmp_path / "data" / "original_exhibition" / "2024" / "03" / "11.csv"


# ---------------------------------------------------------------------------
# _process_one_date — skip path when CSV already exists
# ---------------------------------------------------------------------------


def test_process_one_date_skips_existing_csv(bf, tmp_path):
    # Pre-create the CSV
    target = tmp_path / "data" / "original_exhibition" / "2024" / "03" / "11.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("existing\n", encoding="utf-8")

    scraper = MagicMock()
    rate_limiter = MagicMock()

    stats = bf._process_one_date(
        "2024-03-11",
        scraper,
        config={},
        rate_limiter=rate_limiter,
        project_root=tmp_path,
        force_overwrite=False,
        dry_run=False,
    )

    assert stats["skipped_existing"] is True
    assert stats["csv_written"] is False
    # Scraper must not have been called.
    scraper.scrape_race.assert_not_called()


# ---------------------------------------------------------------------------
# _process_one_date — happy path with mocked B-file and scraper
# ---------------------------------------------------------------------------


def _fake_program(stadium: str, race_round: str) -> RaceProgram:
    """Create a minimal RaceProgram suitable for the backfill loop."""
    # RaceProgram has required fields. Use getattr defaults where possible.
    prog = RaceProgram.__new__(RaceProgram)
    # Fill in only the fields backfill uses.
    prog.stadium = stadium
    prog.race_round = race_round
    prog.date = "2024-03-11"
    prog.title = ""
    prog.racers_frames = []
    return prog


def _fake_data(stadium_number: int, race_number: int) -> OriginalExhibitionData:
    return OriginalExhibitionData(
        date="2024-03-11",
        stadium_number=stadium_number,
        race_number=race_number,
        race_code=f"20240311{stadium_number:02d}{race_number:02d}",
        status="1",
        measure_count=3,
        measure_labels=["一周", "まわり足", "直線"],
        boats=[
            OriginalExhibitionBoat(
                boat_number=i,
                racer_name=f"選手{i}",
                value1=36.0,
                value2=5.0,
                value3=7.0,
            )
            for i in range(1, 7)
        ],
    )


def test_process_one_date_writes_csv_on_happy_path(bf, tmp_path):
    # Two stadiums × 2 races.
    programs = [
        _fake_program("ボートレース宮島", "01R"),
        _fake_program("ボートレース宮島", "02R"),
        _fake_program("ボートレース大村", "01R"),
    ]

    scraper = MagicMock()
    # Map (stadium, race) → canned data.
    def _scrape(date, stadium, race):
        return _fake_data(stadium, race)

    scraper.scrape_race.side_effect = _scrape

    with patch.object(
        bf, "download_file", return_value=(b"fake-lzh-bytes", 200)
    ), patch.object(bf, "extract_b_file", return_value="fake-b-text"), patch.object(
        bf, "parse_program_file", return_value=programs
    ):
        stats = bf._process_one_date(
            "2024-03-11",
            scraper,
            config={"max_retries": 1},
            rate_limiter=MagicMock(),
            project_root=tmp_path,
            force_overwrite=False,
            dry_run=False,
        )

    assert stats["skipped_existing"] is False
    assert stats["no_races"] is False
    assert stats["error"] is None
    assert stats["races_scraped"] == 3
    assert stats["csv_written"] is True
    assert stats["csv_row_count"] == 3

    csv_path = tmp_path / "data" / "original_exhibition" / "2024" / "03" / "11.csv"
    assert csv_path.exists()
    text = csv_path.read_text(encoding="utf-8")
    assert text.startswith("レースコード,")
    # One header row + 3 data rows.
    assert len(text.strip().splitlines()) == 4


def test_process_one_date_marks_no_races_when_b_file_missing(bf, tmp_path):
    scraper = MagicMock()

    with patch.object(bf, "download_file", return_value=(None, 404)):
        stats = bf._process_one_date(
            "2024-03-11",
            scraper,
            config={"max_retries": 1},
            rate_limiter=MagicMock(),
            project_root=tmp_path,
            force_overwrite=False,
            dry_run=False,
        )

    assert stats["no_races"] is True
    assert stats["csv_written"] is False
    scraper.scrape_race.assert_not_called()


def test_process_one_date_dry_run_does_not_write(bf, tmp_path):
    programs = [_fake_program("ボートレース宮島", "01R")]
    scraper = MagicMock()
    scraper.scrape_race.side_effect = lambda d, s, r: _fake_data(s, r)

    with patch.object(
        bf, "download_file", return_value=(b"fake-lzh-bytes", 200)
    ), patch.object(bf, "extract_b_file", return_value="fake-b-text"), patch.object(
        bf, "parse_program_file", return_value=programs
    ):
        stats = bf._process_one_date(
            "2024-03-11",
            scraper,
            config={"max_retries": 1},
            rate_limiter=MagicMock(),
            project_root=tmp_path,
            force_overwrite=False,
            dry_run=True,
        )

    assert stats["csv_written"] is True  # reported as written in dry-run mode
    assert stats["csv_row_count"] == 1
    csv_path = tmp_path / "data" / "original_exhibition" / "2024" / "03" / "11.csv"
    # But the file must not actually be on disk.
    assert not csv_path.exists()


def test_process_one_date_counts_not_measurable_races(bf, tmp_path):
    programs = [_fake_program("ボートレース宮島", "01R")]

    not_measurable = OriginalExhibitionData(
        date="2024-03-11",
        stadium_number=17,
        race_number=1,
        race_code="202403111701",
        status="2",  # not measurable
        measure_count=3,
        measure_labels=["一周", "まわり足", "直線"],
        boats=[],
    )

    scraper = MagicMock()
    scraper.scrape_race.return_value = not_measurable

    with patch.object(
        bf, "download_file", return_value=(b"fake-lzh-bytes", 200)
    ), patch.object(bf, "extract_b_file", return_value="fake-b-text"), patch.object(
        bf, "parse_program_file", return_value=programs
    ):
        stats = bf._process_one_date(
            "2024-03-11",
            scraper,
            config={"max_retries": 1},
            rate_limiter=MagicMock(),
            project_root=tmp_path,
            force_overwrite=False,
            dry_run=False,
        )

    assert stats["races_scraped"] == 1
    assert stats["races_not_measurable"] == 1
    assert stats["csv_written"] is True


def test_process_one_date_counts_missing_races(bf, tmp_path):
    programs = [
        _fake_program("ボートレース宮島", "01R"),
        _fake_program("ボートレース宮島", "02R"),
    ]

    scraper = MagicMock()
    # First race: ok; second race: returns None (no file).
    scraper.scrape_race.side_effect = [_fake_data(17, 1), None]

    with patch.object(
        bf, "download_file", return_value=(b"fake-lzh-bytes", 200)
    ), patch.object(bf, "extract_b_file", return_value="fake-b-text"), patch.object(
        bf, "parse_program_file", return_value=programs
    ):
        stats = bf._process_one_date(
            "2024-03-11",
            scraper,
            config={"max_retries": 1},
            rate_limiter=MagicMock(),
            project_root=tmp_path,
            force_overwrite=False,
            dry_run=False,
        )

    assert stats["races_scraped"] == 1
    assert stats["races_missing"] == 1
    assert stats["csv_written"] is True
