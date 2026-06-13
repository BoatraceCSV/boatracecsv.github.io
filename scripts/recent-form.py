#!/usr/bin/env python3
"""Scrape recent-form data (全国・当地近況成績) from race.boatcast.jp.

For a given date, this script:
  1. Resolves the day's open (stadium, race) list from boatcast.jp's
     ``getHoldingList2`` JSON API (B-file 不要)。
  2. Reads the boat ↔ registration-number mapping from the
     ``race_cards`` CSV that ``race-card.py`` writes earlier in the
     daily-sync pipeline.
  3. For each open stadium, fetches both ``bc_zensou`` (全国近況5節) and
     ``bc_zensou_touchi`` (当地近況5節) from race.boatcast.jp once each.
  4. Joins by registration number to produce per-race, per-boat
     :class:`RecentForm` objects.
  5. Writes two CSV files for the date:
       - ``data/programs/recent_national/YYYY/MM/DD.csv``
       - ``data/programs/recent_local/YYYY/MM/DD.csv``

Both files follow the same shape (196 columns: 4 meta + 6 boats × 32),
so the converter is shared between variants.
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.converter import recent_forms_to_csv
from boatrace.downloader import RateLimiter
from boatrace.holding_list import (
    HoldingListError,
    fetch_holding_list,
    load_holding_from_title_csv,
)
from boatrace.models import RecentForm, RecentFormBoat, RecentFormSession
from boatrace.recent_form_scraper import RecentFormScraper, RecentFormRow
from boatrace.storage import write_csv


OUTPUT_DIR_NATIONAL = "data/programs/recent_national"
OUTPUT_DIR_LOCAL = "data/programs/recent_local"
RACE_CARDS_DIR = "data/programs/race_cards"


def _race_code(date_str: str, stadium_code: int, race_number: int) -> str:
    return f"{date_str.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _load_boat_to_reg_map(
    project_root: Path,
    date_str: str,
) -> Dict[Tuple[int, int], Dict[int, str]]:
    """Read race_cards CSV and return ``{(stadium, race): {boat: reg}}``.

    Empty dict when the file does not exist yet (e.g. if ``race-card.py``
    has not been run for this date).
    """
    year, month, day = date_str.split("-")
    path = (
        project_root
        / RACE_CARDS_DIR
        / year
        / month
        / f"{day}.csv"
    )
    if not path.exists():
        logging_module.warning(
            "recent_form_race_cards_csv_missing",
            date=date_str,
            path=str(path),
        )
        return {}

    mapping: Dict[Tuple[int, int], Dict[int, str]] = {}
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    stadium_code = int((row.get("レース場コード") or "").strip())
                except (TypeError, ValueError):
                    continue
                if stadium_code < 1 or stadium_code > 24:
                    continue
                race_str = (row.get("レース回") or "").strip().rstrip("R")
                try:
                    race_number = int(race_str)
                except ValueError:
                    continue
                if not (1 <= race_number <= 12):
                    continue
                boat_to_reg: Dict[int, str] = {}
                for slot in range(1, 7):
                    reg = (row.get(f"艇{slot}_登録番号") or "").strip()
                    if reg:
                        boat_to_reg[slot] = reg
                if boat_to_reg:
                    mapping[(stadium_code, race_number)] = boat_to_reg
    except OSError as exc:
        logging_module.warning(
            "recent_form_race_cards_csv_read_error",
            path=str(path),
            error=str(exc),
        )
        return {}

    logging_module.info(
        "recent_form_race_cards_csv_loaded",
        date=date_str,
        path=str(path),
        races=len(mapping),
    )
    return mapping


def _collect_race_inputs(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
) -> Dict[int, List[Tuple[int, Dict[int, str]]]]:
    """Return ``{stadium_code: [(race_number, {boat: reg}), ...]}``.

    Combines (stadium, race) list from the holding-list API/title CSV
    with the boat ↔ registration mapping that ``race-card.py`` already
    wrote into ``data/programs/race_cards/YYYY/MM/DD.csv``.
    """
    project_root = Path(__file__).parent.parent

    try:
        races = fetch_holding_list(date_str, rate_limiter=rate_limiter)
    except HoldingListError as exc:
        logging_module.warning(
            "recent_form_holding_list_fallback",
            date=date_str,
            error=str(exc),
        )
        races = load_holding_from_title_csv(project_root, date_str)

    if not races:
        logging_module.warning(
            "recent_form_holding_list_empty", date=date_str
        )
        return {}

    boat_map = _load_boat_to_reg_map(project_root, date_str)

    races_by_stadium: Dict[int, List[Tuple[int, Dict[int, str]]]] = {}
    for r in races:
        if not r.is_open:
            continue
        if not (1 <= r.race_number <= 12):
            continue
        boat_to_reg = boat_map.get((r.stadium_code, r.race_number), {})
        races_by_stadium.setdefault(r.stadium_code, []).append(
            (r.race_number, boat_to_reg)
        )

    return races_by_stadium


def _build_recent_forms(
    date_str: str,
    stadium_code: int,
    race_number: int,
    boat_to_reg: Dict[int, str],
    racer_index: Dict[str, RecentFormRow],
) -> RecentForm:
    """Produce a :class:`RecentForm` for one race by joining B-file mapping
    with the per-stadium-day racer index.
    """
    form = RecentForm(
        date=date_str,
        stadium_number=stadium_code,
        race_number=race_number,
        race_code=_race_code(date_str, stadium_code, race_number),
    )
    for slot in range(1, 7):
        reg = boat_to_reg.get(slot)
        if not reg:
            form.boats.append(RecentFormBoat(boat_number=slot))
            continue
        row = racer_index.get(reg)
        if row is None:
            form.boats.append(
                RecentFormBoat(boat_number=slot, registration_number=reg)
            )
            continue
        racer_name, sessions = row
        form.boats.append(
            RecentFormBoat(
                boat_number=slot,
                registration_number=reg,
                racer_name=racer_name,
                sessions=list(sessions),
            )
        )
    return form


def _write_variant_csv(
    forms: List[RecentForm],
    variant: str,
    output_dir: str,
    date_str: str,
    force_overwrite: bool,
    dry_run: bool,
    stats: dict,
) -> Optional[str]:
    """Write the CSV file for one variant; return the relative path on success."""
    if not forms:
        return None
    csv_content = recent_forms_to_csv(forms, variant=variant)
    if not csv_content:
        return None

    year, month, day = date_str.split("-")
    rel_path = f"{output_dir}/{year}/{month}/{day}.csv"

    if dry_run:
        stats[f"csv_files_created_{variant}"] += 1
        logging_module.info(
            "recent_form_csv_dry_run",
            variant=variant,
            date=date_str,
            row_count=len(forms),
        )
        return rel_path

    project_root = Path(__file__).parent.parent
    csv_path = project_root / rel_path

    if write_csv(str(csv_path), csv_content, force_overwrite):
        stats[f"csv_files_created_{variant}"] += 1
        logging_module.info(
            "recent_form_csv_write_success",
            variant=variant,
            date=date_str,
            path=str(csv_path),
        )
        return rel_path

    stats[f"csv_files_skipped_{variant}"] += 1
    logging_module.warning(
        "recent_form_csv_write_skipped",
        variant=variant,
        date=date_str,
        path=str(csv_path),
    )
    return rel_path  # still return path for visibility


def process_recent_forms(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> Tuple[dict, List[str]]:
    """Scrape recent-form data for one day. Returns (stats, written_paths)."""
    stats = {
        "stadiums_scraped": 0,
        "stadiums_failed": 0,
        "races_with_data_national": 0,
        "races_with_data_local": 0,
        "csv_files_created_national": 0,
        "csv_files_created_local": 0,
        "csv_files_skipped_national": 0,
        "csv_files_skipped_local": 0,
        "errors": [],
    }

    logging_module.info("recent_form_processing_start", date=date_str)

    races_by_stadium = _collect_race_inputs(date_str, config, rate_limiter)
    if not races_by_stadium:
        logging_module.info("recent_form_skipped_no_races", date=date_str)
        return stats, []

    scraper = RecentFormScraper(
        timeout_seconds=config.get("recent_form_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    national_forms: List[RecentForm] = []
    local_forms: List[RecentForm] = []

    for stadium_code in sorted(races_by_stadium.keys()):
        race_list = sorted(races_by_stadium[stadium_code], key=lambda x: x[0])
        logging_module.info(
            "recent_form_stadium_start",
            date=date_str,
            stadium=stadium_code,
            race_count=len(race_list),
        )

        national_index = scraper.scrape_stadium_day(
            date_str, stadium_code, RecentFormScraper.VARIANT_NATIONAL
        )
        local_index = scraper.scrape_stadium_day(
            date_str, stadium_code, RecentFormScraper.VARIANT_LOCAL
        )

        if national_index is None and local_index is None:
            stats["stadiums_failed"] += 1
            stats["errors"].append(
                {
                    "date": date_str,
                    "error_type": "recent_form_stadium_missing",
                    "message": "both bc_zensou and bc_zensou_touchi unavailable",
                    "stadium": stadium_code,
                }
            )
            continue

        stats["stadiums_scraped"] += 1

        for race_number, boat_to_reg in race_list:
            if national_index is not None:
                national_forms.append(
                    _build_recent_forms(
                        date_str,
                        stadium_code,
                        race_number,
                        boat_to_reg,
                        national_index,
                    )
                )
                stats["races_with_data_national"] += 1
            if local_index is not None:
                local_forms.append(
                    _build_recent_forms(
                        date_str,
                        stadium_code,
                        race_number,
                        boat_to_reg,
                        local_index,
                    )
                )
                stats["races_with_data_local"] += 1

    written: List[str] = []

    nat_path = _write_variant_csv(
        national_forms,
        "national",
        OUTPUT_DIR_NATIONAL,
        date_str,
        force_overwrite,
        dry_run,
        stats,
    )
    if nat_path:
        written.append(nat_path)

    loc_path = _write_variant_csv(
        local_forms,
        "local",
        OUTPUT_DIR_LOCAL,
        date_str,
        force_overwrite,
        dry_run,
        stats,
    )
    if loc_path:
        written.append(loc_path)

    return stats, written


def load_config(config_path: str = ".boatrace/config.json") -> dict:
    try:
        config_file = Path(config_path)
        if not config_file.is_absolute() and not config_file.exists():
            config_file = Path(__file__).parent.parent / config_path
        if config_file.exists():
            with open(config_file) as f:
                return json.load(f)
    except Exception as e:
        logging_module.error("config_load_error", error=str(e))
    return {}


def parse_arguments():
    jst = timezone(timedelta(hours=9))
    yesterday_jst = (datetime.now(jst) - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description=(
            "Scrape recent-form data (全国・当地近況成績) from race.boatcast.jp. "
            "Writes data/programs/recent_national/YYYY/MM/DD.csv and "
            "data/programs/recent_local/YYYY/MM/DD.csv."
        )
    )
    parser.add_argument(
        "--date",
        type=str,
        default=yesterday_jst,
        help="Date to process (YYYY-MM-DD). Default: yesterday (JST)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing CSV files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files or push to git",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )
    return parser.parse_args()


def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main():
    args = parse_arguments()

    if not validate_date_format(args.date):
        print(f"Error: Invalid date format: {args.date}. Expected YYYY-MM-DD")
        sys.exit(1)

    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    logging_module.info(
        "recent_form_cli_start",
        date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        stats, written_paths = process_recent_forms(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        print()
        print(f"Recent Form Data Processing Complete for {args.date}")
        print(f"  Stadiums scraped: {stats['stadiums_scraped']}")
        print(f"  Stadiums failed: {stats['stadiums_failed']}")
        print(f"  Races (national): {stats['races_with_data_national']}")
        print(f"  Races (local):    {stats['races_with_data_local']}")
        print(
            f"  CSV created: national={stats['csv_files_created_national']} "
            f"local={stats['csv_files_created_local']}"
        )
        print(
            f"  CSV skipped: national={stats['csv_files_skipped_national']} "
            f"local={stats['csv_files_skipped_local']}"
        )
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        created_total = (
            stats["csv_files_created_national"] + stats["csv_files_created_local"]
        )
        skipped_total = (
            stats["csv_files_skipped_national"] + stats["csv_files_skipped_local"]
        )

        if created_total > 0 and not args.dry_run and written_paths:
            message = f"Update boatrace recent form data: {args.date}"
            if git_operations.commit_and_push(written_paths, message):
                for p in written_paths:
                    print(f"Git commit and push successful for {p}")
            else:
                print(f"Git commit and push failed for {written_paths}")

        sys.exit(0 if (created_total > 0 or skipped_total > 0) else 1)

    except Exception as e:
        logging_module.critical(
            "recent_form_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
