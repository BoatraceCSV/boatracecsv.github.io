#!/usr/bin/env python3
"""Scrape recent-form data (全国・当地近況成績) from race.boatcast.jp.

For a given date, this script:
  1. Downloads the same-day B-file from mbrace.or.jp to determine which
     races exist and which racer is in which boat (entry_number ↔
     registration_number).
  2. For each open stadium, fetches both ``bc_zensou`` (全国近況5節) and
     ``bc_zensou_touchi`` (当地近況5節) from race.boatcast.jp once each.
  3. Joins by registration number to produce per-race, per-boat
     :class:`RecentForm` objects.
  4. Writes two CSV files for the date:
       - ``data/recent_national/YYYY/MM/DD.csv``
       - ``data/recent_local/YYYY/MM/DD.csv``

Both files follow the same shape (196 columns: 4 meta + 6 boats × 32),
so the converter is shared between variants.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.converter import VENUE_CODES, recent_forms_to_csv
from boatrace.downloader import RateLimiter, download_file
from boatrace.extractor import extract_b_file
from boatrace.models import RecentForm, RecentFormBoat, RecentFormSession
from boatrace.parser import parse_program_file
from boatrace.recent_form_scraper import RecentFormScraper, RecentFormRow
from boatrace.storage import write_csv


OUTPUT_DIR_NATIONAL = "data/recent_national"
OUTPUT_DIR_LOCAL = "data/recent_local"


def _race_code(date_str: str, stadium_code: int, race_number: int) -> str:
    return f"{date_str.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _collect_programs(
    date_str: str, config: dict, rate_limiter: RateLimiter
):
    """Return the list of ``RaceProgram`` objects for the given date.

    Same B-file flow as ``original-exhibition.py`` and ``race-card.py``,
    but we keep the ``RaceProgram`` objects (not just race tuples)
    because we need each boat's registration number for the JOIN.
    """
    year = date_str[0:4]
    month = date_str[5:7]
    day = date_str[8:10]
    year_short = year[2:]
    file_date = f"{year_short}{month}{day}"
    year_month = f"{year}{month}"

    base_url = "https://www1.mbrace.or.jp/od2"
    b_file_url = f"{base_url}/B/{year_month}/b{file_date}.lzh"

    logging_module.info(
        "recent_form_b_file_downloading",
        date=date_str,
        url=b_file_url,
    )

    b_content, _b_status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    if not b_content:
        logging_module.warning("recent_form_b_file_missing", date=date_str)
        return []

    try:
        b_text = extract_b_file(b_content)
    except Exception as e:
        logging_module.warning(
            "recent_form_b_file_extract_error",
            date=date_str,
            error=str(e),
        )
        return []

    if not b_text:
        return []

    try:
        programs = parse_program_file(b_text, date=date_str)
    except Exception as e:
        logging_module.warning(
            "recent_form_b_file_parse_error",
            date=date_str,
            error=str(e),
        )
        return []

    return programs


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

    programs = _collect_programs(date_str, config, rate_limiter)
    if not programs:
        logging_module.info("recent_form_skipped_no_programs", date=date_str)
        return stats, []

    # Build (stadium_code -> [(race_number, {boat: reg})]) map.
    races_by_stadium: Dict[int, List[Tuple[int, Dict[int, str]]]] = {}
    for program in programs:
        stadium_code_str = VENUE_CODES.get(program.stadium)
        if not stadium_code_str:
            continue
        try:
            race_number = int(program.race_round.rstrip("R"))
        except (ValueError, AttributeError):
            continue
        if not (1 <= race_number <= 12):
            continue
        boat_to_reg: Dict[int, str] = {}
        for frame in program.racer_frames:
            if frame.entry_number and frame.registration_number:
                boat_to_reg[int(frame.entry_number)] = frame.registration_number
        races_by_stadium.setdefault(int(stadium_code_str), []).append(
            (race_number, boat_to_reg)
        )

    if not races_by_stadium:
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
            "Writes data/recent_national/YYYY/MM/DD.csv and "
            "data/recent_local/YYYY/MM/DD.csv."
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
