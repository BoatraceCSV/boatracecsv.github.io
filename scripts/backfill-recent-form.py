#!/usr/bin/env python3
"""Backfill recent-form data (全国・当地近況5節) for a date range.

Usage:
    # Default: from 2024-03-12 (earliest available on race.boatcast.jp)
    # up to yesterday (JST), skipping dates whose CSVs already exist.
    python scripts/backfill-recent-form.py

    # Explicit range
    python scripts/backfill-recent-form.py \\
        --start-date 2024-03-12 --end-date 2024-03-31

    # Overwrite existing CSVs
    python scripts/backfill-recent-form.py \\
        --start-date 2024-03-12 --end-date 2024-03-31 --force

    # Dry run — fetch and parse but do not write files or push git
    python scripts/backfill-recent-form.py --dry-run

    # Push to git after each day
    python scripts/backfill-recent-form.py --push

NOTES:
    * race.boatcast.jp carries bc_zensou (and bc_zensou_touchi) from
      2024-03-12 onwards. Earlier dates return HTTP 403.
    * Each open stadium needs only 2 boatcast requests per day (one
      bc_zensou + one bc_zensou_touchi), so even a 24-stadium peak day
      is just ~48 requests. A 12-month backfill runs in well under an hour.
    * A day's two CSVs (recent_national + recent_local) are written
      together. Skip-if-exists is keyed off ``recent_national``; if only
      ``recent_local`` is missing, use ``--force`` to regenerate both.
    * Each request is rate-limited via .boatrace/config.json
      (rate_limit_interval_seconds).
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import git_operations
from boatrace import logger as logging_module
from boatrace.converter import VENUE_CODES, recent_forms_to_csv
from boatrace.downloader import RateLimiter, download_file
from boatrace.extractor import extract_b_file
from boatrace.models import RecentForm, RecentFormBoat
from boatrace.parser import parse_program_file
from boatrace.recent_form_scraper import RecentFormScraper, RecentFormRow
from boatrace.storage import file_exists, write_csv


# Earliest date confirmed empirically: 2024-03-12 is the first hit on both
# bc_zensou and bc_zensou_touchi; 2024-03-11 and earlier return HTTP 403.
EARLIEST_AVAILABLE = "2024-03-12"

OUTPUT_DIR_NATIONAL = "data/programs/recent_national"
OUTPUT_DIR_LOCAL = "data/programs/recent_local"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_config(config_path: str = ".boatrace/config.json") -> dict:
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


def _csv_path_for(project_root: Path, output_dir: str, date_str: str) -> Path:
    year, month, day = date_str.split("-")
    return project_root / f"{output_dir}/{year}/{month}/{day}.csv"


def _iter_dates(start_date: str, end_date: str):
    cur = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if end < cur:
        return
    while cur <= end:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def _validate_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _race_code(date_str: str, stadium_code: int, race_number: int) -> str:
    return f"{date_str.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _collect_programs(date_str: str, config: dict, rate_limiter: RateLimiter):
    """Download the B-file for ``date_str`` and return parsed programs."""
    y = date_str[0:4]
    m = date_str[5:7]
    d = date_str[8:10]
    y_short = y[2:]
    file_date = f"{y_short}{m}{d}"
    y_m = f"{y}{m}"

    b_file_url = f"https://www1.mbrace.or.jp/od2/B/{y_m}/b{file_date}.lzh"

    logging_module.info(
        "backfill_recent_form_b_file_downloading",
        date=date_str,
        url=b_file_url,
    )

    b_content, _status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )
    if not b_content:
        logging_module.warning(
            "backfill_recent_form_b_file_missing", date=date_str
        )
        return []

    try:
        b_text = extract_b_file(b_content)
    except Exception as e:
        logging_module.warning(
            "backfill_recent_form_b_file_extract_error",
            date=date_str,
            error=str(e),
        )
        return []
    if not b_text:
        return []

    try:
        return parse_program_file(b_text, date=date_str)
    except Exception as e:
        logging_module.warning(
            "backfill_recent_form_b_file_parse_error",
            date=date_str,
            error=str(e),
        )
        return []


def _build_recent_form(
    date_str: str,
    stadium_code: int,
    race_number: int,
    boat_to_reg: Dict[int, str],
    racer_index: Dict[str, RecentFormRow],
) -> RecentForm:
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


def _process_one_date(
    date_str: str,
    scraper: RecentFormScraper,
    config: dict,
    rate_limiter: RateLimiter,
    project_root: Path,
    force_overwrite: bool,
    dry_run: bool,
) -> dict:
    stats = {
        "date": date_str,
        "skipped_existing": False,
        "stadiums_scraped": 0,
        "stadiums_failed": 0,
        "races_national": 0,
        "races_local": 0,
        "csv_written_national": False,
        "csv_written_local": False,
        "no_races": False,
        "error": None,
    }

    nat_path = _csv_path_for(project_root, OUTPUT_DIR_NATIONAL, date_str)
    loc_path = _csv_path_for(project_root, OUTPUT_DIR_LOCAL, date_str)

    # Skip-if-exists: keyed off recent_national. If recent_local alone is
    # missing the user can re-run with --force.
    if (
        file_exists(str(nat_path))
        and not force_overwrite
        and not dry_run
    ):
        stats["skipped_existing"] = True
        return stats

    try:
        programs = _collect_programs(date_str, config, rate_limiter)
    except Exception as e:
        stats["error"] = f"b_file_error: {e}"
        return stats

    if not programs:
        stats["no_races"] = True
        return stats

    # Build (stadium -> [(race, {boat: reg})]).
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
        stats["no_races"] = True
        return stats

    national_forms: List[RecentForm] = []
    local_forms: List[RecentForm] = []

    for stadium_code in sorted(races_by_stadium.keys()):
        race_list = sorted(races_by_stadium[stadium_code], key=lambda x: x[0])
        national_index = scraper.scrape_stadium_day(
            date_str, stadium_code, RecentFormScraper.VARIANT_NATIONAL
        )
        local_index = scraper.scrape_stadium_day(
            date_str, stadium_code, RecentFormScraper.VARIANT_LOCAL
        )

        if national_index is None and local_index is None:
            stats["stadiums_failed"] += 1
            continue
        stats["stadiums_scraped"] += 1

        for race_number, boat_to_reg in race_list:
            if national_index is not None:
                national_forms.append(
                    _build_recent_form(
                        date_str,
                        stadium_code,
                        race_number,
                        boat_to_reg,
                        national_index,
                    )
                )
                stats["races_national"] += 1
            if local_index is not None:
                local_forms.append(
                    _build_recent_form(
                        date_str,
                        stadium_code,
                        race_number,
                        boat_to_reg,
                        local_index,
                    )
                )
                stats["races_local"] += 1

    if not national_forms and not local_forms:
        return stats

    # National.
    if national_forms:
        nat_csv = recent_forms_to_csv(national_forms, variant="national")
        if nat_csv:
            if dry_run:
                stats["csv_written_national"] = True
            elif write_csv(str(nat_path), nat_csv, force_overwrite):
                stats["csv_written_national"] = True
            else:
                # write_csv returns False when file exists w/o --force, or
                # on actual failure. We don't distinguish; the higher-level
                # skip-if-exists guard above handles the common case.
                pass

    # Local.
    if local_forms:
        loc_csv = recent_forms_to_csv(local_forms, variant="local")
        if loc_csv:
            if dry_run:
                stats["csv_written_local"] = True
            elif write_csv(str(loc_path), loc_csv, force_overwrite):
                stats["csv_written_local"] = True

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _default_end_date() -> str:
    jst = timezone(timedelta(hours=9))
    return (datetime.now(jst) - timedelta(days=1)).strftime("%Y-%m-%d")


def _parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Backfill recent-form data (全国・当地近況5節) across a date range. "
            "Writes both data/programs/recent_national/ and data/programs/recent_local/ "
            "from the same boatcast TSVs."
        )
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=EARLIEST_AVAILABLE,
        help=(
            "Start date (YYYY-MM-DD). Default: "
            f"{EARLIEST_AVAILABLE} (earliest available on race.boatcast.jp)"
        ),
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=_default_end_date(),
        help="End date (YYYY-MM-DD). Default: yesterday (JST)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite existing CSVs (default: skip dates whose national "
            "CSV exists)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write any CSV or push to git",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help=(
            "Commit and push each day's CSVs (default: OFF — backfills "
            "stay local so the user can batch-commit manually)"
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help=(
            "Print a progress line every N dates processed "
            "(default: 10). Set to 1 to print every day."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )
    return parser.parse_args()


def main():
    args = _parse_arguments()

    if not _validate_date(args.start_date):
        print(f"Error: invalid --start-date: {args.start_date}")
        sys.exit(1)
    if not _validate_date(args.end_date):
        print(f"Error: invalid --end-date: {args.end_date}")
        sys.exit(1)
    if datetime.strptime(args.end_date, "%Y-%m-%d") < datetime.strptime(
        args.start_date, "%Y-%m-%d"
    ):
        print("Error: --end-date must be >= --start-date")
        sys.exit(1)

    if datetime.strptime(args.start_date, "%Y-%m-%d") < datetime.strptime(
        EARLIEST_AVAILABLE, "%Y-%m-%d"
    ):
        print(
            f"Warning: race.boatcast.jp has no recent-form data before "
            f"{EARLIEST_AVAILABLE}; earlier dates will be recorded as 'no_races'."
        )

    config = _load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    logging_module.info(
        "backfill_recent_form_start",
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
        dry_run=args.dry_run,
        push=args.push,
    )

    project_root = Path(__file__).parent.parent
    rate_limiter = RateLimiter(
        interval_seconds=config.get("rate_limit_interval_seconds", 3)
    )
    scraper = RecentFormScraper(
        timeout_seconds=config.get("recent_form_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    total = {
        "dates_seen": 0,
        "dates_processed": 0,
        "dates_skipped_existing": 0,
        "dates_no_races": 0,
        "dates_with_error": 0,
        "stadiums_scraped": 0,
        "stadiums_failed": 0,
        "csv_files_written_national": 0,
        "csv_files_written_local": 0,
        "pushed_files": 0,
        "failed_pushes": 0,
    }

    date_list = list(_iter_dates(args.start_date, args.end_date))
    print(
        f"Backfilling {len(date_list)} day(s) from "
        f"{args.start_date} to {args.end_date}"
        f"{' (dry-run)' if args.dry_run else ''}"
    )

    start_time = time.time()
    for i, date_str in enumerate(date_list, start=1):
        total["dates_seen"] += 1

        day_stats = _process_one_date(
            date_str,
            scraper,
            config,
            rate_limiter,
            project_root,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        if day_stats["skipped_existing"]:
            total["dates_skipped_existing"] += 1
        elif day_stats["no_races"]:
            total["dates_no_races"] += 1
        elif day_stats["error"]:
            total["dates_with_error"] += 1
            logging_module.warning(
                "backfill_recent_form_date_error",
                date=date_str,
                error=day_stats["error"],
            )
        else:
            total["dates_processed"] += 1
            total["stadiums_scraped"] += day_stats["stadiums_scraped"]
            total["stadiums_failed"] += day_stats["stadiums_failed"]

            written_paths = []
            if day_stats["csv_written_national"]:
                total["csv_files_written_national"] += 1
                if not args.dry_run:
                    year, month, day = date_str.split("-")
                    written_paths.append(
                        f"{OUTPUT_DIR_NATIONAL}/{year}/{month}/{day}.csv"
                    )
            if day_stats["csv_written_local"]:
                total["csv_files_written_local"] += 1
                if not args.dry_run:
                    year, month, day = date_str.split("-")
                    written_paths.append(
                        f"{OUTPUT_DIR_LOCAL}/{year}/{month}/{day}.csv"
                    )

            if args.push and not args.dry_run and written_paths:
                message = f"Backfill boatrace recent form data: {date_str}"
                if git_operations.commit_and_push(written_paths, message):
                    total["pushed_files"] += len(written_paths)
                else:
                    total["failed_pushes"] += len(written_paths)

        if i % max(1, args.progress_every) == 0 or i == len(date_list):
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(date_list) - i) / rate if rate > 0 else 0
            print(
                f"  [{i}/{len(date_list)}] {date_str}  "
                f"nat_written={total['csv_files_written_national']}  "
                f"loc_written={total['csv_files_written_local']}  "
                f"skipped={total['dates_skipped_existing']}  "
                f"no_races={total['dates_no_races']}  "
                f"err={total['dates_with_error']}  "
                f"elapsed={elapsed:.0f}s  eta={remaining:.0f}s",
                flush=True,
            )

    print()
    print(f"Backfill complete: {args.start_date} → {args.end_date}")
    print(f"  Dates seen:                 {total['dates_seen']}")
    print(f"  Dates processed:            {total['dates_processed']}")
    print(f"  Dates skipped (exists):     {total['dates_skipped_existing']}")
    print(f"  Dates with no races:        {total['dates_no_races']}")
    print(f"  Dates with error:           {total['dates_with_error']}")
    print(f"  Stadiums scraped (sum):     {total['stadiums_scraped']}")
    print(f"  Stadiums failed (sum):      {total['stadiums_failed']}")
    print(f"  recent_national written:    {total['csv_files_written_national']}")
    print(f"  recent_local written:       {total['csv_files_written_local']}")
    if args.push and not args.dry_run:
        print(f"  Pushed:                     {total['pushed_files']}")
        print(f"  Failed pushes:              {total['failed_pushes']}")
    print(f"  Elapsed:                    {time.time() - start_time:.0f}s")

    logging_module.info("backfill_recent_form_complete", totals=total)

    if total["failed_pushes"] > 0:
        sys.exit(2)
    if (
        total["csv_files_written_national"] == 0
        and total["csv_files_written_local"] == 0
        and total["dates_with_error"] > 0
    ):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
