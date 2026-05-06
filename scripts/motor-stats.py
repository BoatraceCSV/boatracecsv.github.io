#!/usr/bin/env python3
"""Scrape motor period statistics (モーター期成績) from race.boatcast.jp.

For a given date, this script:
  1. Downloads the same-day B-file from mbrace.or.jp to determine which
     stadiums have races (so we only fetch motor data for open stadiums).
  2. For each open stadium, fetches ``bc_mst`` (motor period start date)
     followed by ``bc_mdc_{period}_{jo}`` (one row per motor at the stadium).
  3. Aggregates all stadium-motor rows into a single CSV at
     ``data/programs/motor_stats/YYYY/MM/DD.csv``.

Note on history: race.boatcast.jp only carries the **current** motor
period for each stadium. Historic periods are not retained server-side,
so backfilling the past is not possible. Only daily snapshots taken
forward in time accumulate useful time-series data. The ``記録日``
column captures the snapshot date (= the ``--date`` argument).
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Set

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.converter import VENUE_CODES, motor_stats_to_csv
from boatrace.downloader import RateLimiter, download_file
from boatrace.extractor import extract_b_file
from boatrace.models import MotorStat
from boatrace.motor_stats_scraper import MotorStatsScraper
from boatrace.parser import parse_program_file
from boatrace.storage import write_csv


OUTPUT_DIR = "data/programs/motor_stats"


def _collect_open_stadiums(
    date_str: str, config: dict, rate_limiter: RateLimiter
) -> Set[int]:
    """Return the set of stadium codes (1..24) that hold races on the date."""
    open_stadiums: Set[int] = set()

    year = date_str[0:4]
    month = date_str[5:7]
    day = date_str[8:10]
    year_short = year[2:]
    file_date = f"{year_short}{month}{day}"
    year_month = f"{year}{month}"

    base_url = "https://www1.mbrace.or.jp/od2"
    b_file_url = f"{base_url}/B/{year_month}/b{file_date}.lzh"

    logging_module.info(
        "motor_stats_b_file_downloading",
        date=date_str,
        url=b_file_url,
    )

    b_content, _b_status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    if not b_content:
        logging_module.warning("motor_stats_b_file_missing", date=date_str)
        return open_stadiums

    try:
        b_text = extract_b_file(b_content)
    except Exception as e:
        logging_module.warning(
            "motor_stats_b_file_extract_error",
            date=date_str,
            error=str(e),
        )
        return open_stadiums

    if not b_text:
        return open_stadiums

    try:
        programs = parse_program_file(b_text, date=date_str)
    except Exception as e:
        logging_module.warning(
            "motor_stats_b_file_parse_error",
            date=date_str,
            error=str(e),
        )
        return open_stadiums

    for program in programs:
        stadium_code_str = VENUE_CODES.get(program.stadium)
        if not stadium_code_str:
            continue
        try:
            open_stadiums.add(int(stadium_code_str))
        except ValueError:
            continue

    return open_stadiums


def process_motor_stats(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Scrape motor stats for one day's open stadiums."""
    stats = {
        "stadiums_open": 0,
        "stadiums_scraped": 0,
        "stadiums_failed": 0,
        "motors_scraped": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "errors": [],
    }

    logging_module.info("motor_stats_processing_start", date=date_str)

    open_stadiums = _collect_open_stadiums(date_str, config, rate_limiter)
    stats["stadiums_open"] = len(open_stadiums)

    if not open_stadiums:
        logging_module.info("motor_stats_skipped_no_stadiums", date=date_str)
        return stats

    scraper = MotorStatsScraper(
        timeout_seconds=config.get("motor_stats_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    all_motors: List[MotorStat] = []

    for stadium_code in sorted(open_stadiums):
        logging_module.info(
            "motor_stats_stadium_start",
            date=date_str,
            stadium=stadium_code,
        )
        try:
            motors = scraper.scrape_stadium(date_str, stadium_code)
        except Exception as e:
            stats["stadiums_failed"] += 1
            stats["errors"].append(
                {
                    "date": date_str,
                    "error_type": "motor_stats_scrape_error",
                    "message": str(e),
                    "stadium": stadium_code,
                }
            )
            continue

        if motors is None:
            stats["stadiums_failed"] += 1
            continue

        stats["stadiums_scraped"] += 1
        stats["motors_scraped"] += len(motors)
        all_motors.extend(motors)

    logging_module.info(
        "motor_stats_scraping_complete",
        date=date_str,
        stadiums_scraped=stats["stadiums_scraped"],
        motors_scraped=stats["motors_scraped"],
    )

    if not all_motors:
        return stats

    csv_content = motor_stats_to_csv(all_motors)
    if not csv_content:
        return stats

    year, month, day = date_str.split("-")
    if dry_run:
        stats["csv_files_created"] += 1
        logging_module.info(
            "motor_stats_csv_dry_run",
            date=date_str,
            row_count=len(all_motors),
        )
        return stats

    project_root = Path(__file__).parent.parent
    csv_path = project_root / f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"

    logging_module.info(
        "motor_stats_csv_write_start",
        date=date_str,
        path=str(csv_path),
    )

    if write_csv(str(csv_path), csv_content, force_overwrite):
        stats["csv_files_created"] += 1
        logging_module.info(
            "motor_stats_csv_write_success",
            date=date_str,
            path=str(csv_path),
        )
    else:
        stats["csv_files_skipped"] += 1
        logging_module.warning(
            "motor_stats_csv_write_skipped",
            date=date_str,
            path=str(csv_path),
        )

    return stats


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
            "Scrape motor period statistics (モーター期成績) from race.boatcast.jp. "
            "Writes data/programs/motor_stats/YYYY/MM/DD.csv (one row per motor at each "
            "open stadium). Note: only the current motor period is exposed by "
            "boatcast — historical backfill is not possible."
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
        help="Overwrite existing CSV file",
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
        "motor_stats_cli_start",
        date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        stats = process_motor_stats(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        print()
        print(f"Motor Stats Data Processing Complete for {args.date}")
        print(f"  Stadiums open: {stats['stadiums_open']}")
        print(f"  Stadiums scraped: {stats['stadiums_scraped']}")
        print(f"  Stadiums failed: {stats['stadiums_failed']}")
        print(f"  Motors scraped: {stats['motors_scraped']}")
        print(f"  CSV files created: {stats['csv_files_created']}")
        print(f"  CSV files skipped: {stats['csv_files_skipped']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        if stats["csv_files_created"] > 0 and not args.dry_run:
            year, month, day = args.date.split("-")
            csv_file = f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"
            message = f"Update boatrace motor stats data: {args.date}"
            if git_operations.commit_and_push([csv_file], message):
                print(f"Git commit and push successful for {csv_file}")
            else:
                print(f"Git commit and push failed for {csv_file}")

        sys.exit(
            0
            if stats["csv_files_created"] > 0 or stats["csv_files_skipped"] > 0
            else 1
        )

    except Exception as e:
        logging_module.critical(
            "motor_stats_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
