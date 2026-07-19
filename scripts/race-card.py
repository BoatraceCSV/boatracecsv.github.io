#!/usr/bin/env python3
"""Scrape race-card detail (出走表詳細) from race.boatcast.jp.

For a given date, this script:
  1. Resolves the day's open (stadium, race) list from
     boatcast.jp's ``getHoldingList2`` JSON API (with the locally
     written title CSV as a fallback). No B-file / LZH dependency.
  2. Fetches the per-race ``bc_j_str3`` TSV from race.boatcast.jp for each
     race, plus the sibling ``bc_j_waku10`` (枠番別過去10走) TSV.
  3. Writes one CSV per date to ``data/programs/race_cards/YYYY/MM/DD.csv``
     and ``data/programs/waku10/YYYY/MM/DD.csv``.
  4. Fetches the monthly holding schedule ``bc_mon_2`` for all 24 stadiums
     and (over)writes ``data/programs/monthly_schedule/YYYY/MM.csv``
     (the source spans ~3 months ahead; the daily refresh captures
     schedule changes in git history).

Race-card columns: 全国/当地 3連対率, 全国平均ST, F/L counts,
モーター/ボート 3連対率, and the 14-slot 節間成績 (R番号/進入/枠/ST/着順).
Waku10 columns: 枠番別 勝率/平均ST/平均スタート順 + 過去10走の
着順/進入/グレード (新しい順).
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.converter import (
    monthly_schedule_to_csv,
    race_cards_to_csv,
    waku10_to_csv,
)
from boatrace.downloader import RateLimiter
from boatrace.holding_list import (
    HoldingListError,
    fetch_holding_list,
    load_holding_from_title_csv,
)
from boatrace.monthly_schedule_scraper import MonthlyScheduleScraper
from boatrace.race_card_scraper import RaceCardScraper
from boatrace.storage import write_csv


OUTPUT_DIR = "data/programs/race_cards"
WAKU10_OUTPUT_DIR = "data/programs/waku10"
SCHEDULE_OUTPUT_DIR = "data/programs/monthly_schedule"


def _collect_actual_races(date_str: str, config: dict, rate_limiter: RateLimiter):
    """Return a set of (stadium_code, race_number) for the given date.

    Pulls the canonical open-race list from boatcast.jp's
    ``getHoldingList2`` JSON API. If the API is unreachable, falls back
    to the locally-written title CSV (produced by ``race-title.py``).
    Cancelled / postponed races are filtered out.
    """
    project_root = Path(__file__).parent.parent

    try:
        races = fetch_holding_list(date_str, rate_limiter=rate_limiter)
    except HoldingListError as exc:
        logging_module.warning(
            "race_card_holding_list_fallback",
            date=date_str,
            error=str(exc),
        )
        races = load_holding_from_title_csv(project_root, date_str)

    if not races:
        logging_module.warning("race_card_holding_list_empty", date=date_str)
        return set()

    return {
        (r.stadium_code, r.race_number)
        for r in races
        if r.is_open and 1 <= r.race_number <= 12
    }


def process_race_cards(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Scrape race-card detail data for one day."""
    stats = {
        "races_scraped": 0,
        "races_failed": 0,
        "waku10_scraped": 0,
        "waku10_failed": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "csv_paths": [],
        "errors": [],
    }

    logging_module.info("race_card_processing_start", date=date_str)

    actual_races = _collect_actual_races(date_str, config, rate_limiter)
    if not actual_races:
        logging_module.info("race_card_skipped_no_races", date=date_str)
        return stats

    scraper = RaceCardScraper(
        timeout_seconds=config.get("race_card_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    logging_module.info(
        "race_card_scraping_start",
        date=date_str,
        total_expected=len(actual_races),
    )

    results = []
    waku10_results = []

    races_by_stadium = {}
    for stadium_code, race_number in actual_races:
        races_by_stadium.setdefault(stadium_code, []).append(race_number)

    for stadium_code in sorted(races_by_stadium.keys()):
        race_numbers = sorted(races_by_stadium[stadium_code])
        logging_module.info(
            "race_card_stadium_start",
            date=date_str,
            stadium=stadium_code,
            races=race_numbers,
        )

        for race_number in race_numbers:
            try:
                data = scraper.scrape_race(date_str, stadium_code, race_number)
            except Exception as e:
                stats["races_failed"] += 1
                stats["errors"].append(
                    {
                        "date": date_str,
                        "error_type": "race_card_scrape_error",
                        "message": str(e),
                        "stadium": stadium_code,
                        "race": race_number,
                    }
                )
                data = None
            else:
                if data is None:
                    stats["races_failed"] += 1
                else:
                    results.append(data)
                    stats["races_scraped"] += 1

            # 枠番別過去10走 — independent of the str3 outcome so a
            # failure on one file does not lose the other.
            try:
                waku10 = scraper.scrape_waku10(
                    date_str, stadium_code, race_number
                )
            except Exception as e:
                stats["waku10_failed"] += 1
                stats["errors"].append(
                    {
                        "date": date_str,
                        "error_type": "waku10_scrape_error",
                        "message": str(e),
                        "stadium": stadium_code,
                        "race": race_number,
                    }
                )
                continue

            if waku10 is None:
                stats["waku10_failed"] += 1
                continue

            waku10_results.append(waku10)
            stats["waku10_scraped"] += 1

    logging_module.info(
        "race_card_scraping_complete",
        date=date_str,
        total_scraped=len(results),
        waku10_scraped=len(waku10_results),
    )

    year, month, day = date_str.split("-")
    project_root = Path(__file__).parent.parent

    for label, items, to_csv, rel_path in (
        (
            "race_card",
            results,
            race_cards_to_csv,
            f"{OUTPUT_DIR}/{year}/{month}/{day}.csv",
        ),
        (
            "waku10",
            waku10_results,
            waku10_to_csv,
            f"{WAKU10_OUTPUT_DIR}/{year}/{month}/{day}.csv",
        ),
    ):
        if not items:
            continue
        csv_content = to_csv(items)
        if not csv_content:
            continue

        if dry_run:
            stats["csv_files_created"] += 1
            logging_module.info(
                f"{label}_csv_dry_run",
                date=date_str,
                row_count=len(items),
            )
            continue

        csv_path = project_root / rel_path

        logging_module.info(
            f"{label}_csv_write_start",
            date=date_str,
            path=str(csv_path),
        )

        if write_csv(str(csv_path), csv_content, force_overwrite):
            stats["csv_files_created"] += 1
            stats["csv_paths"].append(rel_path)
            logging_module.info(
                f"{label}_csv_write_success",
                date=date_str,
                path=str(csv_path),
            )
        else:
            stats["csv_files_skipped"] += 1
            logging_module.warning(
                f"{label}_csv_write_skipped",
                date=date_str,
                path=str(csv_path),
            )

    return stats


def process_monthly_schedule(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Fetch bc_mon_2 for all 24 stadiums and write the month CSV.

    The CSV is keyed by the fetch month (``data/programs/monthly_schedule/
    YYYY/MM.csv``) and overwritten on each run — schedule changes over the
    month are captured by git history. Entries beyond the fetch month are
    included as-is (the source file spans ~3 months ahead).
    """
    stats = {
        "stadiums_scraped": 0,
        "stadiums_failed": 0,
        "entries": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "csv_paths": [],
        "errors": [],
    }

    year, month, _day = date_str.split("-")
    year_month = f"{year}{month}"

    scraper = MonthlyScheduleScraper(
        timeout_seconds=config.get("race_card_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    entries = []
    for stadium_code in range(1, 25):
        try:
            stadium_entries = scraper.scrape_stadium(year_month, stadium_code)
        except Exception as e:
            stats["stadiums_failed"] += 1
            stats["errors"].append(
                {
                    "date": date_str,
                    "error_type": "monthly_schedule_scrape_error",
                    "message": str(e),
                    "stadium": stadium_code,
                }
            )
            continue

        if stadium_entries is None:
            stats["stadiums_failed"] += 1
            continue

        stats["stadiums_scraped"] += 1
        entries.extend(stadium_entries)

    stats["entries"] = len(entries)
    logging_module.info(
        "monthly_schedule_scraping_complete",
        year_month=year_month,
        stadiums_scraped=stats["stadiums_scraped"],
        entries=len(entries),
    )

    if not entries:
        return stats

    csv_content = monthly_schedule_to_csv(entries)
    if not csv_content:
        return stats

    if dry_run:
        stats["csv_files_created"] += 1
        logging_module.info(
            "monthly_schedule_csv_dry_run",
            year_month=year_month,
            row_count=len(entries),
        )
        return stats

    rel_path = f"{SCHEDULE_OUTPUT_DIR}/{year}/{month}.csv"
    project_root = Path(__file__).parent.parent
    csv_path = project_root / rel_path

    if write_csv(str(csv_path), csv_content, force_overwrite):
        stats["csv_files_created"] += 1
        stats["csv_paths"].append(rel_path)
        logging_module.info(
            "monthly_schedule_csv_write_success",
            year_month=year_month,
            path=str(csv_path),
        )
    else:
        stats["csv_files_skipped"] += 1
        logging_module.warning(
            "monthly_schedule_csv_write_skipped",
            year_month=year_month,
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
        description="Scrape race-card detail data (出走表詳細) from race.boatcast.jp"
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
        "race_card_cli_start",
        date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        stats = process_race_cards(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        schedule_stats = process_monthly_schedule(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        print()
        print(f"Race Card Data Processing Complete for {args.date}")
        print(f"  Races scraped: {stats['races_scraped']}")
        print(f"  Races failed / missing: {stats['races_failed']}")
        print(f"  Waku10 scraped: {stats['waku10_scraped']}")
        print(f"  Waku10 failed / missing: {stats['waku10_failed']}")
        print(
            "  Monthly schedule: "
            f"{schedule_stats['stadiums_scraped']} stadiums, "
            f"{schedule_stats['entries']} entries"
        )
        created = (
            stats["csv_files_created"] + schedule_stats["csv_files_created"]
        )
        skipped = (
            stats["csv_files_skipped"] + schedule_stats["csv_files_skipped"]
        )
        print(f"  CSV files created: {created}")
        print(f"  CSV files skipped: {skipped}")
        errors = stats["errors"] + schedule_stats["errors"]
        if errors:
            print(f"  Errors: {len(errors)}")
            for error in errors:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        csv_paths = stats["csv_paths"] + schedule_stats["csv_paths"]
        if csv_paths and not args.dry_run:
            message = f"Update boatrace race card data: {args.date}"
            if git_operations.commit_and_push(csv_paths, message):
                print(f"Git commit and push successful for {csv_paths}")
            else:
                print(f"Git commit and push failed for {csv_paths}")

        sys.exit(0 if created > 0 or skipped > 0 else 1)

    except Exception as e:
        logging_module.critical(
            "race_card_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
