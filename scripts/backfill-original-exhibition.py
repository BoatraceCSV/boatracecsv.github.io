#!/usr/bin/env python3
"""Backfill original exhibition data (オリジナル展示データ) for a date range.

Usage:
    # Default: from 2024-03-11 (earliest available on race.boatcast.jp)
    # up to yesterday (JST), skipping dates whose CSV already exists.
    python scripts/backfill-original-exhibition.py

    # Explicit range
    python scripts/backfill-original-exhibition.py \\
        --start-date 2024-03-11 --end-date 2024-03-31

    # Overwrite existing CSVs
    python scripts/backfill-original-exhibition.py \\
        --start-date 2024-03-11 --end-date 2024-03-31 --force

    # Dry run — fetch and parse but do not write files or push git
    python scripts/backfill-original-exhibition.py --dry-run

    # Push to git after each day (default is OFF — backfills stay local
    # unless --push is given, so the user can commit in batches manually).
    python scripts/backfill-original-exhibition.py --push

NOTES:
    * race.boatcast.jp only carries original exhibition data from 2024-03-11
      onwards. Earlier dates return HTTP 403 (no file). The default
      --start-date reflects this.
    * By default, days whose CSV already exists are skipped. Use --force to
      re-scrape and overwrite them.
    * Each request is rate-limited via .boatrace/config.json
      (rate_limit_interval_seconds). Lower this value for faster backfills
      when you trust the source can handle it.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import git_operations
from boatrace import logger as logging_module
from boatrace.converter import VENUE_CODES, original_exhibition_to_csv
from boatrace.downloader import RateLimiter, download_file
from boatrace.extractor import extract_b_file
from boatrace.original_exhibition_scraper import OriginalExhibitionScraper
from boatrace.parser import parse_program_file
from boatrace.storage import file_exists, write_csv


# The earliest date boatcast.jp has data for. Verified empirically:
# 2024-03-10 and earlier return HTTP 403; 2024-03-11 is the first hit.
EARLIEST_AVAILABLE = "2024-03-11"

OUTPUT_DIR = "data/original_exhibition"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_config(config_path: str = ".boatrace/config.json") -> dict:
    """Load configuration from .boatrace/config.json."""
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


def _csv_path_for(project_root: Path, date_str: str) -> Path:
    year, month, day = date_str.split("-")
    return project_root / f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"


def _iter_dates(start_date: str, end_date: str):
    """Yield YYYY-MM-DD strings from start_date to end_date inclusive."""
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


def _collect_actual_races(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
) -> set:
    """Download the B-file for a date and return {(stadium_code, race_no)}.

    Returns an empty set if the B-file is missing or unreadable.
    """
    actual_races = set()

    y = date_str[0:4]
    m = date_str[5:7]
    d = date_str[8:10]
    y_short = y[2:]
    file_date = f"{y_short}{m}{d}"
    y_m = f"{y}{m}"

    b_file_url = f"https://www1.mbrace.or.jp/od2/B/{y_m}/b{file_date}.lzh"

    logging_module.info(
        "backfill_original_exhibition_b_file_downloading",
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
            "backfill_original_exhibition_b_file_missing",
            date=date_str,
        )
        return actual_races

    try:
        b_text = extract_b_file(b_content)
    except Exception as e:
        logging_module.warning(
            "backfill_original_exhibition_b_file_extract_error",
            date=date_str,
            error=str(e),
        )
        return actual_races
    if not b_text:
        return actual_races

    try:
        programs = parse_program_file(b_text, date=date_str)
    except Exception as e:
        logging_module.warning(
            "backfill_original_exhibition_b_file_parse_error",
            date=date_str,
            error=str(e),
        )
        return actual_races
    if not programs:
        return actual_races

    for program in programs:
        stadium_code = VENUE_CODES.get(program.stadium)
        if not stadium_code:
            continue
        try:
            if not program.race_round:
                continue
            race_num = int(program.race_round.rstrip("R"))
            if 1 <= race_num <= 12:
                actual_races.add((int(stadium_code), race_num))
        except (ValueError, AttributeError):
            continue

    return actual_races


def _process_one_date(
    date_str: str,
    scraper: OriginalExhibitionScraper,
    config: dict,
    rate_limiter: RateLimiter,
    project_root: Path,
    force_overwrite: bool,
    dry_run: bool,
) -> dict:
    """Scrape one date end-to-end. Returns a per-date stats dict."""
    stats = {
        "date": date_str,
        "skipped_existing": False,
        "races_scraped": 0,
        "races_not_measurable": 0,
        "races_missing": 0,
        "csv_written": False,
        "csv_row_count": 0,
        "no_races": False,
        "error": None,
    }

    csv_path = _csv_path_for(project_root, date_str)
    if file_exists(str(csv_path)) and not force_overwrite and not dry_run:
        stats["skipped_existing"] = True
        return stats

    try:
        actual_races = _collect_actual_races(date_str, config, rate_limiter)
    except Exception as e:
        stats["error"] = f"b_file_error: {e}"
        return stats

    if not actual_races:
        stats["no_races"] = True
        return stats

    results = []
    for stadium_code, race_number in sorted(actual_races):
        try:
            data = scraper.scrape_race(date_str, stadium_code, race_number)
        except Exception as e:
            logging_module.warning(
                "backfill_original_exhibition_scrape_error",
                date=date_str,
                stadium=stadium_code,
                race=race_number,
                error=str(e),
            )
            stats["races_missing"] += 1
            continue

        if data is None:
            stats["races_missing"] += 1
            continue
        if data.status == "2":
            stats["races_not_measurable"] += 1
        results.append(data)
        stats["races_scraped"] += 1

    if not results:
        return stats

    csv_content = original_exhibition_to_csv(results)
    if not csv_content:
        stats["error"] = "csv_generation_failed"
        return stats

    stats["csv_row_count"] = len(results)
    if dry_run:
        stats["csv_written"] = True
        return stats

    if write_csv(str(csv_path), csv_content, force_overwrite):
        stats["csv_written"] = True
    else:
        stats["error"] = "csv_write_failed_or_exists"

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _default_end_date() -> str:
    jst = timezone(timedelta(hours=9))
    return (datetime.now(jst) - timedelta(days=1)).strftime("%Y-%m-%d")


def _parse_arguments():
    parser = argparse.ArgumentParser(
        description="Backfill original exhibition data across a date range"
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
        help="Overwrite existing CSVs (default: skip dates whose CSV exists)",
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
            "Commit and push each written CSV to git (default: OFF — "
            "backfills stay local so the user can batch-commit manually)"
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

    # Warn if start-date predates the known coverage.
    if datetime.strptime(args.start_date, "%Y-%m-%d") < datetime.strptime(
        EARLIEST_AVAILABLE, "%Y-%m-%d"
    ):
        print(
            f"Warning: race.boatcast.jp has no data before "
            f"{EARLIEST_AVAILABLE}; earlier dates will be recorded as 'no_races'."
        )

    config = _load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    logging_module.info(
        "backfill_original_exhibition_start",
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
    scraper = OriginalExhibitionScraper(
        timeout_seconds=config.get("original_exhibition_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    # Aggregate stats
    total = {
        "dates_seen": 0,
        "dates_processed": 0,
        "dates_skipped_existing": 0,
        "dates_no_races": 0,
        "dates_with_error": 0,
        "races_scraped": 0,
        "races_not_measurable": 0,
        "races_missing": 0,
        "csv_files_written": 0,
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
                "backfill_original_exhibition_date_error",
                date=date_str,
                error=day_stats["error"],
            )
        else:
            total["dates_processed"] += 1
            total["races_scraped"] += day_stats["races_scraped"]
            total["races_not_measurable"] += day_stats["races_not_measurable"]
            total["races_missing"] += day_stats["races_missing"]
            if day_stats["csv_written"]:
                total["csv_files_written"] += 1

                # Optional per-day git push.
                if args.push and not args.dry_run:
                    year, month, day = date_str.split("-")
                    csv_file = f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"
                    message = (
                        f"Backfill boatrace original exhibition data: {date_str}"
                    )
                    if git_operations.commit_and_push([csv_file], message):
                        total["pushed_files"] += 1
                    else:
                        total["failed_pushes"] += 1

        # Progress
        if i % max(1, args.progress_every) == 0 or i == len(date_list):
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(date_list) - i) / rate if rate > 0 else 0
            print(
                f"  [{i}/{len(date_list)}] {date_str}  "
                f"scraped={total['races_scraped']}  "
                f"written={total['csv_files_written']}  "
                f"skipped={total['dates_skipped_existing']}  "
                f"no_races={total['dates_no_races']}  "
                f"err={total['dates_with_error']}  "
                f"elapsed={elapsed:.0f}s  eta={remaining:.0f}s",
                flush=True,
            )

    # Summary
    print()
    print(f"Backfill complete: {args.start_date} → {args.end_date}")
    print(f"  Dates seen:              {total['dates_seen']}")
    print(f"  Dates processed:         {total['dates_processed']}")
    print(f"  Dates skipped (exists):  {total['dates_skipped_existing']}")
    print(f"  Dates with no races:     {total['dates_no_races']}")
    print(f"  Dates with error:        {total['dates_with_error']}")
    print(f"  Races scraped:           {total['races_scraped']}")
    print(f"  Races not measurable:    {total['races_not_measurable']}")
    print(f"  Races missing / failed:  {total['races_missing']}")
    print(f"  CSV files written:       {total['csv_files_written']}")
    if args.push and not args.dry_run:
        print(f"  Pushed:                  {total['pushed_files']}")
        print(f"  Failed pushes:           {total['failed_pushes']}")
    print(f"  Elapsed:                 {time.time() - start_time:.0f}s")

    logging_module.info(
        "backfill_original_exhibition_complete",
        totals=total,
    )

    # Exit code:
    #   0 = success (something was written, or everything legitimately skipped)
    #   1 = nothing written and at least one error
    #   2 = push errors
    if total["failed_pushes"] > 0:
        sys.exit(2)
    if total["csv_files_written"] == 0 and total["dates_with_error"] > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
