#!/usr/bin/env python3
"""Scrape motor period statistics (モーター期成績) from race.boatcast.jp.

For a given date, this script:
  1. Resolves the day's open stadiums from boatcast.jp's
     ``getHoldingList2`` JSON API (with title CSV fallback). No B-file.
  2. For each open stadium, fetches ``bc_mst`` (motor period start date)
     followed by ``bc_mdc_{period}_{jo}`` (one row per motor at the stadium).
  3. Aggregates all stadium-motor rows into a single CSV at
     ``data/programs/motor_stats/YYYY/MM/DD.csv``.
  4. For each open stadium, derives the most recent completed 節's end
     date from ``bc_mon_2`` and fetches the motor usage history
     ``bc_mrireki_{節終了日}_{jo}`` (one row per motor × past 節).
     Rows are appended to ``data/programs/motor_history/YYYY/MM/DD.csv``
     where the date is the 節終了日 — the file is static per key, so a
     stadium already present in the CSV is skipped (idempotent daily
     runs).

Note on history: race.boatcast.jp only carries the **current** motor
period for each stadium. Historic periods are not retained server-side,
so backfilling the past is not possible. Only daily snapshots taken
forward in time accumulate useful time-series data. The ``記録日``
column captures the snapshot date (= the ``--date`` argument).
(``bc_mrireki`` is the exception: past 節 keys are retained for at
least ~1 month, so a missed day is recoverable.)
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Set

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.converter import (
    MOTOR_HISTORY_HEADERS,
    motor_history_entry_to_row,
    motor_stats_to_csv,
)
from boatrace.downloader import RateLimiter
from boatrace.holding_list import (
    HoldingListError,
    fetch_holding_list,
    load_holding_from_title_csv,
)
from boatrace.models import MotorStat
from boatrace.monthly_schedule_scraper import MonthlyScheduleScraper
from boatrace.motor_stats_scraper import MotorStatsScraper
from boatrace.storage import write_csv


OUTPUT_DIR = "data/programs/motor_stats"
HISTORY_OUTPUT_DIR = "data/programs/motor_history"


def _collect_open_stadiums(
    date_str: str, config: dict, rate_limiter: RateLimiter
) -> Set[int]:
    """Return the set of stadium codes (1..24) that hold races on the date.

    Pulls the canonical list from boatcast.jp's ``getHoldingList2`` JSON
    API; falls back to the locally-written title CSV when the API is
    unreachable. A stadium counts as open when at least one of its races
    is not cancelled / postponed.
    """
    project_root = Path(__file__).parent.parent

    try:
        races = fetch_holding_list(date_str, rate_limiter=rate_limiter)
    except HoldingListError as exc:
        logging_module.warning(
            "motor_stats_holding_list_fallback",
            date=date_str,
            error=str(exc),
        )
        races = load_holding_from_title_csv(project_root, date_str)

    if not races:
        logging_module.warning("motor_stats_holding_list_empty", date=date_str)
        return set()

    return {r.stadium_code for r in races if r.is_open}


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
        "open_stadium_codes": [],
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
    stats["open_stadium_codes"] = sorted(open_stadiums)

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


def _previous_session_end(
    schedule_scraper: MonthlyScheduleScraper,
    date_str: str,
    stadium_code: int,
) -> Optional[str]:
    """Derive the end date (YYYYMMDD) of the last 節 completed before *date_str*.

    Looks at the stadium's ``bc_mon_2`` for the month of *date_str*, and
    falls back to the previous month when no completed 節 is found there
    (e.g. early in a month whose first 節 is still running).
    """
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    months = [target.strftime("%Y%m")]
    prev_month_last_day = target.replace(day=1) - timedelta(days=1)
    months.append(prev_month_last_day.strftime("%Y%m"))

    for year_month in months:
        entries = schedule_scraper.scrape_stadium(year_month, stadium_code)
        if not entries:
            continue
        ended = [
            e.end_date
            for e in entries
            if e.end_date is not None and e.end_date < date_str
        ]
        if ended:
            return max(ended).replace("-", "")
    return None


def history_csv_path(project_root: Path, session_end_yyyymmdd: str) -> Path:
    """Resolve ``data/programs/motor_history/{YYYY}/{MM}/{DD}.csv``.

    The path date is the 節終了日 key, not the run date.
    """
    y, m, d = (
        session_end_yyyymmdd[0:4],
        session_end_yyyymmdd[4:6],
        session_end_yyyymmdd[6:8],
    )
    return project_root / HISTORY_OUTPUT_DIR / y / m / f"{d}.csv"


def recorded_stadiums(path: Path) -> Set[str]:
    """Return the set of 場コード already present in the history CSV."""
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header
            except StopIteration:
                return set()
            return {row[0] for row in reader if row}
    except OSError as exc:
        logging_module.warning(
            "motor_history_existing_read_failed",
            path=str(path),
            error=str(exc),
        )
        return set()


def append_history_rows(path: Path, rows: List[List[str]]) -> int:
    """Append history rows to *path*, writing the header first if needed."""
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0

    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    if new_file:
        writer.writerow(MOTOR_HISTORY_HEADERS)
    for row in rows:
        writer.writerow(row)

    with open(path, "a", encoding="utf-8") as f:
        f.write(buf.getvalue())

    logging_module.info(
        "motor_history_csv_appended",
        path=str(path),
        rows=len(rows),
        new_file=new_file,
    )
    return len(rows)


def process_motor_history(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    open_stadiums: Set[int],
    dry_run: bool = False,
) -> dict:
    """Fetch bc_mrireki for each open stadium's previous 節 and append rows.

    The mrireki file is static per (stadium, 節終了日); a stadium already
    present in the target CSV is skipped without fetching, so the daily
    re-run is a cheap no-op until the next 節 completes.
    """
    stats = {
        "stadiums_recorded": 0,
        "stadiums_skipped": 0,
        "stadiums_failed": 0,
        "entries": 0,
        "csv_paths": [],
        "errors": [],
    }

    if not open_stadiums:
        return stats

    project_root = Path(__file__).parent.parent
    schedule_scraper = MonthlyScheduleScraper(
        timeout_seconds=config.get("motor_stats_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )
    scraper = MotorStatsScraper(
        timeout_seconds=config.get("motor_stats_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )

    for stadium_code in sorted(open_stadiums):
        try:
            session_end = _previous_session_end(
                schedule_scraper, date_str, stadium_code
            )
            if session_end is None:
                stats["stadiums_failed"] += 1
                logging_module.info(
                    "motor_history_no_previous_session",
                    date=date_str,
                    stadium=stadium_code,
                )
                continue

            path = history_csv_path(project_root, session_end)
            if f"{stadium_code:02d}" in recorded_stadiums(path):
                stats["stadiums_skipped"] += 1
                continue

            entries = scraper.scrape_motor_history(session_end, stadium_code)
            if entries is None:
                stats["stadiums_failed"] += 1
                logging_module.info(
                    "motor_history_unavailable",
                    date=date_str,
                    stadium=stadium_code,
                    session_end=session_end,
                )
                continue

            rows = [motor_history_entry_to_row(e) for e in entries]
            if dry_run:
                logging_module.info(
                    "motor_history_dry_run",
                    stadium=stadium_code,
                    session_end=session_end,
                    rows=len(rows),
                )
            else:
                append_history_rows(path, rows)
                rel = str(path.relative_to(project_root))
                if rel not in stats["csv_paths"]:
                    stats["csv_paths"].append(rel)
            stats["stadiums_recorded"] += 1
            stats["entries"] += len(rows)

        except Exception as e:
            stats["stadiums_failed"] += 1
            stats["errors"].append(
                {
                    "date": date_str,
                    "error_type": "motor_history_scrape_error",
                    "message": str(e),
                    "stadium": stadium_code,
                }
            )

    logging_module.info(
        "motor_history_complete",
        date=date_str,
        recorded=stats["stadiums_recorded"],
        skipped=stats["stadiums_skipped"],
        failed=stats["stadiums_failed"],
        entries=stats["entries"],
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

        history_stats = process_motor_history(
            args.date,
            config,
            rate_limiter,
            set(stats["open_stadium_codes"]),
            dry_run=args.dry_run,
        )

        print()
        print(f"Motor Stats Data Processing Complete for {args.date}")
        print(f"  Stadiums open: {stats['stadiums_open']}")
        print(f"  Stadiums scraped: {stats['stadiums_scraped']}")
        print(f"  Stadiums failed: {stats['stadiums_failed']}")
        print(f"  Motors scraped: {stats['motors_scraped']}")
        print(
            "  Motor history: "
            f"{history_stats['stadiums_recorded']} recorded, "
            f"{history_stats['stadiums_skipped']} already present, "
            f"{history_stats['stadiums_failed']} failed, "
            f"{history_stats['entries']} entries"
        )
        print(f"  CSV files created: {stats['csv_files_created']}")
        print(f"  CSV files skipped: {stats['csv_files_skipped']}")
        errors = stats["errors"] + history_stats["errors"]
        if errors:
            print(f"  Errors: {len(errors)}")
            for error in errors:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        commit_paths: List[str] = []
        if stats["csv_files_created"] > 0:
            year, month, day = args.date.split("-")
            commit_paths.append(f"{OUTPUT_DIR}/{year}/{month}/{day}.csv")
        commit_paths.extend(history_stats["csv_paths"])

        if commit_paths and not args.dry_run:
            message = f"Update boatrace motor stats data: {args.date}"
            if git_operations.commit_and_push(commit_paths, message):
                print(f"Git commit and push successful for {commit_paths}")
            else:
                print(f"Git commit and push failed for {commit_paths}")

        sys.exit(
            0
            if stats["csv_files_created"] > 0
            or stats["csv_files_skipped"] > 0
            or history_stats["csv_paths"]
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
