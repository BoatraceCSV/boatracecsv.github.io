#!/usr/bin/env python3
"""Main entry point for boatrace data fetch and convert automation."""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace.models import ConversionSession
from boatrace.downloader import RateLimiter
from boatrace import git_operations
from result import process_result
from program import process_program
from preview import process_preview


def load_config(config_path: str = ".boatrace/config.json") -> dict:
    """Load configuration from file."""
    try:
        # Ensure config_path is relative to project root
        config_file = Path(config_path)
        
        # If config_path is relative and doesn't exist from current directory,
        # try from parent directory (project root)
        if not config_file.is_absolute() and not config_file.exists():
            # Try one level up
            config_file = Path(__file__).parent.parent / config_path
        
        if config_file.exists():
            with open(config_file) as f:
                return json.load(f)
    except Exception as e:
        logging_module.error(
            "config_load_error",
            error=str(e),
        )
    
    # Return defaults if config not found
    return {}


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch and convert boatrace data to CSV"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). Default: yesterday",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD). Default: today",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["daily", "backfill"],
        default="daily",
        help="Execution mode",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing CSV files",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing files",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    return parser.parse_args()


def validate_date_format(date_str: str) -> bool:
    """Validate date format YYYY-MM-DD."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_date_range(
    start_date: str = None,
    end_date: str = None,
    mode: str = "daily",
) -> tuple:
    """Get date range for processing.

    Returns:
        Tuple of (start_date_str, end_date_str)
    """
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    if mode == "daily":
        # Default: yesterday to today
        start = start_date or yesterday.strftime("%Y-%m-%d")
        end = end_date or today.strftime("%Y-%m-%d")
    else:
        # Backfill mode: explicit dates required
        if not start_date or not end_date:
            raise ValueError(
                "Backfill mode requires --start-date and --end-date"
            )
        start = start_date
        end = end_date

    # Validate formats
    if not validate_date_format(start):
        raise ValueError(f"Invalid start date format: {start}")
    if not validate_date_format(end):
        raise ValueError(f"Invalid end date format: {end}")

    # Validate range
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    if start_dt > end_dt:
        raise ValueError(
            f"Start date ({start}) must be before or equal to end date ({end})"
        )

    return start, end


def process_date(
    date_str: str,
    session: ConversionSession,
    config: dict,
    rate_limiter: RateLimiter,
) -> bool:
    """Process a single date by calling dedicated processing functions.

    Returns:
        True if any files were successfully processed, False otherwise
    """
    logging_module.info(
        "processing_date",
        date=date_str,
    )

    session.dates_processed += 1

    files_processed = False

    # Process results (K-file)
    result_stats = process_result(
        date_str,
        config,
        rate_limiter,
        force_overwrite=session.force_overwrite,
        dry_run=session.dry_run,
    )
    session.files_downloaded += result_stats["files_downloaded"]
    session.files_decompressed += result_stats["files_decompressed"]
    session.files_parsed += result_stats["files_parsed"]
    session.files_converted += result_stats["files_converted"]
    session.csv_files_created += result_stats["csv_files_created"]
    session.csv_files_skipped += result_stats["csv_files_skipped"]
    for error in result_stats["errors"]:
        session.add_error(
            date=error["date"],
            error_type=error["error_type"],
            message=error["message"],
            file_type="K",
        )
    if result_stats["csv_files_created"] > 0:
        files_processed = True

    # Process programs (B-file)
    program_stats = process_program(
        date_str,
        config,
        rate_limiter,
        force_overwrite=session.force_overwrite,
        dry_run=session.dry_run,
    )
    session.files_downloaded += program_stats["files_downloaded"]
    session.files_decompressed += program_stats["files_decompressed"]
    session.files_parsed += program_stats["files_parsed"]
    session.files_converted += program_stats["files_converted"]
    session.csv_files_created += program_stats["csv_files_created"]
    session.csv_files_skipped += program_stats["csv_files_skipped"]
    for error in program_stats["errors"]:
        session.add_error(
            date=error["date"],
            error_type=error["error_type"],
            message=error["message"],
            file_type="B",
        )
    if program_stats["csv_files_created"] > 0:
        files_processed = True

    # Process previews
    preview_stats = process_preview(
        date_str,
        config,
        rate_limiter,
        force_overwrite=session.force_overwrite,
        dry_run=session.dry_run,
    )
    session.previews_scraped += preview_stats["previews_scraped"]
    session.previews_failed += preview_stats["previews_failed"]
    session.csv_files_created += preview_stats["csv_files_created"]
    session.csv_files_skipped += preview_stats["csv_files_skipped"]
    for error in preview_stats["errors"]:
        session.add_error(
            date=error["date"],
            error_type=error["error_type"],
            message=error["message"],
            file_type="Preview",
        )
    if preview_stats["csv_files_created"] > 0:
        files_processed = True

    return files_processed


def main():
    """Main execution."""
    args = parse_arguments()

    # Load configuration
    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    # Log start
    logging_module.info(
        "start",
        mode=args.mode,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        # Get date range
        start_date, end_date = get_date_range(
            start_date=args.start_date,
            end_date=args.end_date,
            mode=args.mode,
        )

        # Create session
        session = ConversionSession(
            start_date=start_date,
            end_date=end_date,
            mode=args.mode,
            dry_run=args.dry_run,
            force_overwrite=args.force,
        )
        session.start_time = datetime.now()

        logging_module.info(
            "processing_range",
            start_date=start_date,
            end_date=end_date,
            mode=args.mode,
        )

        # Create rate limiter
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        # Generate date range
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        current_dt = start_dt

        git_push_results = []  # Track git push results for each date

        while current_dt <= end_dt:
            current_date = current_dt.strftime("%Y-%m-%d")

            if process_date(current_date, session, config, rate_limiter):
                # Collect CSV file paths for git commit (only if they exist)
                project_root = Path(__file__).parent.parent
                year, month, day = current_date.split("-")

                results_csv = project_root / f"data/results/{year}/{month}/{day}.csv"
                programs_csv = project_root / f"data/programs/{year}/{month}/{day}.csv"
                previews_csv = project_root / f"data/previews/{year}/{month}/{day}.csv"

                day_csv_files = []
                if results_csv.exists():
                    day_csv_files.append(f"data/results/{year}/{month}/{day}.csv")
                if programs_csv.exists():
                    day_csv_files.append(f"data/programs/{year}/{month}/{day}.csv")
                if previews_csv.exists():
                    day_csv_files.append(f"data/previews/{year}/{month}/{day}.csv")

                # Git operations for this day (if files exist and not dry-run)
                if day_csv_files and not session.dry_run:
                    logging_module.info(
                        "git_commit_start",
                        date=current_date,
                        files_count=len(day_csv_files),
                    )

                    message = f"Update boatrace data: {current_date}"
                    if git_operations.commit_and_push(day_csv_files, message):
                        session.git_push_success = True
                        git_push_results.append(True)
                        logging_module.info(
                            "git_success",
                            date=current_date,
                        )
                    else:
                        session.git_push_success = False
                        git_push_results.append(False)
                        logging_module.error(
                            "git_failed",
                            date=current_date,
                        )

            current_dt += timedelta(days=1)

        session.end_time = datetime.now()

        # Print summary
        print()
        print(session.summary())
        print()

        # Exit with appropriate code
        sys.exit(session.exit_code())

    except ValueError as e:
        logging_module.error(
            "configuration_error",
            error=str(e),
        )
        print(f"Error: {str(e)}")
        sys.exit(3)

    except Exception as e:
        logging_module.critical(
            "unexpected_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
