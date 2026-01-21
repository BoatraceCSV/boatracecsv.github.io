#!/usr/bin/env python3
"""Process B-file (programs) boatrace data."""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace.downloader import download_file, RateLimiter
from boatrace.extractor import extract_b_file
from boatrace.parser import parse_program_file
from boatrace.converter import programs_to_csv
from boatrace.storage import write_csv
from boatrace import git_operations


def process_program(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Process B-file (programs) for next day given a date.

    The B-file contains programs for the next day, so if you want programs for
    a given date, you need to download the B-file from the previous day.

    Args:
        date_str: Date in YYYY-MM-DD format (program date, not download date)
        config: Configuration dictionary
        rate_limiter: Rate limiter instance
        force_overwrite: Whether to overwrite existing files
        dry_run: If True, don't write files

    Returns:
        Dictionary with processing statistics:
        {
            "files_downloaded": int,
            "files_decompressed": int,
            "files_parsed": int,
            "files_converted": int,
            "csv_files_created": int,
            "csv_files_skipped": int,
            "errors": list of error dicts,
        }
    """
    stats = {
        "files_downloaded": 0,
        "files_decompressed": 0,
        "files_parsed": 0,
        "files_converted": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "errors": [],
    }

    logging_module.info(
        "program_processing_start",
        date=date_str,
    )

    # Prepare date components for current date (program date)
    date_parts = date_str.split("-")
    year = date_parts[0]
    month = date_parts[1]
    day = date_parts[2]

    # For B-file, we need to download from previous day
    prev_date_obj = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    prev_date_str = prev_date_obj.strftime("%Y-%m-%d")
    prev_date_parts = prev_date_str.split("-")
    prev_year = prev_date_parts[0]
    prev_month = prev_date_parts[1]
    prev_day = prev_date_parts[2]
    prev_year_short = prev_year[2:]
    prev_file_date = f"{prev_year_short}{prev_month}{prev_day}"
    prev_year_month = f"{prev_year}{prev_month}"

    # Download B-file (programs) for next day
    base_url = "https://www1.mbrace.or.jp/od2"
    b_file_url = f"{base_url}/B/{prev_year_month}/b{prev_file_date}.lzh"

    logging_module.info(
        "program_downloading",
        date=date_str,
        download_date=prev_date_str,
        url=b_file_url,
    )

    b_content, b_status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    # Check if file is missing
    if b_status == 404:
        logging_module.info(
            "program_not_found",
            date=date_str,
            status=b_status,
        )
        return stats

    # Process B-file (programs)
    if b_content:
        try:
            stats["files_downloaded"] += 1

            # Extract
            b_text = extract_b_file(b_content)
            if b_text:
                stats["files_decompressed"] += 1

                # Parse
                programs = parse_program_file(b_text, date=date_str)
                if programs:
                    stats["files_parsed"] += 1

                    # Convert
                    csv_content = programs_to_csv(programs)
                    if csv_content:
                        stats["files_converted"] += 1

                        # Write
                        if not dry_run:
                            project_root = Path(__file__).parent.parent
                            csv_path = project_root / f"data/programs/{year}/{month}/{day}.csv"
                            if write_csv(str(csv_path), csv_content, force_overwrite):
                                stats["csv_files_created"] += 1
                                logging_module.info(
                                    "program_csv_written",
                                    date=date_str,
                                    path=str(csv_path),
                                )
                            else:
                                stats["csv_files_skipped"] += 1
                                logging_module.warning(
                                    "program_csv_skipped",
                                    date=date_str,
                                    path=str(csv_path),
                                )
                        else:
                            stats["csv_files_created"] += 1
                            logging_module.info(
                                "program_csv_dry_run",
                                date=date_str,
                            )

        except Exception as e:
            stats["errors"].append({
                "date": date_str,
                "error_type": "program_processing_error",
                "message": str(e),
            })
            logging_module.error(
                "program_processing_error",
                date=date_str,
                error=str(e),
            )

    logging_module.info(
        "program_processing_complete",
        date=date_str,
        stats=stats,
    )

    return stats


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
    # Get today's date in JST (UTC+9)
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Process B-file (programs) boatrace data"
    )

    parser.add_argument(
        "--date",
        type=str,
        default=today_jst,
        help="Date to process (YYYY-MM-DD format). Default: today (JST)",
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


def main():
    """Main execution."""
    args = parse_arguments()

    # Validate date format
    if not validate_date_format(args.date):
        print(f"Error: Invalid date format: {args.date}. Expected YYYY-MM-DD")
        sys.exit(1)

    # Load configuration
    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    # Log start
    logging_module.info(
        "program_cli_start",
        date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        # Create rate limiter
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        # Process the date
        stats = process_program(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        # Print summary
        print()
        print(f"Programs CSV Processing Complete for {args.date}")
        print(f"  Files downloaded: {stats['files_downloaded']}")
        print(f"  Files decompressed: {stats['files_decompressed']}")
        print(f"  Files parsed: {stats['files_parsed']}")
        print(f"  Files converted: {stats['files_converted']}")
        print(f"  CSV files created: {stats['csv_files_created']}")
        print(f"  CSV files skipped: {stats['csv_files_skipped']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        # Git commit and push if CSV files were created (not dry-run)
        if stats["csv_files_created"] > 0 and not args.dry_run:
            # Programs are saved with the program date, which is the argument date
            year, month, day = args.date.split("-")
            csv_file = f"data/programs/{year}/{month}/{day}.csv"

            logging_module.info(
                "program_git_commit_start",
                date=args.date,
                file=csv_file,
            )

            message = f"Update boatrace programs: {args.date}"
            if git_operations.commit_and_push([csv_file], message):
                print(f"Git commit and push successful for {csv_file}")
                logging_module.info(
                    "program_git_commit_success",
                    date=args.date,
                    file=csv_file,
                )
            else:
                print(f"Git commit and push failed for {csv_file}")
                logging_module.error(
                    "program_git_commit_failed",
                    date=args.date,
                    file=csv_file,
                )

        # Exit with appropriate code
        sys.exit(0 if stats["csv_files_created"] > 0 or stats["csv_files_skipped"] > 0 else 1)

    except Exception as e:
        logging_module.critical(
            "program_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
