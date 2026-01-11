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
from boatrace.downloader import download_boatrace_files, RateLimiter
from boatrace.extractor import extract_k_file, extract_b_file
from boatrace.parser import parse_result_file, parse_program_file
from boatrace.converter import races_to_csv, programs_to_csv, previews_to_csv
from boatrace.preview_scraper import PreviewScraper
from boatrace.storage import write_csv
from boatrace import git_operations


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
    """Process a single date.

    Returns:
        True if any files were successfully processed, False otherwise
    """
    logging_module.info(
        "processing_date",
        date=date_str,
    )

    session.dates_processed += 1

    # Download files
    k_content, b_content = download_boatrace_files(
        date_str,
        rate_limiter=rate_limiter,
        max_retries=config.get("max_retries", 3),
    )

    # Check if both files are missing (no races scheduled)
    if k_content is None and b_content is None:
        logging_module.info(
            "date_skipped",
            date=date_str,
            reason="no_races_scheduled",
        )
        return False

    files_processed = False

    # Determine project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent

    # Process K-file (results)
    if k_content:
        try:
            session.files_downloaded += 1

            # Extract
            k_text = extract_k_file(k_content)
            if k_text:
                session.files_decompressed += 1

                # Parse
                races = parse_result_file(k_text, date=date_str)
                if races:
                    session.files_parsed += 1

                    # Convert
                    csv_content = races_to_csv(races)
                    if csv_content:
                        session.files_converted += 1

                        # Write
                        if not session.dry_run:
                            year, month, day = date_str.split("-")
                            csv_path = project_root / f"data/results/{year}/{month}/{day}.csv"
                            if write_csv(str(csv_path), csv_content, session.force_overwrite):
                                session.csv_files_created += 1
                                files_processed = True
                            else:
                                session.csv_files_skipped += 1
                        else:
                            session.csv_files_created += 1
                            files_processed = True

        except Exception as e:
            session.add_error(
                date=date_str,
                error_type="k_file_processing_error",
                message=str(e),
                file_type="K",
            )
            logging_module.error(
                "k_file_processing_error",
                date=date_str,
                error=str(e),
            )

    # Process B-file (programs)
    if b_content:
        try:
            session.files_downloaded += 1

            # Extract
            b_text = extract_b_file(b_content)
            if b_text:
                session.files_decompressed += 1

                # Parse
                programs = parse_program_file(b_text, date=date_str)
                if programs:
                    session.files_parsed += 1

                    # Convert
                    csv_content = programs_to_csv(programs)
                    if csv_content:
                        session.files_converted += 1

                        # Write
                        if not session.dry_run:
                            year, month, day = date_str.split("-")
                            csv_path = project_root / f"data/programs/{year}/{month}/{day}.csv"
                            if write_csv(str(csv_path), csv_content, session.force_overwrite):
                                session.csv_files_created += 1
                                files_processed = True
                            else:
                                session.csv_files_skipped += 1
                        else:
                            session.csv_files_created += 1
                            files_processed = True

        except Exception as e:
            session.add_error(
                date=date_str,
                error_type="b_file_processing_error",
                message=str(e),
                file_type="B",
            )
            logging_module.error(
                "b_file_processing_error",
                date=date_str,
                error=str(e),
            )

    # Process Preview data (HTML scraping)
    if config.get("enable_preview_scraping", True):
        try:
            previews = []
            preview_scraper = PreviewScraper(
                timeout_seconds=config.get("preview_scraper_timeout", 30),
                rate_limiter=rate_limiter,
            )

            # Scrape all stadiums (1-24) and races (1-12)
            for stadium_code in range(1, 25):
                for race_number in range(1, 13):
                    try:
                        preview = preview_scraper.scrape_race_preview(
                            date_str, stadium_code, race_number
                        )
                        if preview:
                            previews.append(preview)
                            session.previews_scraped += 1
                    except Exception as e:
                        session.previews_failed += 1
                        session.add_error(
                            date=date_str,
                            error_type="preview_scrape_error",
                            message=str(e),
                            file_type="Preview",
                        )
                        logging_module.debug(
                            "preview_scrape_failed",
                            date=date_str,
                            stadium=stadium_code,
                            race=race_number,
                            error=str(e),
                        )

            # Convert and save previews if any were scraped
            if previews:
                csv_content = previews_to_csv(previews)
                if csv_content:
                    # Write
                    if not session.dry_run:
                        year, month, day = date_str.split("-")
                        csv_path = project_root / f"data/previews/{year}/{month}/{day}.csv"
                        if write_csv(str(csv_path), csv_content, session.force_overwrite):
                            session.csv_files_created += 1
                            files_processed = True
                        else:
                            session.csv_files_skipped += 1
                    else:
                        session.csv_files_created += 1
                        files_processed = True

        except Exception as e:
            session.add_error(
                date=date_str,
                error_type="preview_processing_error",
                message=str(e),
                file_type="Preview",
            )
            logging_module.error(
                "preview_processing_error",
                date=date_str,
                error=str(e),
            )

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

        csv_files = []

        while current_dt <= end_dt:
            current_date = current_dt.strftime("%Y-%m-%d")

            if process_date(current_date, session, config, rate_limiter):
                # Collect CSV file paths for git commit (only if they exist)
                project_root = Path(__file__).parent.parent
                year, month, day = current_date.split("-")

                results_csv = project_root / f"data/results/{year}/{month}/{day}.csv"
                programs_csv = project_root / f"data/programs/{year}/{month}/{day}.csv"
                previews_csv = project_root / f"data/previews/{year}/{month}/{day}.csv"

                if results_csv.exists():
                    csv_files.append(f"data/results/{year}/{month}/{day}.csv")
                if programs_csv.exists():
                    csv_files.append(f"data/programs/{year}/{month}/{day}.csv")
                if previews_csv.exists():
                    csv_files.append(f"data/previews/{year}/{month}/{day}.csv")

            current_dt += timedelta(days=1)

        session.end_time = datetime.now()

        # Git operations (if files were created and not dry-run)
        if csv_files and not session.dry_run:
            logging_module.info(
                "git_commit_start",
                files_count=len(csv_files),
            )

            message = f"Update boatrace data: {start_date} to {end_date}"
            if git_operations.commit_and_push(csv_files, message):
                session.git_push_success = True
                # Extract commit hash from git
                result = git_operations.get_git_config("user.name")  # Just to get last commit
                logging_module.info("git_success")
            else:
                session.git_push_success = False
                logging_module.error("git_failed")

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
