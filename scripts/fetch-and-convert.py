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
from boatrace.downloader import download_file, RateLimiter
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

    # Prepare date components for current date
    date_parts = date_str.split("-")
    year = date_parts[0]
    month = date_parts[1]
    day = date_parts[2]
    year_short = year[2:]
    file_date = f"{year_short}{month}{day}"
    year_month = f"{year}{month}"

    # Prepare next date for Programs download
    next_date_obj = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
    next_date_str = next_date_obj.strftime("%Y-%m-%d")
    next_date_parts = next_date_str.split("-")
    next_year = next_date_parts[0]
    next_month = next_date_parts[1]
    next_day = next_date_parts[2]
    next_year_short = next_year[2:]
    next_file_date = f"{next_year_short}{next_month}{next_day}"
    next_year_month = f"{next_year}{next_month}"

    # Download K-file (results) for current date
    base_url = "https://www1.mbrace.or.jp/od2"
    k_file_url = f"{base_url}/K/{year_month}/k{file_date}.lzh"
    
    logging_module.info(
        "downloading_file",
        file_type="K",
        date=date_str,
        url=k_file_url,
    )
    
    k_content, k_status = download_file(
        k_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    # Download B-file (programs) for next day
    b_file_url = f"{base_url}/B/{next_year_month}/b{next_file_date}.lzh"
    
    logging_module.info(
        "downloading_file",
        file_type="B",
        date=next_date_str,
        url=b_file_url,
    )
    
    b_content, b_status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    # Check if both files are missing (no races scheduled)
    if k_status == 404 and b_status == 404:
        logging_module.info(
            "date_skipped",
            date=date_str,
            reason="no_races_scheduled",
        )
        return False

    files_processed = False

    # Determine project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent

    # Process K-file (results) - use current date
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

    # Process Preview data (HTML scraping) using current date programs
    # First, extract actual races from current date programs (from next day's B-file)
    actual_races = set()  # Set of (stadium_code, race_number) tuples
    programs = None

    if b_content:
        try:
            # Extract next day's B-file for preview scraping reference
            b_text = extract_b_file(b_content)
            if b_text:
                programs = parse_program_file(b_text, date=next_date_str)
        except Exception:
            pass

    if programs:
        logging_module.info(
            "preview_races_extraction_start",
            date=date_str,
            program_count=len(programs),
        )

        # Import VENUE_CODES from converter for stadium name -> code mapping
        from boatrace.converter import VENUE_CODES

        for program in programs:
            # Extract stadium_code from program.stadium using VENUE_CODES mapping
            stadium_code = VENUE_CODES.get(program.stadium)
            if not stadium_code:
                logging_module.debug(
                    "preview_stadium_not_found",
                    date=date_str,
                    stadium_name=program.stadium,
                )
                continue

            # Extract race_number from program.race_round (e.g., "1R" -> 1, "01R" -> 1)
            try:
                if not program.race_round:
                    continue

                # Remove 'R' suffix and convert to int
                race_round_num = program.race_round.rstrip('R')  # "01R" -> "01", "1R" -> "1"
                race_number = int(race_round_num)  # "01" or "1" -> 1

                # Validate race number is in valid range (1-12)
                if race_number < 1 or race_number > 12:
                    logging_module.debug(
                        "preview_race_number_invalid",
                        date=date_str,
                        race_round=program.race_round,
                        race_number=race_number,
                        stadium=program.stadium,
                    )
                    continue

                actual_races.add((int(stadium_code), race_number))
            except (ValueError, IndexError, AttributeError):
                logging_module.debug(
                    "preview_race_number_parse_error",
                    date=date_str,
                    race_round=program.race_round,
                    stadium=program.stadium,
                )

        logging_module.info(
            "preview_races_extraction_complete",
            date=date_str,
            actual_race_count=len(actual_races),
        )

    # Only scrape if we have actual races from programs
    if config.get("enable_preview_scraping", True) and actual_races:
        try:
            previews = []
            preview_scraper = PreviewScraper(
                timeout_seconds=config.get("preview_scraper_timeout", 30),
                rate_limiter=rate_limiter,
            )

            logging_module.info(
                "preview_scraping_start",
                date=date_str,
                total_expected=len(actual_races),
            )

            # Group races by stadium for better logging
            races_by_stadium = {}
            for stadium_code, race_number in actual_races:
                if stadium_code not in races_by_stadium:
                    races_by_stadium[stadium_code] = []
                races_by_stadium[stadium_code].append(race_number)

            # Scrape only actual races from stadiums with programs
            for stadium_code in sorted(races_by_stadium.keys()):
                race_numbers = sorted(races_by_stadium[stadium_code])

                logging_module.info(
                    "preview_stadium_start",
                    date=date_str,
                    stadium=stadium_code,
                    races=race_numbers,
                    race_count=len(race_numbers),
                )

                for race_number in race_numbers:
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

                logging_module.info(
                    "preview_stadium_complete",
                    date=date_str,
                    stadium=stadium_code,
                    scraped_count=sum(1 for p in previews if p.stadium_number == stadium_code),
                )

            # Convert and save previews if any were scraped
            logging_module.info(
                "preview_scraping_complete",
                date=date_str,
                total_scraped=len(previews),
            )

            if previews:
                logging_module.info(
                    "preview_csv_conversion_start",
                    date=date_str,
                    preview_count=len(previews),
                )

                csv_content = previews_to_csv(previews)

                logging_module.info(
                    "preview_csv_conversion_complete",
                    date=date_str,
                    csv_size_bytes=len(csv_content.encode("utf-8")),
                )

                if csv_content:
                    # Write
                    if not session.dry_run:
                        csv_path = project_root / f"data/previews/{year}/{month}/{day}.csv"
                        logging_module.info(
                            "preview_csv_write_start",
                            date=date_str,
                            path=str(csv_path),
                        )

                        if write_csv(str(csv_path), csv_content, session.force_overwrite):
                            session.csv_files_created += 1
                            files_processed = True
                            logging_module.info(
                                "preview_csv_write_success",
                                date=date_str,
                                path=str(csv_path),
                            )
                        else:
                            session.csv_files_skipped += 1
                            logging_module.warning(
                                "preview_csv_write_skipped",
                                date=date_str,
                                path=str(csv_path),
                            )
                    else:
                        session.csv_files_created += 1
                        files_processed = True
                        logging_module.info(
                            "preview_csv_write_dry_run",
                            date=date_str,
                            preview_count=len(previews),
                        )

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

    # Process B-file (programs) for next day
    if b_content:
        try:
            session.files_downloaded += 1

            # Extract
            b_text = extract_b_file(b_content)
            if b_text:
                session.files_decompressed += 1

                # Parse
                programs_tomorrow = parse_program_file(b_text, date=next_date_str)
                if programs_tomorrow:
                    session.files_parsed += 1

                    # Convert
                    csv_content = programs_to_csv(programs_tomorrow)
                    if csv_content:
                        session.files_converted += 1

                        # Write to NEXT DAY directory
                        if not session.dry_run:
                            csv_path = project_root / f"data/programs/{next_year}/{next_month}/{next_day}.csv"
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
                date=next_date_str,
                error_type="b_file_processing_error",
                message=str(e),
                file_type="B",
            )
            logging_module.error(
                "b_file_processing_error",
                date=next_date_str,
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
