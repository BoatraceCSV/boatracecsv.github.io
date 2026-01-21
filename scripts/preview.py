#!/usr/bin/env python3
"""Process preview data via web scraping."""

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
from boatrace.converter import previews_to_csv, VENUE_CODES
from boatrace.preview_scraper import PreviewScraper
from boatrace.storage import write_csv
from boatrace import git_operations


def process_preview(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Process preview data via web scraping.

    Preview data is scraped for the given date. Programs are obtained from
    the next day's B-file to determine which races exist.

    Args:
        date_str: Date in YYYY-MM-DD format
        config: Configuration dictionary
        rate_limiter: Rate limiter instance
        force_overwrite: Whether to overwrite existing files
        dry_run: If True, don't write files

    Returns:
        Dictionary with processing statistics:
        {
            "previews_scraped": int,
            "previews_failed": int,
            "csv_files_created": int,
            "csv_files_skipped": int,
            "errors": list of error dicts,
        }
    """
    stats = {
        "previews_scraped": 0,
        "previews_failed": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "errors": [],
    }

    logging_module.info(
        "preview_processing_start",
        date=date_str,
    )

    # Prepare date components for current date
    date_parts = date_str.split("-")
    year = date_parts[0]
    month = date_parts[1]
    day = date_parts[2]

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

    # Download B-file (programs) for next day
    base_url = "https://www1.mbrace.or.jp/od2"
    b_file_url = f"{base_url}/B/{next_year_month}/b{next_file_date}.lzh"

    logging_module.info(
        "preview_b_file_downloading",
        date=date_str,
        url=b_file_url,
    )

    b_content, b_status = download_file(
        b_file_url,
        max_retries=config.get("max_retries", 3),
        rate_limiter=rate_limiter,
    )

    # Extract actual races from current date programs
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
                            stats["previews_scraped"] += 1
                    except Exception as e:
                        stats["previews_failed"] += 1
                        stats["errors"].append({
                            "date": date_str,
                            "error_type": "preview_scrape_error",
                            "message": str(e),
                            "stadium": stadium_code,
                            "race": race_number,
                        })
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
                    if not dry_run:
                        project_root = Path(__file__).parent.parent
                        csv_path = project_root / f"data/previews/{year}/{month}/{day}.csv"
                        logging_module.info(
                            "preview_csv_write_start",
                            date=date_str,
                            path=str(csv_path),
                        )

                        if write_csv(str(csv_path), csv_content, force_overwrite):
                            stats["csv_files_created"] += 1
                            logging_module.info(
                                "preview_csv_write_success",
                                date=date_str,
                                path=str(csv_path),
                            )
                        else:
                            stats["csv_files_skipped"] += 1
                            logging_module.warning(
                                "preview_csv_write_skipped",
                                date=date_str,
                                path=str(csv_path),
                            )
                    else:
                        stats["csv_files_created"] += 1
                        logging_module.info(
                            "preview_csv_write_dry_run",
                            date=date_str,
                            preview_count=len(previews),
                        )

        except Exception as e:
            stats["errors"].append({
                "date": date_str,
                "error_type": "preview_processing_error",
                "message": str(e),
            })
            logging_module.error(
                "preview_processing_error",
                date=date_str,
                error=str(e),
            )
    else:
        if not actual_races:
            logging_module.info(
                "preview_skipped_no_races",
                date=date_str,
            )

    logging_module.info(
        "preview_processing_complete",
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
    # Get yesterday's date in JST (UTC+9)
    jst = timezone(timedelta(hours=9))
    yesterday_jst = (datetime.now(jst) - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description="Process preview data via web scraping"
    )

    parser.add_argument(
        "--date",
        type=str,
        default=yesterday_jst,
        help="Date to process (YYYY-MM-DD format). Default: yesterday (JST)",
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
        "preview_cli_start",
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
        stats = process_preview(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        # Print summary
        print()
        print(f"Preview CSV Processing Complete for {args.date}")
        print(f"  Previews scraped: {stats['previews_scraped']}")
        print(f"  Previews failed: {stats['previews_failed']}")
        print(f"  CSV files created: {stats['csv_files_created']}")
        print(f"  CSV files skipped: {stats['csv_files_skipped']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        # Git commit and push if CSV files were created (not dry-run)
        if stats["csv_files_created"] > 0 and not args.dry_run:
            year, month, day = args.date.split("-")
            csv_file = f"data/previews/{year}/{month}/{day}.csv"

            logging_module.info(
                "preview_git_commit_start",
                date=args.date,
                file=csv_file,
            )

            message = f"Update boatrace previews: {args.date}"
            if git_operations.commit_and_push([csv_file], message):
                print(f"Git commit and push successful for {csv_file}")
                logging_module.info(
                    "preview_git_commit_success",
                    date=args.date,
                    file=csv_file,
                )
            else:
                print(f"Git commit and push failed for {csv_file}")
                logging_module.error(
                    "preview_git_commit_failed",
                    date=args.date,
                    file=csv_file,
                )

        # Exit with appropriate code
        sys.exit(0 if stats["csv_files_created"] > 0 or stats["csv_files_skipped"] > 0 else 1)

    except Exception as e:
        logging_module.critical(
            "preview_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
