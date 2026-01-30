#!/usr/bin/env python3
"""
Scrape and append preview data for a specific race.

This script fetches preview data for a single race and appends it to the
existing CSV file, rather than replacing the entire day's data like preview.py does.

Usage:
    python preview-race.py --date 2026-01-01 --stadium 14 --race 1
    python preview-race.py --date 2026-01-01 --race-code 202601011401
"""

import argparse
import csv
import io
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from io import StringIO

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace.downloader import RateLimiter
from boatrace.preview_scraper import PreviewScraper
from boatrace.converter import previews_to_csv, VENUE_CODES
from boatrace.storage import write_csv
from boatrace import git_operations


def parse_race_code(race_code: str) -> tuple:
    """
    Parse race code to extract date, stadium, and race number.

    Race code format: YYYYMMDDSSRR
    - YYYY: year
    - MM: month
    - DD: day
    - SS: stadium code (2 digits)
    - RR: race round (2 digits)

    Args:
        race_code: Race code string (e.g., "202601011401")

    Returns:
        Tuple of (date_str, stadium_code, race_number)
        Example: ("2026-01-01", 14, 1)
    """
    try:
        if len(race_code) != 12:
            raise ValueError(f"Invalid race code length: {race_code}")

        year = race_code[0:4]
        month = race_code[4:6]
        day = race_code[6:8]
        stadium = int(race_code[8:10])
        race = int(race_code[10:12])

        date_str = f"{year}-{month}-{day}"
        return date_str, stadium, race
    except (ValueError, IndexError) as e:
        raise ValueError(f"Failed to parse race code '{race_code}': {e}")


def load_config(config_path: str = ".boatrace/config.json") -> dict:
    """Load configuration from file."""
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


def race_exists_in_csv(csv_path: Path, stadium_code: int, race_number: int) -> bool:
    """Check if a race already exists in the CSV file."""
    if not csv_path.exists():
        return False

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return False

            for row in reader:
                if (row.get('レース場') and row.get('レース回') and
                    str(row['レース場']).strip() == str(stadium_code) and
                    str(row['レース回']).strip() == f"{race_number:02d}R"):
                    return True
    except Exception as e:
        logging_module.debug("race_check_error", error=str(e))

    return False


def remove_race_from_csv(csv_path: Path, stadium_code: int, race_number: int) -> bool:
    """Remove existing race entries from CSV file (for overwrite)."""
    if not csv_path.exists():
        return True

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                return True

            rows = [row for row in reader
                    if not (str(row.get('レース場', '')).strip() == str(stadium_code) and
                            str(row.get('レース回', '')).strip() == f"{race_number:02d}R")]

        # Write back without the target race
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return True
    except Exception as e:
        logging_module.error("race_removal_error", error=str(e))
        return False


def append_preview_to_csv(csv_path: Path, preview_data: str, overwrite: bool = False) -> bool:
    """
    Append preview data to existing CSV file.

    Args:
        csv_path: Path to CSV file
        preview_data: CSV content to append
        overwrite: If True, replace existing entries for the same race

    Returns:
        True if successful, False otherwise
    """
    try:
        # Parse incoming data
        incoming_reader = csv.DictReader(StringIO(preview_data))
        incoming_rows = list(incoming_reader)

        if not incoming_rows:
            logging_module.warning("append_preview_no_data", path=str(csv_path))
            return False

        # Extract stadium and race from first row
        first_row = incoming_rows[0]
        stadium_code = str(first_row.get('レース場', '')).strip()
        race_round = str(first_row.get('レース回', '')).strip()

        fieldnames = incoming_reader.fieldnames
        if fieldnames is None:
            fieldnames = list(first_row.keys())

        # If file exists, merge data
        existing_rows = []
        if csv_path.exists():
            with open(csv_path, 'r', encoding='utf-8') as f:
                existing_reader = csv.DictReader(f)
                if existing_reader.fieldnames:
                    fieldnames = existing_reader.fieldnames
                    existing_rows = list(existing_reader)

            # Remove existing entries for this race if overwrite is True
            if overwrite:
                existing_rows = [row for row in existing_rows
                                if not (str(row.get('レース場', '')).strip() == stadium_code and
                                        str(row.get('レース回', '')).strip() == race_round)]

            # Add new rows
            existing_rows.extend(incoming_rows)
            all_rows = existing_rows
        else:
            all_rows = incoming_rows

        # Convert to CSV string with explicit line terminator
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerows(all_rows)
        csv_content = output.getvalue()
        output.close()

        # Write using write_csv for consistency with preview.py
        if not write_csv(str(csv_path), csv_content, force_overwrite=True):
            logging_module.warning("append_preview_write_failed", path=str(csv_path))
            return False

        logging_module.info(
            "append_preview_success",
            path=str(csv_path),
            rows_appended=len(incoming_rows),
        )
        return True

    except Exception as e:
        logging_module.error("append_preview_error", error=str(e))
        return False


def scrape_single_race(
    date_str: str,
    stadium_code: int,
    race_number: int,
    config: dict,
    rate_limiter: RateLimiter,
) -> str:
    """
    Scrape preview data for a single race.

    Returns:
        CSV content string, or empty string if scraping failed
    """
    try:
        preview_scraper = PreviewScraper(
            timeout_seconds=config.get("preview_scraper_timeout", 30),
            rate_limiter=rate_limiter,
        )

        logging_module.info(
            "race_scrape_start",
            date=date_str,
            stadium=stadium_code,
            race=race_number,
        )

        preview = preview_scraper.scrape_race_preview(
            date_str, stadium_code, race_number
        )

        if not preview:
            logging_module.warning(
                "race_scrape_empty",
                date=date_str,
                stadium=stadium_code,
                race=race_number,
            )
            return ""

        # Convert to CSV
        csv_content = previews_to_csv([preview])

        logging_module.info(
            "race_scrape_success",
            date=date_str,
            stadium=stadium_code,
            race=race_number,
            csv_size=len(csv_content.encode("utf-8")),
        )

        return csv_content

    except Exception as e:
        logging_module.error(
            "race_scrape_error",
            date=date_str,
            stadium=stadium_code,
            race=race_number,
            error=str(e),
        )
        return ""


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Scrape and append preview data for a specific race"
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Date in YYYY-MM-DD format (required if not using --race-code)",
    )

    parser.add_argument(
        "--stadium",
        type=int,
        help="Stadium code (required if not using --race-code)",
    )

    parser.add_argument(
        "--race",
        type=int,
        help="Race round number 1-12 (required if not using --race-code)",
    )

    parser.add_argument(
        "--race-code",
        type=str,
        help="Race code in format YYYYMMDDSSRR (alternative to --date/--stadium/--race)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing data for this race instead of appending",
    )

    parser.add_argument(
        "--push",
        action="store_true",
        help="Commit and push changes to git",
    )

    return parser, parser.parse_args()


def main():
    """Main execution."""
    parser, args = parse_arguments()

    # Parse arguments
    if args.race_code:
        try:
            date_str, stadium_code, race_number = parse_race_code(args.race_code)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    elif args.date and args.stadium is not None and args.race is not None:
        date_str = args.date
        stadium_code = args.stadium
        race_number = args.race

        # Validate
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            if not (1 <= stadium_code <= 24) or not (1 <= race_number <= 12):
                raise ValueError()
        except ValueError:
            print("Error: Invalid date, stadium code, or race number")
            sys.exit(1)
    else:
        print("Error: Specify either --race-code or --date/--stadium/--race")
        parser.print_help()
        sys.exit(1)

    # Load config and initialize logger
    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    logging_module.info(
        "race_cli_start",
        date=date_str,
        stadium=stadium_code,
        race=race_number,
        overwrite=args.overwrite,
    )

    print(f"Processing race: {date_str} Stadium {stadium_code} Race {race_number}")

    try:
        # Create rate limiter
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        # Scrape preview data
        csv_content = scrape_single_race(
            date_str, stadium_code, race_number, config, rate_limiter
        )

        if not csv_content:
            print("Error: Failed to scrape preview data")
            sys.exit(1)

        # Determine output path
        year, month, day = date_str.split("-")
        project_root = Path(__file__).parent.parent
        csv_path = project_root / f"data/previews/{year}/{month}/{day}.csv"

        print(f"Appending to: {csv_path}")

        # Append or overwrite preview data
        if append_preview_to_csv(csv_path, csv_content, overwrite=args.overwrite):
            print(f"✓ Preview data saved to {csv_path}")

            # Git commit and push if requested
            if args.push:
                relative_path = f"data/previews/{year}/{month}/{day}.csv"
                message = f"Update boatrace preview: {date_str} Stadium {stadium_code} Race {race_number}"

                logging_module.info(
                    "race_git_commit_start",
                    date=date_str,
                    file=relative_path,
                )

                if git_operations.commit_and_push([relative_path], message):
                    print(f"✓ Git commit and push successful")
                    logging_module.info(
                        "race_git_commit_success",
                        date=date_str,
                        file=relative_path,
                    )
                else:
                    print(f"✗ Git commit and push failed")
                    logging_module.error(
                        "race_git_commit_failed",
                        date=date_str,
                        file=relative_path,
                    )
            sys.exit(0)
        else:
            print("Error: Failed to append preview data")
            sys.exit(1)

    except Exception as e:
        logging_module.critical(
            "race_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
