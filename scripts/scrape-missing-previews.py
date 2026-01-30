#!/usr/bin/env python3
"""
Scrape missing preview data and batch commit by date.

This script processes missing preview race codes by date:
1. Groups races by date
2. Scrapes all races for that date
3. Commits changes for that date
4. Repeats for next date

Usage:
    python scrape-missing-previews.py
    python scrape-missing-previews.py --input missing_previews_recent.csv
    python scrape-missing-previews.py --push  # Commit and push changes
"""

import argparse
import csv
import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace.downloader import RateLimiter
from boatrace.preview_scraper import PreviewScraper
from boatrace.converter import previews_to_csv, VENUE_CODES
from boatrace.storage import write_csv
from boatrace import git_operations


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
        print(f"config_load_error: {e}", file=sys.stderr)

    return {}


def load_missing_previews(csv_path):
    """Load missing previews from CSV file."""
    races_by_date = defaultdict(list)

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                race_code = row.get('race_code', '').strip()
                date_str = row.get('date', '').strip()
                stadium = int(row.get('stadium', 0))
                race = int(row.get('race', 0))

                if race_code and date_str:
                    races_by_date[date_str].append({
                        'race_code': race_code,
                        'date': date_str,
                        'stadium': stadium,
                        'race': race,
                    })
    except Exception as e:
        print(f"Error loading {csv_path}: {e}", file=sys.stderr)
        return None

    return races_by_date


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def scrape_race_preview(date_str, stadium_code, race_number, config, rate_limiter):
    """Scrape preview data for a single race."""
    try:
        preview_scraper = PreviewScraper(
            timeout_seconds=config.get("preview_scraper_timeout", 30),
            rate_limiter=rate_limiter,
        )

        preview = preview_scraper.scrape_race_preview(
            date_str, stadium_code, race_number
        )

        if not preview:
            return None

        csv_content = previews_to_csv([preview])
        return csv_content if csv_content else None

    except Exception as e:
        print(f"Error scraping {date_str} Stadium {stadium_code} Race {race_number}: {e}", file=sys.stderr)
        return None


def append_preview_csv(csv_path, csv_content):
    """Append preview CSV content to existing file."""
    import io
    from io import StringIO

    try:
        incoming_reader = csv.DictReader(StringIO(csv_content))
        incoming_rows = list(incoming_reader)

        if not incoming_rows:
            return False

        fieldnames = incoming_reader.fieldnames
        if fieldnames is None:
            fieldnames = list(incoming_rows[0].keys())

        # Load existing rows
        existing_rows = []
        if csv_path.exists():
            with open(csv_path, 'r', encoding='utf-8') as f:
                existing_reader = csv.DictReader(f)
                if existing_reader.fieldnames:
                    fieldnames = existing_reader.fieldnames
                    existing_rows = list(existing_reader)

        # Combine and deduplicate by race code
        existing_set = {row.get('レースコード'): row for row in existing_rows}
        for row in incoming_rows:
            race_code = row.get('レースコード')
            if race_code:
                existing_set[race_code] = row

        all_rows = list(existing_set.values())

        # Write as CSV string
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerows(all_rows)
        csv_content_str = output.getvalue()
        output.close()

        # Write using write_csv
        return write_csv(str(csv_path), csv_content_str, force_overwrite=True)

    except Exception as e:
        print(f"Error appending to {csv_path}: {e}", file=sys.stderr)
        return False


def process_date(date_str, races, config, rate_limiter, repo_root):
    """Process all races for a given date."""
    print(f"\nProcessing {date_str} ({len(races)} races)...", file=sys.stderr)

    year, month, day = date_str.split('-')
    csv_path = repo_root / f"data/previews/{year}/{month}/{day}.csv"

    succeeded = 0
    failed = 0

    for idx, race_info in enumerate(races, 1):
        print(f"  [{idx}/{len(races)}] Stadium {race_info['stadium']:2d} Race {race_info['race']:2d}... ", end='', file=sys.stderr, flush=True)

        csv_content = scrape_race_preview(
            date_str,
            race_info['stadium'],
            race_info['race'],
            config,
            rate_limiter
        )

        if csv_content:
            if append_preview_csv(csv_path, csv_content):
                print("✓", file=sys.stderr)
                succeeded += 1
            else:
                print("✗ (append failed)", file=sys.stderr)
                failed += 1
        else:
            print("✗ (scrape failed)", file=sys.stderr)
            failed += 1

    print(f"  Result: {succeeded} succeeded, {failed} failed", file=sys.stderr)
    return succeeded, failed


def main():
    parser = argparse.ArgumentParser(
        description="Scrape missing preview data and batch commit by date"
    )

    parser.add_argument(
        '--input',
        type=str,
        default='missing_previews_recent.csv',
        help='Input CSV file with missing previews (default: missing_previews_recent.csv)'
    )

    parser.add_argument(
        '--push',
        action='store_true',
        help='Commit and push changes to git'
    )

    args = parser.parse_args()

    repo_root = get_repo_root()
    input_file = Path(args.input)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}", file=sys.stderr)
        sys.exit(1)

    # Load configuration
    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    print("Loading missing previews from CSV...", file=sys.stderr)
    races_by_date = load_missing_previews(input_file)

    if not races_by_date:
        print("No missing previews found.", file=sys.stderr)
        sys.exit(0)

    print(f"Loaded {len(races_by_date)} dates with missing previews", file=sys.stderr)
    print(f"Total races: {sum(len(races) for races in races_by_date.values())}", file=sys.stderr)
    print("-" * 70, file=sys.stderr)

    # Initialize rate limiter
    rate_limiter = RateLimiter(
        interval_seconds=config.get("rate_limit_interval_seconds", 3)
    )

    # Process each date
    total_succeeded = 0
    total_failed = 0

    for date_str in sorted(races_by_date.keys()):
        succeeded, failed = process_date(
            date_str,
            races_by_date[date_str],
            config,
            rate_limiter,
            repo_root
        )
        total_succeeded += succeeded
        total_failed += failed

        # Commit for this date
        if succeeded > 0 and args.push:
            year, month, day = date_str.split('-')
            csv_file = f"data/previews/{year}/{month}/{day}.csv"
            message = f"Update missing boatrace previews: {date_str}"

            if git_operations.commit_and_push([csv_file], message):
                print(f"  ✓ Git commit and push successful for {date_str}", file=sys.stderr)
            else:
                print(f"  ✗ Git commit and push failed for {date_str}", file=sys.stderr)

    print("-" * 70, file=sys.stderr)
    print(f"Total result: {total_succeeded} succeeded, {total_failed} failed", file=sys.stderr)

    if total_failed == 0:
        print("✓ All previews processed successfully!", file=sys.stderr)
        sys.exit(0)
    else:
        print(f"✗ {total_failed} races failed", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
