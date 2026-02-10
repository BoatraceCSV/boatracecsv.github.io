#!/usr/bin/env python3
"""
Find missing preview data for races that have results.

This script compares Results and Previews data to identify races that have
results but are missing preview data. The output is saved to a CSV file
that can be used with preview-race.py to fetch missing previews.

Usage:
    python find-missing-previews.py
    python find-missing-previews.py --output missing_previews.csv
"""

import argparse
import csv
import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.common import get_repo_root


def load_race_codes_from_csv(csv_path):
    """Load unique race codes from a CSV file."""
    race_codes = set()

    if not csv_path.exists():
        return race_codes

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                race_code = row.get('レースコード', '').strip()
                if race_code:
                    race_codes.add(race_code)
    except Exception as e:
        print(f"Error reading {csv_path}: {e}", file=sys.stderr)

    return race_codes


def scan_directory_for_race_codes(base_dir):
    """Scan directory recursively to collect all race codes from CSV files."""
    race_codes = set()

    if not base_dir.exists():
        return race_codes

    try:
        for csv_file in base_dir.glob('**/*.csv'):
            race_codes.update(load_race_codes_from_csv(csv_file))
    except Exception as e:
        print(f"Error scanning {base_dir}: {e}", file=sys.stderr)

    return race_codes


def find_missing_previews(repo_root, min_date=None):
    """Find race codes that have results but no previews.

    Args:
        repo_root: Repository root path
        min_date: Minimum date filter in YYYYMMDD format (e.g., '20260101')
    """
    results_dir = repo_root / 'data' / 'results'
    previews_dir = repo_root / 'data' / 'previews'

    # Load all race codes from results and previews
    print("Scanning results directory...", file=sys.stderr)
    results_codes = scan_directory_for_race_codes(results_dir)
    print(f"  Found {len(results_codes)} unique race codes in results", file=sys.stderr)

    print("Scanning previews directory...", file=sys.stderr)
    previews_codes = scan_directory_for_race_codes(previews_dir)
    print(f"  Found {len(previews_codes)} unique race codes in previews", file=sys.stderr)

    # Find missing previews
    missing_codes = results_codes - previews_codes

    # Filter by minimum date if specified
    if min_date:
        missing_codes = {code for code in missing_codes if code >= min_date}
        print(f"  Filtered to codes >= {min_date}: {len(missing_codes)} race codes", file=sys.stderr)

    print(f"  Missing {len(missing_codes)} race codes", file=sys.stderr)

    return sorted(missing_codes)


def parse_race_code_info(race_code):
    """Extract date, stadium, and race number from race code."""
    try:
        if len(race_code) != 12:
            return None

        year = race_code[0:4]
        month = race_code[4:6]
        day = race_code[6:8]
        stadium = int(race_code[8:10])
        race = int(race_code[10:12])

        date_str = f"{year}-{month}-{day}"
        return {
            'race_code': race_code,
            'date': date_str,
            'stadium': stadium,
            'race': race,
        }
    except (ValueError, IndexError):
        return None


def save_missing_previews(missing_codes, output_path):
    """Save missing preview race codes to CSV."""
    try:
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['race_code', 'date', 'stadium', 'race'],
                lineterminator='\n'
            )
            writer.writeheader()

            for race_code in missing_codes:
                info = parse_race_code_info(race_code)
                if info:
                    writer.writerow(info)

        print(f"Saved {len(missing_codes)} missing preview race codes to {output_path}")
        return True
    except Exception as e:
        print(f"Error saving to {output_path}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Find missing preview data for races with results"
    )

    parser.add_argument(
        '--output',
        type=str,
        default='missing_previews.csv',
        help='Output CSV file path (default: missing_previews.csv)'
    )

    parser.add_argument(
        '--min-date',
        type=str,
        help='Minimum date filter in YYYYMMDD format (e.g., 20260101 for recent data)'
    )

    args = parser.parse_args()

    repo_root = get_repo_root()

    print("Finding missing previews...", file=sys.stderr)
    print("-" * 70, file=sys.stderr)

    missing_codes = find_missing_previews(repo_root, min_date=args.min_date)

    print("-" * 70, file=sys.stderr)
    print(f"Total missing preview race codes: {len(missing_codes)}", file=sys.stderr)

    if not missing_codes:
        print("No missing previews found!")
        sys.exit(0)

    output_path = Path(args.output)
    if save_missing_previews(missing_codes, output_path):
        print()
        print("Sample missing previews (first 10):")
        for i, code in enumerate(missing_codes[:10], 1):
            info = parse_race_code_info(code)
            if info:
                print(f"  {i}. {code} ({info['date']} Stadium {info['stadium']} Race {info['race']})")
        print()
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
