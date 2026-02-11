#!/usr/bin/env python3
"""Convert daily CSV files to monthly Parquet files.

Aggregates daily CSV files into monthly Parquet files for faster I/O.
Original CSVs are preserved; Parquet files are written to parallel directories.

Directory structure:
    data/programs/2025/01/01.csv  →  data/programs_parquet/2025/01.parquet
    data/results/2025/01/01.csv   →  data/results_parquet/2025/01.parquet
    data/previews/2025/01/01.csv  →  data/previews_parquet/2025/01.parquet

Usage:
    python scripts/convert_to_parquet.py
    python scripts/convert_to_parquet.py --data-types programs results
    python scripts/convert_to_parquet.py --years 2024 2025
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.common import get_repo_root


DATA_TYPES = ['programs', 'results', 'previews']


def convert_month_to_parquet(repo_root, data_type, year, month):
    """Convert all daily CSVs for a given month to a single Parquet file.

    Returns:
        Number of CSV files merged, or 0 if no files found.
    """
    csv_dir = repo_root / 'data' / data_type / year / month
    if not csv_dir.exists():
        return 0

    csv_files = sorted(csv_dir.glob('*.csv'))
    if not csv_files:
        return 0

    frames = []
    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  Warning: Failed to read {csv_path}: {e}")

    if not frames:
        return 0

    combined = pd.concat(frames, ignore_index=True)

    # Convert mixed-type columns to string to avoid pyarrow errors
    for col in combined.columns:
        if combined[col].dtype == object:
            combined[col] = combined[col].astype(str).replace('nan', pd.NA)

    parquet_dir = repo_root / 'data' / f'{data_type}_parquet' / year
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / f'{month}.parquet'

    combined.to_parquet(parquet_path, index=False, engine='pyarrow')
    return len(frames)


def convert_all(repo_root, data_types, years):
    """Convert all specified data types and years to Parquet."""
    total_files = 0
    total_parquets = 0

    for data_type in data_types:
        print(f"\nConverting {data_type}...")
        data_dir = repo_root / 'data' / data_type

        if not data_dir.exists():
            print(f"  Directory not found: {data_dir}")
            continue

        # Discover available years
        if years:
            year_dirs = [data_dir / str(y) for y in years]
        else:
            year_dirs = sorted(d for d in data_dir.iterdir() if d.is_dir() and d.name.isdigit())

        for year_dir in year_dirs:
            if not year_dir.exists():
                continue
            year = year_dir.name
            month_dirs = sorted(d for d in year_dir.iterdir() if d.is_dir() and d.name.isdigit())

            for month_dir in month_dirs:
                month = month_dir.name
                count = convert_month_to_parquet(repo_root, data_type, year, month)
                if count > 0:
                    total_files += count
                    total_parquets += 1
                    print(f"  {data_type}/{year}/{month}: {count} CSVs → 1 Parquet")

    print(f"\nTotal: {total_files} CSV files → {total_parquets} Parquet files")


def main():
    parser = argparse.ArgumentParser(description='Convert daily CSV files to monthly Parquet.')
    parser.add_argument(
        '--data-types', nargs='+', default=DATA_TYPES,
        choices=DATA_TYPES,
        help=f'Data types to convert (default: {DATA_TYPES})'
    )
    parser.add_argument(
        '--years', nargs='+', type=str, default=None,
        help='Years to convert (default: all available)'
    )
    args = parser.parse_args()

    repo_root = get_repo_root()
    convert_all(repo_root, args.data_types, args.years)


if __name__ == '__main__':
    main()
