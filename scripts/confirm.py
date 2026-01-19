#!/usr/bin/env python3
"""
Boat race prediction confirmation script.

This script compares predicted results with actual race results and records
whether predictions were correct.

Usage:
    python confirm.py --date 2022-12-23
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def load_estimate(date, repo_root):
    """Load prediction results for a given date."""
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    estimate_path = repo_root / 'data' / 'estimate' / year / month / f'{day}.csv'

    if not estimate_path.exists():
        print(f"Estimate file not found: {estimate_path}", file=sys.stderr)
        return None

    try:
        estimate = pd.read_csv(estimate_path)
        return estimate
    except Exception as e:
        print(f"Error loading estimate: {e}", file=sys.stderr)
        return None


def load_results(date, repo_root):
    """Load actual race results for a given date."""
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    results_path = repo_root / 'data' / 'results' / year / month / f'{day}.csv'

    if not results_path.exists():
        print(f"Results file not found: {results_path}", file=sys.stderr)
        return None

    try:
        results = pd.read_csv(results_path)
        return results
    except Exception as e:
        print(f"Error loading results: {e}", file=sys.stderr)
        return None


def compare_predictions(estimate_df, results_df):
    """Compare predictions with actual results."""
    if estimate_df is None or results_df is None:
        return None, None

    # Select required columns from results
    results_cols = results_df[['レースコード', '1着_艇番', '2着_艇番', '3着_艇番']].copy()
    results_cols = results_cols.rename(columns={
        '1着_艇番': '実際1着',
        '2着_艇番': '実際2着',
        '3着_艇番': '実際3着'
    })

    # Merge predictions with actual results
    merged = estimate_df.merge(
        results_cols,
        on='レースコード',
        how='inner'
    )

    if merged.empty:
        print("No matching races found", file=sys.stderr)
        return None, None

    # Determine hits
    merged['1着的中'] = merged.apply(
        lambda row: '○' if row['予想1着'] == row['実際1着'] else '×',
        axis=1
    )
    merged['2着的中'] = merged.apply(
        lambda row: '○' if row['予想2着'] == row['実際2着'] else '×',
        axis=1
    )
    merged['3着的中'] = merged.apply(
        lambda row: '○' if row['予想3着'] == row['実際3着'] else '×',
        axis=1
    )
    merged['全的中'] = merged.apply(
        lambda row: '○' if (row['1着的中'] == '○' and
                             row['2着的中'] == '○' and
                             row['3着的中'] == '○') else '×',
        axis=1
    )

    # Calculate statistics
    total_races = len(merged)
    hit_1st = (merged['1着的中'] == '○').sum()
    hit_2nd = (merged['2着的中'] == '○').sum()
    hit_3rd = (merged['3着的中'] == '○').sum()
    all_hits = (merged['全的中'] == '○').sum()

    stats = {
        'total_races': total_races,
        'hit_1st': hit_1st,
        'hit_2nd': hit_2nd,
        'hit_3rd': hit_3rd,
        'all_hits': all_hits,
    }

    return merged, stats


def save_confirmation(confirmation_df, date, repo_root):
    """Save confirmation results to CSV and commit to git."""
    year = date.strftime('%Y')
    month = date.strftime('%m')

    output_dir = repo_root / 'data' / 'confirm' / year / month
    output_dir.mkdir(parents=True, exist_ok=True)

    day = date.strftime('%d')
    output_path = output_dir / f'{day}.csv'

    # Select and reorder columns
    output_cols = [
        'レースコード',
        '予想1着', '予想2着', '予想3着',
        '実際1着', '実際2着', '実際3着',
        '1着的中', '2着的中', '3着的中', '全的中'
    ]

    confirmation_df[output_cols].to_csv(
        output_path,
        index=False,
        encoding='utf-8-sig'
    )

    # Git commit and push
    relative_path = f'data/confirm/{year}/{month}/{day}.csv'
    message = f'Update prediction confirmations: {date.strftime("%Y-%m-%d")}'
    if git_operations.commit_and_push([relative_path], message):
        print(f"Git commit and push succeeded for {output_path}")
    else:
        print(f"Git commit and push failed for {output_path}")

    return output_path


def print_statistics(stats, date):
    """Print confirmation statistics."""
    print("-" * 70)
    print(f"Confirmation Results for {date.strftime('%Y-%m-%d')}")
    print("-" * 70)
    print(f"Total races: {stats['total_races']}")
    print()
    print("Hits:")
    print(f"  1st place: {stats['hit_1st']}/{stats['total_races']} "
          f"({100*stats['hit_1st']/stats['total_races']:.1f}%)")
    print(f"  2nd place: {stats['hit_2nd']}/{stats['total_races']} "
          f"({100*stats['hit_2nd']/stats['total_races']:.1f}%)")
    print(f"  3rd place: {stats['hit_3rd']}/{stats['total_races']} "
          f"({100*stats['hit_3rd']/stats['total_races']:.1f}%)")
    print(f"  All 3 places: {stats['all_hits']}/{stats['total_races']} "
          f"({100*stats['all_hits']/stats['total_races']:.1f}%)")
    print("-" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='Confirm boat race prediction results.'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Confirmation date in YYYY-MM-DD format (default: yesterday)'
    )

    args = parser.parse_args()

    # Parse confirmation date
    if args.date:
        try:
            confirm_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        confirm_date = datetime.now() - timedelta(days=1)

    repo_root = get_repo_root()

    print(f"Confirmation date: {confirm_date.strftime('%Y-%m-%d')}")

    # Load data
    estimate_df = load_estimate(confirm_date, repo_root)
    results_df = load_results(confirm_date, repo_root)

    if estimate_df is None or results_df is None:
        print("Failed to load required data", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(estimate_df)} predictions")
    print(f"Loaded {len(results_df)} results")

    # Compare predictions with results
    confirmation_df, stats = compare_predictions(estimate_df, results_df)

    if confirmation_df is None:
        print("Failed to compare predictions", file=sys.stderr)
        sys.exit(1)

    # Save results
    output_path = save_confirmation(confirmation_df, confirm_date, repo_root)
    print(f"Confirmation saved to {output_path}")

    # Print statistics
    print_statistics(stats, confirm_date)

    # Display sample results
    print("\nSample results (first 10 races):")
    display_cols = [
        'レースコード', '予想1着', '予想2着', '予想3着',
        '実際1着', '実際2着', '実際3着',
        '1着的中', '2着的中', '3着的中', '全的中'
    ]
    print(confirmation_df[display_cols].head(10).to_string(index=False))


if __name__ == '__main__':
    main()
