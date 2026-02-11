#!/usr/bin/env python3
"""
Boat race prediction confirmation script.

This script compares predicted results with actual race results and records
whether predictions were correct.

Usage:
    python confirm.py --date 2022-12-23
    python confirm.py --backfill-from 2026-01-01
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations
from boatrace.common import get_repo_root


def load_estimate(date, repo_root):
    """Load prediction results for a given date."""
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d')

    estimate_path = repo_root / 'data' / 'estimate' / year / month / f'{day}.csv'

    if not estimate_path.exists():
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


def save_confirmation(confirmation_df, date, repo_root, commit=True):
    """Save confirmation results to CSV and optionally commit to git."""
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

    if commit:
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


def save_metrics_json(stats, date, repo_root):
    """Save daily metrics as JSON and append to aggregate JSONL file."""
    metrics_dir = repo_root / 'data' / 'metrics'
    metrics_dir.mkdir(parents=True, exist_ok=True)

    date_str = date.strftime('%Y-%m-%d')
    total = stats['total_races']

    metrics = {
        'date': date_str,
        'total_races': total,
        'hit_1st': int(stats['hit_1st']),
        'hit_2nd': int(stats['hit_2nd']),
        'hit_3rd': int(stats['hit_3rd']),
        'all_hits': int(stats['all_hits']),
        'rate_1st': round(stats['hit_1st'] / total, 4) if total > 0 else 0,
        'rate_all': round(stats['all_hits'] / total, 4) if total > 0 else 0,
    }

    # Daily JSON
    daily_path = metrics_dir / f'{date_str}.json'
    with open(daily_path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # Aggregate JSONL (append, but avoid duplicates)
    aggregate_path = metrics_dir / 'aggregate.jsonl'
    existing_dates = set()
    if aggregate_path.exists():
        with open(aggregate_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        existing_dates.add(entry.get('date'))
                    except json.JSONDecodeError:
                        pass

    if date_str not in existing_dates:
        with open(aggregate_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(metrics, ensure_ascii=False) + '\n')

    return daily_path


def check_moving_average_alert(repo_root, date):
    """Check 7-day moving average and print warnings if below thresholds."""
    aggregate_path = repo_root / 'data' / 'metrics' / 'aggregate.jsonl'
    if not aggregate_path.exists():
        return

    entries = []
    with open(aggregate_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    if not entries:
        return

    # Filter to last 7 days up to and including the given date
    date_str = date.strftime('%Y-%m-%d')
    cutoff = (date - timedelta(days=6)).strftime('%Y-%m-%d')
    recent = [e for e in entries if cutoff <= e['date'] <= date_str]

    if len(recent) < 3:
        return

    avg_rate_1st = sum(e['rate_1st'] for e in recent) / len(recent)
    avg_rate_all = sum(e['rate_all'] for e in recent) / len(recent)

    print(f"\n7-day moving average ({len(recent)} days):")
    print(f"  1st place hit rate: {avg_rate_1st:.1%}")
    print(f"  Trifecta hit rate:  {avg_rate_all:.1%}")

    if avg_rate_1st < 0.20:
        print(f"  WARNING: 1st place hit rate ({avg_rate_1st:.1%}) "
              "is below 20% threshold")
    if avg_rate_all < 0.03:
        print(f"  WARNING: Trifecta hit rate ({avg_rate_all:.1%}) "
              "is below 3% threshold")


def process_single_date(confirm_date, repo_root, commit=True, verbose=True):
    """Process confirmation for a single date. Returns (stats, success)."""
    if verbose:
        print(f"Confirmation date: {confirm_date.strftime('%Y-%m-%d')}")

    # Load data
    estimate_df = load_estimate(confirm_date, repo_root)
    results_df = load_results(confirm_date, repo_root)

    if estimate_df is None or results_df is None:
        if verbose:
            print(f"Skipping {confirm_date.strftime('%Y-%m-%d')}: "
                  "missing estimate or results", file=sys.stderr)
        return None, False

    if verbose:
        print(f"Loaded {len(estimate_df)} predictions")
        print(f"Loaded {len(results_df)} results")

    # Compare predictions with results
    confirmation_df, stats = compare_predictions(estimate_df, results_df)

    if confirmation_df is None:
        if verbose:
            print("Failed to compare predictions", file=sys.stderr)
        return None, False

    # Save results
    output_path = save_confirmation(
        confirmation_df, confirm_date, repo_root, commit=commit
    )
    if verbose:
        print(f"Confirmation saved to {output_path}")

    # Save metrics JSON
    save_metrics_json(stats, confirm_date, repo_root)

    # Print statistics
    if verbose:
        print_statistics(stats, confirm_date)

    return stats, True


def main():
    # Get yesterday's date in JST (UTC+9)
    jst = timezone(timedelta(hours=9))
    yesterday_jst = (datetime.now(jst) - timedelta(days=1)).strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(
        description='Confirm boat race prediction results.'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=yesterday_jst,
        help='Confirmation date in YYYY-MM-DD format (default: yesterday JST)'
    )
    parser.add_argument(
        '--backfill-from',
        type=str,
        default=None,
        help='Start date for backfill in YYYY-MM-DD format'
    )

    args = parser.parse_args()

    # Parse confirmation date
    try:
        confirm_date = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    repo_root = get_repo_root()

    if args.backfill_from:
        # Backfill mode
        try:
            backfill_start = datetime.strptime(args.backfill_from, '%Y-%m-%d')
        except ValueError:
            print("Invalid backfill-from date format. Use YYYY-MM-DD",
                  file=sys.stderr)
            sys.exit(1)

        print(f"Backfill mode: {backfill_start.strftime('%Y-%m-%d')} "
              f"to {confirm_date.strftime('%Y-%m-%d')}")
        print("=" * 70)

        current = backfill_start
        processed = 0
        skipped = 0
        git_paths = []

        while current <= confirm_date:
            stats, success = process_single_date(
                current, repo_root, commit=False, verbose=False
            )
            if success:
                processed += 1
                year = current.strftime('%Y')
                month = current.strftime('%m')
                day = current.strftime('%d')
                git_paths.append(
                    f'data/confirm/{year}/{month}/{day}.csv'
                )
                print(f"  {current.strftime('%Y-%m-%d')}: "
                      f"1st={stats['hit_1st']}/{stats['total_races']} "
                      f"({100*stats['hit_1st']/stats['total_races']:.1f}%) "
                      f"all={stats['all_hits']}/{stats['total_races']} "
                      f"({100*stats['all_hits']/stats['total_races']:.1f}%)")
            else:
                skipped += 1
            current += timedelta(days=1)

        print("=" * 70)
        print(f"Processed: {processed}, Skipped: {skipped}")

        # Batch git commit
        if git_paths:
            message = (f'Backfill prediction confirmations: '
                       f'{backfill_start.strftime("%Y-%m-%d")} '
                       f'to {confirm_date.strftime("%Y-%m-%d")}')
            if git_operations.commit_and_push(git_paths, message):
                print("Git commit and push succeeded for backfill")
            else:
                print("Git commit and push failed for backfill")

        # Check moving average alert for the end date
        check_moving_average_alert(repo_root, confirm_date)
    else:
        # Single date mode
        stats, success = process_single_date(confirm_date, repo_root)

        if not success:
            sys.exit(1)

        # Check moving average alert
        check_moving_average_alert(repo_root, confirm_date)

        # Display sample results
        estimate_df = load_estimate(confirm_date, repo_root)
        results_df = load_results(confirm_date, repo_root)
        confirmation_df, _ = compare_predictions(estimate_df, results_df)
        if confirmation_df is not None:
            print("\nSample results (first 10 races):")
            display_cols = [
                'レースコード', '予想1着', '予想2着', '予想3着',
                '実際1着', '実際2着', '実際3着',
                '1着的中', '2着的中', '3着的中', '全的中'
            ]
            print(confirmation_df[display_cols].head(10).to_string(index=False))


if __name__ == '__main__':
    main()
