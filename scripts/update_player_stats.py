#!/usr/bin/env python3
"""Update player statistics from recent race results.

Computes sliding-window statistics (recent 5/10 races, ST stats) from
result CSV files and saves to models/player_stats_latest.json.
This file can be used by estimate.py to override stale stats from
the training pickle.

Usage:
    python scripts/update_player_stats.py
    python scripts/update_player_stats.py --lookback-days 90
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.common import get_repo_root


def load_results_in_range(repo_root, start_date, end_date):
    """Load all results CSVs within the given date range."""
    all_results = []
    current = start_date
    while current <= end_date:
        year = current.strftime('%Y')
        month = current.strftime('%m')
        day = current.strftime('%d')
        path = repo_root / 'data' / 'results' / year / month / f'{day}.csv'
        if path.exists():
            try:
                df = pd.read_csv(path)
                df['_date'] = current.strftime('%Y-%m-%d')
                all_results.append(df)
            except Exception:
                pass
        current += timedelta(days=1)
    return all_results


def compute_sliding_window_stats(repo_root, lookback_days=90):
    """Compute recent player statistics from result data.

    Computes per-player stats:
    - 直近5走_平均着順, 直近10走_平均着順, 直近5走_1着率
    - ST_mean, ST_std, ST_min
    """
    jst = timezone(timedelta(hours=9))
    end_date = datetime.now(jst).date()
    start_date = end_date - timedelta(days=lookback_days)

    print(f"Loading results from {start_date} to {end_date}...")
    results_list = load_results_in_range(
        repo_root,
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time()),
    )

    if not results_list:
        print("No results data found in range", file=sys.stderr)
        return

    print(f"Loaded {len(results_list)} days of results")

    # Extract per-race, per-player records
    records = []
    for res_df in results_list:
        for _, row in res_df.iterrows():
            race_code = row['レースコード']
            race_date = row.get('_date', '')
            for place in range(1, 7):
                reg_col = f'{place}着_登録番号'
                boat_col = f'{place}着_艇番'
                st_col = f'{place}着_スタートタイミング'
                if reg_col not in res_df.columns:
                    continue
                reg_val = pd.to_numeric(row.get(reg_col), errors='coerce')
                if pd.isna(reg_val):
                    continue
                st_val = pd.to_numeric(row.get(st_col), errors='coerce')
                records.append({
                    '登録番号': int(reg_val),
                    'レース日': race_date,
                    '着順': place,
                    'ST': st_val if pd.notna(st_val) else None,
                })

    if not records:
        print("No player records extracted", file=sys.stderr)
        return

    rec_df = pd.DataFrame(records)
    rec_df['レース日'] = pd.to_datetime(rec_df['レース日'], errors='coerce')
    rec_df = rec_df.sort_values(['登録番号', 'レース日'], ascending=[True, False])

    stats_dict = {}
    for reg, grp in rec_df.groupby('登録番号'):
        last5 = grp.head(5)['着順']
        last10 = grp.head(10)['着順']

        entry = {
            '直近5走_平均着順': round(float(last5.mean()), 3),
            '直近10走_平均着順': round(float(last10.mean()), 3),
            '直近5走_1着率': round(float((last5 == 1).mean()), 3),
        }

        # ST stats
        st_vals = grp['ST'].dropna()
        if len(st_vals) >= 3:
            entry['ST_mean'] = round(float(st_vals.mean()), 4)
            entry['ST_std'] = round(float(st_vals.std()), 4)
            entry['ST_min'] = round(float(st_vals.min()), 4)

        stats_dict[str(int(reg))] = entry

    # Save
    output_path = repo_root / 'models' / 'player_stats_latest.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'lookback_days': lookback_days,
        'date_range': f'{start_date} to {end_date}',
        'player_count': len(stats_dict),
        'stats': stats_dict,
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(stats_dict)} player stats to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Update player statistics from recent race results.'
    )
    parser.add_argument(
        '--lookback-days', type=int, default=90,
        help='Number of days to look back for results (default: 90)'
    )
    args = parser.parse_args()
    repo_root = get_repo_root()
    compute_sliding_window_stats(repo_root, args.lookback_days)


if __name__ == '__main__':
    main()
