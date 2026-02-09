#!/usr/bin/env python3
"""
Build weather statistics from historical previews data.

Aggregates stadium × month weather statistics (wind speed, wind direction,
wave height, weather, temperature, water temperature) from past previews CSVs
and saves them as a JSON file for use by prediction-preview.py.

Usage:
    python build_weather_stats.py
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def build_weather_stats(repo_root):
    """Build stadium × month weather statistics from all previews data."""
    previews_dir = repo_root / 'data' / 'previews'
    csv_files = sorted(previews_dir.glob('**/*.csv'))

    print(f"Found {len(csv_files)} preview CSV files")

    weather_cols = ['風速(m)', '風向', '波の高さ(cm)', '天候', '気温(℃)', '水温(℃)']
    records = []

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path, usecols=['レース場', 'レース日'] + weather_cols)
        except (ValueError, KeyError):
            continue

        df['レース日'] = pd.to_datetime(df['レース日'], errors='coerce')
        df['月'] = df['レース日'].dt.month
        df['レース場'] = pd.to_numeric(df['レース場'], errors='coerce')

        for col in weather_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        records.append(df[['レース場', '月'] + weather_cols].dropna(subset=['レース場', '月']))

    if not records:
        print("No valid data found", file=sys.stderr)
        return None

    all_data = pd.concat(records, ignore_index=True)
    print(f"Total records: {len(all_data)}")

    # Continuous columns: use mean
    continuous_cols = ['風速(m)', '波の高さ(cm)', '気温(℃)', '水温(℃)']
    # Categorical columns: use mode (most frequent value)
    categorical_cols = ['風向', '天候']

    stats = {}

    for (stadium, month), group in all_data.groupby(['レース場', '月']):
        stadium_int = int(stadium)
        month_int = int(month)
        key = f"{stadium_int}_{month_int}"

        entry = {'count': len(group)}

        for col in continuous_cols:
            valid = group[col].dropna()
            if len(valid) > 0:
                entry[col] = round(float(valid.mean()), 1)
            else:
                entry[col] = 0.0

        for col in categorical_cols:
            valid = group[col].dropna()
            if len(valid) > 0:
                entry[col] = int(valid.mode().iloc[0])
            else:
                # Defaults: 風向=1(北), 天候=1(晴)
                entry[col] = 1

        stats[key] = entry

    return stats


def main():
    repo_root = get_repo_root()
    print("Building weather statistics from previews data...")

    stats = build_weather_stats(repo_root)
    if stats is None:
        sys.exit(1)

    output_path = repo_root / 'models' / 'weather_stats.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print(f"Weather stats saved to {output_path}")
    print(f"Total stadium×month entries: {len(stats)}")

    # Show a sample
    sample_key = list(stats.keys())[0]
    print(f"\nSample ({sample_key}): {stats[sample_key]}")


if __name__ == '__main__':
    main()
