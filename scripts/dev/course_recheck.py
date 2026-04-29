"""Course prediction re-examination (2025 train, 2026 Q1 test).

This is a one-off analysis script for evaluating whether a rule+history-based
course prediction beats the current 枠番=コース固定 baseline used in
prediction-preview.py.

Outputs go to /tmp/course_recheck_*.csv|.txt for inspection.
"""
from __future__ import annotations

import calendar
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path('/sessions/optimistic-pensive-bohr/mnt/boatracecsv.github.io')


def load_pair(year: int, month: int, day: int):
    p = REPO / 'data' / 'programs' / f'{year}' / f'{month:02d}' / f'{day:02d}.csv'
    v = REPO / 'data' / 'previews' / f'{year}' / f'{month:02d}' / f'{day:02d}.csv'
    if not (p.exists() and v.exists()):
        return None
    try:
        prog = pd.read_csv(p)
        prev = pd.read_csv(v)
    except Exception:
        return None
    return prog, prev


def reshape_one_day(prog: pd.DataFrame, prev: pd.DataFrame) -> pd.DataFrame:
    """Return long-format rows with 登録番号, 級別, 艇番, 実コース, レース場, レース名, レース日, レースコード."""
    rows = []
    # Build lookup from previews: race_code -> {boat: 実コース}
    prev_idx = prev.set_index('レースコード')
    for _, row in prog.iterrows():
        rc = row['レースコード']
        if rc not in prev_idx.index:
            continue
        prev_row = prev_idx.loc[rc]
        race_name = row.get('レース名', '')
        stadium = row.get('レース場', '')
        race_date = row.get('レース日', '')
        for f in range(1, 7):
            reg = row.get(f'{f}枠_登録番号')
            grade = row.get(f'{f}枠_級別')
            age = row.get(f'{f}枠_年齢')
            course_col = f'艇{f}_コース'
            if course_col not in prev.columns:
                continue
            actual = prev_row[course_col] if course_col in prev_row.index else np.nan
            if pd.isna(actual):
                continue
            try:
                actual_i = int(actual)
            except Exception:
                continue
            rows.append({
                'レースコード': rc,
                'レース日': race_date,
                'レース場': stadium,
                'レース名': race_name if pd.notna(race_name) else '',
                '艇番': f,
                '実コース': actual_i,
                '登録番号': int(reg) if pd.notna(reg) else None,
                '級別': grade if pd.notna(grade) else '',
                '年齢': int(age) if pd.notna(age) else None,
            })
    return pd.DataFrame(rows)


def load_range(year_start: int, year_end: int, months_end: dict | None = None) -> pd.DataFrame:
    """months_end: {year: last_month_inclusive} for partial years (e.g., {2026: 4})."""
    frames = []
    for year in range(year_start, year_end + 1):
        max_m = months_end.get(year, 12) if months_end else 12
        for month in range(1, max_m + 1):
            _, last_day = calendar.monthrange(year, month)
            for day in range(1, last_day + 1):
                pair = load_pair(year, month, day)
                if pair is None:
                    continue
                df = reshape_one_day(*pair)
                if not df.empty:
                    frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def report_baseline(df: pd.DataFrame, label: str):
    df = df.copy()
    df['一致'] = df['艇番'] == df['実コース']
    overall = df['一致'].mean()
    print(f'== {label} (n={len(df):,}) ==')
    print(f'  ベースライン(枠番=コース) 艇単位正解率: {overall:.4f}')
    # 枠番別
    print('  枠番別:')
    for f in range(1, 7):
        sub = df[df['艇番'] == f]
        if len(sub):
            print(f'    {f}枠: {sub["一致"].mean():.4f}  (n={len(sub):,})')
    # 進入固定
    fixed = df[df['レース名'].str.contains('進入固定', na=False)]
    if len(fixed):
        print(f'  進入固定レース: {fixed["一致"].mean():.4f}  (n={len(fixed):,})')
    # 江戸川
    edo = df[df['レース場'].astype(str) == '3']
    if len(edo):
        print(f'  江戸川: {edo["一致"].mean():.4f}  (n={len(edo):,})')
    print()


if __name__ == '__main__':
    print('Loading 2025 ...', flush=True)
    df_2025 = load_range(2025, 2025)
    print(f'2025 rows: {len(df_2025):,}, races: {df_2025["レースコード"].nunique() if len(df_2025) else 0:,}', flush=True)

    print('Loading 2026/01-04 ...', flush=True)
    df_2026 = load_range(2026, 2026, months_end={2026: 4})
    print(f'2026 Q1+04 rows: {len(df_2026):,}, races: {df_2026["レースコード"].nunique() if len(df_2026) else 0:,}', flush=True)

    df_2025.to_pickle('/tmp/course_2025.pkl')
    df_2026.to_pickle('/tmp/course_2026.pkl')

    report_baseline(df_2025, '2025 全期間')
    report_baseline(df_2026, '2026/01-04')
