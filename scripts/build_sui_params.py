#!/usr/bin/env python3
"""
Fit sui_params.csv from real historical race data for all 24 stadiums.

Joins data/previews/YYYY/MM/DD.csv (weather + exhibition) with
data/results/YYYY/MM/DD.csv (course + finish) by レースコード, then fits
a per-(stadium, course) linear regression in advantage-point space.

Usage:
    python scripts/build_sui_params.py \
        --start-date 2025-01-01 --end-date 2026-05-02 \
        --out data/stadium/sui_params.csv
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

# --------------------------------------------------------------------------
# 1. Stadium master
# --------------------------------------------------------------------------
# Code -> name. The boat racing standard 1..24 numbering.
STADIUM_NAMES = {
    1: "桐生", 2: "戸田", 3: "江戸川", 4: "平和島", 5: "多摩川", 6: "浜名湖",
    7: "蒲郡", 8: "常滑", 9: "津", 10: "三国", 11: "びわこ", 12: "住之江",
    13: "尼崎", 14: "鳴門", 15: "丸亀", 16: "児島", 17: "宮島", 18: "徳山",
    19: "下関", 20: "若松", 21: "芦屋", 22: "福岡", 23: "唐津", 24: "大村",
}

# Approximate stand-facing direction (degrees, 0=North).
# Existing 6 values come from tmp/sui_params.py STADIUMS dict; the rest are
# rough estimates. The regression absorbs misalignment into the横風 baseline.
STADIUM_FACING = {
    "桐生": 90, "戸田": 0, "江戸川": 200, "平和島": 270, "多摩川": 180, "浜名湖": 90,
    "蒲郡": 90, "常滑": 270, "津": 90, "三国": 270, "びわこ": 0, "住之江": 0,
    "尼崎": 0, "鳴門": 0, "丸亀": 0, "児島": 0, "宮島": 90, "徳山": 0,
    "下関": 0, "若松": 0, "芦屋": 0, "福岡": 0, "唐津": 0, "大村": 0,
}

# Wind direction code (1..8) -> degrees (0=N, 90=E, 180=S, 270=W).
WIND_CODE_TO_DEG = {
    1: 0, 2: 45, 3: 90, 4: 135, 5: 180, 6: 225, 7: 270, 8: 315,
}

# Weather code -> "晴" / "曇" / "雨" (collapse rare codes into 雨/晴).
WEATHER_CODE_TO_LABEL = {
    1: "晴", 2: "曇", 3: "雨", 4: "雨", 5: "雨", 6: "晴", 9: "晴",
}

PARAM_FEATURES = [
    "wave_cm", "temp_diff",
    "wind_tail_ms", "wind_head_ms",
    "is_cloudy", "is_rainy",
]


# --------------------------------------------------------------------------
# 2. Build long-format dataset from previews + results
# --------------------------------------------------------------------------
def iter_dates(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def load_day(repo_root: Path, day: dt.date) -> list[dict]:
    """Load (previews + results) for one day and return long-format rows."""
    prev_path = repo_root / "data" / "previews" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    res_path = repo_root / "data" / "results" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    if not prev_path.exists() or not res_path.exists():
        return []

    try:
        prev = pd.read_csv(prev_path, dtype=str)
        res = pd.read_csv(res_path, dtype=str)
    except Exception as e:
        print(f"  skip {day}: {e}", file=sys.stderr)
        return []

    if prev.empty or res.empty:
        return []

    # Index results by レースコード for fast lookup
    res_by_code = {row["レースコード"]: row for _, row in res.iterrows()}

    rows = []
    for _, p in prev.iterrows():
        code = p["レースコード"]
        r = res_by_code.get(code)
        if r is None:
            continue
        try:
            stadium_code = int(p["レース場"])
        except (ValueError, TypeError):
            continue
        stadium = STADIUM_NAMES.get(stadium_code)
        if stadium is None:
            continue

        # Parse weather features
        try:
            wind_ms = float(p["風速(m)"])
            wind_code = int(float(p["風向"]))
            wave_cm = float(p["波の高さ(cm)"])
            weather_code = int(float(p["天候"]))
            air_temp = float(p["気温(℃)"])
            water_temp = float(p["水温(℃)"])
        except (ValueError, TypeError, KeyError):
            continue

        wind_deg = WIND_CODE_TO_DEG.get(wind_code)
        weather_label = WEATHER_CODE_TO_LABEL.get(weather_code, "晴")
        if wind_deg is None:
            continue

        # Build (course -> finish) map from results 1..6着
        course_to_finish = {}  # course (1..6) -> finish position (1..6)
        valid = True
        for rank in range(1, 7):
            try:
                course = int(float(r[f"{rank}着_進入コース"]))
            except (ValueError, TypeError, KeyError):
                valid = False
                break
            if course < 1 or course > 6:
                valid = False
                break
            course_to_finish[course] = rank
        if not valid or len(course_to_finish) != 6:
            continue

        for course, finish in course_to_finish.items():
            rows.append({
                "stadium": stadium,
                "race_id": code,
                "course": course,
                "finish": finish,
                "wind_ms": wind_ms,
                "wind_deg": wind_deg,
                "wave_cm": wave_cm,
                "weather": weather_label,
                "air_temp": air_temp,
                "water_temp": water_temp,
            })
    return rows


def build_dataset(repo_root: Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    all_rows = []
    n_days = (end - start).days + 1
    for i, day in enumerate(iter_dates(start, end)):
        rows = load_day(repo_root, day)
        all_rows.extend(rows)
        if (i + 1) % 30 == 0 or i + 1 == n_days:
            print(f"  loaded {i+1}/{n_days} days, {len(all_rows):,} race-course rows so far",
                  file=sys.stderr)
    return pd.DataFrame(all_rows)


# --------------------------------------------------------------------------
# 3. Feature engineering
# --------------------------------------------------------------------------
def build_features(df: pd.DataFrame, facing_deg: float) -> pd.DataFrame:
    X = pd.DataFrame(index=df.index)
    X["wave_cm"] = df["wave_cm"]
    X["temp_diff"] = df["air_temp"] - df["water_temp"]

    rel = (df["wind_deg"] - facing_deg) % 360
    is_tail = ((rel < 45) | (rel >= 315)).astype(int)
    is_head = ((rel >= 135) & (rel < 225)).astype(int)
    X["wind_tail_ms"] = is_tail * df["wind_ms"]
    X["wind_head_ms"] = is_head * df["wind_ms"]

    X["is_cloudy"] = (df["weather"] == "曇").astype(int)
    X["is_rainy"] = (df["weather"] == "雨").astype(int)
    return X


# --------------------------------------------------------------------------
# 4. Fit per stadium
# --------------------------------------------------------------------------
def fit_stadium(df_st: pd.DataFrame, facing_deg: float):
    """Returns (intercepts, coefs) in advantage-point space (a higher pt = better)."""
    X_all = build_features(df_st, facing_deg)
    feat_names = X_all.columns.tolist()

    # finish-space regression
    coefs_finish = pd.DataFrame(index=feat_names, columns=range(1, 7), dtype=float)
    intercepts_finish = pd.Series(index=range(1, 7), dtype=float)

    for c in range(1, 7):
        mask = df_st["course"] == c
        if mask.sum() < 50:
            # Too few samples for this course; fall back to baseline
            intercepts_finish[c] = float(df_st.loc[mask, "finish"].mean()
                                          if mask.any() else 3.5)
            coefs_finish[c] = 0.0
            continue
        X = X_all.loc[mask].values
        y = df_st.loc[mask, "finish"].values.astype(float)
        m = LinearRegression()
        m.fit(X, y)
        coefs_finish[c] = m.coef_
        intercepts_finish[c] = m.intercept_

    # Convert to advantage-point space:  adv = 7 - finish
    intercepts_adv = 7.0 - intercepts_finish
    coefs_adv = -coefs_finish
    return intercepts_adv, coefs_adv


# --------------------------------------------------------------------------
# 5. Save
# --------------------------------------------------------------------------
def save_params_csv(path: Path, params: dict):
    rows = []
    for stadium in STADIUM_NAMES.values():
        if stadium not in params:
            continue
        intercepts, coefs = params[stadium]
        row = {"stadium": stadium}
        for c in range(1, 7):
            row[f"base_c{c}"] = round(float(intercepts[c]), 4)
        for feat in PARAM_FEATURES:
            for c in range(1, 7):
                row[f"{feat}_c{c}"] = round(float(coefs.loc[feat, c]), 4)
        rows.append(row)

    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


# --------------------------------------------------------------------------
# 6. Main
# --------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(Path(__file__).parent.parent))
    p.add_argument("--start-date", default="2025-01-01")
    p.add_argument("--end-date", default="2026-05-02")
    p.add_argument("--out", default="data/stadium/sui_params.csv")
    args = p.parse_args()

    repo_root = Path(args.repo_root).resolve()
    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)

    print(f"▼ Building dataset {start} → {end}", file=sys.stderr)
    df = build_dataset(repo_root, start, end)
    print(f"  total rows: {len(df):,} (= {len(df)//6:,} races × 6 courses)",
          file=sys.stderr)
    if df.empty:
        print("No data; aborting.", file=sys.stderr)
        sys.exit(1)

    print("\n▼ Per-stadium row counts:", file=sys.stderr)
    counts = df["stadium"].value_counts()
    for st in STADIUM_NAMES.values():
        n = int(counts.get(st, 0))
        print(f"  {st:>5}: {n:>7,}", file=sys.stderr)

    print("\n▼ Fitting per stadium...", file=sys.stderr)
    params = {}
    for stadium in STADIUM_NAMES.values():
        sub = df[df["stadium"] == stadium]
        if len(sub) < 300:  # skip stadiums with too little data
            print(f"  {stadium}: only {len(sub)} rows, skipping", file=sys.stderr)
            continue
        facing = STADIUM_FACING.get(stadium, 0)
        intercepts, coefs = fit_stadium(sub, facing)
        params[stadium] = (intercepts, coefs)
        print(f"  {stadium}: fit on {len(sub):,} rows (facing={facing})", file=sys.stderr)

    out_path = repo_root / args.out
    save_params_csv(out_path, params)
    print(f"\n▼ Saved {len(params)} stadiums → {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
