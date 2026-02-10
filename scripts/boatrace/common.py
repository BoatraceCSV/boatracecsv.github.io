"""Shared utility functions for boatrace scripts."""

from pathlib import Path

import pandas as pd

from boatrace.constants import PLACE_COLS, RATE_COLS


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def reshape_programs(df, include_title=False):
    """Reshape Programs data from wide format (1枠～6枠) to long format.

    Args:
        df: Programs DataFrame in wide format.
        include_title: If True, include 'タイトル' in race-level columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    race_id_cols = ['レースコード', 'レース日', 'レース場', 'レース回']
    if include_title:
        race_id_cols = ['レースコード', 'タイトル', 'レース日', 'レース場', 'レース回']
    # Use only columns that exist
    race_id_cols = [c for c in race_id_cols if c in df.columns]
    program_frames = []

    for frame_num in range(1, 7):
        frame_prefix = f'{frame_num}枠_'
        frame_cols = [col for col in df.columns if col.startswith(frame_prefix)]

        if frame_cols:
            tmp = df[race_id_cols + frame_cols].copy()
            rename_map = {col: col[len(frame_prefix):] for col in frame_cols}
            tmp = tmp.rename(columns=rename_map)
            tmp['艇番'] = frame_num
            program_frames.append(tmp)

    if program_frames:
        return pd.concat(program_frames, ignore_index=True)
    return pd.DataFrame()


def reshape_previews(df, include_weather=False):
    """Reshape Previews data from wide format (艇1～艇6) to long format.

    Args:
        df: Previews DataFrame in wide format.
        include_weather: If True, preserve race-level weather columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    race_id_cols = ['レースコード', 'レース日', 'レース場', 'レース回']
    weather_cols = ['風速(m)', '風向', '波の高さ(cm)', '天候', '気温(℃)', '水温(℃)']
    base_cols = list(race_id_cols)
    if include_weather:
        base_cols += [c for c in weather_cols if c in df.columns]

    preview_frames = []

    for boat_num in range(1, 7):
        boat_prefix = f'艇{boat_num}_'
        boat_cols = [col for col in df.columns if col.startswith(boat_prefix)]

        if boat_cols:
            tmp = df[base_cols + boat_cols].copy()
            rename_map = {col: col[len(boat_prefix):] for col in boat_cols}
            tmp = tmp.rename(columns=rename_map)
            tmp['艇番'] = boat_num
            preview_frames.append(tmp)

    if preview_frames:
        return pd.concat(preview_frames, ignore_index=True)
    return pd.DataFrame()


def reshape_results(df):
    """Reshape Results data from wide format (1着～6着) to long format (boat-based)."""
    if df is None or df.empty:
        return pd.DataFrame()

    result_list = []

    for _, row in df.iterrows():
        race_code = row['レースコード']

        for place in range(1, 7):
            boat_col = f'{place}着_艇番'

            if boat_col in df.columns and pd.notna(row[boat_col]):
                try:
                    boat_num = int(row[boat_col])
                    if 1 <= boat_num <= 6:
                        result_list.append({
                            'レースコード': race_code,
                            '艇番': boat_num,
                            '着順': place
                        })
                except (ValueError, TypeError):
                    continue

    return pd.DataFrame(result_list) if result_list else pd.DataFrame()


def prepare_features(data, feature_cols):
    """Prepare feature matrix from data with improved NaN handling."""
    X = pd.DataFrame(index=data.index)

    for col in feature_cols:
        if col in data.columns:
            X[col] = pd.to_numeric(data[col], errors='coerce')
        else:
            X[col] = 0.0

    for col in X.columns:
        if col in PLACE_COLS:
            X[col] = X[col].fillna(3.5)
        elif col in RATE_COLS:
            if X[col].notna().any():
                X[col] = X[col].fillna(X[col].median())
            else:
                X[col] = X[col].fillna(0)
        else:
            if X[col].notna().any():
                X[col] = X[col].fillna(X[col].median())
            else:
                X[col] = X[col].fillna(0)

    return X
