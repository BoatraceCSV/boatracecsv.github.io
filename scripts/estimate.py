#!/usr/bin/env python3
"""
Boat race result estimation script.

This script uses pre-trained stadium-specific models to make predictions
for a specified date. Models are trained from program.ipynb and saved to
models/program_models.pkl.

The models use programs data only (without previews/weather data).

Usage:
    python estimate.py --date 2026-01-30
"""

import argparse
import pickle
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations


# Stadium name to code mapping (standard boatrace stadium codes 1-24)
STADIUM_NAME_TO_CODE = {
    'ボートレース桐生': 1,
    'ボートレース戸田': 2,
    'ボートレース江戸川': 3,
    'ボートレース平和島': 4,
    'ボートレース多摩川': 5,
    'ボートレース浜名湖': 6,
    'ボートレース蒲郡': 7,
    'ボートレース常滑': 8,
    'ボートレース津': 9,
    'ボートレース三国': 10,
    'ボートレースびわこ': 11,
    'ボートレース琵琶湖': 11,  # Alternative name for びわこ
    'ボートレース住之江': 12,
    'ボートレース尼崎': 13,
    'ボートレース鳴門': 14,
    'ボートレース丸亀': 15,
    'ボートレース児島': 16,
    'ボートレース宮島': 17,
    'ボートレース徳山': 18,
    'ボートレース下関': 19,
    'ボートレース若松': 20,
    'ボートレース芦屋': 21,
    'ボートレース福岡': 22,
    'ボートレース唐津': 23,
    'ボートレース大村': 24,
}


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def load_data(year, month, day, repo_root):
    """Load Programs, Previews, and Results data for a specific date."""
    programs_path = repo_root / 'data' / 'programs' / year / month / f'{day}.csv'
    previews_path = repo_root / 'data' / 'prediction-preview' / year / month / f'{day}.csv'
    results_path = repo_root / 'data' / 'results' / year / month / f'{day}.csv'

    programs = None
    previews = None
    results = None

    try:
        if programs_path.exists():
            programs = pd.read_csv(programs_path)
        if previews_path.exists():
            previews = pd.read_csv(previews_path)
        if results_path.exists():
            results = pd.read_csv(results_path)
    except Exception as e:
        print(f"Error loading data for {year}-{month}-{day}: {e}", file=sys.stderr)
        return None, None, None

    return programs, previews, results


def reshape_programs(programs):
    """Reshape Programs data from wide format (1枠～6枠) to long format."""
    if programs is None or programs.empty:
        return pd.DataFrame()

    race_id_cols = ['レースコード', 'レース日', 'レース場', 'レース回']
    program_frames = []

    for frame_num in range(1, 7):
        frame_prefix = f'{frame_num}枠_'
        frame_cols = [col for col in programs.columns if col.startswith(frame_prefix)]

        if frame_cols:
            tmp = programs[race_id_cols + frame_cols].copy()
            rename_map = {col: col[len(frame_prefix):] for col in frame_cols}
            tmp = tmp.rename(columns=rename_map)
            tmp['艇番'] = frame_num
            program_frames.append(tmp)

    if program_frames:
        return pd.concat(program_frames, ignore_index=True)
    return pd.DataFrame()


def reshape_previews(previews):
    """Reshape Previews data from wide format (艇1～艇6) to long format."""
    if previews is None or previews.empty:
        return pd.DataFrame()

    race_id_cols = ['レースコード', 'レース日', 'レース場', 'レース回']
    preview_frames = []

    for boat_num in range(1, 7):
        boat_prefix = f'艇{boat_num}_'
        boat_cols = [col for col in previews.columns if col.startswith(boat_prefix)]

        if boat_cols:
            tmp = previews[race_id_cols + boat_cols].copy()
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

        # Extract finish positions from result columns
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


def merge_data(programs_long, previews_long=None, results_long=None):
    """Merge Programs with Previews and Results data."""
    if programs_long.empty:
        return pd.DataFrame()

    # Use programs as base
    merged = programs_long.copy()

    # Merge with previews if available
    if previews_long is not None and not previews_long.empty:
        preview_cols = ['レースコード', '艇番', 'コース', 'スタート展示', 'チルト調整', '展示タイム']
        # Filter to only columns that exist in previews_long
        existing_cols = [c for c in preview_cols if c in previews_long.columns]
        if len(existing_cols) > 2:  # At least レースコード and 艇番
            merged = merged.merge(
                previews_long[existing_cols],
                on=['レースコード', '艇番'],
                how='left',
                suffixes=('', '_preview')
            )

    # Merge with results if available
    if results_long is not None and not results_long.empty:
        merged = merged.merge(
            results_long[['レースコード', '艇番', '着順']],
            on=['レースコード', '艇番'],
            how='left'
        )

    return merged


def extract_day_number(day_str):
    """Extract day number from 日次 string ('第1日' -> 1)."""
    if pd.isna(day_str):
        return np.nan
    day_str = str(day_str)
    if '第' in day_str and '日' in day_str:
        try:
            return int(day_str.replace('第', '').replace('日', ''))
        except Exception:
            return np.nan
    return np.nan


def compute_konseki_features(df):
    """Compute features from 今節成績 columns (着順0 = accident → NaN)."""
    konseki_cols_2 = [f'今節成績_{i}-2' for i in range(1, 7)]
    existing_place_cols = [c for c in konseki_cols_2 if c in df.columns]

    if not existing_place_cols:
        df['今節_平均着順'] = np.nan
        df['今節_1着回数'] = 0
        df['今節_3連対率'] = 0.0
        df['今節_出走回数'] = 0
        df['今節_最新着順'] = np.nan
        return df

    place_data = df[existing_place_cols].copy()
    place_data = place_data.apply(pd.to_numeric, errors='coerce')
    place_data = place_data.replace(0, np.nan)

    df['今節_平均着順'] = place_data.mean(axis=1)
    df['今節_1着回数'] = (place_data == 1).sum(axis=1)
    df['今節_3連対率'] = (place_data <= 3).sum(axis=1) / place_data.notna().sum(axis=1)
    df['今節_出走回数'] = place_data.notna().sum(axis=1)

    latest = np.full(len(df), np.nan)
    for col in reversed(existing_place_cols):
        vals = pd.to_numeric(df[col], errors='coerce').replace(0, np.nan)
        mask = np.isnan(latest) & vals.notna().values
        latest[mask] = vals.values[mask]
    df['今節_最新着順'] = latest

    return df


def compute_relative_features(df):
    """Compute race-relative features."""
    if '全国勝率' in df.columns:
        grp = df.groupby('レースコード')['全国勝率']
        df['全国勝率_偏差'] = df['全国勝率'] - grp.transform('mean')
        df['全国勝率_最大差'] = df['全国勝率'] - grp.transform('max')

    if 'モーター2連対率' in df.columns:
        df['モーター2連対率_順位'] = df.groupby('レースコード')['モーター2連対率'].rank(
            ascending=False, method='min'
        )

    if '当地勝率' in df.columns:
        grp = df.groupby('レースコード')['当地勝率']
        df['当地勝率_偏差'] = df['当地勝率'] - grp.transform('mean')

    return df


def compute_course_features(df):
    """Compute course-related features."""
    if '全国勝率' in df.columns and '枠' in df.columns:
        df['枠×全国勝率'] = df['枠'] * df['全国勝率'].fillna(0)

    in_advantage_map = {1: 5, 2: 3, 3: 2, 4: 1, 5: 0, 6: 0}
    if '枠' in df.columns:
        df['イン有利度'] = df['枠'].map(in_advantage_map).fillna(0)

    return df


def load_models(repo_root):
    """Load pre-trained program-based stadium models from pickle file."""
    model_path = repo_root / 'models' / 'program_models_v2.pkl'

    if not model_path.exists():
        print(f"Model file not found: {model_path}", file=sys.stderr)
        return None

    try:
        with open(model_path, 'rb') as f:
            models_dict = pickle.load(f)
        stadium_count = sum(1 for k in models_dict if not str(k).startswith('_'))
        print(f"Loaded {stadium_count} stadium models from {model_path}")
        return models_dict
    except Exception as e:
        print(f"Error loading models: {e}", file=sys.stderr)
        return None


def prepare_features(data, feature_cols):
    """Prepare feature matrix from data."""
    X = pd.DataFrame(index=data.index)

    # 利用可能な特徴量のみを抽出
    for col in feature_cols:
        if col in data.columns:
            X[col] = pd.to_numeric(data[col], errors='coerce')
        else:
            # 特徴量が存在しない場合は0で埋める
            X[col] = 0.0

    # Fill NaN with median or 0 if all values are NaN
    for col in X.columns:
        # 非NaNの値が存在するかチェック
        if X[col].notna().any():
            median_val = X[col].median()
            X[col] = X[col].fillna(median_val)
        else:
            # すべてNaNの場合は0で埋める
            X[col] = X[col].fillna(0)

    return X


def make_predictions(models_dict, predict_date, repo_root):
    """Make predictions for a specific date using ensemble of stadium-specific models."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')
    day = predict_date.strftime('%d')

    programs, previews, results = load_data(year, month, day, repo_root)

    if programs is None:
        print(f"Missing programs data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Reshape data
    programs_long = reshape_programs(programs)
    previews_long = reshape_previews(previews) if previews is not None else None
    results_long = reshape_results(results) if results is not None else None

    # Merge data
    merged = merge_data(programs_long, previews_long, results_long)

    if merged.empty:
        print(f"No merged data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Add 枠 column (= 艇番) for feature compatibility with notebook training
    merged['枠'] = merged['艇番']

    # Extract day number
    if '日次' in merged.columns:
        merged['日次数'] = merged['日次'].apply(extract_day_number)

    # Apply feature engineering
    merged = compute_konseki_features(merged)
    merged = compute_relative_features(merged)
    merged = compute_course_features(merged)

    # Encode grade
    if '級別' in merged.columns:
        grade_map = {'A1': 0, 'A2': 1, 'B1': 2, 'B2': 3}
        merged['級別_encoded'] = merged['級別'].map(grade_map).fillna(3)

    # Map stadium names to numbers for stats merge
    merged['レース場_num'] = merged['レース場'].apply(
        lambda x: STADIUM_NAME_TO_CODE.get(str(x).strip()) if pd.notna(x) else np.nan
    )

    # Merge player/stadium stats from model
    ensemble_weights = models_dict.get('_ensemble_weights', (0.5, 0.3, 0.2))
    player_stats = models_dict.get('_player_stats')
    stadium_player_stats = models_dict.get('_stadium_player_stats')

    if player_stats is not None and '登録番号' in merged.columns:
        merged['登録番号'] = pd.to_numeric(merged['登録番号'], errors='coerce')
        merged = merged.merge(player_stats, on='登録番号', how='left')

    if stadium_player_stats is not None and '登録番号' in merged.columns:
        merged = merged.merge(
            stadium_player_stats,
            left_on=['登録番号', 'レース場_num'],
            right_on=['登録番号', 'レース場'],
            how='left',
            suffixes=('', '_stadium_stat')
        )

    w_rank, w_cls, w_gbc = ensemble_weights

    # Make predictions per race
    predictions = []
    merged_reset = merged.reset_index(drop=True)

    for race_code in merged_reset['レースコード'].unique():
        race_mask = merged_reset['レースコード'] == race_code
        race_data = merged_reset[race_mask]

        stadium_name = race_data['レース場'].iloc[0]
        stadium_code = (
            STADIUM_NAME_TO_CODE.get(str(stadium_name).strip())
            if pd.notna(stadium_name) else None
        )

        if stadium_code is None or stadium_code not in models_dict:
            continue

        model_info = models_dict[stadium_code]
        feature_cols = model_info['features']

        X_race = prepare_features(race_data, feature_cols)
        n = len(X_race)

        if n < 2:
            continue

        # LambdaRank scores
        rank_scores = np.zeros(n)
        has_rank = 'ranking_model' in model_info
        if has_rank:
            rank_scores = model_info['ranking_model'].predict(X_race)

        # Classifier scores (expected placement, negated so higher = better)
        cls_scores = np.zeros(n)
        has_cls = 'model' in model_info and 'scaler' in model_info
        if has_cls:
            try:
                X_scaled = model_info['scaler'].transform(X_race)
                proba = model_info['model'].predict_proba(X_scaled)
                classes = model_info['model'].classes_
                expected_place = proba @ classes.astype(float)
                cls_scores = -expected_place
            except Exception:
                has_cls = False

        # Min-max normalize within race
        for arr in [rank_scores, cls_scores]:
            vmin, vmax = arr.min(), arr.max()
            if vmax > vmin:
                arr[:] = (arr - vmin) / (vmax - vmin)
            else:
                arr[:] = 0.5

        # Ensemble: combine available models
        # Merge classifier + GBC weights since only classifier is saved
        if has_rank and has_cls:
            ensemble = w_rank * rank_scores + (w_cls + w_gbc) * cls_scores
        elif has_rank:
            ensemble = rank_scores
        else:
            ensemble = cls_scores

        # Get top 3 boats
        boat_numbers = race_data['艇番'].values
        top_indices = np.argsort(-ensemble)[:3]

        predictions.append({
            'レースコード': race_code,
            '予想1着': int(boat_numbers[top_indices[0]]),
            '予想2着': int(boat_numbers[top_indices[1]]),
            '予想3着': int(boat_numbers[top_indices[2]]),
        })

    return pd.DataFrame(predictions) if predictions else None


def create_ranking_output(predictions_df):
    """Create ranking output (1st, 2nd, 3rd places per race)."""
    ranking_results = []

    for race_id in predictions_df['レースコード'].unique():
        race_data = predictions_df[
            predictions_df['レースコード'] == race_id
        ]

        # Get the prediction (should be the same for all boats in the race)
        if race_data['予想三連単'].notna().any():
            prediction = race_data['予想三連単'].iloc[0]
            if prediction is not None and len(prediction) >= 3:
                ranking_results.append({
                    'レースコード': race_id,
                    '予想1着': prediction[0],
                    '予想2着': prediction[1],
                    '予想3着': prediction[2],
                })

    return pd.DataFrame(ranking_results)


def save_results(ranking_df, predict_date, repo_root):
    """Save prediction results to CSV and commit to git."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')

    output_dir = repo_root / 'data' / 'estimate' / year / month
    output_dir.mkdir(parents=True, exist_ok=True)

    day = predict_date.strftime('%d')
    output_path = output_dir / f'{day}.csv'

    ranking_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    # Git commit and push
    relative_path = f'data/estimate/{year}/{month}/{day}.csv'
    message = f'Update race predictions: {predict_date.strftime("%Y-%m-%d")}'
    if git_operations.commit_and_push([relative_path], message):
        print(f"Git commit and push succeeded for {output_path}")
    else:
        print(f"Git commit and push failed for {output_path}")

    return output_path


def main():
    # Get today's date in JST (UTC+9)
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst).strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description='Estimate boat race results.')
    parser.add_argument(
        '--date',
        type=str,
        default=today_jst,
        help='Prediction date in YYYY-MM-DD format (default: today JST)'
    )

    args = parser.parse_args()

    # Parse prediction date
    try:
        predict_date = datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Use YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)

    repo_root = get_repo_root()

    print(f"Prediction date: {predict_date.strftime('%Y-%m-%d')}")
    print("-" * 70)

    # Load pre-trained models
    print("Loading pre-trained models...")
    models_dict = load_models(repo_root)

    if models_dict is None:
        print("Failed to load models", file=sys.stderr)
        sys.exit(1)

    # Make predictions (returns per-race ranking directly)
    print("Making predictions...")
    ranking_df = make_predictions(models_dict, predict_date, repo_root)

    if ranking_df is None or ranking_df.empty:
        print("Failed to make predictions", file=sys.stderr)
        sys.exit(1)

    print(f"Rankings created for {len(ranking_df)} races")

    # Save results
    output_path = save_results(ranking_df, predict_date, repo_root)
    print(f"Results saved to {output_path}")

    # Display sample results
    print("-" * 70)
    print("Sample predictions (first 5 races):")
    print(ranking_df.head().to_string(index=False))


if __name__ == '__main__':
    main()
