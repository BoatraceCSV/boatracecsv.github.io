#!/usr/bin/env python3
"""
Boat race result estimation script.

This script uses pre-trained stadium-specific models to make predictions
for a specified date. Models are trained from program_v2.ipynb and saved to
models/program_models_v2.pkl.

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
from boatrace.constants import (
    STADIUM_NAME_TO_CODE,
    STADIUM_ADVANTAGE_MAP,
    DEFAULT_ADVANTAGE_MAP,
    PLACE_COLS as _PLACE_COLS,
    RATE_COLS as _RATE_COLS,
    WIND_DIRECTION_TO_ANGLE,
)
from boatrace.common import (
    get_repo_root,
    reshape_programs,
    reshape_previews,
    reshape_results,
    prepare_features,
)


def load_data(year, month, day, repo_root):
    """Load Programs, Previews, and Results data for a specific date."""
    programs_path = repo_root / 'data' / 'programs' / year / month / f'{day}.csv'
    actual_previews_path = repo_root / 'data' / 'previews' / year / month / f'{day}.csv'
    prediction_previews_path = repo_root / 'data' / 'prediction-preview' / year / month / f'{day}.csv'
    previews_path = actual_previews_path if actual_previews_path.exists() else prediction_previews_path
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


def merge_data(programs_long, previews_long=None, results_long=None):
    """Merge Programs with Previews and Results data."""
    if programs_long.empty:
        return pd.DataFrame()

    # Use programs as base
    merged = programs_long.copy()

    # Merge with previews if available
    if previews_long is not None and not previews_long.empty:
        preview_cols = [
            'レースコード', '艇番', 'コース', 'スタート展示', 'チルト調整', '展示タイム',
            '風速(m)', '風向', '波の高さ(cm)', '気温(℃)', '水温(℃)',
        ]
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
        df['今節_平均コース'] = np.nan
        df['今節_イン回数'] = 0
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

    # 今節コース傾向特徴量
    konseki_cols_1 = [f'今節成績_{i}-1' for i in range(1, 7)]
    existing_course = [c for c in konseki_cols_1 if c in df.columns]
    if existing_course:
        course_data = df[existing_course].apply(pd.to_numeric, errors='coerce')
        valid_mask = place_data.notna()
        course_valid = course_data.where(valid_mask.values)
        df['今節_平均コース'] = course_valid.mean(axis=1)
        df['今節_イン回数'] = (course_valid == 1).sum(axis=1)
    else:
        df['今節_平均コース'] = np.nan
        df['今節_イン回数'] = 0

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
    """Compute course-related features with data-driven per-stadium advantage maps."""
    if '全国勝率' in df.columns and '枠' in df.columns:
        df['枠×全国勝率'] = df['枠'] * df['全国勝率'].fillna(0)

    if '枠' in df.columns:
        if 'レース場_num' in df.columns:
            df['イン有利度'] = df.apply(
                lambda row: STADIUM_ADVANTAGE_MAP.get(
                    row['レース場_num'], DEFAULT_ADVANTAGE_MAP
                ).get(row['枠'], 0),
                axis=1,
            )
        else:
            df['イン有利度'] = df['枠'].map(DEFAULT_ADVANTAGE_MAP).fillna(0)

    # Wind speed interaction features
    if '風速(m)' in df.columns and 'イン有利度' in df.columns:
        wind = pd.to_numeric(df['風速(m)'], errors='coerce').fillna(3.0)
        df['風速×イン有利度'] = wind * df['イン有利度']
        df['強風フラグ'] = (wind >= 5).astype(int)
        if '枠' in df.columns:
            df['強風×枠'] = df['強風フラグ'] * df['枠']

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
        metadata = models_dict.get('_metadata')
        if metadata:
            print(f"Model v{metadata.get('model_version', '?')}, "
                  f"trained {metadata.get('training_date', '?')[:10]}, "
                  f"{metadata.get('feature_count', '?')} features")
        return models_dict
    except Exception as e:
        print(f"Error loading models: {e}", file=sys.stderr)
        return None




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
    previews_long = reshape_previews(previews, include_weather=True) if previews is not None else None
    results_long = reshape_results(results) if results is not None else None

    # Merge data
    merged = merge_data(programs_long, previews_long, results_long)

    if merged.empty:
        print(f"No merged data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Supplement 体重(kg) from programs' 体重 when previews lack weight data
    if '体重(kg)' in merged.columns and '体重' in merged.columns:
        weight_kg_missing = merged['体重(kg)'].isna()
        weight_available = merged['体重'].notna()
        fill_mask = weight_kg_missing & weight_available
        merged.loc[fill_mask, '体重(kg)'] = pd.to_numeric(
            merged.loc[fill_mask, '体重'], errors='coerce'
        )

    # Add 枠 column (= 艇番) for feature compatibility with notebook training
    merged['枠'] = merged['艇番']

    # Extract day number
    if '日次' in merged.columns:
        merged['日次数'] = merged['日次'].apply(extract_day_number)

    # Map stadium names to numbers (needed before compute_course_features)
    merged['レース場_num'] = merged['レース場'].apply(
        lambda x: STADIUM_NAME_TO_CODE.get(str(x).strip()) if pd.notna(x) else np.nan
    )

    # Apply feature engineering
    merged = compute_konseki_features(merged)
    merged = compute_relative_features(merged)
    merged = compute_course_features(merged)

    # Wind direction circular encoding
    if '風向' in merged.columns:
        angles = merged['風向'].map(WIND_DIRECTION_TO_ANGLE)
        merged['風向sin'] = np.sin(np.radians(angles)).fillna(0)
        merged['風向cos'] = np.cos(np.radians(angles)).fillna(0)

    # Encode grade
    if '級別' in merged.columns:
        grade_map = {'A1': 0, 'A2': 1, 'B1': 2, 'B2': 3}
        merged['級別_encoded'] = merged['級別'].map(grade_map).fillna(3)

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

    # Merge player ST stats if available in model
    player_st_stats = models_dict.get('_player_st_stats')
    if player_st_stats is not None and '登録番号' in merged.columns:
        player_st_stats = player_st_stats.copy()
        player_st_stats['登録番号'] = pd.to_numeric(
            player_st_stats['登録番号'], errors='coerce'
        )
        merged = merged.merge(player_st_stats, on='登録番号', how='left')
        # Fill NaN with median defaults
        if 'ST_mean' in merged.columns:
            merged['ST_mean'] = merged['ST_mean'].fillna(0.167)
        if 'ST_std' in merged.columns:
            merged['ST_std'] = merged['ST_std'].fillna(0.068)
        if 'ST_min' in merged.columns:
            merged['ST_min'] = merged['ST_min'].fillna(0.167)

    # Merge recent race stats if available in model
    recent_race_stats = models_dict.get('_recent_race_stats')
    if recent_race_stats is not None and '登録番号' in merged.columns:
        recent_race_stats = recent_race_stats.copy()
        recent_race_stats['登録番号'] = pd.to_numeric(
            recent_race_stats['登録番号'], errors='coerce'
        )
        merged = merged.merge(recent_race_stats, on='登録番号', how='left')
        if '直近5走_平均着順' in merged.columns:
            merged['直近5走_平均着順'] = merged['直近5走_平均着順'].fillna(3.5)
        if '直近10走_平均着順' in merged.columns:
            merged['直近10走_平均着順'] = merged['直近10走_平均着順'].fillna(3.5)
        if '直近5走_1着率' in merged.columns:
            merged['直近5走_1着率'] = merged['直近5走_1着率'].fillna(0.167)

    # Override with external player stats if available (fresher data)
    external_stats_path = repo_root / 'models' / 'player_stats_latest.json'
    if external_stats_path.exists() and '登録番号' in merged.columns:
        try:
            import json
            with open(external_stats_path) as f:
                ext_data = json.load(f)
            ext_stats = ext_data.get('stats', {})
            # Convert to DataFrame for vectorized merge
            ext_records = []
            for reg_str, stats in ext_stats.items():
                row = {'登録番号': int(reg_str)}
                row.update(stats)
                ext_records.append(row)
            if ext_records:
                ext_df = pd.DataFrame(ext_records)
                stat_cols = [c for c in ext_df.columns if c != '登録番号']
                # Rename to avoid conflicts, then override
                rename_map = {c: f'{c}_ext' for c in stat_cols}
                ext_df = ext_df.rename(columns=rename_map)
                merged = merged.merge(ext_df, on='登録番号', how='left')
                override_count = 0
                for col in stat_cols:
                    ext_col = f'{col}_ext'
                    if ext_col in merged.columns:
                        mask = merged[ext_col].notna()
                        if col not in merged.columns:
                            merged[col] = np.nan
                        merged.loc[mask, col] = merged.loc[mask, ext_col]
                        override_count += mask.sum()
                        merged = merged.drop(columns=[ext_col])
                print(f"Applied external stats ({len(ext_records)} players) from {external_stats_path}")
        except Exception as e:
            print(f"Warning: Failed to load external stats: {e}", file=sys.stderr)

    # Kimarite prediction: compute P(逃げ) per race and store predictions for CSV
    kimarite_predictions = {}
    kimarite_info = models_dict.get('_kimarite_model')
    if kimarite_info is not None:
        kim_model = kimarite_info['model']
        kim_features = kimarite_info['feature_cols']
        kim_le = kimarite_info['label_encoder']
        nige_idx = list(kim_le.classes_).index('逃げ') if '逃げ' in kim_le.classes_ else 0

        race_first = merged.groupby('レースコード').first().reset_index()
        kim_X = pd.DataFrame(index=race_first.index)
        for c in kim_features:
            if c == '場コード' and 'レース場_num' in race_first.columns:
                kim_X[c] = race_first['レース場_num']
            elif c == '風速' and '風速(m)' in race_first.columns:
                kim_X[c] = pd.to_numeric(race_first['風速(m)'], errors='coerce').fillna(3.0)
            elif c in ('風向sin', '風向cos') and c in race_first.columns:
                kim_X[c] = race_first[c].fillna(0)
            elif c == '波高' and '波の高さ(cm)' in race_first.columns:
                kim_X[c] = pd.to_numeric(race_first['波の高さ(cm)'], errors='coerce').fillna(3.0)
            elif c == 'イン有利度' and 'イン有利度' in race_first.columns:
                kim_X[c] = race_first['イン有利度']
            elif c.startswith('1枠_'):
                col_name = c[3:]
                boat1 = merged[merged['枠'] == 1][['レースコード', col_name]].drop_duplicates('レースコード')
                boat1 = boat1.rename(columns={col_name: c})
                merged_tmp = race_first[['レースコード']].merge(boat1, on='レースコード', how='left')
                kim_X[c] = merged_tmp[c].fillna(0).values
            elif c == '他艇_最大勝率' and '全国勝率' in merged.columns:
                others = merged[merged['枠'].isin([2, 3, 4, 5, 6])].groupby('レースコード')['全国勝率'].max().reset_index()
                others = others.rename(columns={'全国勝率': '他艇_最大勝率'})
                merged_tmp = race_first[['レースコード']].merge(others, on='レースコード', how='left')
                kim_X[c] = merged_tmp['他艇_最大勝率'].fillna(0).values
            else:
                kim_X[c] = 0
        for c in kim_X.columns:
            kim_X[c] = pd.to_numeric(kim_X[c], errors='coerce').fillna(0)

        try:
            proba = kim_model.predict_proba(kim_X.values)
            race_first['P_逃げ'] = proba[:, nige_idx]
            merged = merged.merge(
                race_first[['レースコード', 'P_逃げ']],
                on='レースコード', how='left'
            )
            merged['P_逃げ'] = merged['P_逃げ'].fillna(0.6)

            # Store per-race kimarite predictions for CSV output
            predicted_classes = kim_le.classes_[proba.argmax(axis=1)]
            for idx, rc in enumerate(race_first['レースコード'].values):
                kimarite_predictions[rc] = {
                    '予想決まり手': predicted_classes[idx],
                }
        except Exception as e:
            print(f"Kimarite prediction failed: {e}", file=sys.stderr)
            merged['P_逃げ'] = 0.6

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
                X_scaled = pd.DataFrame(model_info['scaler'].transform(X_race), columns=X_race.columns, index=X_race.index)
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

        pred = {
            'レースコード': race_code,
            '予想1着': int(boat_numbers[top_indices[0]]),
            '予想2着': int(boat_numbers[top_indices[1]]),
            '予想3着': int(boat_numbers[top_indices[2]]),
        }
        if race_code in kimarite_predictions:
            pred.update(kimarite_predictions[race_code])
        predictions.append(pred)

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
