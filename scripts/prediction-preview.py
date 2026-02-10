#!/usr/bin/env python3
"""
Boat race preview prediction script.

This script uses pre-trained stadium-specific models to make predictions
for boat race previews (exhibition times, course entries, start timings, tilt adjustments)
for a specified date. Models are trained from previews.ipynb and saved to
models/preview_models.pkl.

Output format matches data/previews/ CSV format (53 columns) so that
the estimate pipeline can consume prediction-preview data directly.

Usage:
    python prediction-preview.py --date 2026-01-30
"""

import argparse
import json
import pickle
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import linear_sum_assignment
from sklearn.preprocessing import StandardScaler

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations
from boatrace.constants import STADIUM_NAME_TO_CODE
from boatrace.common import get_repo_root, reshape_programs as _reshape_programs, prepare_features


def load_weather_stats(repo_root):
    """Load pre-computed weather statistics (stadium × month)."""
    stats_path = repo_root / 'models' / 'weather_stats.json'
    if not stats_path.exists():
        print(f"Weather stats not found: {stats_path}", file=sys.stderr)
        print("Run build_weather_stats.py first.", file=sys.stderr)
        return {}

    with open(stats_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_weather_for_race(weather_stats, stadium_code, month):
    """Get weather statistics for a stadium and month."""
    key = f"{stadium_code}_{month}"
    if key in weather_stats:
        entry = weather_stats[key]
        return {
            '風速(m)': entry.get('風速(m)', 0.0),
            '風向': entry.get('風向', 1),
            '波の高さ(cm)': entry.get('波の高さ(cm)', 0.0),
            '天候': entry.get('天候', 1),
            '気温(℃)': entry.get('気温(℃)', 20.0),
            '水温(℃)': entry.get('水温(℃)', 20.0),
        }
    # Fallback defaults
    return {
        '風速(m)': 3.0,
        '風向': 1,
        '波の高さ(cm)': 3.0,
        '天候': 1,
        '気温(℃)': 20.0,
        '水温(℃)': 20.0,
    }


def load_data(year, month, day, repo_root):
    """Load Programs data for a specific date."""
    programs_path = repo_root / 'data' / 'programs' / year / month / f'{day}.csv'

    try:
        if programs_path.exists():
            return pd.read_csv(programs_path)
        else:
            print(f"Programs file not found: {programs_path}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"Error loading programs data for {year}-{month}-{day}: {e}", file=sys.stderr)
        return None


def reshape_programs(programs):
    """Reshape Programs data with title column."""
    return _reshape_programs(programs, include_title=True)


def load_preview_models(repo_root):
    """Load pre-trained preview stadium models from pickle file."""
    model_path = repo_root / 'models' / 'preview_models.pkl'

    if not model_path.exists():
        print(f"Model file not found: {model_path}", file=sys.stderr)
        return None

    try:
        with open(model_path, 'rb') as f:
            models = pickle.load(f)
        print(f"Loaded preview models from {model_path}")
        return models
    except Exception as e:
        print(f"Error loading preview models: {e}", file=sys.stderr)
        return None


def make_predictions(models, predict_date, repo_root):
    """Make preview predictions for a specific date using stadium-specific models."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')
    day = predict_date.strftime('%d')

    programs = load_data(year, month, day, repo_root)

    if programs is None:
        print(f"Missing programs data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Reshape data
    programs_long = reshape_programs(programs)

    if programs_long.empty:
        print(f"No programs data after reshaping for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Prepare predictions for each task
    # Tasks: exhibition_time, course_entry, start_timing, tilt_adjustment
    task_predictions = {
        'exhibition_time': [],
        'course_entry': [],
        'course_entry_proba': [],
        'start_timing': [],
        'tilt_adjustment': [],
    }

    programs_reset = programs_long.reset_index(drop=True)

    for idx, row in programs_reset.iterrows():
        stadium_name = row['レース場']

        # Convert stadium name to stadium code
        stadium_code = STADIUM_NAME_TO_CODE.get(stadium_name)
        if stadium_code is None:
            print(f"Unknown stadium name: {stadium_name}", file=sys.stderr)
            for task in task_predictions:
                task_predictions[task].append(None)
            continue

        # Make predictions for each task
        for task in task_predictions:
            # course_entry_proba is populated alongside course_entry
            if task == 'course_entry_proba':
                continue

            if task not in models:
                print(f"Task {task} not found in models", file=sys.stderr)
                task_predictions[task].append(None)
                if task == 'course_entry':
                    task_predictions['course_entry_proba'].append(None)
                continue

            if stadium_code not in models[task]:
                task_predictions[task].append(None)
                if task == 'course_entry':
                    task_predictions['course_entry_proba'].append(None)
                continue

            model_info = models[task][stadium_code]
            model = model_info['model']
            scaler = model_info['scaler']
            feature_cols = model_info['features']

            try:
                # Prepare features
                X_row = prepare_features(programs_reset.iloc[idx:idx+1], feature_cols)
                X_scaled = pd.DataFrame(scaler.transform(X_row), columns=X_row.columns)

                # Make prediction based on task type
                if task == 'course_entry':
                    # Classification task: predict course (1-6)
                    prediction = model.predict(X_scaled)[0]
                    # Also collect predict_proba for Hungarian assignment
                    try:
                        proba = model.predict_proba(X_scaled)[0]
                        classes = model.classes_
                        proba_dict = {int(c): float(p) for c, p in zip(classes, proba)}
                    except Exception:
                        proba_dict = None
                    task_predictions['course_entry_proba'].append(proba_dict)
                else:
                    # Regression task: predict value (time, timing, tilt)
                    prediction = model.predict(X_scaled)[0]

                # Restore exhibition_time variance compressed by model averaging
                # Actual std=0.114, predicted std=0.064 → noise σ=sqrt(0.114²-0.064²)≈0.094
                if task == 'exhibition_time':
                    prediction += np.random.normal(0, 0.094)

                task_predictions[task].append(prediction)
            except Exception as e:
                print(f"Error predicting {task} for row {idx}: {e}", file=sys.stderr)
                task_predictions[task].append(None)
                if task == 'course_entry':
                    task_predictions['course_entry_proba'].append(None)

    # Add predictions to programs_long
    programs_long['予測コース'] = task_predictions['course_entry']
    programs_long['予測コース_proba'] = task_predictions['course_entry_proba']
    programs_long['予測スタート展示'] = task_predictions['start_timing']
    programs_long['予測チルト調整'] = task_predictions['tilt_adjustment']
    programs_long['予測展示タイム'] = task_predictions['exhibition_time']

    return programs_long


def _hungarian_course_assignment(probas):
    """Assign courses to boats using Hungarian algorithm on predict_proba results.

    Args:
        probas: list of 6 dicts mapping course (int 1-6) -> probability, one per boat.

    Returns:
        dict mapping boat_num (1-6) -> assigned course (1-6), or None on failure.
    """
    if len(probas) != 6 or any(p is None for p in probas):
        return None

    try:
        courses = list(range(1, 7))
        # Build 6x6 cost matrix (negative probability for minimization)
        cost = np.zeros((6, 6))
        for boat_idx in range(6):
            for course_idx, course in enumerate(courses):
                cost[boat_idx, course_idx] = -probas[boat_idx].get(course, 0.0)

        row_ind, col_ind = linear_sum_assignment(cost)
        return {boat_idx + 1: courses[col] for boat_idx, col in zip(row_ind, col_ind)}
    except Exception:
        return None


def reshape_to_wide_format(predictions_long, weather_stats, predict_date):
    """Reshape long format predictions to wide format matching previews CSV format.

    Output columns (53 total):
        レースコード, タイトル, レース日, レース場, レース回,
        風速(m), 風向, 波の高さ(cm), 天候, 気温(℃), 水温(℃),
        艇N_艇番, 艇N_コース, 艇N_体重(kg), 艇N_体重調整(kg),
        艇N_展示タイム, 艇N_チルト調整, 艇N_スタート展示  (×6)
    """
    if predictions_long is None or predictions_long.empty:
        return pd.DataFrame()

    month = predict_date.month
    wide_data = []

    for race_id in predictions_long['レースコード'].unique():
        race_data = predictions_long[predictions_long['レースコード'] == race_id]
        first = race_data.iloc[0]

        # Convert stadium name to code
        stadium_name = first['レース場']
        stadium_code = STADIUM_NAME_TO_CODE.get(stadium_name)

        # Zero-pad race number: "1R" -> "01R"
        race_num_raw = str(first['レース回'])
        if race_num_raw.endswith('R') and len(race_num_raw) == 2:
            race_num = race_num_raw[0].zfill(2) + 'R'
        else:
            race_num = race_num_raw

        # Get weather stats for this stadium and month
        weather = get_weather_for_race(weather_stats, stadium_code, month)

        # Get title from programs (may be a long string with race info)
        title = first.get('タイトル', '')

        row = {
            'レースコード': race_id,
            'タイトル': title,
            'レース日': first['レース日'],
            'レース場': stadium_code if stadium_code is not None else first['レース場'],
            'レース回': race_num,
            '風速(m)': weather['風速(m)'],
            '風向': weather['風向'],
            '波の高さ(cm)': weather['波の高さ(cm)'],
            '天候': weather['天候'],
            '気温(℃)': weather['気温(℃)'],
            '水温(℃)': weather['水温(℃)'],
        }

        # Try Hungarian assignment using predict_proba
        probas = []
        for boat_num in range(1, 7):
            boat_data = race_data[race_data['艇番'] == boat_num]
            if not boat_data.empty:
                probas.append(boat_data.iloc[0].get('予測コース_proba'))
            else:
                probas.append(None)

        hungarian_result = _hungarian_course_assignment(probas)

        # Add boat-specific data
        for boat_num in range(1, 7):
            boat_data = race_data[race_data['艇番'] == boat_num]
            if not boat_data.empty:
                bd = boat_data.iloc[0]
                weight = pd.to_numeric(bd.get('体重'), errors='coerce')
                if hungarian_result is not None:
                    course = hungarian_result[boat_num]
                else:
                    # Fallback: frame number = course
                    course = boat_num
                row[f'艇{boat_num}_艇番'] = boat_num
                row[f'艇{boat_num}_コース'] = course
                row[f'艇{boat_num}_体重(kg)'] = weight if pd.notna(weight) else 0.0
                row[f'艇{boat_num}_体重調整(kg)'] = 0.0
                row[f'艇{boat_num}_展示タイム'] = bd['予測展示タイム']
                row[f'艇{boat_num}_チルト調整'] = bd['予測チルト調整']
                row[f'艇{boat_num}_スタート展示'] = bd['予測スタート展示']
            else:
                row[f'艇{boat_num}_艇番'] = boat_num
                row[f'艇{boat_num}_コース'] = boat_num  # Fallback
                row[f'艇{boat_num}_体重(kg)'] = 0.0
                row[f'艇{boat_num}_体重調整(kg)'] = 0.0
                row[f'艇{boat_num}_展示タイム'] = None
                row[f'艇{boat_num}_チルト調整'] = None
                row[f'艇{boat_num}_スタート展示'] = None

        wide_data.append(row)

    return pd.DataFrame(wide_data)


def save_results(predictions_df, predict_date, repo_root):
    """Save prediction results to CSV and commit to git."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')

    output_dir = repo_root / 'data' / 'prediction-preview' / year / month
    output_dir.mkdir(parents=True, exist_ok=True)

    day = predict_date.strftime('%d')
    output_path = output_dir / f'{day}.csv'

    predictions_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    # Git commit and push
    relative_path = f'data/prediction-preview/{year}/{month}/{day}.csv'
    message = f'Update preview predictions: {predict_date.strftime("%Y-%m-%d")}'
    if git_operations.commit_and_push([relative_path], message):
        print(f"Git commit and push succeeded for {output_path}")
    else:
        print(f"Git commit and push failed for {output_path}")

    return output_path


def main():
    # Get today's date in JST (UTC+9)
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst).strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description='Generate preview predictions for boat races.')
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

    # Load weather statistics
    print("Loading weather statistics...")
    weather_stats = load_weather_stats(repo_root)

    # Load pre-trained models
    print("Loading pre-trained preview models...")
    models = load_preview_models(repo_root)

    if models is None:
        print("Failed to load preview models", file=sys.stderr)
        sys.exit(1)

    # Make predictions
    print("Making preview predictions...")
    predictions_long = make_predictions(models, predict_date, repo_root)

    if predictions_long is None:
        print("Failed to make preview predictions", file=sys.stderr)
        sys.exit(1)

    print(f"Predictions made for {len(predictions_long)} boats")

    # Reshape to wide format (matching previews CSV format)
    predictions_df = reshape_to_wide_format(predictions_long, weather_stats, predict_date)
    print(f"Reshaped predictions to wide format ({len(predictions_df)} races)")

    # Save results
    output_path = save_results(predictions_df, predict_date, repo_root)
    print(f"Results saved to {output_path}")

    # Display sample results
    print("-" * 70)
    print("Sample predictions (first 2 races):")
    cols = ['レースコード', 'レース場', 'レース回', '風速(m)', '気温(℃)',
            '艇1_艇番', '艇1_コース', '艇1_体重(kg)', '艇1_展示タイム']
    available_cols = [c for c in cols if c in predictions_df.columns]
    print(predictions_df[available_cols].head(2).to_string(index=False))
    print(f"\nTotal columns: {len(predictions_df.columns)}")
    print(f"Columns: {list(predictions_df.columns)}")


if __name__ == '__main__':
    main()
