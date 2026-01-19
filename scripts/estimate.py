#!/usr/bin/env python3
"""
Boat race result estimation script.

This script loads training data from multiple dates and makes predictions
for a specified date using a Random Forest model based on the PCA notebook.

Usage:
    python estimate.py --date 2022-12-23
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def load_data(year, month, day, repo_root):
    """Load Programs, Previews, and Results data for a specific date."""
    programs_path = repo_root / 'data' / 'programs' / year / month / f'{day}.csv'
    previews_path = repo_root / 'data' / 'previews' / year / month / f'{day}.csv'
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


def reshape_results(results):
    """Reshape Results data from wide format (1着～6着) to long format."""
    if results is None or results.empty:
        return pd.DataFrame()

    result_frames = []

    for place in range(1, 7):
        place_prefix = f'{place}着_'
        place_cols = [col for col in results.columns if col.startswith(place_prefix)]

        if place_cols:
            tmp = results[['レースコード'] + place_cols].copy()
            rename_map = {col: col[len(place_prefix):] for col in place_cols}
            tmp = tmp.rename(columns=rename_map)
            tmp['着順'] = place
            result_frames.append(tmp)

    if result_frames:
        return pd.concat(result_frames, ignore_index=True)
    return pd.DataFrame()


def merge_data(programs_long, previews_long, results_long=None):
    """Merge Programs, Previews, and optionally Results data."""
    if programs_long.empty or previews_long.empty:
        return pd.DataFrame()

    # Merge programs and previews
    programs_merge = programs_long.drop(
        columns=['レース日', 'レース場', 'レース回'],
        errors='ignore'
    )
    programs_merge = programs_merge.rename(columns={'艇番': '艇番_prog'}) \
        if '艇番' in programs_merge.columns else programs_merge

    merged = previews_long.merge(
        programs_merge,
        on='レースコード',
        how='left',
        suffixes=('_preview', '_program')
    )

    # Filter by matching boat numbers
    if '艇番' in merged.columns and '艇番_prog' in merged.columns:
        merged = merged[merged['艇番'] == merged['艇番_prog']].copy()
        merged = merged.drop(columns=['艇番_prog'], errors='ignore')

    # Merge with results if available
    if results_long is not None and not results_long.empty:
        merged = merged.merge(
            results_long[['レースコード', '艇番', '着順']],
            on=['レースコード', '艇番'],
            how='left'
        )

    return merged


def get_available_features(final_data):
    """Get available features for model training."""
    program_features = [
        '全国勝率', '全国2連対率', '当地勝率', '当地2連対率',
        'モーター2連対率', 'ボート2連対率'
    ]
    preview_features = [
        '6日間勝率', '6日間2連対率', '当地勝率', '当地2連対率',
        'モーター2連対率', 'ボート2連対率', '伸び率', '足踏み率',
        'スタート率'
    ]

    available_features = []
    for feat in program_features + preview_features:
        if feat in final_data.columns:
            available_features.append(feat)

    return available_features


def prepare_features(data, available_features):
    """Prepare feature matrix from data."""
    X = data[available_features].copy()
    X_numeric = X.apply(pd.to_numeric, errors='coerce')
    return X_numeric


def get_training_dates(predict_date, num_days=30):
    """Get list of training dates (previous days)."""
    training_dates = []
    current_date = predict_date - timedelta(days=1)

    while len(training_dates) < num_days and current_date.year >= 2020:
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        training_dates.append((year, month, day, current_date))
        current_date -= timedelta(days=1)

    return training_dates


def train_model(training_dates, repo_root):
    """Train the Random Forest model using training data from multiple dates."""
    all_data = []

    for year, month, day, _ in training_dates:
        programs, previews, results = load_data(year, month, day, repo_root)

        if programs is not None and previews is not None and results is not None:
            programs_long = reshape_programs(programs)
            previews_long = reshape_previews(previews)
            results_long = reshape_results(results)

            merged = merge_data(programs_long, previews_long, results_long)
            if not merged.empty:
                all_data.append(merged)

    if not all_data:
        print("No training data found", file=sys.stderr)
        return None, None, None

    # Combine all training data
    combined_data = pd.concat(all_data, ignore_index=True)

    # Get available features
    available_features = get_available_features(combined_data)

    if not available_features:
        print("No available features in training data", file=sys.stderr)
        return None, None, None

    # Prepare features and target
    X_all = prepare_features(combined_data, available_features)
    y_all = combined_data['着順'].copy()

    # Filter valid rows
    valid_idx = X_all.notna().all(axis=1) & y_all.notna()
    X_train = X_all[valid_idx].copy()
    y_train = y_all[valid_idx].copy()

    if len(X_train) == 0:
        print("No valid training samples", file=sys.stderr)
        return None, None, None

    # Train model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    return model, available_features, X_train.columns.tolist()


def make_predictions(model, available_features, predict_date, repo_root):
    """Make predictions for a specific date."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')
    day = predict_date.strftime('%d')

    programs, previews, _ = load_data(year, month, day, repo_root)

    if programs is None or previews is None:
        print(f"Missing data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Reshape data
    programs_long = reshape_programs(programs)
    previews_long = reshape_previews(previews)

    # Merge data
    merged = merge_data(programs_long, previews_long)

    if merged.empty:
        print(f"No merged data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Prepare features
    X_test = prepare_features(merged, available_features)

    # Make predictions
    predictions = model.predict(X_test)

    # Create results dataframe
    results_df = pd.DataFrame({
        'レースコード': merged['レースコード'].values,
        '艇番': merged['艇番'].values,
        '予想着順': predictions
    })

    return results_df


def create_ranking_output(predictions_df):
    """Create ranking output (1st, 2nd, 3rd places per race)."""
    ranking_results = []

    for race_id in predictions_df['レースコード'].unique():
        race_data = predictions_df[
            predictions_df['レースコード'] == race_id
        ].copy()

        # Sort by predicted position
        race_data = race_data.sort_values('予想着順')

        # Get predicted 1st, 2nd, 3rd place boats
        predicted_boats = race_data['艇番'].head(3).values

        ranking_results.append({
            'レースコード': race_id,
            '予想1着': predicted_boats[0] if len(predicted_boats) > 0 else None,
            '予想2着': predicted_boats[1] if len(predicted_boats) > 1 else None,
            '予想3着': predicted_boats[2] if len(predicted_boats) > 2 else None,
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
    parser = argparse.ArgumentParser(description='Estimate boat race results.')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Prediction date in YYYY-MM-DD format (default: yesterday)'
    )
    parser.add_argument(
        '--training-days',
        type=int,
        default=30,
        help='Number of previous days to use for training (default: 30)'
    )

    args = parser.parse_args()

    # Parse prediction date
    if args.date:
        try:
            predict_date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        predict_date = datetime.now() - timedelta(days=1)

    repo_root = get_repo_root()

    print(f"Prediction date: {predict_date.strftime('%Y-%m-%d')}")
    print(f"Training days: {args.training_days}")
    print("-" * 70)

    # Get training dates
    training_dates = get_training_dates(predict_date, args.training_days)
    print(f"Using {len(training_dates)} training dates")

    # Train model
    print("Training model...")
    model, available_features, feature_cols = train_model(training_dates, repo_root)

    if model is None:
        print("Failed to train model", file=sys.stderr)
        sys.exit(1)

    print(f"Model trained with {len(available_features)} features")

    # Make predictions
    print("Making predictions...")
    predictions_df = make_predictions(model, available_features, predict_date, repo_root)

    if predictions_df is None:
        print("Failed to make predictions", file=sys.stderr)
        sys.exit(1)

    print(f"Predictions made for {len(predictions_df)} boats")

    # Create ranking output
    ranking_df = create_ranking_output(predictions_df)
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
