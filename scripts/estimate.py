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

import pandas as pd
from sklearn.preprocessing import StandardScaler

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))
from boatrace import git_operations


def get_repo_root():
    """Get the repository root directory."""
    cwd = Path.cwd()
    return cwd if (cwd / 'data').exists() else cwd.parent


def load_data(year, month, day, repo_root):
    """Load Programs and Results data for a specific date."""
    programs_path = repo_root / 'data' / 'programs' / year / month / f'{day}.csv'
    results_path = repo_root / 'data' / 'results' / year / month / f'{day}.csv'

    programs = None
    results = None

    try:
        if programs_path.exists():
            programs = pd.read_csv(programs_path)
        if results_path.exists():
            results = pd.read_csv(results_path)
    except Exception as e:
        print(f"Error loading data for {year}-{month}-{day}: {e}", file=sys.stderr)
        return None, None

    return programs, results


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


def merge_data(programs_long, results_long=None):
    """Merge Programs with Results data."""
    if programs_long.empty:
        return pd.DataFrame()

    # Use programs as base
    merged = programs_long.copy()

    # Merge with results if available
    if results_long is not None and not results_long.empty:
        merged = merged.merge(
            results_long[['レースコード', '艇番', '着順']],
            on=['レースコード', '艇番'],
            how='left'
        )

    return merged


def load_models(repo_root):
    """Load pre-trained program-based stadium models from pickle file."""
    model_path = repo_root / 'models' / 'program_models.pkl'

    if not model_path.exists():
        print(f"Model file not found: {model_path}", file=sys.stderr)
        return None

    try:
        with open(model_path, 'rb') as f:
            models_dict = pickle.load(f)
        print(f"Loaded {len(models_dict)} stadium models from {model_path}")
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
    """Make predictions for a specific date using stadium-specific models."""
    year = predict_date.strftime('%Y')
    month = predict_date.strftime('%m')
    day = predict_date.strftime('%d')

    programs, results = load_data(year, month, day, repo_root)

    if programs is None:
        print(f"Missing programs data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Reshape data
    programs_long = reshape_programs(programs)
    results_long = reshape_results(results) if results is not None else None

    # Merge data
    merged = merge_data(programs_long, results_long)

    if merged.empty:
        print(f"No merged data for {year}-{month}-{day}", file=sys.stderr)
        return None

    # Make predictions for each boat
    predictions = []
    merged_reset = merged.reset_index(drop=True)

    for idx, row in merged_reset.iterrows():
        stadium = row['レース場']

        # Skip if no model for this stadium
        if stadium not in models_dict:
            predictions.append(None)
            continue

        model_info = models_dict[stadium]
        model = model_info['model']
        scaler = model_info['scaler']
        feature_cols = model_info['features']

        # Prepare features
        X_row = prepare_features(merged_reset.iloc[idx:idx+1], feature_cols)

        try:
            # Get predicted probabilities
            X_scaled = scaler.transform(X_row)
            proba = model.predict_proba(X_scaled)[0]
            classes = model.classes_

            # Get top 3 predictions (finish positions)
            prob_dict = {cls: prob for cls, prob in zip(classes, proba)}
            sorted_probs = sorted(prob_dict.items(), key=lambda x: x[1], reverse=True)
            top_3 = sorted_probs[:3]

            # Return tuple of top 3 predicted boat numbers
            predictions.append(tuple(int(boat) for boat, _ in top_3))
        except Exception as e:
            print(f"Error predicting for row {idx}: {e}", file=sys.stderr)
            predictions.append(None)

    # Create results dataframe
    results_df = pd.DataFrame({
        'レースコード': merged['レースコード'].values,
        '艇番': merged['艇番'].values,
        'レース場': merged['レース場'].values,
        '予想三連単': predictions
    })

    return results_df


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

    # Make predictions
    print("Making predictions...")
    predictions_df = make_predictions(models_dict, predict_date, repo_root)

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
