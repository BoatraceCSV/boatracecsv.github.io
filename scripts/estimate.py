#!/usr/bin/env python3
"""
Boat race result estimation script.

This script uses pre-trained stadium-specific models to make predictions
for a specified date. Models are trained from stadium.ipynb and saved to
models/stadium_models.pkl.

Usage:
    python estimate.py --date 2022-12-23
"""

import argparse
import pickle
import sys
from datetime import datetime, timedelta
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


def load_models(repo_root):
    """Load pre-trained stadium models from pickle file."""
    model_path = repo_root / 'models' / 'stadium_models.pkl'

    if not model_path.exists():
        print(f"Model file not found: {model_path}", file=sys.stderr)
        return None

    try:
        with open(model_path, 'rb') as f:
            models_dict = pickle.load(f)
        print(f"Loaded {len(models_dict)} stadium models")
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

            # Return tuple of top 3 predicted positions
            predictions.append(tuple(int(pos) for pos, _ in top_3))
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
    parser = argparse.ArgumentParser(description='Estimate boat race results.')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Prediction date in YYYY-MM-DD format (default: yesterday)'
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
