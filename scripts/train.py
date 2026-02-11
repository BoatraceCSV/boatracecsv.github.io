#!/usr/bin/env python3
"""Training script for boatrace prediction models.

Extracts the core training logic from program_v2.ipynb into a CLI-runnable
script for automated retraining.

Usage:
    python scripts/train.py
    python scripts/train.py --train-years 2016-2025 --val-year 2026
    python scripts/train.py --output models/program_models_v2.pkl
"""

import argparse
import calendar
import json
import pickle
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.common import get_repo_root
from boatrace.constants import (
    STADIUM_NAME_TO_CODE,
    STADIUM_ADVANTAGE_MAP,
    DEFAULT_ADVANTAGE_MAP,
    WIND_DIRECTION_TO_ANGLE,
)


# ── Stadium name mapping ────────────────────────────────────────────
STADIUM_NAME_TO_NUMBER = {
    'ボートレース桐生': 1, 'ボートレース戸田': 2, 'ボートレース江戸川': 3,
    'ボートレース平和島': 4, 'ボートレース多摩川': 5, 'ボートレース浜名湖': 6,
    'ボートレース蒲郡': 7, 'ボートレース常滑': 8, 'ボートレース津': 9,
    'ボートレース三国': 10, 'ボートレースびわこ': 11, 'ボートレース琵琶湖': 11,
    'ボートレース住之江': 12, 'ボートレース尼崎': 13, 'ボートレース鳴門': 14,
    'ボートレース丸亀': 15, 'ボートレース児島': 16, 'ボートレース宮島': 17,
    'ボートレース徳山': 18, 'ボートレース下関': 19, 'ボートレース若松': 20,
    'ボートレース芦屋': 21, 'ボートレース福岡': 22, 'ボートレース唐津': 23,
    'ボートレース大村': 24,
}


def map_stadium_name_to_number(name):
    if pd.isna(name):
        return np.nan
    return STADIUM_NAME_TO_NUMBER.get(str(name).strip(), np.nan)


# ── Data reshaping ───────────────────────────────────────────────────

def reshape_programs(df):
    """Programs を艇単位に変形"""
    frames = []
    race_cols = ['レースコード', '日次', 'レース日', 'レース場', 'レース回']
    for frame in range(1, 7):
        prefix = f'{frame}枠_'
        cols = [c for c in df.columns if c.startswith(prefix)]
        if cols:
            tmp = df[race_cols + cols].copy()
            tmp.columns = race_cols + [c[len(prefix):] for c in cols]
            tmp['枠'] = frame
            frames.append(tmp)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def reshape_results(df):
    """Results を艇単位に変形"""
    result_list = []
    for _, row in df.iterrows():
        race_code = row['レースコード']
        for place in range(1, 7):
            boat_col = f'{place}着_艇番'
            if boat_col not in df.columns:
                continue
            boat_num = row[boat_col]
            if pd.isna(boat_num):
                continue
            try:
                boat_num = int(boat_num)
                if 1 <= boat_num <= 6:
                    result_list.append({
                        'レースコード': race_code,
                        '艇番': boat_num,
                        '着順': place,
                    })
            except (ValueError, TypeError):
                continue
    return pd.DataFrame(result_list) if result_list else pd.DataFrame()


def reshape_previews(df):
    """Previews を艇単位に変形（気象列を保持）"""
    if df is None or df.empty:
        return pd.DataFrame()
    race_id_cols = ['レースコード', 'レース日', 'レース場', 'レース回']
    weather_cols = [c for c in ['風速(m)', '風向', '波の高さ(cm)', '天候', '気温(℃)', '水温(℃)'] if c in df.columns]
    preview_frames = []
    for boat_num in range(1, 7):
        boat_prefix = f'艇{boat_num}_'
        boat_cols = [col for col in df.columns if col.startswith(boat_prefix)]
        if boat_cols:
            tmp = df[race_id_cols + weather_cols + boat_cols].copy()
            rename_map = {col: col[len(boat_prefix):] for col in boat_cols}
            tmp = tmp.rename(columns=rename_map)
            tmp['艇番'] = boat_num
            preview_frames.append(tmp)
    return pd.concat(preview_frames, ignore_index=True) if preview_frames else pd.DataFrame()


def extract_day_number(day_text):
    """Extract numeric day from text like '初日', '2日', '最終日'."""
    if pd.isna(day_text):
        return np.nan
    s = str(day_text)
    if '初日' in s:
        return 1
    if '最終' in s:
        return 7
    import re
    m = re.search(r'(\d+)', s)
    return int(m.group(1)) if m else np.nan


# ── Feature engineering ──────────────────────────────────────────────

def compute_konseki_features(df):
    """今節成績特徴量を生成。"""
    konseki_cols = [f'今節成績_{i}-{j}' for i in range(1, 7) for j in [1, 2]]
    existing = [c for c in konseki_cols if c in df.columns]
    if not existing:
        return df

    finish_cols = [f'今節成績_{i}-2' for i in range(1, 7) if f'今節成績_{i}-2' in df.columns]
    course_cols = [f'今節成績_{i}-1' for i in range(1, 7) if f'今節成績_{i}-1' in df.columns]

    if finish_cols:
        finish_vals = df[finish_cols].apply(pd.to_numeric, errors='coerce')
        df['今節_平均着順'] = finish_vals.mean(axis=1)
        df['今節_最新着順'] = finish_vals.iloc[:, -1] if len(finish_cols) > 0 else np.nan
        df['今節_3連対率'] = (finish_vals <= 3).mean(axis=1)
        df['今節_レース数'] = finish_vals.notna().sum(axis=1)

    if course_cols:
        course_vals = df[course_cols].apply(pd.to_numeric, errors='coerce')
        df['今節_平均コース'] = course_vals.mean(axis=1)

    return df


def compute_relative_features(df):
    """レース内相対特徴量"""
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
    """コース特徴量"""
    if '全国勝率' in df.columns and '枠' in df.columns:
        df['枠×全国勝率'] = df['枠'] * df['全国勝率'].fillna(0)

    if '枠' in df.columns:
        if 'レース場' in df.columns:
            df['イン有利度'] = df.apply(
                lambda row: STADIUM_ADVANTAGE_MAP.get(
                    row['レース場'], DEFAULT_ADVANTAGE_MAP
                ).get(row['枠'], 0) if pd.notna(row['レース場']) else DEFAULT_ADVANTAGE_MAP.get(row['枠'], 0),
                axis=1,
            )
        else:
            df['イン有利度'] = df['枠'].map(DEFAULT_ADVANTAGE_MAP).fillna(0)

    if '風速(m)' in df.columns and 'イン有利度' in df.columns:
        wind = pd.to_numeric(df['風速(m)'], errors='coerce').fillna(3.0)
        df['風速×イン有利度'] = wind * df['イン有利度']
        df['強風フラグ'] = (wind >= 5).astype(int)
        if '枠' in df.columns:
            df['強風×枠'] = df['強風フラグ'] * df['枠']

    return df


def compute_player_historical_stats(train_df):
    """選手履歴統計"""
    if '登録番号' not in train_df.columns or '着順' not in train_df.columns:
        return pd.DataFrame()

    valid = train_df[train_df['着順'].notna()].copy()
    valid['着順'] = valid['着順'].astype(float)

    stats = valid.groupby('登録番号').agg(
        履歴_平均着順=('着順', 'mean'),
        履歴_1着率=('着順', lambda x: (x == 1).mean()),
        履歴_出走回数=('着順', 'count'),
    ).reset_index()

    in_data = valid[valid['枠'] == 1]
    if not in_data.empty:
        in_stats = in_data.groupby('登録番号').agg(
            イン1着率=('着順', lambda x: (x == 1).mean()),
        ).reset_index()
        stats = stats.merge(in_stats, on='登録番号', how='left')
    else:
        stats['イン1着率'] = np.nan

    return stats


def compute_stadium_player_stats(train_df):
    """選手×レース場統計"""
    if not all(c in train_df.columns for c in ['登録番号', '着順', 'レース場']):
        return pd.DataFrame()

    valid = train_df[train_df['着順'].notna()].copy()
    valid['着順'] = valid['着順'].astype(float)

    stats = valid.groupby(['登録番号', 'レース場']).agg(
        当場_平均着順=('着順', 'mean'),
        当場_1着率=('着順', lambda x: (x == 1).mean()),
        当場_出走回数=('着順', 'count'),
    ).reset_index()
    return stats


def compute_player_st_stats(results_list):
    """選手ST統計"""
    st_records = []
    for res_df in results_list:
        for _, row in res_df.iterrows():
            for place in range(1, 7):
                st_col = f'{place}着_スタートタイミング'
                reg_col = f'{place}着_登録番号'
                if st_col not in res_df.columns or reg_col not in res_df.columns:
                    continue
                st_val = pd.to_numeric(row.get(st_col), errors='coerce')
                reg_val = pd.to_numeric(row.get(reg_col), errors='coerce')
                if pd.notna(st_val) and pd.notna(reg_val) and 0 <= st_val <= 0.50:
                    st_records.append({'登録番号': int(reg_val), 'ST': st_val})
    if not st_records:
        return pd.DataFrame(columns=['登録番号', 'ST_mean', 'ST_std', 'ST_min'])
    st_df = pd.DataFrame(st_records)
    stats = st_df.groupby('登録番号')['ST'].agg(
        ST_mean='mean', ST_std='std', ST_min='min'
    ).reset_index()
    stats['ST_std'] = stats['ST_std'].fillna(0)
    return stats


def compute_recent_race_stats(results_list):
    """直近N走統計"""
    records = []
    for res_df in results_list:
        for _, row in res_df.iterrows():
            race_date = row.get('レース日')
            for place in range(1, 7):
                reg_col = f'{place}着_登録番号'
                if reg_col not in res_df.columns:
                    continue
                reg_val = pd.to_numeric(row.get(reg_col), errors='coerce')
                if pd.notna(reg_val) and 1 <= place <= 6:
                    records.append({
                        '登録番号': int(reg_val),
                        'レース日': race_date,
                        '着順': place,
                    })
    if not records:
        return pd.DataFrame(columns=['登録番号', '直近5走_平均着順', '直近10走_平均着順', '直近5走_1着率'])

    rec_df = pd.DataFrame(records)
    rec_df['レース日'] = pd.to_datetime(rec_df['レース日'], errors='coerce')
    rec_df = rec_df.sort_values(['登録番号', 'レース日'], ascending=[True, False])

    stats_list = []
    for reg, grp in rec_df.groupby('登録番号'):
        last5 = grp.head(5)['着順']
        last10 = grp.head(10)['着順']
        stats_list.append({
            '登録番号': reg,
            '直近5走_平均着順': last5.mean(),
            '直近10走_平均着順': last10.mean(),
            '直近5走_1着率': (last5 == 1).mean(),
        })
    return pd.DataFrame(stats_list)


# ── Data loading ─────────────────────────────────────────────────────

def load_all_data(repo_root, year_start, year_end):
    """Load all program/result/preview data for the given year range."""
    all_data = {}
    for year in range(year_start, year_end + 1):
        year_str = str(year)
        for month in range(1, 13):
            _, max_day = calendar.monthrange(year, month)
            for day in range(1, max_day + 1):
                month_str = f'{month:02d}'
                day_str = f'{day:02d}'

                # Try Parquet first, then CSV
                prog_df = _load_data_file(repo_root, 'programs', year_str, month_str, day_str)
                res_df = _load_data_file(repo_root, 'results', year_str, month_str, day_str)

                if prog_df is not None and res_df is not None:
                    date_key = f'{year_str}-{month_str}-{day_str}'
                    entry = {'programs': prog_df, 'results': res_df}
                    prev_df = _load_data_file(repo_root, 'previews', year_str, month_str, day_str)
                    if prev_df is not None:
                        entry['previews'] = prev_df
                    all_data[date_key] = entry

    print(f'Loaded {len(all_data)} days of data')
    return all_data


def _load_data_file(repo_root, data_type, year, month, day):
    """Load a single data file, trying Parquet first then CSV."""
    parquet_path = repo_root / 'data' / f'{data_type}_parquet' / year / f'{month}.parquet'
    csv_path = repo_root / 'data' / data_type / year / month / f'{day}.csv'

    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            # Filter to specific day if the parquet contains multiple days
            if 'レース日' in df.columns:
                target_date = f'{year}-{month}-{day}'
                df_filtered = df[df['レース日'] == target_date]
                if not df_filtered.empty:
                    return df_filtered
            return df
        except Exception:
            pass

    if csv_path.exists():
        try:
            return pd.read_csv(csv_path)
        except Exception:
            pass
    return None


# ── Training ─────────────────────────────────────────────────────────

def merge_and_engineer(all_data):
    """Merge programs/results/previews and engineer features."""
    combined_data = []
    processed_count = 0

    for date_str, data in all_data.items():
        try:
            prog = reshape_programs(data['programs'])
            res = reshape_results(data['results'])
            if prog.empty or res.empty:
                continue

            prog['日次数'] = prog['日次'].apply(extract_day_number)
            prog['レース場'] = prog['レース場'].apply(map_stadium_name_to_number)
            prog = prog[prog['レース場'].notna()].reset_index(drop=True)
            if prog.empty:
                continue

            merged = prog.merge(
                res[['レースコード', '艇番', '着順']],
                on=['レースコード', '艇番'],
                how='left'
            )

            if 'previews' in data:
                prev_long = reshape_previews(data['previews'])
                if not prev_long.empty:
                    prev_cols_to_use = [c for c in prev_long.columns
                                        if c not in ['レース日', 'レース場', 'レース回']]
                    merged = merged.merge(
                        prev_long[prev_cols_to_use],
                        on=['レースコード', '艇番'],
                        how='left'
                    )

            combined_data.append(merged)
            processed_count += 1
        except Exception:
            continue

    print(f'Processed {processed_count} days')
    if not combined_data:
        raise RuntimeError('No data merged')

    final_data = pd.concat(combined_data, ignore_index=True)
    print(f'Combined shape: {final_data.shape}')

    # Feature engineering
    final_data = compute_konseki_features(final_data)
    final_data = compute_relative_features(final_data)
    final_data = compute_course_features(final_data)

    # Wind direction sin/cos
    if '風向' in final_data.columns:
        angles = final_data['風向'].map(WIND_DIRECTION_TO_ANGLE)
        final_data['風向sin'] = np.sin(np.radians(angles)).fillna(0)
        final_data['風向cos'] = np.cos(np.radians(angles)).fillna(0)

    # Grade encoding
    if '級別' in final_data.columns:
        le_grade = LabelEncoder()
        final_data['級別_encoded'] = le_grade.fit_transform(final_data['級別'].fillna('未知'))

    # Date extraction
    final_data['レース日_dt'] = pd.to_datetime(final_data['レース日'], errors='coerce')
    final_data['年'] = final_data['レース日_dt'].dt.year
    final_data['月'] = final_data['レース日_dt'].dt.month

    return final_data


def compute_all_stats(final_data, all_data, train_year_end):
    """Compute player/stadium/ST/recent stats from training data only."""
    train_mask = final_data['年'] <= train_year_end
    train_subset = final_data[train_mask].copy()

    player_stats = compute_player_historical_stats(train_subset)
    stadium_player_stats = compute_stadium_player_stats(train_subset)

    # Collect raw results for ST and recent stats
    results_list = []
    for date_str, entry in all_data.items():
        year = int(date_str[:4])
        if year <= train_year_end:
            results_list.append(entry['results'])

    player_st_stats = compute_player_st_stats(results_list)
    recent_race_stats = compute_recent_race_stats(results_list)

    # Merge stats
    final_data = final_data.merge(player_stats, on='登録番号', how='left')
    final_data = final_data.merge(stadium_player_stats, on=['登録番号', 'レース場'], how='left')

    if not player_st_stats.empty:
        final_data = final_data.merge(player_st_stats, on='登録番号', how='left')
        for col, default in [('ST_mean', 0.167), ('ST_std', 0.068), ('ST_min', 0.167)]:
            if col in final_data.columns:
                final_data[col] = final_data[col].fillna(default)

    if not recent_race_stats.empty:
        final_data = final_data.merge(recent_race_stats, on='登録番号', how='left')
        for col, default in [('直近5走_平均着順', 3.5), ('直近10走_平均着順', 3.5), ('直近5走_1着率', 0.167)]:
            if col in final_data.columns:
                final_data[col] = final_data[col].fillna(default)

    return final_data, player_stats, stadium_player_stats, player_st_stats, recent_race_stats


def build_feature_matrix(final_data):
    """Build feature matrix X and labels y."""
    exclude_cols = {
        'レースコード', '日次', 'レース日', 'レース場', 'レース回',
        '艇番', '登録番号', '選手名', '支部', '早見',
        '枠', '着順',
        'モーター番号', 'ボート番号',
        'レース日_dt', '年', '月',
        'タイトル', 'レース名', '距離(m)', '電話投票締切予定',
    }
    konseki_raw_cols = {f'今節成績_{i}-{j}' for i in range(1, 7) for j in [1, 2]}
    exclude_cols |= konseki_raw_cols
    exclude_cols |= {'風向', '天候'}

    categorical_cols = {'級別'}

    numeric_cols = []
    for col in final_data.columns:
        if col not in exclude_cols and col not in categorical_cols:
            test_vals = pd.to_numeric(final_data[col], errors='coerce')
            if test_vals.notna().sum() > 0:
                numeric_cols.append(col)

    feature_cols = numeric_cols.copy()
    if '級別_encoded' in final_data.columns and '級別_encoded' not in feature_cols:
        feature_cols.append('級別_encoded')

    X = final_data[feature_cols].copy()
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors='coerce')

    place_cols = {'今節_平均着順', '今節_最新着順', '履歴_平均着順', '当場_平均着順',
                  '今節_平均コース', '直近5走_平均着順', '直近10走_平均着順'}
    rate_cols = {'全国勝率', '全国2連対率', '当地勝率', '当地2連対率',
                 'モーター2連対率', 'ボート2連対率', '今節_3連対率',
                 '履歴_1着率', 'イン1着率', '当場_1着率', '直近5走_1着率'}

    for col in X.columns:
        if col in place_cols:
            X[col] = X[col].fillna(3.5)
        elif col in rate_cols:
            median_val = X[col].median()
            X[col] = X[col].fillna(median_val if pd.notna(median_val) else 0)
        else:
            if X[col].notna().any():
                X[col] = X[col].fillna(X[col].median())
            else:
                X[col] = X[col].fillna(0)

    y = final_data['着順']
    return X, y, feature_cols


def train_models(X, y, final_data, feature_cols, val_year):
    """Train LambdaRank + Classifier models per stadium."""
    try:
        import lightgbm as lgb
    except ImportError:
        print("lightgbm not installed, skipping LambdaRank training", file=sys.stderr)
        lgb = None

    year = final_data['年']
    train_idx = year < val_year
    val_idx = year == val_year

    X_tr, y_tr = X[train_idx], y[train_idx]
    X_v, y_v = X[val_idx], y[val_idx]
    stadiums_tr = final_data.loc[train_idx, 'レース場']
    stadiums_v = final_data.loc[val_idx, 'レース場']
    race_codes_tr = final_data.loc[train_idx, 'レースコード']
    race_codes_v = final_data.loc[val_idx, 'レースコード']

    ranking_models = {}
    classifier_models = {}
    classifier_scalers = {}

    stadiums = sorted(final_data['レース場'].dropna().unique())

    for stadium in stadiums:
        s_tr = stadiums_tr == stadium
        s_v = stadiums_v == stadium

        X_s_tr, y_s_tr = X_tr[s_tr], y_tr[s_tr]
        X_s_v, y_s_v = X_v[s_v], y_v[s_v]

        if len(X_s_tr) < 100 or len(X_s_v) < 10:
            continue

        # LambdaRank
        if lgb is not None:
            try:
                rc_s_tr = race_codes_tr[s_tr]
                rc_s_v = race_codes_v[s_v]
                groups_tr = rc_s_tr.value_counts().sort_index().values
                groups_v = rc_s_v.value_counts().sort_index().values

                # Sort by race code for proper grouping
                sort_tr = rc_s_tr.argsort()
                sort_v = rc_s_v.argsort()

                dtrain = lgb.Dataset(
                    X_s_tr.iloc[sort_tr], label=y_s_tr.iloc[sort_tr],
                    group=groups_tr
                )
                dval = lgb.Dataset(
                    X_s_v.iloc[sort_v], label=y_s_v.iloc[sort_v],
                    group=groups_v, reference=dtrain
                )

                params = {
                    'objective': 'lambdarank',
                    'metric': 'ndcg',
                    'ndcg_eval_at': [1],
                    'num_leaves': 31,
                    'learning_rate': 0.05,
                    'min_child_samples': 20,
                    'verbosity': -1,
                }
                model = lgb.train(
                    params, dtrain,
                    num_boost_round=300,
                    valid_sets=[dval],
                    callbacks=[lgb.early_stopping(50, verbose=False)],
                )
                ranking_models[stadium] = model
            except Exception as e:
                print(f"  LambdaRank failed for stadium {stadium}: {e}")

        # Classifier
        try:
            scaler = StandardScaler()
            X_s_tr_scaled = pd.DataFrame(
                scaler.fit_transform(X_s_tr), columns=X_s_tr.columns, index=X_s_tr.index
            )
            clf = GradientBoostingClassifier(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                subsample=0.8, random_state=42,
            )
            clf.fit(X_s_tr_scaled, y_s_tr)
            classifier_models[stadium] = clf
            classifier_scalers[stadium] = scaler
        except Exception as e:
            print(f"  Classifier failed for stadium {stadium}: {e}")

    print(f"Trained: {len(ranking_models)} ranking, {len(classifier_models)} classifier models")
    return ranking_models, classifier_models, classifier_scalers


def optimize_ensemble_weights(ranking_models, classifier_models, classifier_scalers,
                               X, y, final_data, val_year):
    """Find optimal ensemble weights on validation data."""
    year = final_data['年']
    val_idx = year == val_year
    X_v, y_v = X[val_idx], y[val_idx]
    stadiums_v = final_data.loc[val_idx, 'レース場']
    race_codes_v = final_data.loc[val_idx, 'レースコード']

    best_w = (0.5, 0.3, 0.2)
    best_score = 0

    for w_rank in np.arange(0.3, 0.8, 0.1):
        for w_cls in np.arange(0.1, 0.5, 0.1):
            w_gbc = round(1.0 - w_rank - w_cls, 2)
            if w_gbc < 0:
                continue
            # Simplified evaluation: count correct 1st place predictions
            correct = 0
            total = 0
            for rc in race_codes_v.unique():
                mask = race_codes_v == rc
                if mask.sum() < 2:
                    continue
                race_X = X_v[mask]
                race_y = y_v[mask]
                stadium = stadiums_v[mask].iloc[0]

                scores = np.zeros(len(race_X))
                if stadium in ranking_models:
                    scores += w_rank * ranking_models[stadium].predict(race_X)
                if stadium in classifier_models:
                    try:
                        X_scaled = pd.DataFrame(
                            classifier_scalers[stadium].transform(race_X),
                            columns=race_X.columns, index=race_X.index
                        )
                        proba = classifier_models[stadium].predict_proba(X_scaled)
                        classes = classifier_models[stadium].classes_
                        expected = proba @ classes.astype(float)
                        scores -= (w_cls + w_gbc) * expected
                    except Exception:
                        pass

                predicted_1st = race_y.index[np.argmax(scores)]
                if race_y[predicted_1st] == 1:
                    correct += 1
                total += 1

            if total > 0:
                score = correct / total
                if score > best_score:
                    best_score = score
                    best_w = (round(w_rank, 2), round(w_cls, 2), round(w_gbc, 2))

    print(f"Best ensemble weights: {best_w} (1st place acc: {best_score:.3f})")
    return best_w


def save_model(save_path, feature_cols, ranking_models, classifier_models,
               classifier_scalers, player_stats, stadium_player_stats,
               player_st_stats, recent_race_stats, ensemble_weights,
               kimarite_model_info, X):
    """Save all models and stats to pickle with metadata."""
    stadiums = sorted(set(list(ranking_models.keys()) + list(classifier_models.keys())))
    save_dict = {}

    for stadium in stadiums:
        entry = {'features': feature_cols}
        if stadium in classifier_models:
            entry['model'] = classifier_models[stadium]
            entry['scaler'] = classifier_scalers[stadium]
        if stadium in ranking_models:
            entry['ranking_model'] = ranking_models[stadium]
        if stadium in classifier_models:
            entry['classifier_model'] = classifier_models[stadium]
            entry['classifier_scaler'] = classifier_scalers[stadium]
        save_dict[stadium] = entry

    save_dict['_ensemble_weights'] = ensemble_weights
    save_dict['_player_stats'] = player_stats
    save_dict['_stadium_player_stats'] = stadium_player_stats
    save_dict['_player_st_stats'] = player_st_stats
    save_dict['_recent_race_stats'] = recent_race_stats

    if kimarite_model_info is not None:
        save_dict['_kimarite_model'] = kimarite_model_info

    save_dict['_metadata'] = {
        'model_version': '2.2.0',
        'training_date': datetime.now().isoformat(),
        'feature_count': len(feature_cols),
        'feature_names': feature_cols,
        'train_samples': len(X),
        'eval_metric': 'ndcg@1',
        'ensemble_weights': ensemble_weights,
    }

    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, 'wb') as f:
        pickle.dump(save_dict, f)

    # Write manifest
    manifest_path = save_path.parent / 'manifest.json'
    manifest = {
        'current_model': save_path.name,
        'model_version': '2.2.0',
        'training_date': datetime.now().strftime('%Y-%m-%d'),
        'feature_count': len(feature_cols),
        'eval_metric': 'ndcg@1',
    }
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"Saved model to {save_path}")
    print(f"Saved manifest to {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description='Train boatrace prediction models.')
    parser.add_argument(
        '--train-years', type=str, default='2016-2025',
        help='Training year range (e.g., 2016-2025)'
    )
    parser.add_argument(
        '--val-year', type=int, default=None,
        help='Validation year (default: last year of train range)'
    )
    parser.add_argument(
        '--output', type=str, default=None,
        help='Output model path (default: models/program_models_v2.pkl)'
    )
    args = parser.parse_args()

    # Parse year range
    parts = args.train_years.split('-')
    year_start = int(parts[0])
    year_end = int(parts[1]) if len(parts) > 1 else year_start
    val_year = args.val_year if args.val_year else year_end

    repo_root = get_repo_root()
    output_path = Path(args.output) if args.output else repo_root / 'models' / 'program_models_v2.pkl'

    print(f"Training: {year_start}-{year_end}, Validation: {val_year}")
    print("-" * 70)

    # Load data
    print("Loading data...")
    all_data = load_all_data(repo_root, year_start, year_end)

    # Merge and engineer features
    print("Engineering features...")
    final_data = merge_and_engineer(all_data)

    # Compute stats
    print("Computing player stats...")
    train_year_end = val_year - 1
    final_data, player_stats, stadium_player_stats, player_st_stats, recent_race_stats = \
        compute_all_stats(final_data, all_data, train_year_end)

    # Build feature matrix
    print("Building feature matrix...")
    X, y, feature_cols = build_feature_matrix(final_data)
    print(f"Features: {len(feature_cols)}, Samples: {len(X)}")

    # Train models
    print("Training models...")
    ranking_models, classifier_models, classifier_scalers = \
        train_models(X, y, final_data, feature_cols, val_year)

    # Optimize ensemble
    print("Optimizing ensemble...")
    ensemble_weights = optimize_ensemble_weights(
        ranking_models, classifier_models, classifier_scalers,
        X, y, final_data, val_year
    )

    # Save
    print("Saving model...")
    save_model(
        output_path, feature_cols, ranking_models, classifier_models,
        classifier_scalers, player_stats, stadium_player_stats,
        player_st_stats, recent_race_stats, ensemble_weights,
        None,  # kimarite model trained separately in notebook for now
        X,
    )

    print("-" * 70)
    print("Training complete!")


if __name__ == '__main__':
    main()
