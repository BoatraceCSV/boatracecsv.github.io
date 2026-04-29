"""Build time-series leak-free player history and evaluate rule-based course prediction.

Optimized: precompute per-race arrays once, evaluate many rule configs with
numpy operations.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

df_2025 = pd.read_pickle('/tmp/course_2025.pkl')
df_2026 = pd.read_pickle('/tmp/course_2026.pkl')

df_all = pd.concat([df_2025, df_2026], ignore_index=True)
df_all['レース日'] = pd.to_datetime(df_all['レース日'])
df_all = df_all.sort_values(['レース日', 'レースコード', '艇番']).reset_index(drop=True)
df_all['年'] = df_all['レース日'].dt.year

df_all['前付け'] = (df_all['実コース'] < df_all['艇番']).astype(int)
df_all['外寄せ'] = (df_all['実コース'] > df_all['艇番']).astype(int)
df_all['6コース'] = (df_all['実コース'] == 6).astype(int)
df_all['一致'] = (df_all['実コース'] == df_all['艇番']).astype(int)

# Per-player cumulative stats up to (but not including) current row.
# Strategy: cumsum() then subtract current row value.
g = df_all.groupby('登録番号', sort=False)
df_all['出走数_累'] = g.cumcount().astype(int)  # excludes current row
for col in ['前付け', '外寄せ', '6コース', '一致']:
    cs = g[col].cumsum()
    df_all[f'{col}_累'] = (cs - df_all[col]).astype(int)


def safe_rate(num, den):
    return np.where(den > 0, num / np.maximum(den, 1), np.nan)


df_all['前付け率_累'] = safe_rate(df_all['前付け_累'], df_all['出走数_累'])
df_all['外寄せ率_累'] = safe_rate(df_all['外寄せ_累'], df_all['出走数_累'])
df_all['6コース率_累'] = safe_rate(df_all['6コース_累'], df_all['出走数_累'])

test = df_all[df_all['年'] == 2026].copy().reset_index(drop=True)
print(f'2026 rows: {len(test):,}, races: {test["レースコード"].nunique():,}', flush=True)

# Build per-race arrays
test = test.sort_values(['レースコード', '艇番']).reset_index(drop=True)
# Filter races with exactly 6 boats
race_sizes = test.groupby('レースコード').size()
keep = race_sizes[race_sizes == 6].index
test = test[test['レースコード'].isin(keep)].reset_index(drop=True)
print(f'After 6-boat filter: rows={len(test):,}, races={test["レースコード"].nunique():,}', flush=True)

n_races = len(test) // 6
boats = test['艇番'].values.reshape(n_races, 6)              # always [1..6]
actual = test['実コース'].values.reshape(n_races, 6)
n6 = test['6コース率_累'].fillna(0).values.reshape(n_races, 6)
nm = test['前付け率_累'].fillna(0).values.reshape(n_races, 6)
nN = test['出走数_累'].values.reshape(n_races, 6)
race_name_arr = test.groupby('レースコード')['レース名'].first().reindex(
    test['レースコード'].drop_duplicates().tolist()).values
stadium_arr = test.groupby('レースコード')['レース場'].first().reindex(
    test['レースコード'].drop_duplicates().tolist()).astype(str).values

is_fixed = np.array(['進入固定' in str(s) for s in race_name_arr])
is_edogawa = np.array(['江戸川' in s for s in stadium_arr])

print(f'進入固定 races: {is_fixed.sum()}, 江戸川 races: {is_edogawa.sum()}', flush=True)


def predict_all_races(theta_6, min_n_6, theta_m, min_n_m, edo_fix=True):
    """Vectorized rule application. Returns predicted course array shaped (n_races, 6)."""
    pred = np.tile(np.array([1, 2, 3, 4, 5, 6]), (n_races, 6 // 6 * 1)).reshape(n_races, 6)
    pred = np.broadcast_to(np.arange(1, 7), (n_races, 6)).copy()  # default 枠番=コース

    # Mask races needing rule processing
    needs_rule = ~is_fixed
    if edo_fix:
        needs_rule &= ~is_edogawa
    rule_idx = np.where(needs_rule)[0]

    for ri in rule_idx:
        # 6コース candidate per boat
        six_mask = (n6[ri] >= theta_6) & (nN[ri] >= min_n_6) & (boats[ri] != 6)
        # Maemae candidate
        mae_mask = (nm[ri] >= theta_m) & (nN[ri] >= min_n_m) & (boats[ri] > 1)

        if not (six_mask.any() or mae_mask.any()):
            continue

        assigned = {}
        used = set()

        # Step 1: 6コース 1人 — 6コース率最大、同率なら外枠
        if six_mask.any():
            cand_idx = np.where(six_mask)[0]
            best = cand_idx[np.lexsort((-boats[ri][cand_idx], -n6[ri][cand_idx]))][0]
            assigned[int(boats[ri][best])] = 6
            used.add(6)

        # Step 2: 前付け — 高率順、内側コースへ
        if mae_mask.any():
            cand_idx = np.where(mae_mask)[0]
            order = cand_idx[np.argsort(-nm[ri][cand_idx])]
            for ci in order:
                b = int(boats[ri][ci])
                if b in assigned:
                    continue
                tgt = None
                for c in range(1, b):
                    if c not in used:
                        tgt = c
                        break
                if tgt is not None:
                    assigned[b] = tgt
                    used.add(tgt)

        # Step 3: 残り艇を枠番順 → 空きコース昇順
        rem_b = sorted(int(b) for b in boats[ri] if int(b) not in assigned)
        rem_c = sorted(c for c in range(1, 7) if c not in used)
        for b, c in zip(rem_b, rem_c):
            assigned[b] = c

        for j, b in enumerate(boats[ri]):
            pred[ri, j] = assigned[int(b)]

    return pred


def evaluate(theta_6, min_n_6, theta_m, min_n_m, edo_fix=True):
    pred = predict_all_races(theta_6, min_n_6, theta_m, min_n_m, edo_fix=edo_fix)
    correct = (pred == actual)
    base_correct = (boats == actual)
    improved = (correct & ~base_correct).sum()
    degraded = (~correct & base_correct).sum()
    return {
        'baseline': base_correct.mean(),
        'rule': correct.mean(),
        'delta_pt': (correct.mean() - base_correct.mean()) * 100,
        'improved': int(improved),
        'degraded': int(degraded),
        'net': int(improved - degraded),
        'all6': (correct.all(axis=1)).mean(),
    }


configs = []
configs.append(('baseline (枠番=コース のみ)', dict(theta_6=1.01, min_n_6=10**9, theta_m=1.01, min_n_m=10**9, edo_fix=False)))
configs.append(('進入固定 のみ (rule内固定で 枠番=コース→進入固定行は元から枠番なので変化なし)', dict(theta_6=1.01, min_n_6=10**9, theta_m=1.01, min_n_m=10**9, edo_fix=False)))
configs.append(('+江戸川固定', dict(theta_6=1.01, min_n_6=10**9, theta_m=1.01, min_n_m=10**9, edo_fix=True)))

for theta_6 in [0.5, 0.6, 0.7, 0.8, 0.9]:
    for min_n_6 in [10, 20, 50]:
        configs.append((f'6コ {theta_6} n>={min_n_6}',
                        dict(theta_6=theta_6, min_n_6=min_n_6,
                             theta_m=1.01, min_n_m=10**9, edo_fix=True)))

for theta_m in [0.15, 0.25, 0.35, 0.5]:
    for min_n_m in [30, 50, 100]:
        configs.append((f'前付 {theta_m} n>={min_n_m}',
                        dict(theta_6=1.01, min_n_6=10**9,
                             theta_m=theta_m, min_n_m=min_n_m, edo_fix=True)))

for theta_6 in [0.6, 0.7, 0.8]:
    for theta_m in [0.25, 0.35, 0.5]:
        configs.append((f'Combo 6={theta_6}/前={theta_m}',
                        dict(theta_6=theta_6, min_n_6=20,
                             theta_m=theta_m, min_n_m=50, edo_fix=True)))

results = []
print(f'\n{"label":<40} {"base":>7} {"rule":>7} {"Δpt":>7} {"imp":>5} {"deg":>5} {"net":>6} {"all6":>7}', flush=True)
print('-' * 90)
for label, kw in configs:
    r = evaluate(**kw)
    print(f'{label:<40} {r["baseline"]:.4f} {r["rule"]:.4f} {r["delta_pt"]:+7.3f} '
          f'{r["improved"]:>5} {r["degraded"]:>5} {r["net"]:>+6} {r["all6"]:.4f}', flush=True)
    results.append({'label': label, **kw, **r})

res_df = pd.DataFrame(results)
res_df.to_csv('/tmp/course_recheck_results.csv', index=False)
print('\nSaved to /tmp/course_recheck_results.csv')
