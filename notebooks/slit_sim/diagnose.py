#!/usr/bin/env python3
"""Phase 0 の不合格原因の切り分け.

仮説A: レース単位のスタート散らばり (dispersion) が予測できていない
       → 実測 in_margin は「締まったレース」と「バラけたレース」の混合で、
         独立ノイズの単一 σ では芯の鋭さと裾の重さを同時に再現できない
仮説B: そもそも μ (推定ST のレース内偏差) に隊形を当てる情報が無い
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from st_features import V, load_panel  # noqa: E402

p = load_panel()
p = p[p["actual_course"].between(1, 6) & p["actual_st"].notna()]

w = p.pivot_table(index="race_code", columns="actual_course", values="actual_st", aggfunc="first")
w = w.reindex(columns=range(1, 7)).dropna()
st = w.to_numpy(float)
dev = st - st.mean(1, keepdims=True)
disp = dev.std(1)  # レース単位のスタート散らばり

meta = p.drop_duplicates("race_code").set_index("race_code").reindex(w.index)
grade = p.pivot_table(index="race_code", columns="actual_course",
                      values="class_grade", aggfunc="first").reindex(w.index)
a1 = (grade == "A1").sum(1).to_numpy()

print(f"レース数 {len(w)}")
print(f"レース内 dispersion: mean {disp.mean():.4f} sd {disp.std():.4f} "
      f"CV {disp.std()/disp.mean():.3f}")
print(f"  分位: {np.percentile(disp, [10, 25, 50, 75, 90]).round(4)}")

print("\n--- 仮説A: dispersion は観測可能な変数から予測できるか ---")
X = pd.DataFrame({
    "a1_count": a1,
    "wind": pd.to_numeric(meta["wind_ms"], errors="coerce").fillna(0).to_numpy(),
    "wave": pd.to_numeric(meta["wave_cm"], errors="coerce").fillna(0).to_numpy(),
    "stadium": pd.to_numeric(meta["stadium"], errors="coerce").fillna(0).to_numpy(),
}, index=w.index)
for c in X.columns:
    print(f"  corr(disp, {c:>9}) = {np.corrcoef(disp, X[c])[0,1]: .4f}")

# 場別の dispersion
byj = pd.DataFrame({"j": X["stadium"], "d": disp}).groupby("j")["d"].agg(["mean", "size"])
print(f"  場別 dispersion: min {byj['mean'].min():.4f} / max {byj['mean'].max():.4f} "
      f"(場間 sd {byj['mean'].std():.4f} vs レース間 sd {disp.std():.4f})")

# 前日までの同一場 dispersion で予測できるか (因果的な代理)
meta_d = pd.to_datetime(meta["race_date"])
t = pd.DataFrame({"j": X["stadium"], "d": disp, "date": meta_d.to_numpy()}, index=w.index)
t = t.sort_values("date")
prev = t.groupby("j")["d"].transform(lambda s: s.shift(1).rolling(50, min_periods=10).mean())
m = prev.notna()
print(f"  corr(disp, 同一場の直近50走平均) = {np.corrcoef(t.loc[m,'d'], prev[m])[0,1]: .4f}")

print("\n--- 仮説B: μ のレース内偏差は隊形を説明するか ---")
# base_st を使わず、公表 全国平均ST の偏差で上限をざっくり見る
avg = p.pivot_table(index="race_code", columns="actual_course",
                    values="avg_st_pub", aggfunc="first").reindex(w.index)
avg = avg.replace(0, np.nan)
ok = avg.notna().all(1).to_numpy()
da = dev[ok]
dp = (avg[ok].to_numpy(float) - avg[ok].to_numpy(float).mean(1, keepdims=True))


def inm(x):
    lead = (x.mean(-1, keepdims=True) - x) * V
    return lead[..., 0] - lead[..., 1:].max(-1)


print(f"  corr(実測 in_margin, 予測 in_margin) = {np.corrcoef(inm(da), inm(dp))[0,1]: .4f}")
print(f"  R^2 = {np.corrcoef(inm(da), inm(dp))[0,1]**2: .4f}")
print("  → in_margin の分散のうち事前に説明できるのはこの割合のみ")

print("\n--- 独立ノイズで in_margin の芯と裾を同時に再現できるか ---")
rng = np.random.default_rng(0)
act = inm(dev)
print(f"  実測      : sd {act.std():.3f} / |·|<=0.6 {np.abs(act).mean() and (np.abs(act)<=0.6).mean():.3f} "
      f"/ 尖度 {pd.Series(act).kurt():.2f}")
for s in [0.036, 0.042, 0.048]:
    sim = inm(rng.normal(0, s, (60000, 6)))
    print(f"  正規 σ={s}: sd {sim.std():.3f} / |·|<=0.6 {(np.abs(sim)<=0.6).mean():.3f} "
          f"/ 尖度 {pd.Series(sim).kurt():.2f}")
# dispersion を実測分布からリサンプルした混合モデル
for _ in range(1):
    d_s = rng.choice(disp, 60000)
    sim = inm(rng.normal(0, 1, (60000, 6)) * d_s[:, None])
    print(f"  混合(実測dispersionをリサンプル): sd {sim.std():.3f} / "
          f"|·|<=0.6 {(np.abs(sim)<=0.6).mean():.3f} / 尖度 {pd.Series(sim).kurt():.2f}")
