#!/usr/bin/env python3
"""Phase 0 (縮退版): ST 帯 (予測区間) の校正.

calibrate.py で隊形確率が受け入れ基準を落としたため、実装計画 §6 の撤退ライン 3
「確率をやめ、ST 帯 (p25〜p75) の描画だけ実装する」が成立するかを判定する。

判定するのは 2 点:
  A. 区間の被覆率が名目値どおりか (p25-p75 で 50%, p10-p90 で 80%)
  B. 選手別 σ で帯幅を変えることに意味があるか
     (= 帯幅一定より pinball loss が下がり、σ 分位ごとの被覆率が揃うか)

    python notebooks/slit_sim/band_calibration.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from st_features import TEST_START, TRAIN_START, causal_ewma, load_panel  # noqa: E402

sys.path.insert(0, str(_HERE.parents[1] / "scripts"))
from boatrace.racer_st import F_OFFSET  # noqa: E402

p = load_panel()
p["base_st"], p["racer_sd_raw"], p["n_eff"] = causal_ewma(p)

tr_m = (p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START) & p["valid"]
te_m = (p["race_date"] >= TEST_START) & p["valid"]
tr = p[tr_m]

course_offset = (tr["actual_st"] - tr["base_st"]).groupby(tr["actual_course"]).mean().to_dict()
p["mu"] = (
    p["base_st"]
    + p["actual_course"].map(course_offset).astype(float)
    + p["flying_count"].clip(0, 2).map(F_OFFSET).fillna(0.0)
)
p["resid"] = p["actual_st"] - p["mu"]

# --- 選手別 σ の相対倍率 (収縮つき) ---
sd_prior = float(tr["racer_sd_raw"].mean())
K_SIGMA = 10.0
raw = p["racer_sd_raw"].fillna(sd_prior)
n = p["n_eff"].clip(lower=0.0)
p["sig_mult"] = ((n * raw + K_SIGMA * sd_prior) / (n + K_SIGMA)) / sd_prior

# --- 学習窓で帯の基準幅と分布形を決める ---
# base_st は各選手の初走で NaN (履歴ゼロ) になるため落とす
p = p[p["mu"].notna()].copy()
tr_m, te_m = tr_m.reindex(p.index), te_m.reindex(p.index)
tr = p[tr_m]
SIGMA_BASE = float((tr["resid"] / tr["sig_mult"]).std())
z = (tr["resid"] / (tr["sig_mult"] * SIGMA_BASE)).to_numpy()
T_DF, _, T_SCALE = stats.t.fit(z, floc=0)
print(f"予測残差 sd (帯の基準幅) = {SIGMA_BASE:.4f}")
print(f"標準化残差: 超過尖度 {stats.kurtosis(z):.1f} → Student-t df={T_DF:.2f} scale={T_SCALE:.3f}")

te = p[te_m].copy()
report = {"train": [TRAIN_START, TEST_START], "test_start": TEST_START, "n_test_runs": int(len(te))}

# ---------------------------------------------------------------------------
# A. 被覆率
# ---------------------------------------------------------------------------
print(f"\n=== A. 区間被覆率 (テスト窓 {len(te)} 艇走) ===")
rows = []
for nominal, q in [(0.50, 0.75), (0.80, 0.90), (0.95, 0.975)]:
    for name, dist in [("正規", "normal"), ("Student-t", "t")]:
        k = stats.norm.ppf(q) if dist == "normal" else stats.t.ppf(q, T_DF) * T_SCALE
        half = k * te["sig_mult"] * SIGMA_BASE
        cov = float(((te["resid"].abs()) <= half).mean())
        rows.append({"名目": nominal, "分布": name, "実被覆": round(cov, 4),
                     "差(pt)": round(100 * (cov - nominal), 2),
                     "平均帯幅(秒)": round(float(2 * half.mean()), 4)})
cov_df = pd.DataFrame(rows)
print(cov_df.to_string(index=False))
DIST = "t" if cov_df[cov_df["分布"] == "Student-t"]["差(pt)"].abs().mean() < \
              cov_df[cov_df["分布"] == "正規"]["差(pt)"].abs().mean() else "normal"
print(f"→ 被覆の名目差が小さいのは {DIST}")
report["coverage"] = cov_df.to_dict("list")
report["dist"] = DIST
a_pass = bool(cov_df[cov_df["分布"] == ("Student-t" if DIST == "t" else "正規")]["差(pt)"].abs().max() <= 3.0)

# ---------------------------------------------------------------------------
# B. 選手別 σ に意味があるか
# ---------------------------------------------------------------------------
print("\n=== B. 選手別σ の効果 ===")


def pinball(resid: np.ndarray, half: np.ndarray, q: float) -> float:
    """下側 q / 上側 1-q 分位の pinball loss 平均。"""
    loss = 0.0
    for qq, pred in ((q, -half), (1 - q, half)):
        d = resid - pred
        loss += np.mean(np.maximum(qq * d, (qq - 1) * d))
    return float(loss / 2)


k25 = stats.t.ppf(0.75, T_DF) * T_SCALE if DIST == "t" else stats.norm.ppf(0.75)
res_te = te["resid"].to_numpy()
half_var = (k25 * te["sig_mult"] * SIGMA_BASE).to_numpy()
half_const = np.full(len(te), k25 * SIGMA_BASE)
pb_var, pb_const = pinball(res_te, half_var, 0.25), pinball(res_te, half_const, 0.25)
print(f"pinball loss (p25/p75): 選手別σ {pb_var:.5f} vs 帯幅一定 {pb_const:.5f} "
      f"({100*(pb_const-pb_var)/pb_const:+.2f}%)")

te["q"] = pd.qcut(te["sig_mult"], 5, labels=["最安定", "2", "3", "4", "最不安定"])
g = te.groupby("q", observed=True).apply(
    lambda x: pd.Series({
        "n": len(x),
        "帯幅(秒)": 2 * k25 * x["sig_mult"].mean() * SIGMA_BASE,
        "被覆_選手別σ": (x["resid"].abs() <= k25 * x["sig_mult"] * SIGMA_BASE).mean(),
        "被覆_帯幅一定": (x["resid"].abs() <= k25 * SIGMA_BASE).mean(),
    }), include_groups=False)
print(g.round(4).to_string())
spread_var = float(g["被覆_選手別σ"].max() - g["被覆_選手別σ"].min())
spread_const = float(g["被覆_帯幅一定"].max() - g["被覆_帯幅一定"].min())
print(f"σ分位間の被覆ばらつき: 選手別σ {spread_var:.3f} / 帯幅一定 {spread_const:.3f} "
      f"(小さいほど良い。名目 0.50)")
b_pass = bool(pb_var < pb_const and spread_var < spread_const)
report["pinball"] = {"racer_sigma": pb_var, "constant": pb_const}
report["coverage_by_sigma_quintile"] = g.round(4).to_dict("list")
report["sigma_spread"] = {"racer_sigma": round(spread_var, 4), "constant": round(spread_const, 4)}

# ---------------------------------------------------------------------------
# 参考: 帯の幅が実際どれくらいか (UI 判断用)
# ---------------------------------------------------------------------------
print("\n=== 参考: p25-p75 帯の幅 ===")
w = 2 * k25 * te["sig_mult"] * SIGMA_BASE
print(f"帯幅(秒): 中央値 {w.median():.4f} / 10-90分位 {w.quantile(0.1):.4f}-{w.quantile(0.9):.4f}")
print(f"距離換算(m): 中央値 {w.median()*13.9:.2f} / 10-90分位 {w.quantile(0.1)*13.9:.2f}-{w.quantile(0.9)*13.9:.2f}")
print(f"(参考) 艇長 3m = ST 差 {3/13.9:.3f} 秒")
report["band_width_sec"] = {"median": round(float(w.median()), 4),
                            "p10": round(float(w.quantile(0.1)), 4),
                            "p90": round(float(w.quantile(0.9)), 4)}

report["constants"] = {"SIGMA_BASE": round(SIGMA_BASE, 4), "K_SIGMA": K_SIGMA,
                       "SD_PRIOR": round(sd_prior, 4), "DIST": DIST,
                       "T_DF": round(float(T_DF), 2), "T_SCALE": round(float(T_SCALE), 3),
                       "COURSE_OFFSET_ENTRY": {int(k): round(float(v), 4) for k, v in course_offset.items()}}
report["a_pass"], report["b_pass"] = a_pass, b_pass
with open(_HERE / "band_report.json", "w") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 60)
print(f"A 被覆率 = {a_pass} / B 選手別σ有効 = {b_pass} → "
      f"{'撤退ライン3 は成立' if a_pass else 'ST 帯も出せない'}")
print("=" * 60)
