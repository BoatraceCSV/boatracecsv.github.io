#!/usr/bin/env python3
"""Phase 0: 確率的スリット (slit_sim) の校正パラメータ確定.

docs/design/slit_sim_plan.md §2 の受け入れ基準を判定し、合格した定数を
calibration_report.json に凍結する。

学習窓 2026-03-01〜06-20 / テスト窓 2026-06-21〜 (racer_st.py Phase 2 と同一)。
すべて因果的: 各レースの μ / σ はそのレース日より前の実測 ST のみから作る。

前提: build_panel.py で st_panel.csv.gz が生成済みであること。

    python notebooks/st_estimation/build_panel.py . notebooks/slit_sim
    python notebooks/slit_sim/calibrate.py
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from st_features import TEST_START, TRAIN_START, V, causal_ewma, load_panel  # noqa: E402

sys.path.insert(0, str(_HERE.parents[1] / "scripts"))
from boatrace.racer_st import F_OFFSET  # noqa: E402

N_SAMPLES = 2000
#: 隊形分類のしきい値 (m)。slit_tenkai.md §3.1 と同一
TH_LEAD, TH_BEHIND = 0.6, 1.75
FORMATIONS = [
    "イン明確先手",
    "ほぼ揃い",
    "半艇身遅れ/センター",
    "半艇身遅れ/外",
    "大幅遅れ/センター",
    "大幅遅れ/外",
]

# ---------------------------------------------------------------------------
# 1. パネル読み込み
# ---------------------------------------------------------------------------
p = load_panel()
print(f"panel: {len(p)} boat-runs / {p['race_code'].nunique()} races")

p["base_st"], p["racer_sd_raw"], p["n_eff"] = causal_ewma(p)

# ---------------------------------------------------------------------------
# 3. 学習窓で定数を推定
# ---------------------------------------------------------------------------
tr = p[(p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START) & p["valid"]]

# 3a. 進入コース別オフセット (racer_st の COURSE_OFFSET は枠番ベース。
#     slit_sim は進入コースで並べるため進入コースで引き直す)
resid = tr["actual_st"] - tr["base_st"]
course_offset = resid.groupby(tr["actual_course"]).mean().round(4).to_dict()
print("\n進入コース別オフセット:", {int(k): round(v, 4) for k, v in course_offset.items()})

p["mu_raw"] = (
    p["base_st"]
    + p["actual_course"].map(course_offset).astype(float)
    + p["flying_count"].clip(0, 2).map(F_OFFSET).fillna(0.0)
)

# 3b. 選手別 σ の相対倍率 (絶対水準は 3d の σ_within で決まるので比のみ使う)
sd_prior = float(tr["racer_sd_raw"].mean())
K_SIGMA = 20.0  # 3e で探索
print(f"racer_sd_raw の平均: {sd_prior:.4f}")


def sigma_multiplier(k_sigma: float) -> pd.Series:
    """収縮した選手別 sd を、全体平均で割った相対倍率にする。"""
    raw = p["racer_sd_raw"].fillna(sd_prior)
    n = p["n_eff"].clip(lower=0.0)
    shrunk = (n * raw + k_sigma * sd_prior) / (n + k_sigma)
    return shrunk / sd_prior


# ---------------------------------------------------------------------------
# 4. レース単位のワイド化 (6 艇立てのみ)
# ---------------------------------------------------------------------------
def to_wide(df: pd.DataFrame, cols: list[str]) -> dict[str, np.ndarray]:
    """1 艇走 long → (レース, 6コース) のワイド行列。全コース揃う 6 艇立てのみ残す。"""
    d = df[df["actual_course"].between(1, 6)]
    out = {}
    for c in cols:
        w = d.pivot_table(index="race_code", columns="actual_course", values=c, aggfunc="first")
        w = w.reindex(columns=range(1, 7))
        out[c] = w
    keep = out[cols[0]].notna().all(1)
    for c in cols[1:]:
        keep &= out[c].notna().all(1)
    races = out[cols[0]].index[keep]
    return {c: out[c].loc[races].to_numpy(float) for c in cols} | {"races": races.to_numpy()}


p["sig_mult"] = sigma_multiplier(K_SIGMA)
wide_cols = ["actual_st", "mu_raw", "sig_mult"]
W_tr = to_wide(p[(p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START)], wide_cols)
W_te = to_wide(p[p["race_date"] >= TEST_START], wide_cols)
print(f"\n6艇立てレース: train {len(W_tr['races'])} / test {len(W_te['races'])}")


def demean(x: np.ndarray) -> np.ndarray:
    return x - x.mean(-1, keepdims=True)


# 3c/3d. 収縮係数 β と レース内残差 σ
dev_a, dev_p = demean(W_tr["actual_st"]), demean(W_tr["mu_raw"])
BETA = float(np.cov(dev_p.ravel(), dev_a.ravel())[0, 1] / dev_p.var())
res_tr = dev_a - BETA * dev_p
SIGMA_WITHIN = float(res_tr.std())
print(f"BETA = {BETA:.3f} / SIGMA_WITHIN = {SIGMA_WITHIN:.4f}")

# コース別 σ (残差の sd をコースごとに)
SIGMA_BY_COURSE = {c + 1: float(res_tr[:, c].std()) for c in range(6)}
print("SIGMA_BY_COURSE:", {k: round(v, 4) for k, v in SIGMA_BY_COURSE.items()})

# 分布形: 標準化残差に Student-t を当てる
z = (res_tr / np.array([SIGMA_BY_COURSE[c + 1] for c in range(6)])).ravel()
T_DF, _, T_SCALE = stats.t.fit(z, floc=0)
print(f"Student-t fit on standardized resid: df={T_DF:.2f} scale={T_SCALE:.3f} "
      f"(excess kurtosis {stats.kurtosis(z):.1f})")


# ---------------------------------------------------------------------------
# 5. モンテカルロ
# ---------------------------------------------------------------------------
def classify(st: np.ndarray) -> np.ndarray:
    lead = (st.mean(-1, keepdims=True) - st) * V
    inm = lead[..., 0] - lead[..., 1:].max(-1)
    center = lead[..., 1:4].max(-1) >= lead[..., 4:].max(-1)
    lab = np.where(inm >= TH_LEAD, 0, np.where(inm >= -TH_LEAD, 1, np.where(inm >= -TH_BEHIND, 2, 4)))
    return np.where(lab >= 2, lab + np.where(center, 0, 1), lab)


def simulate(W: dict, dist: str, scale: float = 1.0, seed: int = 0) -> np.ndarray:
    """(n_races, N_SAMPLES, 6) の ST サンプルを返す。"""
    rng = np.random.default_rng(seed)
    mu = W["mu_raw"]
    mu = mu.mean(1, keepdims=True) + BETA * demean(mu)
    sd = np.array([SIGMA_BY_COURSE[c + 1] for c in range(6)]) * W["sig_mult"] * scale
    shape = (len(mu), N_SAMPLES, 6)
    if dist == "normal":
        e = rng.normal(0, 1, shape)
    else:
        e = stats.t.rvs(T_DF, scale=T_SCALE, size=shape, random_state=rng)
    return mu[:, None, :] + e * sd[:, None, :]


def in_margin(st: np.ndarray) -> np.ndarray:
    lead = (st.mean(-1, keepdims=True) - st) * V
    return lead[..., 0] - lead[..., 1:].max(-1)


# 5a. σ のスケール補正: 学習窓で in_margin の sd を実測に合わせる
target_sd = float(in_margin(W_tr["actual_st"]).std())
grid = np.arange(0.75, 1.45, 0.05)
sds = [float(in_margin(simulate(W_tr, "normal", s, seed=1)).std()) for s in grid]
SCALE = float(grid[int(np.argmin(np.abs(np.array(sds) - target_sd)))])
print(f"\nin_margin sd: 実測 {target_sd:.3f} → σ スケール {SCALE:.2f} を採用")

# 5b. K_SIGMA の探索 (選手別 σ が隊形の Brier を改善するか)
best = None
for k in [5.0, 10.0, 20.0, 40.0, 1e9]:
    p["sig_mult"] = sigma_multiplier(k)
    Wk = to_wide(p[(p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START)], wide_cols)
    pat = classify(simulate(Wk, "normal", SCALE, seed=2))
    pr = np.stack([(pat == j).mean(1) for j in range(6)], 1)
    act = classify(Wk["actual_st"])
    onehot = np.eye(6)[act]
    brier = float(((pr - onehot) ** 2).sum(1).mean())
    print(f"  K_SIGMA={k:>6}: 隊形 Brier {brier:.4f}")
    if best is None or brier < best[1]:
        best = (k, brier)
K_SIGMA = best[0]
p["sig_mult"] = sigma_multiplier(K_SIGMA)
print(f"K_SIGMA = {K_SIGMA} を採用")

# 分布形の選択 (学習窓の Brier で比較)
W_tr = to_wide(p[(p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START)], wide_cols)
act_tr = classify(W_tr["actual_st"])
onehot_tr = np.eye(6)[act_tr]
dist_brier = {}
for dist in ("normal", "t"):
    pat = classify(simulate(W_tr, dist, SCALE, seed=3))
    pr = np.stack([(pat == j).mean(1) for j in range(6)], 1)
    dist_brier[dist] = float(((pr - onehot_tr) ** 2).sum(1).mean())
DIST = min(dist_brier, key=dist_brier.get)
print(f"分布形 Brier: {({k: round(v, 4) for k, v in dist_brier.items()})} → {DIST} を採用")

# 5c. 隊形 → 決まり手 テーブル (学習窓の実測から)
km = p.drop_duplicates("race_code").set_index("race_code")["kimarite"]
km_tr = km.reindex(W_tr["races"]).str.replace("　", "", regex=False)
KIM_TABLE = {}
for j, name in enumerate(FORMATIONS):
    m = act_tr == j
    KIM_TABLE[name] = [
        float((km_tr[m] == "逃げ").mean()),
        float(km_tr[m].isin(["まくり", "まくり差し"]).mean()),
        float((km_tr[m] == "差し").mean()),
    ]

# ---------------------------------------------------------------------------
# 6. テスト窓で受け入れ基準を判定
# ---------------------------------------------------------------------------
W_te = to_wide(p[p["race_date"] >= TEST_START], wide_cols)
sim_te = simulate(W_te, DIST, SCALE, seed=42)
pat_te = classify(sim_te)
p_pat = np.stack([(pat_te == j).mean(1) for j in range(6)], 1)
act_te = classify(W_te["actual_st"])
inm_sim, inm_act = in_margin(sim_te), in_margin(W_te["actual_st"])

report = {"train": [TRAIN_START, TEST_START], "test_start": TEST_START,
          "n_train_races": len(W_tr["races"]), "n_test_races": len(W_te["races"])}

# 基準 1: in_margin の分布
c1 = {
    "mean": [float(inm_act.mean()), float(inm_sim.mean())],
    "sd": [float(inm_act.std()), float(inm_sim.std())],
    "within_0.6": [float((np.abs(inm_act) <= TH_LEAD).mean()), float((np.abs(inm_sim) <= TH_LEAD).mean())],
}
c1["pass"] = bool(
    abs(c1["mean"][0] - c1["mean"][1]) <= 0.05
    and abs(c1["sd"][0] - c1["sd"][1]) <= 0.05
    and abs(c1["within_0.6"][0] - c1["within_0.6"][1]) <= 0.03
)
print("\n=== 基準1: in_margin 分布 ===")
print(pd.DataFrame(c1, index=["実測", "MC"]).T.to_string())

# 基準 2: 隊形の周辺確率
c2 = {"actual": [float((act_te == j).mean()) for j in range(6)], "mc": p_pat.mean(0).tolist()}
c2["diff_pt"] = [round(100 * (b - a), 2) for a, b in zip(c2["actual"], c2["mc"])]
c2["pass"] = bool(max(abs(d) for d in c2["diff_pt"]) <= 3.0)
print("\n=== 基準2: 隊形の周辺確率 ===")
print(pd.DataFrame({"隊形": FORMATIONS, "実測": c2["actual"], "MC": c2["mc"], "差(pt)": c2["diff_pt"]}).round(4).to_string(index=False))

# 基準 3: 決まり手の五分位校正
KIM = np.array([KIM_TABLE[n] for n in FORMATIONS])
p_kim = p_pat @ KIM
km_te = km.reindex(W_te["races"]).str.replace("　", "", regex=False)
c3 = {}
worst = 0.0
for j, (name, mask) in enumerate([
    ("逃げ", (km_te == "逃げ").to_numpy()),
    ("まくり系", km_te.isin(["まくり", "まくり差し"]).to_numpy()),
    ("差し", (km_te == "差し").to_numpy()),
]):
    b = pd.qcut(p_kim[:, j], 5, duplicates="drop")
    t = pd.DataFrame({"p": p_kim[:, j], "y": mask.astype(float), "b": b})
    g = t.groupby("b", observed=True).agg(n=("y", "size"), pred=("p", "mean"), actual=("y", "mean"))
    gap = float((g["pred"] - g["actual"]).abs().max())
    worst = max(worst, gap)
    c3[name] = {"max_gap_pt": round(100 * gap, 2), "bins": g.round(4).to_dict("list")}
    print(f"\n=== 基準3: 決まり手 {name} (最大乖離 {100*gap:.1f}pt) ===")
    print(g.round(3).to_string())
c3["pass"] = bool(worst <= 0.05)

# 基準 4: 隊形 Brier が定数予測を下回るか
onehot_te = np.eye(6)[act_te]
brier_mc = float(((p_pat - onehot_te) ** 2).sum(1).mean())
const = np.array(c2["actual"])
brier_const = float(((const - onehot_te) ** 2).sum(1).mean())
c4 = {"mc": round(brier_mc, 4), "constant": round(brier_const, 4), "pass": bool(brier_mc < brier_const)}
print(f"\n=== 基準4: 隊形 Brier === MC {brier_mc:.4f} vs 定数 {brier_const:.4f} → {'PASS' if c4['pass'] else 'FAIL'}")

report["criteria"] = {"c1_in_margin": c1, "c2_formation_marginal": c2, "c3_kimarite_calib": c3, "c4_brier": c4}
report["all_pass"] = bool(c1["pass"] and c2["pass"] and c3["pass"] and c4["pass"])
report["constants"] = {
    "V_MPS": V, "N_SAMPLES": N_SAMPLES, "TH_LEAD": TH_LEAD, "TH_BEHIND": TH_BEHIND,
    "COURSE_OFFSET_ENTRY": {int(k): round(float(v), 4) for k, v in course_offset.items()},
    "BETA": round(BETA, 4), "SIGMA_BY_COURSE": {k: round(v, 4) for k, v in SIGMA_BY_COURSE.items()},
    "SIGMA_SCALE": SCALE, "K_SIGMA": K_SIGMA, "SD_PRIOR": round(sd_prior, 4),
    "DIST": DIST, "T_DF": round(float(T_DF), 2), "T_SCALE": round(float(T_SCALE), 3),
    "KIMARITE_BY_FORMATION": {k: [round(x, 4) for x in v] for k, v in KIM_TABLE.items()},
}
with open(_HERE / "calibration_report.json", "w") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 60)
print(f"受け入れ基準: 1={c1['pass']} 2={c2['pass']} 3={c3['pass']} 4={c4['pass']} → "
      f"{'ALL PASS' if report['all_pass'] else 'FAIL (撤退ライン §6 を検討)'}")
print("=" * 60)
