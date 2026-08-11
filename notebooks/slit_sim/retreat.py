#!/usr/bin/env python3
"""実装計画 §6 の撤退ライン 1 / 2 の判定.

1. 隊形 6 分類 → 3 分類 (イン先手 / 揃い / イン遅れ) に粗くして校正が取れるか
2. 決まり手をやめ、隊形確率だけなら成立するか

判定は「周辺確率が実測 ±3pt」かつ「Brier が定数予測を下回る」。
σ スケールは sd 合わせではなく Brier 最小で選び、縮退版に最も有利な条件で測る。

    python notebooks/slit_sim/retreat.py
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from st_features import TEST_START, TRAIN_START, V, causal_ewma, load_panel  # noqa: E402

sys.path.insert(0, str(_HERE.parents[1] / "scripts"))
from boatrace.racer_st import F_OFFSET  # noqa: E402

N_SAMPLES = 2000
TH = 0.6

p = load_panel()
p["base_st"], p["racer_sd_raw"], p["n_eff"] = causal_ewma(p)
tr_m = (p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START) & p["valid"]
tr = p[tr_m]
course_offset = (tr["actual_st"] - tr["base_st"]).groupby(tr["actual_course"]).mean().to_dict()
p["mu_raw"] = (
    p["base_st"]
    + p["actual_course"].map(course_offset).astype(float)
    + p["flying_count"].clip(0, 2).map(F_OFFSET).fillna(0.0)
)
sd_prior = float(tr["racer_sd_raw"].mean())
raw, n = p["racer_sd_raw"].fillna(sd_prior), p["n_eff"].clip(lower=0.0)
p["sig_mult"] = ((n * raw + 10.0 * sd_prior) / (n + 10.0)) / sd_prior


def to_wide(df, cols):
    d = df[df["actual_course"].between(1, 6)]
    out = {c: d.pivot_table(index="race_code", columns="actual_course", values=c,
                            aggfunc="first").reindex(columns=range(1, 7)) for c in cols}
    keep = out[cols[0]].notna().all(axis=1)
    for c in cols[1:]:
        keep &= out[c].notna().all(axis=1)
    races = out[cols[0]].index[keep]
    return {c: out[c].loc[races].to_numpy(float) for c in cols} | {"races": races.to_numpy()}


COLS = ["actual_st", "mu_raw", "sig_mult"]
W_tr = to_wide(p[tr_m], COLS)
W_te = to_wide(p[p["race_date"] >= TEST_START], COLS)


def demean(x):
    return x - x.mean(-1, keepdims=True)


dev_a, dev_p = demean(W_tr["actual_st"]), demean(W_tr["mu_raw"])
BETA = float(np.cov(dev_p.ravel(), dev_a.ravel())[0, 1] / dev_p.var())
res = dev_a - BETA * dev_p
SIG_C = np.array([res[:, c].std() for c in range(6)])


def simulate(W, scale, seed):
    rng = np.random.default_rng(seed)
    mu = W["mu_raw"]
    mu = mu.mean(1, keepdims=True) + BETA * demean(mu)
    sd = SIG_C * W["sig_mult"] * scale
    return mu[:, None, :] + rng.normal(0, 1, (len(mu), N_SAMPLES, 6)) * sd[:, None, :]


def in_margin(st):
    lead = (st.mean(-1, keepdims=True) - st) * V
    return lead[..., 0] - lead[..., 1:].max(-1)


def classify3(st):
    """0=イン先手 / 1=揃い / 2=イン遅れ"""
    inm = in_margin(st)
    return np.where(inm >= TH, 0, np.where(inm >= -TH, 1, 2))


NAMES3 = ["イン先手", "揃い", "イン遅れ"]
report = {"note": "撤退ライン1/2 の判定", "beta": round(BETA, 4)}

# --- σ スケール探索。受け入れ基準は 周辺 / Brier / 校正 の 3 つとも満たすこと ---
#     Brier だけを最小化すると σ が縮んで確率が過信になり校正が落ちるため、
#     3 指標を並べて「全部通る scale が存在するか」を見る。
act_tr = classify3(W_tr["actual_st"])
oh_tr = np.eye(3)[act_tr]
marg_tr = np.array([(act_tr == j).mean() for j in range(3)])
brier_const_tr = float(((marg_tr - oh_tr) ** 2).sum(1).mean())


def calib_gap(pr_, act_, j=1):
    b_ = pd.qcut(pr_[:, j], 5, duplicates="drop")
    t_ = pd.DataFrame({"p": pr_[:, j], "y": (act_ == j).astype(float), "b": b_})
    g_ = t_.groupby("b", observed=True).agg(pred=("p", "mean"), actual=("y", "mean"))
    return float((g_["pred"] - g_["actual"]).abs().max())


print("σ スケール探索 (学習窓): 3 指標すべてを満たす scale を探す")
rows = []
for s in np.arange(0.60, 1.45, 0.05):
    pat = classify3(simulate(W_tr, s, seed=1))
    pr_ = np.stack([(pat == j).mean(1) for j in range(3)], 1)
    b = float(((pr_ - oh_tr) ** 2).sum(1).mean())
    md = float(np.abs(pr_.mean(0) - marg_tr).max())
    cg = calib_gap(pr_, act_tr)
    rows.append({"scale": round(float(s), 2), "周辺差(pt)": round(100 * md, 2),
                 "Brier": round(b, 4), "定数比": round(b - brier_const_tr, 4),
                 "校正乖離(pt)": round(100 * cg, 2),
                 "全通過": bool(md <= 0.03 and b < brier_const_tr and cg <= 0.05)})
grid = pd.DataFrame(rows)
print(grid.to_string(index=False))
ok = grid[grid["全通過"]]
if len(ok):
    SCALE = float(ok.sort_values("Brier").iloc[0]["scale"])
    print(f"→ 3 指標を満たす scale が存在: SCALE = {SCALE:.2f}")
else:
    SCALE = float(grid.sort_values("校正乖離(pt)").iloc[0]["scale"])
    print(f"→ 3 指標を同時に満たす scale は無い。校正が最良の SCALE = {SCALE:.2f} で判定を続行")

# --- テスト窓で判定 ---
act = classify3(W_te["actual_st"])
pat = classify3(simulate(W_te, SCALE, seed=42))
pr = np.stack([(pat == j).mean(1) for j in range(3)], 1)
oh = np.eye(3)[act]

marg_act = [float((act == j).mean()) for j in range(3)]
marg_mc = pr.mean(0).tolist()
diff = [round(100 * (b - a), 2) for a, b in zip(marg_act, marg_mc)]
print("\n=== 撤退1: 3分類の周辺確率 (テスト窓) ===")
print(pd.DataFrame({"隊形": NAMES3, "実測": marg_act, "MC": marg_mc, "差(pt)": diff}).round(4).to_string(index=False))

brier_mc = float(((pr - oh) ** 2).sum(1).mean())
brier_const = float(((np.array(marg_act) - oh) ** 2).sum(1).mean())
print(f"\nBrier: MC {brier_mc:.4f} vs 定数 {brier_const:.4f} "
      f"({100*(brier_const-brier_mc)/brier_const:+.2f}%)")

# 五分位校正 (「揃い」確率で)
b = pd.qcut(pr[:, 1], 5, duplicates="drop")
t = pd.DataFrame({"p": pr[:, 1], "y": (act == 1).astype(float), "b": b})
g = t.groupby("b", observed=True).agg(n=("y", "size"), pred=("p", "mean"), actual=("y", "mean"))
print("\n「揃い」確率の五分位校正:")
print(g.round(3).to_string())
gap = float((g["pred"] - g["actual"]).abs().max())
print(f"最大乖離 {100*gap:.1f}pt")

# MC 確率がレース間でどれだけ動くか (動かないなら定数と同じ = 情報が無い)
print(f"\nMC 確率のレース間 sd: {pr.std(0).round(4).tolist()} (0 に近いほど定数予測と同じ)")

c1_pass = bool(max(abs(d) for d in diff) <= 3.0)
c2_pass = bool(brier_mc < brier_const)
c3_pass = bool(gap <= 0.05)
report["retreat1_3class"] = {
    "marginal_actual": marg_act, "marginal_mc": marg_mc, "diff_pt": diff,
    "brier_mc": round(brier_mc, 4), "brier_const": round(brier_const, 4),
    "max_calib_gap_pt": round(100 * gap, 2),
    "prob_sd_across_races": pr.std(0).round(4).tolist(),
    "marginal_pass": c1_pass, "brier_pass": c2_pass, "calib_pass": c3_pass,
    "pass": bool(c1_pass and c2_pass and c3_pass),
}
with open(_HERE / "retreat_report.json", "w") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

print("\n" + "=" * 60)
print(f"撤退1 (3分類): 周辺={c1_pass} Brier={c2_pass} 校正={c3_pass} "
      f"→ {'成立' if report['retreat1_3class']['pass'] else '不成立'}")
print("撤退2 (決まり手を落とす) は撤退1 の隊形確率が前提なので、上が不成立なら同時に不成立")
print("=" * 60)
