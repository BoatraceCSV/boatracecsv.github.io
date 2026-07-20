#!/usr/bin/env python3
"""Phase 2: 仮説 ablation (H1 EWMA → H2 コース補正 → H4 F補正 → H3 展示Fシグナル).

すべての予測は因果的 (各レースの予測にはそのレース日より前の実測 ST のみ使用)。
ハイパーパラメータと補正量は学習窓 (TRAIN_START〜TEST_START 前日) で決め、
テスト窓 (TEST_START〜) の指標で評価する。

前提: build_panel.py で st_panel.csv.gz が生成済みであること。
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

_HERE = Path(__file__).resolve().parent
FALLBACK = 0.25
TRAIN_START = "2026-03-01"  # H1 のウォームアップ (2025-11〜2026-02) を確保
TEST_START = "2026-06-21"

p = pd.read_csv(_HERE / "st_panel.csv.gz", dtype={"race_code": str})
p["valid_gt"] = (p["f_flag"] == 0) & p["actual_st"].notna() & (p["actual_st"] >= 0)
p["b0"] = p["avg_st_pub"].where(p["avg_st_pub"] > 0, np.nan).fillna(FALLBACK)
p["date"] = pd.to_datetime(p["race_date"])
p = p.sort_values(["regno", "date", "race_code"]).reset_index(drop=True)

is_train = (p["race_date"] >= TRAIN_START) & (p["race_date"] < TEST_START)
is_test = p["race_date"] >= TEST_START

# 級別事前分布 (学習窓より前のデータのみで算出 = テスト窓に対して因果的)
pre = p[(p["race_date"] < TRAIN_START) & p["valid_gt"]]
class_prior = pre.groupby("class_grade")["actual_st"].mean().to_dict()
global_prior = float(pre["actual_st"].mean())


def h1_predict(half_life_days: float, k_prior: float) -> np.ndarray:
    """選手ごとの因果的 時間減衰平均 + 事前分布への収縮.

    同日レースは履歴に含めない (daily バッチは朝時点の情報しか持たないため)。
    F レース (valid_gt=False) は履歴を更新しない。
    """
    out = np.full(len(p), np.nan)
    decay_per_day = 0.5 ** (1.0 / half_life_days) if np.isfinite(half_life_days) else 1.0
    for _, g in p.groupby("regno", sort=False):
        idx = g.index.to_numpy()
        days = g["date"].to_numpy(dtype="datetime64[D]").astype(int)
        st = g["actual_st"].to_numpy()
        ok = g["valid_gt"].to_numpy()
        prior = np.where(
            g["avg_st_pub"].to_numpy() > 0,
            g["avg_st_pub"].to_numpy(),
            np.array([class_prior.get(c, global_prior) for c in g["class_grade"]]),
        )
        wsum = 0.0  # Σ w_i * st_i
        wtot = 0.0  # Σ w_i
        last_day = None
        pend_wsum = 0.0  # 同日ぶんの保留更新
        pend_wtot = 0.0
        for j in range(len(idx)):
            d = days[j]
            if last_day is not None and d != last_day:
                # 日付が進んだ: 保留していた前日ぶんを取り込み、経過日数ぶん減衰
                wsum += pend_wsum
                wtot += pend_wtot
                pend_wsum = pend_wtot = 0.0
                f = decay_per_day ** (d - last_day)
                wsum *= f
                wtot *= f
            est_num = wsum + k_prior * prior[j]
            est_den = wtot + k_prior
            out[idx[j]] = est_num / est_den
            if ok[j]:
                pend_wsum += st[j]
                pend_wtot += 1.0
            last_day = d
    return out


def boat_metrics(df: pd.DataFrame, pred: str) -> dict:
    d = df[df["valid_gt"] & df[pred].notna()]
    err = d[pred] - d["actual_st"]
    return {
        "n": int(len(d)),
        "mae": round(float(err.abs().mean()), 4),
        "rmse": round(float(np.sqrt((err**2).mean())), 4),
        "bias": round(float(err.mean()), 4),
    }


def race_metrics(df: pd.DataFrame, pred: str) -> dict:
    d = df[df["valid_gt"] & df[pred].notna()]
    full = d.groupby("race_code").filter(lambda g: len(g) == 6)
    rhos, lead_hits, form_hits = [], [], []
    for _, g in full.groupby("race_code"):
        a, q = g["actual_st"].to_numpy(), g[pred].to_numpy()
        if np.ptp(q) > 0 and np.ptp(a) > 0:
            rhos.append(spearmanr(a, q).statistic)
        pred_leads = np.flatnonzero(q == q.min())
        lead_hits.append(len(pred_leads) == 1 and a[pred_leads[0]] == a.min())
        inner = g["actual_course"] <= 3

        def klass(v: np.ndarray) -> int:
            diff = v[inner].mean() - v[~inner].mean()
            return 0 if diff > 0.03 + 1e-9 else (2 if diff < -(0.03 + 1e-9) else 1)

        form_hits.append(klass(a) == klass(q))
    return {
        "n_races_full6": int(full["race_code"].nunique()),
        "spearman_mean": round(float(np.nanmean(rhos)), 4) if rhos else None,
        "lead_hit_rate": round(float(np.mean(lead_hits)), 4),
        "formation3_acc": round(float(np.mean(form_hits)), 4),
    }


def metrics(df: pd.DataFrame, pred: str) -> dict:
    return {"boat": boat_metrics(df, pred), "race": race_metrics(df, pred)}


# ---- H1 グリッド探索 (学習窓 / daily フレーム / MAE 最小化) ----
grid_results = {}
if "--skip-grid" not in sys.argv:
    for hl in (14.0, 30.0, 60.0, 120.0, float("inf")):
        for k in (2.0, 5.0, 10.0, 20.0):
            col = f"h1_{hl}_{k}"
            p[col] = h1_predict(hl, k)
            m = boat_metrics(p[is_train], col)
            grid_results[f"HL={hl},k={k}"] = m
            print(f"HL={hl:>6}, k={k:>4}: train MAE={m['mae']} bias={m['bias']}")
            if not (hl, k) == (60.0, 10.0):
                p = p.drop(columns=[col])

best_key = min(grid_results, key=lambda x: grid_results[x]["mae"]) if grid_results else None
print("best:", best_key)

# ベスト構成で再計算 (グリッドの列名に依存しないよう明示的に)
BEST_HL, BEST_K = (
    (float(best_key.split(",")[0].split("=")[1]), float(best_key.split("k=")[1]))
    if best_key
    else (60.0, 10.0)
)
p["m1"] = h1_predict(BEST_HL, BEST_K)

# ---- H2: コース補正 (学習窓の残差から推定) ----
# 予測時に使えるコース: daily = 枠番 / realtime = 展示進入 (無ければ枠番)
p["course_daily"] = p["boat"]
p["course_rt"] = p["exh_course"].fillna(p["boat"]).astype(int)

tr = p[is_train & p["valid_gt"] & p["m1"].notna()]
course_offset = (tr["actual_st"] - tr["m1"]).groupby(tr["actual_course"]).mean().to_dict()
print("course offsets:", {c: round(v, 4) for c, v in course_offset.items()})

p["m2_daily"] = p["m1"] + p["course_daily"].map(course_offset).fillna(0.0)
p["m2_rt"] = p["m1"] + p["course_rt"].map(course_offset).fillna(0.0)

# ---- H4: F本数補正 (学習窓の m2 残差から推定) ----
tr = p[is_train & p["valid_gt"] & p["m2_daily"].notna()]
fc = tr["flying_count"].clip(upper=2)
f_offset = (tr["actual_st"] - tr["m2_daily"]).groupby(fc).mean().to_dict()
print("F offsets:", {k: round(v, 4) for k, v in f_offset.items()})

f_adj = p["flying_count"].clip(upper=2).map(f_offset).fillna(0.0)
p["m3_daily"] = p["m2_daily"] + f_adj
p["m3_rt"] = p["m2_rt"] + f_adj

# ---- H3': 展示F (踏み込み) シグナル (学習窓の m3 残差から推定, realtime のみ) ----
tr = p[is_train & p["valid_gt"] & p["m3_rt"].notna() & p["exh_st"].notna()]
exh_f = tr["exh_st"] < 0
h3_offset = {
    True: float((tr["actual_st"] - tr["m3_rt"])[exh_f].mean()),
    False: float((tr["actual_st"] - tr["m3_rt"])[~exh_f].mean()),
}
print("exh-F offsets:", {k: round(v, 4) for k, v in h3_offset.items()})
p["m4_rt"] = p["m3_rt"] + (p["exh_st"] < 0).map(h3_offset).where(p["exh_st"].notna(), 0.0)

# ---- 評価 (テスト窓) ----
exh_full = (
    p[p["exh_st"].notna() & p["valid_gt"]]
    .groupby("race_code")["boat"]
    .count()
    .pipe(lambda s: set(s[s == 6].index))
)
test = p[is_test]
test_rt = test[test["race_code"].isin(exh_full)]

# ---- 参考: ノイズ床の目安 (テスト窓内の選手平均を使うリーキーなオラクル) ----
t = p[is_test & p["valid_gt"]]
oracle_map = t.groupby("regno")["actual_st"].mean()
p.loc[p.index[is_test], "oracle"] = p.loc[p.index[is_test], "regno"].map(oracle_map)

report = {
    "config": {
        "train_window": [TRAIN_START, TEST_START],
        "best_h1": {"half_life_days": BEST_HL, "k_prior": BEST_K},
        "class_prior": {k: round(v, 4) for k, v in class_prior.items()},
        "course_offset": {int(k): round(v, 4) for k, v in course_offset.items()},
        "f_offset": {int(k): round(v, 4) for k, v in f_offset.items()},
        "exh_f_offset": {str(k): round(v, 4) for k, v in h3_offset.items()},
    },
    "h1_grid_train_mae": grid_results,
    "oracle_racer_mean_test": boat_metrics(p[is_test], "oracle"),
    "test_daily": {
        "B0": metrics(test, "b0"),
        "M1_h1": metrics(test, "m1"),
        "M2_h1_course": metrics(test, "m2_daily"),
        "M3_h1_course_f": metrics(test, "m3_daily"),
    },
    "test_realtime": {
        "B0": metrics(test_rt, "b0"),
        "M3_daily_course": metrics(test_rt, "m3_daily"),
        "M3_rt_course": metrics(test_rt, "m3_rt"),
        "M4_exh_f_signal": metrics(test_rt, "m4_rt"),
    },
}

with open(_HERE / "phase2_report.json", "w") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print(json.dumps({k: v for k, v in report.items() if k != "h1_grid_train_mae"}, ensure_ascii=False, indent=2))
