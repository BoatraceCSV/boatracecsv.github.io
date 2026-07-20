#!/usr/bin/env python3
"""Phase 1: ベースライン指標 (B0/B1) の算出.

B0 = 公表 全国平均ST (0.00/欠損は 0.25 補完 = NO_RECORD_ST_FALLBACK)
B0raw = 公表 全国平均ST を補完なしで使う (H6 修正前の距離計算相当)
B1 = スタート展示 ST をそのまま予測値にする (realtime のみ)

評価は「daily 相当 = 全レース」「realtime 相当 = 展示ST が全艇揃うレース」の
2 フレームで行い、後者では B0 と B1 を同一レース集合で比較する。
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

_HERE = Path(__file__).resolve().parent
FALLBACK = 0.25  # fun-site 側 NO_RECORD_ST_FALLBACK と同値
TEST_START = "2026-06-21"  # 直近30日をテスト窓として別掲

p = pd.read_csv(_HERE / "st_panel.csv.gz", dtype={"race_code": str})

# ---- 正解の定義: F(負値)・欠測を除いた実測 ST ----
p["valid_gt"] = (p["f_flag"] == 0) & p["actual_st"].notna() & (p["actual_st"] >= 0)

# ---- 予測値 ----
p["b0"] = p["avg_st_pub"].where(p["avg_st_pub"] > 0, np.nan).fillna(FALLBACK)
p["b0raw"] = p["avg_st_pub"].fillna(0.0)
p["b1"] = p["exh_st"]

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
    """全6艇の正解と予測が揃うレースのみで順序系指標を出す."""
    d = df[df["valid_gt"] & df[pred].notna()]
    full = d.groupby("race_code").filter(lambda g: len(g) == 6)
    rhos, lead_hits, form_hits = [], [], []
    for _, g in full.groupby("race_code"):
        a, q = g["actual_st"].to_numpy(), g[pred].to_numpy()
        if np.ptp(q) > 0 and np.ptp(a) > 0:
            rhos.append(spearmanr(a, q).statistic)
        # 先頭艇 (最小 ST)。予測が同値タイのときは的中扱いにしない (min が複数なら不的中)
        pred_leads = np.flatnonzero(q == q.min())
        lead_hits.append(len(pred_leads) == 1 and a[pred_leads[0]] == a.min())
        # 隊形3分類: 実進入 1-3 コース平均 - 4-6 コース平均, しきい値 ±0.03
        inner = g["actual_course"] <= 3
        def klass(v: np.ndarray) -> int:
            diff = v[inner].mean() - v[~inner].mean()
            return 0 if diff > 0.03 + 1e-9 else (2 if diff < -(0.03 + 1e-9) else 1)  # 0=内凹み,1=横一線,2=外凹み
        form_hits.append(klass(a) == klass(q))
    return {
        "n_races_full6": int(full["race_code"].nunique()),
        "spearman_mean": round(float(np.nanmean(rhos)), 4) if rhos else None,
        "lead_hit_rate": round(float(np.mean(lead_hits)), 4),
        "formation3_acc": round(float(np.mean(form_hits)), 4),
    }

def formation_base(df: pd.DataFrame) -> dict:
    """実測の隊形3分類の分布 (多数派クラス率 = 分類指標のチャンスレート)."""
    d = df[df["valid_gt"]]
    full = d.groupby("race_code").filter(lambda g: len(g) == 6)
    counts = {0: 0, 1: 0, 2: 0}
    for _, g in full.groupby("race_code"):
        inner = g["actual_course"] <= 3
        diff = g.loc[inner, "actual_st"].mean() - g.loc[~inner, "actual_st"].mean()
        counts[0 if diff > 0.03 + 1e-9 else (2 if diff < -(0.03 + 1e-9) else 1)] += 1
    total = sum(counts.values())
    return {"内凹み": counts[0], "横一線": counts[1], "外凹み": counts[2],
            "majority_rate": round(max(counts.values()) / total, 4)}

def frame_report(df: pd.DataFrame, preds: list[str]) -> dict:
    out = {"formation_actual_dist": formation_base(df)}
    for pr in preds:
        out[pr] = {"boat": boat_metrics(df, pr), "race": race_metrics(df, pr)}
    return out

# realtime 相当: 展示 ST が全艇そろうレース
exh_full = (
    p[p["exh_st"].notna() & p["valid_gt"]]
    .groupby("race_code")["boat"].count()
    .pipe(lambda s: set(s[s == 6].index))
)
rt = p[p["race_code"].isin(exh_full)]

report = {}
for label, frame in [("full_period", p), ("test_window", p[p["race_date"] >= TEST_START])]:
    rt_f = frame[frame["race_code"].isin(exh_full)]
    report[label] = {
        "daily_all_races": frame_report(frame, ["b0", "b0raw"]),
        "realtime_exh_races": frame_report(rt_f, ["b0", "b1"]),
    }

# ---- 記述統計 (仮説の当たりを付ける) ----
d = p[p["valid_gt"]]
desc = {
    "st_by_course": d.groupby("actual_course")["actual_st"].mean().round(4).to_dict(),
    "st_by_class": d.groupby("class_grade")["actual_st"].agg(["mean", "count"]).round(4).to_dict("index"),
    "st_by_flying": d.groupby(d["flying_count"].clip(upper=2))["actual_st"].agg(["mean", "count"]).round(4).to_dict("index"),
    "corr_pub_actual": round(float(d[["avg_st_pub", "actual_st"]].dropna().query("avg_st_pub>0").corr().iloc[0, 1]), 4),
    "corr_exh_actual": round(float(d[["exh_st", "actual_st"]].dropna().corr().iloc[0, 1]), 4),
    "racer_st_std_median": round(float(d.groupby("regno")["actual_st"].std().median()), 4),
    "racer_runs_median": float(d.groupby("regno")["actual_st"].count().median()),
    "zero_pub_actual_mean": round(float(d.loc[d["avg_st_pub"] == 0, "actual_st"].mean()), 4),
    "zero_pub_n": int((d["avg_st_pub"] == 0).sum()),
    "nonzero_pub_actual_mean": round(float(d.loc[d["avg_st_pub"] > 0, "actual_st"].mean()), 4),
    "f_rate_overall": round(float((p["f_flag"] == 1).mean()), 5),
}
report["descriptive"] = desc

with open(_HERE / "baseline_report.json", "w") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print(json.dumps(report, ensure_ascii=False, indent=2))
