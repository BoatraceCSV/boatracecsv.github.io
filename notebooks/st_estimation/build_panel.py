#!/usr/bin/env python3
"""Phase 1: 実測 ST パネル構築 (st_estimation H 検証の共通基盤).

data/results/realtime + data/programs/race_cards + data/previews/stt を
レースコード × 艇番で結合し、1 艇走 = 1 行のパネル CSV を出力する。

出力: st_panel.csv.gz
"""

import glob
import json
import sys

import numpy as np
import pandas as pd

from pathlib import Path

_HERE = Path(__file__).resolve().parent
REPO = sys.argv[1] if len(sys.argv) > 1 else str(_HERE.parents[1])  # リポジトリルート
OUT = sys.argv[2] if len(sys.argv) > 2 else str(_HERE)


def load_all(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(f"{REPO}/{pattern}"))
    frames = [pd.read_csv(f, dtype=str) for f in files]
    df = pd.concat(frames, ignore_index=True)
    print(f"{pattern}: {len(files)} files, {len(df)} rows")
    return df


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# ---- results/realtime: コース順の実測 ST → long 化 ----
res = load_all("data/results/realtime/*/*/*.csv")
res_rows = []
for c in range(1, 7):
    part = res[
        [
            "レースコード",
            "レース日",
            "レース場",
            "レース回",
            "決まり手",
            "風向",
            "風速(m)",
            "波の高さ(cm)",
            "天候",
            f"{c}コース_艇番",
            f"{c}コース_スタートタイミング",
            f"{c}コース_F",
        ]
    ].rename(
        columns={
            f"{c}コース_艇番": "boat",
            f"{c}コース_スタートタイミング": "st_raw",
            f"{c}コース_F": "f_flag",
        }
    )
    part["actual_course"] = c
    res_rows.append(part)
res_long = pd.concat(res_rows, ignore_index=True)
res_long["boat"] = to_num(res_long["boat"])
res_long = res_long.dropna(subset=["boat"])
res_long["boat"] = res_long["boat"].astype(int)

# 実測 ST 文字列の実態調査 (F/L/欠損の表現) — レポート用に分布を保存
raw_st = res_long["st_raw"].fillna("")
nonnum = raw_st[~raw_st.str.match(r"^-?\d*\.?\d+$") & (raw_st != "")]
st_repr = {
    "total_rows": int(len(res_long)),
    "empty": int((raw_st == "").sum()),
    "numeric": int(raw_st.str.match(r"^-?\d*\.?\d+$").sum()),
    "nonnumeric_examples": nonnum.value_counts().head(20).to_dict(),
}
res_long["actual_st"] = to_num(res_long["st_raw"])
# F 列は docs の記述 (1/0) と異なり実データでは "F" / 空欄。文字列で判定する。
res_long["f_flag"] = res_long["f_flag"].fillna("").astype(str).str.contains("F").astype(int)

# ---- race_cards: 公表 全国平均ST・級別・F本数 → long 化 ----
cards = load_all("data/programs/race_cards/202[56]/*/*.csv")
card_rows = []
for b in range(1, 7):
    part = cards[
        [
            "レースコード",
            f"艇{b}_登録番号",
            f"艇{b}_級別",
            f"艇{b}_全国平均ST",
            f"艇{b}_F本数",
            f"艇{b}_全国勝率",
        ]
    ].rename(
        columns={
            f"艇{b}_登録番号": "regno",
            f"艇{b}_級別": "class_grade",
            f"艇{b}_全国平均ST": "avg_st_pub",
            f"艇{b}_F本数": "flying_count",
            f"艇{b}_全国勝率": "win_rate",
        }
    )
    part["boat"] = b
    card_rows.append(part)
cards_long = pd.concat(card_rows, ignore_index=True)
for col in ("avg_st_pub", "win_rate"):
    cards_long[col] = to_num(cards_long[col])
cards_long["flying_count"] = (
    cards_long["flying_count"].astype(str).str.extract(r"(\d+)")[0].pipe(to_num)
)

# ---- previews/stt: 展示 ST・展示進入コース → long 化 ----
stt = load_all("data/previews/stt/*/*/*.csv")
stt_rows = []
for b in range(1, 7):
    part = stt[["レースコード", f"艇{b}_コース", f"艇{b}_スタート展示"]].rename(
        columns={f"艇{b}_コース": "exh_course", f"艇{b}_スタート展示": "exh_st"}
    )
    part["boat"] = b
    stt_rows.append(part)
stt_long = pd.concat(stt_rows, ignore_index=True)
stt_long["exh_course"] = to_num(stt_long["exh_course"])
stt_long["exh_st"] = to_num(stt_long["exh_st"])

# ---- 結合 ----
panel = res_long.merge(cards_long, on=["レースコード", "boat"], how="left").merge(
    stt_long, on=["レースコード", "boat"], how="left"
)
panel = panel.rename(
    columns={
        "レースコード": "race_code",
        "レース日": "race_date",
        "レース場": "stadium",
        "レース回": "race_no",
        "決まり手": "kimarite",
        "風向": "wind_dir",
        "風速(m)": "wind_ms",
        "波の高さ(cm)": "wave_cm",
        "天候": "weather",
    }
)
panel = panel.drop(columns=["st_raw"])
panel.to_csv(f"{OUT}/st_panel.csv.gz", index=False)

# ---- データ品質サマリ ----
quality = {
    "st_raw_representation": st_repr,
    "period": [str(panel["race_date"].min()), str(panel["race_date"].max())],
    "n_races": int(panel["race_code"].nunique()),
    "n_boat_runs": int(len(panel)),
    "n_racers": int(panel["regno"].nunique()),
    "actual_st_missing": int(panel["actual_st"].isna().sum()),
    "f_rows": int((panel["f_flag"] == 1).sum()),
    "f_st_values": panel.loc[panel["f_flag"] == 1, "actual_st"]
    .describe()
    .round(4)
    .to_dict(),
    "normal_st_describe": panel.loc[panel["f_flag"] == 0, "actual_st"]
    .describe()
    .round(4)
    .to_dict(),
    "avg_st_pub_missing": int(panel["avg_st_pub"].isna().sum()),
    "avg_st_pub_zero": int((panel["avg_st_pub"] == 0).sum()),
    "cards_join_miss": int(panel["class_grade"].isna().sum()),
    "stt_join_miss": int(panel["exh_course"].isna().sum()),
    "exh_st_present": int(panel["exh_st"].notna().sum()),
    "exh_st_negative": int((panel["exh_st"] < 0).sum()),
    "actual_st_negative_noF": int(
        ((panel["actual_st"] < 0) & (panel["f_flag"] == 0)).sum()
    ),
    "course_ne_boat_rate": float(
        (panel["actual_course"] != panel["boat"]).mean().round(4)
    ),
    "exh_course_ne_actual_rate": float(
        (panel["exh_course"] != panel["actual_course"])
        .loc[panel["exh_course"].notna()]
        .mean()
        .round(4)
    ),
}
with open(f"{OUT}/quality.json", "w") as f:
    json.dump(quality, f, ensure_ascii=False, indent=2, default=str)
print(json.dumps(quality, ensure_ascii=False, indent=2, default=str))
