#!/usr/bin/env python3
"""slit_sim 検証の共通部品: パネル読み込みと因果的 EWMA.

calibrate.py (隊形確率の校正) と band_calibration.py (ST 帯の校正) が共有する。
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parents[1] / "scripts"))
from boatrace.racer_st import (  # noqa: E402
    CLASS_PRIOR,
    GLOBAL_PRIOR,
    HALF_LIFE_DAYS,
    K_PRIOR,
)

TRAIN_START, TEST_START = "2026-03-01", "2026-06-21"
V = 13.9  # スタート艇速 m/s (ST 差 → 距離 の換算)


def load_panel() -> pd.DataFrame:
    p = pd.read_csv(_HERE / "st_panel.csv.gz", dtype={"race_code": str})
    p["date"] = pd.to_datetime(p["race_date"])
    p["valid"] = (p["f_flag"] == 0) & p["actual_st"].notna() & (p["actual_st"] >= 0)
    p["prior"] = np.where(
        p["avg_st_pub"].fillna(0) > 0,
        p["avg_st_pub"],
        p["class_grade"].map(CLASS_PRIOR).fillna(GLOBAL_PRIOR),
    )
    return p.sort_values(["regno", "date", "race_code"]).reset_index(drop=True)


def causal_ewma(p: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(base_st, racer_sd_raw, n_eff) を返す。当日レースは履歴に含めない。

    racer_st.RacerStState と同じ (重み付き和, 重み計) の EWMA に、σ 用の
    重み付き二乗和を足したもの。実装計画 §3.1 で state.csv に追加する列と対応する。
    """
    decay = 0.5 ** (1.0 / HALF_LIFE_DAYS)
    base = np.full(len(p), np.nan)
    sd_raw = np.full(len(p), np.nan)
    n_eff = np.zeros(len(p))
    for _, g in p.groupby("regno", sort=False):
        idx = g.index.to_numpy()
        days = g["date"].to_numpy(dtype="datetime64[D]").astype(int)
        st = g["actual_st"].to_numpy()
        ok = g["valid"].to_numpy()
        pri = g["prior"].to_numpy()
        ws = wt = ws2 = 0.0
        last = None
        i = 0
        while i < len(idx):
            d = days[i]
            if last is not None:
                f = decay ** (d - last)
                ws, wt, ws2 = ws * f, wt * f, ws2 * f
            j = i
            while j < len(idx) and days[j] == d:  # 同日は「その日より前」の状態で予測
                base[idx[j]] = (ws + K_PRIOR * pri[j]) / (wt + K_PRIOR)
                n_eff[idx[j]] = wt
                if wt > 1.0:
                    m1, m2 = ws / wt, ws2 / wt
                    sd_raw[idx[j]] = np.sqrt(max(m2 - m1 * m1, 1e-8))
                j += 1
            for k in range(i, j):  # その日の走行を履歴へ取り込む
                if ok[k]:
                    ws += st[k]
                    wt += 1.0
                    ws2 += st[k] * st[k]
            last = d
            i = j
    return base, sd_raw, n_eff
