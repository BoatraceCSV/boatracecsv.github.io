"""モーターpt パラメータ調整ハーネス。

前計算済み flat run テーブルから、パラメータ一式を受けて
motor_ability_pt を numpy で高速再計算し、エキスパート評価との
場別 Spearman を返す。

パラメータ:
  lane_on      : bool   コース補正(z残差化) ON/OFF
  half_life    : float  時間減衰半減期(日)。None で減衰OFF
  shrink_k     : float  ベイズ収縮強度 (0 = 収縮なし)
  penalty      : float  転/落/沈/エ のスコア
  n_sessions   : int    採用節数 (<= 6)
  gamma[r]     : float  行 r のスコアカーブ形状 pts_k = A_r * ((6-k)/5)^gamma_r
  amp[r]       : float  行 r のスケール A_r
行順 ROWS = (B2,全),(B1,全),(A2,SG_G1),(A2,G2_G3_一般),(A1,SG_G1),(A1,G2_G3_一般)
現行構成: gamma=1, amp=(125,100,125,75,100,50), penalty=-100, H=60, k=10, lane=True, N=6
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
CACHE = HERE / "cache"
GT = HERE / "ground_truth"

ROWS = [("B2", "全"), ("B1", "全"), ("A2", "SG_G1"), ("A2", "G2_G3_一般"),
        ("A1", "SG_G1"), ("A1", "G2_G3_一般")]
ROW_ID = {key: i for i, key in enumerate(ROWS)}

NEG_TOKENS = {"転", "落", "沈", "エ"}
SD_FLOOR = 10.0
MIN_SAMPLES = 5

CURRENT = dict(
    lane_on=True, half_life=60.0, shrink_k=10.0, penalty=-100.0, n_sessions=6,
    gamma=np.ones(6), amp=np.array([125.0, 100.0, 125.0, 75.0, 100.0, 50.0]),
)

TARGETS = [
    ("04", dt.date(2026, 7, 15), "heiwajima_04.csv"),
    ("23", dt.date(2026, 7, 16), "karatsu_23.csv"),
    ("24", dt.date(2026, 7, 18), "omura_24.csv"),
    ("14", dt.date(2026, 6, 14), "naruto_14.csv"),
]

GRADE_NUM_HEI = {"SS": 11, "S": 10, "A+": 9, "A": 8, "A-": 7,
                 "B+": 6, "B": 5, "B-": 4, "C": 3, "D": 2, "E": 1}
RANK_NUM_KAR = {"S": 5, "A": 4, "B": 3, "C": 2, "D": 1}
MEDAL_NUM = {"金": 3, "銀": 2, "銅": 1}


@dataclass
class Target:
    stadium: str
    day: dt.date
    # pool arrays (全24場、baseline 計算用)
    p_row: np.ndarray       # row id 0-5
    p_fin: np.ndarray       # 0-5 = 1-6着, 6 = 転落沈エ
    p_lane: np.ndarray      # 0-6
    p_w_by_h: dict          # {half_life(or None): weight array}  遅延構築
    p_days: np.ndarray
    p_sess: np.ndarray
    # target-stadium arrays
    t_idx: np.ndarray       # pool 内のうち対象場の行 index
    t_motor: np.ndarray     # モーター番号 (t_idx と同順)
    motors: np.ndarray      # 対象場のユニーク機番
    # ground truth
    gt_motor: np.ndarray
    gt_score: np.ndarray
    label: str = ""
    extra: dict = field(default_factory=dict)


def _load_gt(fname: str):
    df = pd.read_csv(GT / fname)
    if "評価" in df.columns:      # 平和島
        return df["機番"].to_numpy(int), df["評価"].map(GRADE_NUM_HEI).to_numpy(float)
    if "素性ランク" in df.columns:  # 唐津
        return df["機番"].to_numpy(int), df["素性ランク"].map(RANK_NUM_KAR).to_numpy(float)
    if "評価平均" in df.columns:   # 大村
        return df["機番"].to_numpy(int), df["評価平均"].to_numpy(float)
    if "メダル" in df.columns:     # 鳴門(上位10のみの打ち切り)
        return df["機番"].to_numpy(int), df["メダル"].map(MEDAL_NUM).to_numpy(float)
    raise ValueError(fname)


def load_targets() -> list[Target]:
    out = []
    for stadium, day, gt_file in TARGETS:
        df = pd.read_csv(CACHE / f"runs_{stadium}_{day.isoformat()}.csv",
                         dtype={"hist_stadium": str, "finish": str})
        row = df.apply(lambda r: ROW_ID[(r["racer_class"], r["grade_bucket"])], axis=1).to_numpy()
        finish = np.full(len(df), -1, dtype=int)
        f = df["finish"].astype(str)
        for k in range(1, 7):
            finish[f == str(k)] = k - 1
        finish[f.isin(NEG_TOKENS)] = 6
        keep = finish >= 0   # F/L/失/妨/欠/不 は除外済みのはずだが安全側
        df, row, finish = df[keep].reset_index(drop=True), row[keep], finish[keep]
        days = (pd.Timestamp(day) - pd.to_datetime(df["race_date"])).dt.days.to_numpy()
        days = np.maximum(days, 0)
        gt_motor, gt_score = _load_gt(gt_file)
        is_t = (df["hist_stadium"] == stadium).to_numpy()
        out.append(Target(
            stadium=stadium, day=day,
            p_row=row, p_fin=finish, p_lane=df["lane"].to_numpy(int),
            p_w_by_h={}, p_days=days, p_sess=df["session_idx"].to_numpy(int),
            t_idx=np.where(is_t)[0], t_motor=df.loc[is_t, "motor_num"].to_numpy(int),
            motors=np.unique(df.loc[is_t, "motor_num"].to_numpy(int)),
            gt_motor=gt_motor, gt_score=gt_score, label=gt_file,
        ))
    return out


def build_table(gamma: np.ndarray, amp: np.ndarray) -> np.ndarray:
    """(6行, 7列) スコア表。列6 はペナルティ枠(別途埋める)。"""
    u = (5 - np.arange(6)) / 5.0        # 1着=1.0 ... 6着=0.0
    return amp[:, None] * (u[None, :] ** gamma[:, None])


def evaluate(t: Target, *, lane_on, half_life, shrink_k, penalty, n_sessions,
             gamma, amp) -> tuple[dict, float]:
    """1 ターゲットの (機番→pt dict, Spearman) を返す。"""
    tbl = build_table(np.asarray(gamma, float), np.asarray(amp, float))
    raw_lookup = np.concatenate([tbl, np.full((6, 1), float(penalty))], axis=1)
    sess_ok = t.p_sess < n_sessions
    raw = raw_lookup[t.p_row, t.p_fin]

    if lane_on:
        # baseline: (row, lane) セル、lane>=1、min5、sd floor
        cell = t.p_row * 7 + t.p_lane
        ok = sess_ok & (t.p_lane >= 1)
        n_c = np.bincount(cell[ok], minlength=42)
        s_c = np.bincount(cell[ok], weights=raw[ok], minlength=42)
        s2_c = np.bincount(cell[ok], weights=raw[ok] ** 2, minlength=42)
        with np.errstate(invalid="ignore", divide="ignore"):
            mu_c = s_c / n_c
            sd_c = np.sqrt(np.maximum(s2_c / n_c - mu_c ** 2, 0.0))
        sd_c = np.maximum(sd_c, SD_FLOOR)
        # (row) フォールバック
        n_r = np.bincount(t.p_row[sess_ok], minlength=6)
        s_r = np.bincount(t.p_row[sess_ok], weights=raw[sess_ok], minlength=6)
        s2_r = np.bincount(t.p_row[sess_ok], weights=raw[sess_ok] ** 2, minlength=6)
        with np.errstate(invalid="ignore", divide="ignore"):
            mu_r = s_r / n_r
            sd_r = np.sqrt(np.maximum(s2_r / n_r - mu_r ** 2, 0.0))
        sd_r = np.maximum(sd_r, SD_FLOOR)
        cell_valid = n_c >= MIN_SAMPLES
        row_valid = n_r >= MIN_SAMPLES
        mu = np.where(cell_valid[cell], mu_c[cell],
                      np.where(row_valid[t.p_row], mu_r[t.p_row], 0.0))
        sd = np.where(cell_valid[cell], sd_c[cell],
                      np.where(row_valid[t.p_row], sd_r[t.p_row], 1.0))
        # lane==0 の走はセル無効 → row フォールバック(cell_valid[cell] は lane0 セルが
        # 集計対象外なので n=0 → False になり自動的に row へ落ちる)
        resid = (raw - mu) / sd
    else:
        resid = raw

    if half_life is None:
        w = np.ones(len(raw))
    else:
        lam = math.log(2) / float(half_life)
        w = np.exp(-lam * t.p_days)

    ti = t.t_idx[sess_ok[t.t_idx]]
    tm = t.t_motor[sess_ok[t.t_idx]]
    midx = np.searchsorted(t.motors, tm)
    nm = len(t.motors)
    sw = np.bincount(midx, weights=w[ti], minlength=nm)
    swr = np.bincount(midx, weights=(w * resid)[ti], minlength=nm)
    sw2 = np.bincount(midx, weights=(w ** 2)[ti], minlength=nm)
    with np.errstate(invalid="ignore", divide="ignore"):
        mean_r = swr / sw
        n_eff = sw ** 2 / sw2
    pt = np.where(sw > 0, n_eff / (n_eff + shrink_k) * mean_r if shrink_k > 0 else mean_r,
                  np.nan)
    pts = dict(zip(t.motors.tolist(), pt.tolist()))

    x, y = [], []
    for m, g in zip(t.gt_motor, t.gt_score):
        v = pts.get(int(m))
        if v is not None and v == v:
            x.append(v); y.append(g)
    if t.stadium == "14":
        # 鳴門: 上位10のみラベル → 非掲載機を 0 として全機で順位相関(打ち切り扱い)
        labeled = {int(m): g for m, g in zip(t.gt_motor, t.gt_score)}
        x, y = [], []
        for m in t.motors:
            v = pts.get(int(m))
            if v is None or v != v:
                continue
            x.append(v); y.append(labeled.get(int(m), 0.0))
    rho = spearmanr(x, y).correlation if len(x) >= 8 else float("nan")
    return pts, float(rho)


def objective(targets, **params) -> tuple[float, dict]:
    """加重平均 Spearman(重み=ラベル数; 鳴門は10)と場別内訳を返す。"""
    detail = {}
    num = den = 0.0
    for t in targets:
        _, rho = evaluate(t, **params)
        n = len(t.gt_motor)
        detail[t.stadium] = rho
        if rho == rho:
            num += n * rho
            den += n
    return (num / den if den else float("nan")), detail
