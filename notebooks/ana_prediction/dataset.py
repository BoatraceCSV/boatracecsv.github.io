"""穴予想 (v9_suji) 検証用のデータセット構築。

`data/` 配下の CSV を突き合わせて、レース 1 行 × 進入コース順の
検証用パネルを作る。設計は docs/design/ana_prediction.md。

進入コースは **展示進入 (previews/stt の 艇N_コース)** で統一する
(賭け時点で使えるのはこれだけ。設計書 §12.2)。stt が取れないレースは
枠なり (枠番 = コース) にフォールバックする。

出力する列:
    レースコード / レース日 / 決まり手
    boat_at_c1..6  … コース c に入った艇番
    strength_c1..6 … コース c に入った艇の 強さpt (v1_basic, 状態=realtime)
    fin1..3        … 1〜3 着の **コース** 番号
    payout         … 3連単 払戻金 (円)
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]

KIMARITE = {
    "逃　げ": "逃げ",
    "差　し": "差し",
    "まくり": "まくり",
    "まくり差し": "まくり差し",
    "抜　き": "抜き",
    "恵まれ": "恵まれ",
}


def _read_all(rel_glob: str) -> pd.DataFrame:
    files = sorted(glob.glob(str(REPO_ROOT / rel_glob)))
    if not files:
        return pd.DataFrame()
    return pd.concat(
        [pd.read_csv(f, dtype={"レースコード": str}) for f in files],
        ignore_index=True,
    )


def load_panel(with_strength: bool = True) -> pd.DataFrame:
    """検証用パネルを返す。

    ``with_strength=False`` なら `estimate/v1_basic` を読まない
    (強さpt が無い代わりに 2025-11 以降の全履歴が使える。スジ表の学習用)。
    """
    res = _read_all("data/results/realtime/*/*/*.csv").drop_duplicates(
        "レースコード", keep="last"
    )
    codes = set(res["レースコード"])

    stt = _read_all("data/previews/stt/*/*/*.csv")
    stt = stt[stt["レースコード"].isin(codes)].drop_duplicates("レースコード", keep="last")

    df = res.merge(stt, on="レースコード", how="left", suffixes=("", "_stt"))
    n = len(df)

    # --- 展示進入コース (stt) → コース c に入った艇番 ---
    course_of_boat = np.tile(np.arange(1, 7), (n, 1))  # 既定は枠なり
    for b in range(1, 7):
        col = f"艇{b}_コース"
        if col not in df.columns:
            continue
        v = pd.to_numeric(df[col], errors="coerce").to_numpy()
        ok = np.isfinite(v) & (v >= 1) & (v <= 6)
        course_of_boat[ok, b - 1] = v[ok].astype(int)

    boat_at = np.zeros((n, 6), dtype=int)
    rows = np.arange(n)
    boat_at[rows[:, None], course_of_boat - 1] = np.arange(1, 7)[None, :]
    valid = (boat_at > 0).all(axis=1)

    out = pd.DataFrame(
        {
            "レースコード": df["レースコード"].to_numpy(),
            "レース日": df["レース日"].to_numpy(),
            "決まり手": df["決まり手"].map(KIMARITE).to_numpy(),
        }
    )
    for c in range(1, 7):
        out[f"boat_at_c{c}"] = boat_at[:, c - 1]

    # --- 着順 (艇番) → コース ---
    finish_ok = np.ones(n, dtype=bool)
    for pos in (1, 2, 3):
        b = pd.to_numeric(df[f"{pos}着_艇番"], errors="coerce").to_numpy()
        ok = np.isfinite(b) & (b >= 1) & (b <= 6)
        finish_ok &= ok
        safe = np.where(ok, b, 1).astype(int)
        out[f"fin{pos}"] = course_of_boat[rows, safe - 1]

    distinct = (
        (out["fin1"] != out["fin2"])
        & (out["fin2"] != out["fin3"])
        & (out["fin1"] != out["fin3"])
    )
    keep = valid & finish_ok & distinct & out["決まり手"].notna().to_numpy()

    # --- 3連単 払戻 ---
    pay = _read_all("data/results/payouts/*/*/*.csv")
    if not pay.empty:
        pay = pay[pay["レースコード"].isin(codes)].drop_duplicates(
            "レースコード", keep="last"
        )
        pay_map = pd.to_numeric(pay["3連単_払戻金"], errors="coerce")
        pay_map.index = pay["レースコード"].to_numpy()
        out["payout"] = out["レースコード"].map(pay_map)
    else:
        out["payout"] = np.nan

    # --- 強さpt (v1_basic, 状態=realtime) ---
    if with_strength:
        idx = _read_all("data/estimate/v1_basic/*/*/*.csv")
        idx = idx[idx["状態"] == "realtime"].drop_duplicates("レースコード", keep="last")
        idx = idx.set_index("レースコード")
        have = out["レースコード"].isin(idx.index).to_numpy()
        strength_by_boat = np.full((n, 6), np.nan)
        codes_arr = out["レースコード"].to_numpy()
        for b in range(1, 7):
            col = f"{b}枠_強さpt"
            if col not in idx.columns:
                continue
            s = pd.to_numeric(idx[col], errors="coerce")
            strength_by_boat[have, b - 1] = s.reindex(codes_arr[have]).to_numpy()
        for c in range(1, 7):
            out[f"strength_c{c}"] = strength_by_boat[rows, boat_at[:, c - 1] - 1]
        keep &= have

    return out[keep].reset_index(drop=True)


if __name__ == "__main__":
    panel = load_panel(with_strength=False)
    print(f"strength 無し: {len(panel)} レース "
          f"({panel['レース日'].min()} 〜 {panel['レース日'].max()})")
    panel2 = load_panel(with_strength=True)
    print(f"strength 有り: {len(panel2)} レース "
          f"({panel2['レース日'].min()} 〜 {panel2['レース日'].max()})")
