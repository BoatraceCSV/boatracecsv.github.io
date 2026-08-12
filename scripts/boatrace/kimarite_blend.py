"""決まり手モデル (Stage1 × Stage2) と Plackett-Luce を合成して 3連単 120 通りの
確率を作る。穴予想 B案 `v10_kimarite` の中核。

設計は ``docs/design/ana_prediction.md`` §4.3:

    P(c2,c3 | cell) ∝ tab[cell][c2][c3] · exp(γ·z_c2) · exp(γ·z_c3/2)
    P(c1,c2,c3)     = Σ_{cell: 1着=c1} P1[cell] · P(c2,c3 | cell)
    最終             = w · P(決まり手) + (1−w) · P(Plackett-Luce, β)
    z_c = (コース c に入る艇の 強さpt − 50) / 10

γ / β / w は **valid (2026-06-25〜07-17, 3,696 レース) の 3連単 log-loss で選定**
(``notebooks/ana_prediction/kimarite_backtest.py``)。設計書に元々あった
γ=1.0 / β=1.4 / w=0.8 は 28 クラス・実験窓の値で、32 クラス・全履歴の本番構成では
選ばれない。選定記録は ``notebooks/ana_prediction/report.md``。

買い目 (``top_picks``) と log-loss 集計 (``build_kimarite_logloss.py``) が
**同じ分布**を見るように、合成はこのモジュールに集約する。意図的に stdlib のみ。
"""
from __future__ import annotations

import csv
import math
from pathlib import Path

from .kimarite import CELLS

# valid で選定した値 (notebooks/ana_prediction/report.md)。
# 感度は平坦で、γ は 0.25〜0.75 / β は 1.8〜2.8 / w は 0.6〜0.8 の範囲なら
# valid log-loss の差が 0.005 nat 以内。**動かすときは valid で選び直すこと。**
GAMMA = 0.5   # 強さpt による 2-3着の変調
BETA = 2.4    # Plackett-Luce の強さ倍率 (ブレンド成分としての最適値)
BLEND_W = 0.7  # 決まり手モデルの重み
TOP_K = 5      # 買い目の点数
# A/B の対照。**強さpt だけで作った 3連単分布**で、control (v1_basic) と A案
# (v9_suji) が持っている情報と等価。PL 単体としての最良 β は 1.4 で、
# ブレンド成分としての BETA (2.4) とは別物なので混同しないこと。
PL_BASELINE_BETA = 1.4
EXCLUDE_FIRST_COURSE = 1  # 1着に取らないコース (穴狙い)

# 3連単 120 通り。**コース番号**で持つ (艇番への写像は買い目を作るときだけ)。
TRIPLES: tuple[tuple[int, int, int], ...] = tuple(
    (a, b, c)
    for a in range(1, 7)
    for b in range(1, 7)
    for c in range(1, 7)
    if a != b and b != c and a != c
)

PAIR_TABLE_RELPATH = Path("data") / "estimate" / "kimarite" / "tables" / "pair_table.csv"


def first_course_of(cell: str) -> int:
    """セル名 (例 ``まくり差し_3``) → 1着コース。"""
    return int(cell.rsplit("_", 1)[1])


def load_pair_table(path: Path) -> dict[str, dict[tuple[int, int], float]]:
    """`build_kimarite_pairs.py` の出力を セル → {(2着, 3着): 確率} で返す。"""
    if not path.exists():
        raise FileNotFoundError(
            f"{path} が見つかりません。\n"
            f"  python scripts/build_kimarite_pairs.py を先に実行してください"
        )
    out: dict[str, dict[tuple[int, int], float]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out.setdefault(r["セル"], {})[
                (int(r["2着コース"]), int(r["3着コース"]))
            ] = float(r["確率"])
    missing = [c for c in CELLS if c not in out]
    if missing:
        raise ValueError(
            f"{path}: セルが足りません ({len(missing)} 個、例 {missing[0]})。\n"
            f"  ペア表が古い可能性があります。build_kimarite_pairs.py を再実行してください"
        )
    return out


def read_cell_probs(path: Path, state: str) -> dict[str, list[float]]:
    """`build_kimarite_probs.py` の日次 CSV → レースコード → 32 クラス確率。

    列は ``P_{セル}``。**ヘッダ名で引く**ので、CELLS の並びが変わっても壊れない。
    """
    if not path.exists():
        return {}
    out: dict[str, list[float]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if (row.get("状態") or "").strip() != state:
                continue
            code = (row.get("レースコード") or "").strip()
            if not code:
                continue
            try:
                out[code] = [float(row[f"P_{c}"]) for c in CELLS]
            except (KeyError, TypeError, ValueError):
                continue
    return out


def plackett_luce(z: list[float], beta: float = BETA) -> dict[tuple[int, int, int], float]:
    """強さpt の z 得点(コース 1..6 の順)→ 120 通りの確率。"""
    s = [math.exp(beta * v) for v in z]
    total = sum(s)
    out: dict[tuple[int, int, int], float] = {}
    for a, b, c in TRIPLES:
        d2 = total - s[a - 1]
        d3 = d2 - s[b - 1]
        out[(a, b, c)] = s[a - 1] / total * s[b - 1] / d2 * s[c - 1] / d3
    return out


def kimarite_joint(
    p1: list[float],
    tab: dict[str, dict[tuple[int, int], float]],
    z: list[float],
    gamma: float = GAMMA,
) -> dict[tuple[int, int, int], float]:
    """Stage1 の 32 クラス確率 × Stage2 のペア表 → 120 通りの確率。"""
    mod2 = [math.exp(gamma * v) for v in z]
    mod3 = [math.exp(gamma * v / 2) for v in z]

    joint = {t: 0.0 for t in TRIPLES}
    for i, cell in enumerate(CELLS):
        weight = p1[i]
        if weight <= 0.0:
            continue
        c1 = first_course_of(cell)
        raw = {
            (c2, c3): p * mod2[c2 - 1] * mod3[c3 - 1]
            for (c2, c3), p in tab[cell].items()
        }
        s = sum(raw.values())
        if s <= 0.0:
            # ペア表が全 0 のセル (学習窓に 1 度も出なかった)。この P1 は捨て、
            # 最後のレース単位の正規化で他のセルに配分される。
            continue
        for (c2, c3), v in raw.items():
            joint[(c1, c2, c3)] += weight * v / s

    total = sum(joint.values())
    if total <= 0.0:
        return {t: 1.0 / len(TRIPLES) for t in TRIPLES}
    return {t: v / total for t, v in joint.items()}


def blend(
    p1: list[float],
    tab: dict[str, dict[tuple[int, int], float]],
    z: list[float],
    gamma: float = GAMMA,
    beta: float = BETA,
    w: float = BLEND_W,
) -> dict[tuple[int, int, int], float]:
    """最終確率。`w · 決まり手モデル + (1−w) · Plackett-Luce`。"""
    kim = kimarite_joint(p1, tab, z, gamma)
    pl = plackett_luce(z, beta)
    return {t: w * kim[t] + (1.0 - w) * pl[t] for t in TRIPLES}


def top_picks(
    probs: dict[tuple[int, int, int], float],
    top_k: int = TOP_K,
    exclude_first_course: int | None = EXCLUDE_FIRST_COURSE,
) -> list[tuple[int, int, int]]:
    """確率の上位 ``top_k`` 点。``exclude_first_course`` を 1着 に持つ出目は除く。

    穴予想なので既定では **1コース頭を買わない**(設計書 §5.2)。
    同確率の並びは出目の昇順で決定的にする(再実行の冪等性のため)。
    """
    items = [
        (t, p) for t, p in probs.items()
        if exclude_first_course is None or t[0] != exclude_first_course
    ]
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return [t for t, _ in items[:top_k]]


def z_scores(strength_by_boat: list[float], boat_at: list[int]) -> list[float]:
    """コース 1..6 に入る艇の 強さpt → z 得点。``boat_at[c]`` は コース c の艇番。"""
    return [(strength_by_boat[boat_at[c] - 1] - 50.0) / 10.0 for c in range(1, 7)]
