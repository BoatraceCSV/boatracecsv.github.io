"""スジ表の構成比較(設計書 §13 の A案)。

`scripts/build_suji_table.py` が出力したスジ表を読み、A案の買い目

    1着 = 1コース以外で 強さpt が最大の艇
    2-3着 = スジ表 P(R2,R3 | R1) の上位 5 ペア

をバックテストする。学習窓と収縮強度の候補を valid で比較し、本番構成を決める。

Usage:
    python notebooks/ana_prediction/suji_backtest.py --tables cfg1=/path/to/suji.csv ...
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import numpy as np

import dataset  # noqa: E402  (同ディレクトリ)

VALID_FROM, TEST_FROM = "2026-06-25", "2026-07-18"
TOP_K = 5
BET_UNIT_YEN = 100
RNG = np.random.default_rng(20260811)


POOLED = "00"


def load_suji_table(path: Path) -> dict[tuple[str, int], list[tuple[int, int]]]:
    """(場コード, 1着コース) → 確率降順の (2着, 3着) リスト。"""
    rows: dict[tuple[str, int], list[tuple[float, int, int]]] = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            key = (str(r.get("場コード", POOLED)), int(r["1着コース"]))
            rows[key].append(
                (float(r["確率"]), int(r["2着コース"]), int(r["3着コース"]))
            )
    # 確率降順。同値は (2着, 3着) の昇順で決定的に並べる
    return {
        k: [(a, b) for _, a, b in sorted(v, key=lambda x: (-x[0], x[1], x[2]))]
        for k, v in rows.items()
    }


def load_suji_probs(path: Path) -> dict[tuple[str, int], dict[tuple[int, int], float]]:
    """(場コード, 1着コース) → {(2着, 3着): 確率}。"""
    out: dict[tuple[str, int], dict[tuple[int, int], float]] = defaultdict(dict)
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            key = (str(r.get("場コード", POOLED)), int(r["1着コース"]))
            out[key][(int(r["2着コース"]), int(r["3着コース"]))] = float(r["確率"])
    return out


def stadium_of(race_code: str) -> str:
    """レースコード = YYYYMMDD + 場コード(2桁) + レース回(2桁)。"""
    return str(race_code)[8:10]


def conditional_logloss(panel, probs) -> float:
    """真の 1着コースを与えたときの P(2着, 3着 | 1着) の log-loss。

    1着の選び方(強さpt)から独立に **スジ表そのものの質**だけを測る指標。
    回収率より遥かに分散が小さく、窓長の比較に耐える(設計書 §13.3 と同じ理屈)。
    """
    fin = np.column_stack([panel[f"fin{p}"].to_numpy(dtype=int) for p in (1, 2, 3)])
    codes = panel["レースコード"].to_numpy()
    total, n = 0.0, 0
    for (c1, c2, c3), code in zip(fin, codes):
        st = stadium_of(code)
        cell = probs.get((st, int(c1))) or probs.get((POOLED, int(c1)), {})
        p = cell.get((int(c2), int(c3)))
        if p is None:
            continue
        total -= np.log(max(p, 1e-12))
        n += 1
    return total / n if n else float("nan")


def backtest(panel, table, top_k: int = TOP_K):
    """1 レース 1 行の (購入額, 払戻, 的中, 配当) を返す。"""
    strength = np.column_stack(
        [panel[f"strength_c{c}"].to_numpy(dtype=float) for c in range(1, 7)]
    )
    fin = np.column_stack([panel[f"fin{p}"].to_numpy(dtype=int) for p in (1, 2, 3)])
    payout = panel["payout"].to_numpy(dtype=float)
    codes = panel["レースコード"].to_numpy()

    # 1着コース = 1コース以外 (index 1..5) で 強さpt 最大
    outer = np.nan_to_num(strength[:, 1:], nan=-np.inf)
    first_course = outer.argmax(axis=1) + 2

    cost = np.zeros(len(panel))
    ret = np.zeros(len(panel))
    hit = np.zeros(len(panel), dtype=bool)
    for i in range(len(panel)):
        c1 = int(first_course[i])
        st = stadium_of(codes[i])
        picks = (table.get((st, c1)) or table.get((POOLED, c1), []))[:top_k]
        if not picks:
            continue
        cost[i] = len(picks) * BET_UNIT_YEN
        if (int(fin[i, 0]) == c1) and (int(fin[i, 1]), int(fin[i, 2])) in set(picks):
            hit[i] = True
            ret[i] = payout[i] if np.isfinite(payout[i]) else 0.0
    fired = cost > 0
    settled = fired & np.isfinite(payout)
    return cost[settled], ret[settled], hit[settled], payout[settled]


def summarize(name: str, cost, ret, hit, payout, n_boot: int = 4000) -> dict:
    n = len(cost)
    if n == 0:
        return {"name": name, "n": 0}
    roi = ret.sum() / cost.sum() * 100
    boots = np.empty(n_boot)
    for i in range(n_boot):
        idx = RNG.integers(0, n, n)
        boots[i] = ret[idx].sum() / cost[idx].sum() * 100
    lo, hi = np.percentile(boots, [2.5, 97.5])
    hits = payout[hit]
    return {
        "name": name,
        "n": n,
        "points": cost.mean() / BET_UNIT_YEN,
        "hit_rate": hit.mean() * 100,
        "roi": roi,
        "ci_lo": lo,
        "ci_hi": hi,
        "avg_payout": hits.mean() if len(hits) else 0.0,
        "big": int((hits >= 10000).sum()),
        "big_per_10k": (hits >= 10000).sum() / (cost.sum() / 10000),
    }


def print_rows(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    print(f"{'構成':22s} {'n':>6s} {'点数':>5s} {'的中率':>7s} {'回収率':>7s} "
          f"{'95%CI':>14s} {'平均配当':>8s} {'万舟':>5s} {'万舟/1万円':>10s}")
    for r in rows:
        if r["n"] == 0:
            print(f"{r['name']:22s} (該当なし)")
            continue
        print(f"{r['name']:22s} {r['n']:6d} {r['points']:5.1f} {r['hit_rate']:6.2f}% "
              f"{r['roi']:6.1f}% [{r['ci_lo']:5.0f},{r['ci_hi']:5.0f}] "
              f"{r['avg_payout']:8.0f} {r['big']:5d} {r['big_per_10k']:10.3f}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--tables", nargs="+", required=True,
                   help="name=path 形式のスジ表 (複数可)")
    p.add_argument("--top-k", type=int, default=TOP_K)
    args = p.parse_args()

    panel = dataset.load_panel(with_strength=True)
    valid = panel[(panel["レース日"] >= VALID_FROM) & (panel["レース日"] < TEST_FROM)]
    test = panel[panel["レース日"] >= TEST_FROM]
    print(f"panel={len(panel)} valid={len(valid)} test={len(test)}")

    valid_rows, test_rows, ll_rows = [], [], []
    for spec in args.tables:
        name, _, path = spec.partition("=")
        table = load_suji_table(Path(path))
        probs = load_suji_probs(Path(path))
        valid_rows.append(summarize(name, *backtest(valid, table, args.top_k)))
        test_rows.append(summarize(name, *backtest(test, table, args.top_k)))
        ll_rows.append(
            (name, conditional_logloss(valid, probs), conditional_logloss(test, probs))
        )

    # 一様分布 (残り 5 コースから 2 つの順列 = 20 通り) が上限の目安
    print("\n=== P(2着,3着 | 真の1着) の log-loss — 構成の選定はこれで行う ===")
    print(f"{'構成':22s} {'valid':>8s} {'test':>8s}   (一様 = {np.log(20):.4f})")
    for name, lv, lt in ll_rows:
        print(f"{name:22s} {lv:8.4f} {lt:8.4f}")

    print_rows("valid (2026-06-25〜07-17) — 参考 (回収率は分散が大きく選定に使えない)",
               valid_rows)
    print_rows("test (2026-07-18〜08-11) — 参考", test_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
