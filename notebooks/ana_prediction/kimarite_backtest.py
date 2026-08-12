"""B案(`v10_kimarite`)の合成とブレンドを valid で選び直す。

設計書 §4.3 は次の式で 3連単 120 通りの確率を作る:

    P(c2,c3 | cell) ∝ tab[cell][c2][c3] · exp(γ·z_c2) · exp(γ·z_c3/2)
    P(c1,c2,c3)     = Σ_{cell: 1着=c1} P1[cell] · P(c2,c3 | cell)
    最終             = w · P(決まり手) + (1−w) · P(Plackett-Luce, β)

そこに書かれた **γ=1.0 / β=1.4 / w=0.8 は 28 クラス・実験窓(train 6,956 レース)**
で選んだ値。本番は **32 クラス・全履歴**なので、同じ手続きで選び直す。
`build_kimarite.py` の学習窓が全履歴になったのと同じ理由で、実験時の
ハイパラをそのまま持ち込むと構成が食い違う(設計書 §12.1 と同種の齟齬)。

選定は **valid の 3連単 log-loss** のみで行い、test は報告にだけ使う
(設計書 §11.3 / §13.3。回収率では 8 ヶ月かけないと差が出ない)。

Usage:
    python notebooks/ana_prediction/kimarite_backtest.py
    python notebooks/ana_prediction/kimarite_backtest.py --suji-table data/estimate/suji/tables/suji_table.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))

import build_kimarite as bk  # noqa: E402
import build_kimarite_pairs as bkp  # noqa: E402
import dataset  # noqa: E402
import suji_backtest as sb  # noqa: E402
from boatrace.kimarite import CELLS  # noqa: E402

VALID_FROM, TEST_FROM = "2026-06-25", "2026-07-18"
STATE = "realtime"
TOP_K = 5

# 探索範囲。設計書の値 (γ=1.0 / β=1.4 / w=0.8) を内側に含むように取る。
K_GRID = (50.0, 150.0, 300.0, 600.0, 1000.0, 2000.0)
GAMMA_GRID = (0.0, 0.25, 0.5, 0.75, 1.0, 1.5)
BETA_GRID = (0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.4, 2.8)
W_GRID = tuple(round(0.1 * i, 1) for i in range(11))

TRIPLES = [
    (a, b, c)
    for a in range(1, 7)
    for b in range(1, 7)
    for c in range(1, 7)
    if a != b and b != c and a != c
]  # 120 通り。コース番号で持つ (艇番への写像は買い目を作るときだけ)


def cells_by_first() -> list[list[int]]:
    """1着コース(1..6) → そのコースが 1着 になるセルの index。"""
    out: list[list[int]] = [[] for _ in range(7)]
    for i, cell in enumerate(CELLS):
        out[int(cell.rsplit("_", 1)[1])].append(i)
    return out


def pair_tab(counts: dict, k: float) -> np.ndarray:
    """`build_kimarite_pairs.build_rows` の出力を (32, 6, 6) の密行列にする。

    本番と同じ関数を通すことで、収縮の実装がここだけ違う事故を防ぐ。
    """
    tab = np.zeros((len(CELLS), 6, 6))
    index = {c: i for i, c in enumerate(CELLS)}
    for cell, c2, c3, _n, p in bkp.build_rows(counts, k):
        tab[index[cell], int(c2) - 1, int(c3) - 1] = float(p)
    return tab


def plackett_luce(z: np.ndarray, beta: float) -> np.ndarray:
    """強さpt の z 得点(コース順)→ 120 通りの確率。"""
    s = np.exp(beta * z)
    total = s.sum()
    out = np.empty(len(TRIPLES))
    for i, (a, b, c) in enumerate(TRIPLES):
        d1 = total
        d2 = total - s[a - 1]
        d3 = d2 - s[b - 1]
        out[i] = s[a - 1] / d1 * s[b - 1] / d2 * s[c - 1] / d3
    return out


def plackett_luce_true(data, beta: float) -> np.ndarray:
    """真の出目に対する PL 確率だけをまとめて計算する(全 120 通りは作らない)。"""
    s = np.exp(beta * data["z"])
    total = s.sum(axis=1)
    rows = np.arange(len(total))
    a, b, c = (np.array([TRIPLES[t][j] for t in data["true"]]) for j in range(3))
    sa, sb, sc = s[rows, a - 1], s[rows, b - 1], s[rows, c - 1]
    return sa / total * sb / (total - sa) * sc / (total - sa - sb)


def kimarite_probs(p1: np.ndarray, tab: np.ndarray, z: np.ndarray,
                   gamma: float, by_first: list[list[int]]) -> np.ndarray:
    """Stage1 の 32 クラス確率 × Stage2 のペア表 → 120 通りの確率。"""
    mod = np.exp(gamma * z)
    modulated = tab * np.outer(mod, np.exp(gamma * z / 2))
    norm = modulated.sum(axis=(1, 2))
    # tab が全 0 のセル (train に 1 度も出なかった) は 0 のまま置く。
    # 該当セルの P1 は捨てられるので、最後にレース単位で正規化する。
    safe = np.where(norm > 0, norm, 1.0)
    cond = modulated / safe[:, None, None]

    grid = np.zeros((6, 6, 6))
    for c1 in range(1, 7):
        idx = by_first[c1]
        if idx:
            grid[c1 - 1] = (p1[idx, None, None] * cond[idx]).sum(axis=0)
    out = np.array([grid[a - 1, b - 1, c - 1] for a, b, c in TRIPLES])
    s = out.sum()
    return out / s if s > 0 else np.full(len(TRIPLES), 1.0 / len(TRIPLES))


def build_panel(repo: Path):
    """Stage1 の特徴量と、着順・強さpt・払戻を レースコード で突き合わせる。

    **すべての行を保持する。** 強さpt (`data/estimate/v1_basic`) は 2026-05-01
    以降しか無いが、Stage1 の学習窓は本番と同じ**全履歴**でなければ意味が無い。
    強さpt が要るのは評価側 (PL とブレンド) だけなので、``has_strength`` で
    評価対象だけを絞る。
    """
    X_by_state, y, days, codes = bk.collect(repo, None, None)
    X = X_by_state[STATE]
    n = len(codes)

    panel = dataset.load_panel(with_strength=True)
    strength = np.column_stack(
        [panel[f"strength_c{c}"].to_numpy(dtype=float) for c in range(1, 7)]
    )
    ok = np.isfinite(strength).all(axis=1)
    panel, strength = panel[ok].reset_index(drop=True), strength[ok]

    pos = {c: i for i, c in enumerate(panel["レースコード"].to_numpy())}
    idx = np.array([pos.get(c, -1) for c in codes])
    has = idx >= 0
    take = np.where(has, idx, 0)  # has が False の行の中身は使わない

    fin = np.column_stack(
        [panel[f"fin{p}"].to_numpy(dtype=int) for p in (1, 2, 3)]
    )[take]
    triple_index = {t: i for i, t in enumerate(TRIPLES)}
    true_idx = np.array([triple_index[tuple(int(v) for v in row)] for row in fin])

    z = np.full((n, 6), np.nan)
    z[has] = (strength[take[has]] - 50.0) / 10.0

    return {
        "X": X,
        "y": y,
        "days": days,
        "codes": codes,
        "has_strength": has,
        "z": z,
        "true": true_idx,
        "rows": panel.iloc[take].reset_index(drop=True),
    }


def true_probs_kimarite(data, model, tab, gamma, by_first, mask) -> np.ndarray:
    """真の出目に対する決まり手モデルの確率(log-loss 用)。"""
    mu, center, scale, W, b = model
    P1 = bk.predict_proba(data["X"][mask], mu, center, scale, W, b)
    z = data["z"][mask]
    true = data["true"][mask]
    return np.array([
        kimarite_probs(P1[i], tab, z[i], gamma, by_first)[true[i]]
        for i in range(len(true))
    ])


def logloss(p: np.ndarray) -> float:
    return float(-np.log(np.clip(p, 1e-12, None)).mean())


def bet_stats(name, data, mask, probs_of_race, top_k=TOP_K, first_not_1=True):
    """「1着コース≠1 の確率上位 k 点」を 1 点 100 円で買う戦略の実測。"""
    rows = data["rows"].iloc[np.flatnonzero(mask)].reset_index(drop=True)
    payout = rows["payout"].to_numpy(dtype=float)
    true = data["true"][mask]
    # 判定はコース番号のまま行う (艇番への写像は買い目 CSV を作るときだけ必要)
    allowed = np.array([
        (not first_not_1) or a != 1 for a, _b, _c in TRIPLES
    ])

    cost = np.zeros(len(true))
    ret = np.zeros(len(true))
    hit = np.zeros(len(true), dtype=bool)
    for i in range(len(true)):
        p = np.where(allowed, probs_of_race[i], -1.0)
        picks = np.argsort(-p, kind="stable")[:top_k]
        cost[i] = len(picks) * sb.BET_UNIT_YEN
        if true[i] in picks:
            hit[i] = True
            ret[i] = payout[i] if np.isfinite(payout[i]) else 0.0
    settled = np.isfinite(payout)
    return sb.summarize(name, cost[settled], ret[settled], hit[settled],
                        payout[settled])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--suji-table", default="data/estimate/suji/tables/suji_table.csv",
                   help="A案との対比に使うスジ表。空文字で対比をスキップ")
    p.add_argument("--k-grid", type=float, nargs="+", default=list(K_GRID),
                   help="収縮強度の探索範囲。Stage2 側で決めた値に固定するとき使う")
    args = p.parse_args()
    k_grid = tuple(args.k_grid)
    repo = Path(args.repo_root).resolve()

    print("収集中 …")
    data = build_panel(repo)
    days = data["days"]
    has = data["has_strength"]
    # 学習は強さpt を要求しない (本番の Stage1 は全履歴で学習する)。
    # 評価は PL とブレンドに強さpt が要るので has で絞る。
    train = days < VALID_FROM
    valid = (days >= VALID_FROM) & (days < TEST_FROM) & has
    test = (days >= TEST_FROM) & has
    span = sorted(days)
    print(f"train={train.sum()} valid={valid.sum()} test={test.sum()} "
          f"({span[0]} 〜 {span[-1]})")

    print("Stage1 学習中 (train のみ) …")
    model = bk.fit(data["X"][train], data["y"][train])

    print("Stage2 集計中 (train のみ) …")
    counts, used = bkp.collect(repo, None, VALID_FROM)
    print(f"  ペア表の母数 {used} レース")
    by_first = cells_by_first()

    # --- Plackett-Luce は β のみに依存する。真の出目の確率だけ先に作る ---
    pl_true = {beta: plackett_luce_true(data, beta) for beta in BETA_GRID}

    # --- 決まり手モデルは (k, γ) に依存する ---
    kim_true: dict[tuple[float, float], np.ndarray] = {}
    for k in k_grid:
        tab = pair_tab(counts, k)
        for gamma in GAMMA_GRID:
            full = np.zeros(len(data["true"]))
            for mask in (valid, test):
                full[mask] = true_probs_kimarite(
                    data, model, tab, gamma, by_first, mask)
            kim_true[(k, gamma)] = full
        print(f"  k={k:g} 完了")

    # --- valid で (k, γ, β, w) を選ぶ ---
    best = None
    for (k, gamma), pk in kim_true.items():
        for beta, pp in pl_true.items():
            for w in W_GRID:
                ll = logloss(w * pk[valid] + (1 - w) * pp[valid])
                if best is None or ll < best[0]:
                    best = (ll, k, gamma, beta, w)
    ll_valid, k, gamma, beta, w = best
    print("\n=== valid で選ばれた構成 ===")
    print(f"  k={k:g}  γ={gamma}  β={beta}  w={w}   valid logloss={ll_valid:.4f}")
    print(f"  (設計書 §4.3 の暫定値: k=1000 / γ=1.0 / β=1.4 / w=0.8 — "
          f"28 クラス・実験窓で選定)")

    # 4 つを同時に valid の argmin で決めると valid に過適合しうる。
    # 各軸を 1 つずつ動かした感度を出し、平坦なら丸い値を採る判断材料にする。
    print("\n=== valid logloss の感度 (他の 3 つは上の値に固定) ===")
    axes = (
        ("k", k_grid, lambda v: logloss(
            w * kim_true[(v, gamma)][valid] + (1 - w) * pl_true[beta][valid])),
        ("γ", GAMMA_GRID, lambda v: logloss(
            w * kim_true[(k, v)][valid] + (1 - w) * pl_true[beta][valid])),
        ("β", BETA_GRID, lambda v: logloss(
            w * kim_true[(k, gamma)][valid] + (1 - w) * pl_true[v][valid])),
        ("w", W_GRID, lambda v: logloss(
            v * kim_true[(k, gamma)][valid] + (1 - v) * pl_true[beta][valid])),
    )
    for label, grid, evaluate in axes:
        cells = "  ".join(f"{v:g}:{evaluate(v):.4f}" for v in grid)
        print(f"  {label:2s} {cells}")

    best_pl = min(BETA_GRID, key=lambda v: logloss(pl_true[v][valid]))
    print(f"\n  PL 単体の最良 β = {best_pl} "
          f"(valid {logloss(pl_true[best_pl][valid]):.4f} / "
          f"test {logloss(pl_true[best_pl][test]):.4f})")

    pk, pp = kim_true[(k, gamma)], pl_true[beta]
    print("\n=== 3連単 log-loss (120 通り) ===")
    print(f"{'予測':34s} {'valid':>8s} {'test':>8s}")
    uniform = float(np.log(len(TRIPLES)))
    print(f"{'一様 (1/120)':34s} {uniform:8.4f} {uniform:8.4f}")
    for label, p in (
        (f"Plackett-Luce (β={best_pl}, PL 単体の最良)", pl_true[best_pl]),
        (f"Plackett-Luce (β={beta}, ブレンド用)", pp),
        (f"決まり手モデル単体 (k={k:g}, γ={gamma})", pk),
        (f"ブレンド (w={w})", w * pk + (1 - w) * pp),
    ):
        print(f"{label:34s} {logloss(p[valid]):8.4f} {logloss(p[test]):8.4f}")

    # 主判定 (§13.3) はこの差。レース単位で対応が取れるので paired bootstrap。
    d = (-np.log(np.clip(pl_true[best_pl][test], 1e-12, None))
         + np.log(np.clip((w * pk + (1 - w) * pp)[test], 1e-12, None)))
    boots = np.array([
        d[sb.RNG.integers(0, len(d), len(d))].mean() for _ in range(4000)
    ])
    lo, hi = np.percentile(boots, [2.5, 97.5])
    print(f"\n  test の改善 (PL単体 → ブレンド) = {d.mean():+.4f} nat "
          f"[{lo:+.4f}, {hi:+.4f}]  n={len(d)}")

    # --- 買い目の実測。点数の選定 (top5) は valid で、test は報告用 (§11.3) ---
    tab = pair_tab(counts, k)
    mu, center, scale, W, b = model
    table = sb.load_suji_table(repo / args.suji_table) if args.suji_table else None

    for title, mask in (("valid — 買い目ルールの選定はここで行う", valid),
                        ("test — 報告用。判定は log-loss で行う", test)):
        P1 = bk.predict_proba(data["X"][mask], mu, center, scale, W, b)
        z_masked = data["z"][mask]
        blended = np.array([
            w * kimarite_probs(P1[i], tab, z_masked[i], gamma, by_first)
            + (1 - w) * plackett_luce(z_masked[i], beta)
            for i in range(len(z_masked))
        ])
        rows = [
            bet_stats(f"B案 1着≠1 top{n}", data, mask, blended, top_k=n)
            for n in (3, 5, 8, 12)
        ]
        rows.append(bet_stats("(参考) 制限なし top5", data, mask, blended,
                              top_k=5, first_not_1=False))
        if table is not None:
            rows.append(sb.summarize(
                "A案 v9_suji (同一レース)",
                *sb.backtest(data["rows"].iloc[np.flatnonzero(mask)], table)))
        sb.print_rows(title, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
