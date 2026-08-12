#!/usr/bin/env python3
"""決まり手セルの多項ロジスティック回帰を学習し、係数 CSV を出力する。

穴予想の Stage1。主目的は **荒れ度 = 1 − P(逃げ_1)** を出すこと
(設計は docs/design/ana_prediction.md §12.8 / §14)。決まり手そのものを
レース単位で当てることはできていないので、argmax は表示に使わない。

出力:
    data/estimate/kimarite/tables/cell_coef_daily.csv     朝バッチ用 (race_cards のみ)
    data/estimate/kimarite/tables/cell_coef_realtime.csv  直前バッチ用 (preview 込み)

いずれも 1 行 = 1 クラス で、標準化パラメータ (mu / sigma) と係数を持つ。
推論側は sklearn を使わず、この CSV だけで softmax を計算できる
(scripts/build_kimarite_probs.py)。

構成は検証で確定済み (docs/design/ana_prediction.md §12):
    学習窓 = 全履歴 / クラス = 凍結 32 個 / 場・天候 one-hot / 風向 sin-cos /
    欠損 = 全体 median / C = 0.006

Usage:
    python scripts/build_kimarite.py                       # 全履歴で学習
    python scripts/build_kimarite.py --to-date 2026-06-25  # 窓を切る (検証用)
    python scripts/build_kimarite.py --report              # ホールドアウト評価も出す
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent))

from boatrace.kimarite import (  # noqa: E402
    CELLS,
    CELL_INDEX,
    KIMARITE_MAP,
    build_features,
    cell_of,
    entry_courses,
    feature_names,
    race_round_of,
    read_by_race,
    stadium_of,
)

C_REGULARIZATION = 0.006
# 静的な係数は日次 CSV (data/estimate/kimarite/YYYY/MM/DD.csv) と別ディレクトリ。
# 同階層に置くと sparse-checkout (cone) が日次の全履歴まで引くため。
OUT_DIR = Path("data") / "estimate" / "kimarite" / "tables"
STATES = ("daily", "realtime")


def collect(repo: Path, from_date: str | None, to_date: str | None):
    """(state → 特徴量行列, ラベル, レース日, レースコード) を返す。

    ``to_date`` は **未満**(学習窓から検証期間を落とすときに端が重ならないよう)。

    レースコードは学習では使わないが、検証スクリプト
    (``notebooks/ana_prediction/kimarite_backtest.py``)が他の CSV と
    突き合わせるために要る。
    """
    rows = {s: [] for s in STATES}
    labels: list[int] = []
    days: list[str] = []
    codes: list[str] = []

    for res_path in sorted(
        (repo / "data" / "results" / "realtime").glob("*/*/*.csv")
    ):
        rel = res_path.relative_to(repo / "data" / "results" / "realtime")
        cards = read_by_race(repo / "data" / "programs" / "race_cards" / rel)
        if not cards:
            continue
        stt = read_by_race(repo / "data" / "previews" / "stt" / rel)
        tkz = read_by_race(repo / "data" / "previews" / "tkz" / rel)
        sui = read_by_race(repo / "data" / "previews" / "sui" / rel)

        with open(res_path, newline="", encoding="utf-8") as fh:
            for res in csv.DictReader(fh):
                day = (res.get("レース日") or "").strip()
                if from_date and day < from_date:
                    continue
                if to_date and day >= to_date:
                    continue
                code = (res.get("レースコード") or "").strip()
                card = cards.get(code)
                if not card:
                    continue
                kim = KIMARITE_MAP.get((res.get("決まり手") or "").strip())
                if kim is None:
                    continue
                try:
                    winner = int(float(res.get("1着_艇番")))
                except (TypeError, ValueError):
                    continue
                if not 1 <= winner <= 6:
                    continue
                courses = entry_courses(stt.get(code))
                label = CELL_INDEX[cell_of(kim, courses[winner - 1])]

                stadium = stadium_of(code)
                rnd = race_round_of(res.get("レース回"))
                for state in STATES:
                    rows[state].append(
                        build_features(
                            state, card, stt.get(code), tkz.get(code),
                            sui.get(code), stadium, rnd,
                        )
                    )
                labels.append(label)
                days.append(day)
                codes.append(code)

    return (
        {s: np.array(rows[s], dtype=float) for s in STATES},
        np.array(labels, dtype=int),
        np.array(days),
        np.array(codes),
    )


def fit(X: np.ndarray, y: np.ndarray):
    """median 補完 → 標準化 → 多項ロジスティック回帰。(mu, sigma, W, b) を返す。"""
    from sklearn.linear_model import LogisticRegression

    mu = np.nanmedian(X, axis=0)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    Xf = np.where(np.isfinite(X), X, mu)
    center = Xf.mean(axis=0)
    scale = Xf.std(axis=0)
    # 定数列 (one-hot で 1 度も立たない場など) は 1 で割って 0 のままにする
    scale = np.where(scale > 1e-9, scale, 1.0)
    Z = (Xf - center) / scale

    model = LogisticRegression(C=C_REGULARIZATION, max_iter=4000)
    model.fit(Z, y)

    # train に出なかったクラスの行は 0 で埋め、全 32 クラス分の形に揃える
    W = np.zeros((len(CELLS), X.shape[1]))
    b = np.full(len(CELLS), -30.0)  # 出現しなかったクラスは確率ほぼ 0
    for i, cls in enumerate(model.classes_):
        W[cls] = model.coef_[i]
        b[cls] = model.intercept_[i]
    return mu, center, scale, W, b


def predict_proba(X, mu, center, scale, W, b) -> np.ndarray:
    """係数だけで softmax を計算する(推論側と同じ式。学習時の検算用)。"""
    Xf = np.where(np.isfinite(X), X, mu)
    Z = (Xf - center) / scale
    logits = Z @ W.T + b
    logits -= logits.max(axis=1, keepdims=True)
    e = np.exp(logits)
    return e / e.sum(axis=1, keepdims=True)


def atomic_write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def write_coefficients(path: Path, state: str, mu, center, scale, W, b) -> None:
    """1 行 = 1 クラス。先頭 3 行に補完値・標準化パラメータを置く。"""
    names = feature_names(state)
    header = ["行種別", "クラス", "切片"] + names
    rows: list[list] = [
        ["median", "", ""] + [f"{v:.6g}" for v in mu],
        ["center", "", ""] + [f"{v:.6g}" for v in center],
        ["scale", "", ""] + [f"{v:.6g}" for v in scale],
    ]
    for i, cls in enumerate(CELLS):
        rows.append(["coef", cls, f"{b[i]:.6g}"] + [f"{v:.6g}" for v in W[i]])
    atomic_write(path, header, rows)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--from-date", default=None, help="学習窓の開始 (YYYY-MM-DD, 以上)")
    p.add_argument("--to-date", default=None, help="学習窓の終了 (YYYY-MM-DD, 未満)")
    p.add_argument("--out-dir", default=None)
    p.add_argument("--report", action="store_true",
                   help="学習窓の末尾 20%% をホールドアウトにして log-loss を出す")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    out_dir = Path(args.out_dir) if args.out_dir else repo / OUT_DIR

    X_by_state, y, days, _codes = collect(repo, args.from_date, args.to_date)
    n = len(y)
    if n == 0:
        print("no races collected", file=sys.stderr)
        return 1
    print(f"races={n} ({days[0]} 〜 {sorted(days)[-1]}) classes={len(set(y.tolist()))}")

    base = np.bincount(y, minlength=len(CELLS)) / n

    for state in STATES:
        X = X_by_state[state]
        if args.report:
            # 末尾 20% をホールドアウト (時系列なので日付順でカット)
            order = np.argsort(days, kind="stable")
            split = int(len(order) * 0.8)
            tr, te = order[:split], order[split:]
            mu, center, scale, W, b = fit(X[tr], y[tr])
            P = predict_proba(X[te], mu, center, scale, W, b)
            ll = -np.log(np.clip(P[np.arange(len(te)), y[te]], 1e-12, None)).mean()
            btr = np.bincount(y[tr], minlength=len(CELLS)) / len(tr)
            lb = -np.log(np.clip(btr[y[te]], 1e-12, None)).mean()
            nige = CELL_INDEX["逃げ_1"]
            print(f"  [{state}] holdout n={len(te)} logloss={ll:.4f} "
                  f"base={lb:.4f} (改善 {lb - ll:.4f} nat) "
                  f"P(逃げ)平均={P[:, nige].mean():.3f} 実測={np.mean(y[te] == nige):.3f}")

        mu, center, scale, W, b = fit(X, y)
        path = out_dir / f"cell_coef_{state}.csv"
        write_coefficients(path, state, mu, center, scale, W, b)
        print(f"  wrote {path} ({len(CELLS)} classes x {X.shape[1]} features)")

    print(f"base rate P(逃げ_1) = {base[CELL_INDEX['逃げ_1']]:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
