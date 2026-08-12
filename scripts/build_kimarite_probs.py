#!/usr/bin/env python3
"""決まり手セルの確率と「荒れ度」を日次 CSV に出力する。

`scripts/build_kimarite.py` が学習した係数 (`data/estimate/kimarite/tables/`) を
読み、対象日の各レースについて 32 クラスの確率と **荒れ度 = 1 − P(逃げ_1)** を出す。

出力: `data/estimate/kimarite/YYYY/MM/DD.csv`

設計は docs/design/ana_prediction.md (§14 決まり手の表示方式)。表示に使うのは:

* **荒れ度** — 校正が取れている (実測とのズレ 0.3pt)。これが主表示
* **決まり手の確率分布** — 全種類を並べて出す

**argmax は使わない。** 「このレースは○○が決まる」は条件付きでもベースレートを
超えないため (§14.2)。94.5% のレースで最有力が「逃げ」になり、意味のある予測に
ならない。

進入コースの扱いは index / suji と同じ規約:

* ``状態=daily``   … 枠なり + race_cards のみ (朝は展示前)
* ``状態=realtime`` … 展示進入 + preview/気象

意図的に sklearn 非依存 (係数 CSV から softmax を直接計算する)。
モデルを学習し直さずに推論だけ回せるようにするため。

Usage:
    python scripts/build_kimarite_probs.py --date 2026-08-12 --mode daily
    python scripts/build_kimarite_probs.py --date 2026-08-12 --mode realtime \
        --update-races 202608122301,202608122302
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent))

from boatrace.kimarite import (  # noqa: E402
    CELLS,
    NIGE_CELL,
    build_features,
    feature_names,
    race_round_of,
    read_by_race,
    stadium_of,
)

STATE_DAILY = "daily"
STATE_REALTIME = "realtime"
TABLES_DIR = Path("data") / "estimate" / "kimarite" / "tables"

HEADER = (
    ["レースコード", "レース日", "レース場コード", "レース回", "状態", "荒れ度"]
    + [f"P_{c}" for c in CELLS]
)


class CellModel:
    """係数 CSV から softmax を計算する軽量モデル (sklearn 非依存)。"""

    def __init__(self, median, center, scale, intercept, weights, names):
        self.median = median
        self.center = center
        self.scale = scale
        self.intercept = intercept
        self.weights = weights
        self.names = names

    @classmethod
    def load(cls, path: Path) -> "CellModel":
        if not path.exists():
            raise FileNotFoundError(
                f"{path} が見つかりません。\n"
                f"  python scripts/build_kimarite.py を先に実行してください"
            )
        median = center = scale = None
        intercept: list[float] = []
        weights: list[list[float]] = []
        classes: list[str] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            names = header[3:]
            for row in reader:
                kind, cls_name, b = row[0], row[1], row[2]
                vals = [float(v) for v in row[3:]]
                if kind == "median":
                    median = vals
                elif kind == "center":
                    center = vals
                elif kind == "scale":
                    scale = vals
                elif kind == "coef":
                    classes.append(cls_name)
                    intercept.append(float(b))
                    weights.append(vals)
        if median is None or center is None or scale is None:
            raise ValueError(f"{path}: median / center / scale 行が足りません")
        if tuple(classes) != CELLS:
            raise ValueError(
                f"{path}: クラス構成が boatrace.kimarite.CELLS と一致しません。\n"
                f"  係数 CSV が古い可能性があります。build_kimarite.py を再実行してください"
            )
        return cls(median, center, scale, intercept, weights, names)

    def predict(self, x: list[float]) -> list[float]:
        """特徴量 1 本 → 32 クラスの確率。"""
        z = [
            ((v if math.isfinite(v) else m) - c) / s
            for v, m, c, s in zip(x, self.median, self.center, self.scale)
        ]
        logits = [
            b + sum(w_i * z_i for w_i, z_i in zip(w, z))
            for b, w in zip(self.intercept, self.weights)
        ]
        hi = max(logits)
        exps = [math.exp(v - hi) for v in logits]
        total = sum(exps)
        return [e / total for e in exps]


def probs_csv_path(repo: Path, day: dt.date) -> Path:
    return (repo / "data" / "estimate" / "kimarite"
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")


def build_day(
    repo: Path, day: dt.date, state: str, race_codes: set[str] | None = None
) -> list[list]:
    model = CellModel.load(repo / TABLES_DIR / f"cell_coef_{state}.csv")
    expected = feature_names(state)
    if model.names != expected:
        raise ValueError(
            f"係数 CSV の特徴量が boatrace.kimarite.feature_names('{state}') と "
            f"一致しません ({len(model.names)} vs {len(expected)})。\n"
            f"  build_kimarite.py を再実行してください"
        )

    rel = Path(f"{day:%Y}") / f"{day:%m}" / f"{day:%d}.csv"
    cards = read_by_race(repo / "data" / "programs" / "race_cards" / rel)
    if not cards:
        return []
    stt = read_by_race(repo / "data" / "previews" / "stt" / rel) if state == STATE_REALTIME else {}
    tkz = read_by_race(repo / "data" / "previews" / "tkz" / rel) if state == STATE_REALTIME else {}
    sui = read_by_race(repo / "data" / "previews" / "sui" / rel) if state == STATE_REALTIME else {}

    nige = CELLS.index(NIGE_CELL)
    out: list[list] = []
    for code, card in sorted(cards.items()):
        if race_codes is not None and code not in race_codes:
            continue
        x = build_features(
            state, card, stt.get(code), tkz.get(code), sui.get(code),
            stadium_of(code), race_round_of(card.get("レース回")),
        )
        p = model.predict(x)
        out.append(
            [
                code,
                (card.get("レース日") or f"{day:%Y-%m-%d}").strip(),
                (card.get("レース場コード") or stadium_of(code)).strip(),
                (card.get("レース回") or "").strip(),
                state,
                f"{1.0 - p[nige]:.4f}",
            ]
            # 6 桁。32 クラスを 4 桁に丸めると合計が 1 から 0.2% ずれ、
            # 下流の log-loss 集計にも効く。
            + [f"{v:.6f}" for v in p]
        )
    return out


def read_existing(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    return rows[1:] if rows else []


def atomic_write(path: Path, rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            w.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def write_day(repo: Path, day: dt.date, state: str, race_codes: set[str] | None) -> int:
    """指定 状態 の行を upsert する。他の 状態 の行は保持する。"""
    new_rows = build_day(repo, day, state, race_codes)
    path = probs_csv_path(repo, day)
    kept = [
        r for r in read_existing(path)
        if len(r) > 4 and not (
            r[4] == state and (race_codes is None or r[0] in race_codes)
        )
    ]
    order = {STATE_DAILY: 0, STATE_REALTIME: 1}
    rows = sorted(kept + new_rows, key=lambda r: (str(r[0]), order.get(str(r[4]), 2)))
    atomic_write(path, rows)
    return len(new_rows)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--mode", choices=[STATE_DAILY, STATE_REALTIME], default=STATE_REALTIME)
    p.add_argument("--update-races", default=None,
                   help="カンマ区切りのレースコード。指定した行だけ upsert する")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    day = dt.date.fromisoformat(args.date)
    codes = (
        {c.strip() for c in args.update_races.split(",") if c.strip()}
        if args.update_races
        else None
    )
    n = write_day(repo, day, args.mode, codes)
    print(f"kimarite: wrote {n} 状態={args.mode} rows → {probs_csv_path(repo, day)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
