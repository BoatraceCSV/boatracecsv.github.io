#!/usr/bin/env python3
"""
Build data/estimate/motor_pt/{runs,motors,baseline}/YYYY/MM/DD.csv — モーターpt 素点の内訳.

``build_index.py`` が ``N枠_モーターpt`` を出すときに内部で組み立てている
「素点の計算過程」を、そのまま CSV に書き出す派生データ。下流 (fun-site) の
モーターpt詳細ページが素点の内訳を表示するために読む。

素点そのものは

    素点 = n_eff / (n_eff + k) × Σ(w × z) / Σw

だが、この z (コース補正残差) が **全 24 場を横断したベースライン** に依存するため、
当日ぶんの CSV しか持たない下流では再現できない。選手pt に対する
``programs/recent_national`` と同じ立ち位置の「内訳を配る」CSV がこれにあたる。

出力 3 種 (スキーマは docs/data/motor_pt.md):

    runs/      1 走 1 行。素点に寄与した走の明細
    motors/    1 モーター 1 行。Σw / n_eff / 素点 の集計
    baseline/  コース補正セルの μ/σ/サンプル数

対象は当日の race_cards に出てくるモーターだけ (2026-08-22 で 567 基 / 約 2 万走)。

計算は ``boatrace.index_features.motor_ability_breakdown()`` 1 本に集約してあり、
``motor_ability_pt()`` はその ``raw_pt`` を返すだけのラッパなので、index CSV の
モーターpt と内訳が食い違うことはない。

Usage:
    python scripts/build_motor_pt_breakdown.py --date 2026-08-22
    python scripts/build_motor_pt_breakdown.py --date 2026-08-22 --force
    python scripts/build_motor_pt_breakdown.py --date 2026-08-22 --dry-run

既存ファイルがある場合は既定でスキップする (--force で上書き)。日次バッチは
``--force`` 付きで呼ぶ (当日の race_cards が後から埋まるケースがあるため)。
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.motor_pt_breakdown import (  # noqa: E402
    baseline_path,
    build_frames,
    motors_path,
    runs_path,
)


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """temp file + os.replace で書く (build_index.py と同じ手順)。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="対象日 YYYY-MM-DD")
    parser.add_argument(
        "--force", action="store_true",
        help="既存の出力があっても上書きする",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="行数だけ出して書き込まない",
    )
    parser.add_argument(
        "--repo",
        default=str(Path(__file__).resolve().parents[1]),
        help="リポジトリルート (default: このスクリプトの親の親)",
    )
    args = parser.parse_args()

    repo = Path(args.repo)
    day = dt.date.fromisoformat(args.date)

    outputs = {
        "runs": runs_path(repo, day),
        "motors": motors_path(repo, day),
        "baseline": baseline_path(repo, day),
    }
    if not args.force and not args.dry_run and all(p.exists() for p in outputs.values()):
        print(f"[build_motor_pt_breakdown] {day} already built (use --force to rebuild)")
        return 0

    runs_df, motors_df, baseline_df = build_frames(repo, day)

    if runs_df.empty and motors_df.empty:
        # race_cards が無い日 (開催なし / 未取得)。空ファイルを置くと下流が
        # 「取得済みだが履歴ゼロ」と誤読するので、何も書かずに正常終了する。
        print(f"[build_motor_pt_breakdown] no race_cards for {day} — nothing to write")
        return 0

    frames = {"runs": runs_df, "motors": motors_df, "baseline": baseline_df}
    if args.dry_run:
        for kind, df in frames.items():
            print(f"[build_motor_pt_breakdown] (dry-run) {kind}: {len(df)} rows → {outputs[kind]}")
        return 0

    for kind, df in frames.items():
        atomic_write_csv(df, outputs[kind])
        size_kb = outputs[kind].stat().st_size / 1024
        print(
            f"[build_motor_pt_breakdown] wrote {outputs[kind]} "
            f"({len(df)} rows, {size_kb:.0f}KB)"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
