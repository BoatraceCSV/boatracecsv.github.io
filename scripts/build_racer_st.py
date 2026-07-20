#!/usr/bin/env python3
"""
Build data/estimate/racer_st/YYYY/MM/DD.csv — 選手別 推定 ST (対象日の全レース分).

fun-site のスリット予想 / 1マーク予想が公表 全国平均ST の代わりに読む派生 CSV。
計算は Phase 2 で確定した M3 構成 (scripts/boatrace/racer_st.py に凍結):

    推定ST = shrunk_EWMA(実測ST履歴, 半減期30日, k=10) + コース補正(枠番) + F本数補正

状態ファイル data/estimate/racer_st/state.csv に選手別 EWMA 状態を永続化し、
日次実行では (前回基準日, 対象日) の結果 CSV だけを増分で取り込む。
同一対象日の再実行は冪等 (新規取り込み日が無ければ状態は変わらない)。

Usage:
    # 日次バッチ (state.csv を前日まで進めて当日ぶんを出力)
    python scripts/build_racer_st.py --date 2026-07-20

    # 初回 / 復旧: 全履歴 (data/results/realtime/**) から状態を再構築
    python scripts/build_racer_st.py --date 2026-07-20 --rebuild

--rebuild は結果 CSV と同日の race_cards CSV の全履歴が checkout されている
必要がある (ローカル実行を想定)。日次の Cloud Run ジョブは増分更新のみ行う。
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from boatrace.racer_st import (  # noqa: E402
    RacerStState,
    advance_state,
    build_day_estimates,
    load_state,
    output_path,
    save_state,
)

#: --rebuild 時の走査開始日 (results/realtime の収集開始日)
HISTORY_START = dt.date(2025, 11, 1)

#: 増分更新で許容する最大キャッチアップ日数。これを超えて基準日が古い場合は
#: 取りこぼしの可能性が高いため --rebuild を促して失敗させる。
MAX_CATCHUP_DAYS = 62


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="対象日 YYYY-MM-DD")
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="state.csv を無視して全履歴から再構築する (要 全履歴 checkout)",
    )
    parser.add_argument(
        "--repo",
        default=str(Path(__file__).resolve().parents[1]),
        help="リポジトリルート (default: このスクリプトの親の親)",
    )
    args = parser.parse_args()

    repo = Path(args.repo)
    day = dt.date.fromisoformat(args.date)

    if args.rebuild:
        state = RacerStState()
        processed, skipped = advance_state(repo, state, day, start_day=HISTORY_START)
        print(f"[build_racer_st] rebuilt state from {HISTORY_START} ({len(processed)} days)")
    else:
        state = load_state(repo)
        if state.base_day is None:
            print(
                "[build_racer_st] state.csv not found. Run once with --rebuild "
                "(requires full results/race_cards history).",
                file=sys.stderr,
            )
            return 1
        gap = (day - dt.timedelta(days=1) - state.base_day).days
        if gap > MAX_CATCHUP_DAYS:
            print(
                f"[build_racer_st] state base_day={state.base_day} is {gap} days old "
                f"(> {MAX_CATCHUP_DAYS}). Sparse checkout may miss the months needed "
                "for catch-up; run with --rebuild locally.",
                file=sys.stderr,
            )
            return 1
        processed, skipped = advance_state(repo, state, day)
        print(f"[build_racer_st] advanced state to {state.base_day} (+{len(processed)} days)")

    if skipped:
        print(
            "[build_racer_st] WARNING: skipped days without results/race_cards: "
            f"{[d.isoformat() for d in skipped]}"
        )

    estimates = build_day_estimates(repo, state, day)
    out = output_path(repo, day)
    out.parent.mkdir(parents=True, exist_ok=True)
    estimates.to_csv(out, index=False)
    print(f"[build_racer_st] wrote {out} ({len(estimates)} races, {len(state.racers)} racers in state)")

    save_state(repo, state)
    print(f"[build_racer_st] saved state (base_day={state.base_day})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
