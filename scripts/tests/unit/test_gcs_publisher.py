"""Unit tests for ``boatrace.gcs_publisher``.

These tests exercise the pure-Python pieces of the realtime fan-out
pipeline (CSV spec enumeration and ``updatedRaces`` payload assembly)
without touching GCS / Pub/Sub. The actual upload / publish helpers
short-circuit when ``BOATRACE_GCS_CSV_BUCKET`` / ``BOATRACE_PUBSUB_TOPIC``
are unset and are covered by integration tests instead.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from boatrace.gcs_publisher import (
    CsvUploadSpec,
    UploadResult,
    _build_csv_specs,
    assemble_updated_races,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


RACE_CARD_HEADERS = "レースコード,レース場コード,レース回"


def _write_race_cards(repo: Path, day: dt.date, rows: list[tuple[str, str, str]]) -> Path:
    """Create ``data/programs/race_cards/YYYY/MM/DD.csv`` populated with
    the given (race_code, stadium_id, race_number) triples."""
    ymd_path = repo / "data" / "programs" / "race_cards" / f"{day:%Y}" / f"{day:%m}"
    ymd_path.mkdir(parents=True, exist_ok=True)
    csv_path = ymd_path / f"{day:%d}.csv"
    lines = [RACE_CARD_HEADERS]
    for race_code, stadium_id, race_number in rows:
        lines.append(f"{race_code},{stadium_id},{race_number}")
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path


def _make_upload_results(*changed_csv_types: str) -> list[UploadResult]:
    """Build UploadResult fixtures with the specified csv_types marked
    changed=True. Always emits the full 5-spec list so the result mirrors
    what ``upload_csvs`` would return in production."""
    out: list[UploadResult] = []
    for spec in _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7)):
        out.append(
            UploadResult(spec=spec, changed=spec.csv_type in changed_csv_types)
        )
    return out


# ---------------------------------------------------------------------------
# _build_csv_specs
# ---------------------------------------------------------------------------


def test_build_csv_specs_includes_results():
    """``_build_csv_specs`` must include the realtime results and payouts
    CSVs so fun-site can read finished-race data via the GCS mirror.

    Index CSVs are now predictor-specific (``index:{predictor_id}``); one
    spec per active predictor is inserted between ``stt`` and ``results``.
    """
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    csv_types = [s.csv_type for s in specs]

    assert "results" in csv_types
    assert "payouts" in csv_types
    # active 予想者ごとに 1 件ずつ ``index:{predictor_id}`` が挟まる。
    # v2_tenkai / v3_tenkai は 2026-07-19 に、v6_course / v7_aggregate /
    # v8_aionly は 2026-08-09 に (control 比で有意に低回収率)、v4_motor /
    # v5_slit は 2026-08-10 に (control 比で有意差なし) 退役したため、
    # 現在の active は control (v1_basic) のみ。
    # racer_st は予想者非依存の固定 spec なので、v5_slit 退役後も出力し続ける。
    # kimarite / kimarite_picks も同じく予想者 ID を持たない固定 spec
    # (買い目そのものを配るため。docs/design/ana_prediction.md §13)。
    # suji (v9_suji の買い目) は 2026-08-22 の退役で生成が止まったため spec ごと
    # 外した。決まり手注釈テーブル (data/estimate/suji/tables/) は
    # v10_kimarite が使うので残っているが、GCS ミラー対象ではない。
    # 直前情報の残り (tkz / sui / original_exhibition / tokuten_hayami) と
    # daily-sync 系 (recent_national / recent_local / waku10 / motor_stats) は
    # 2026-08-12 に mirror 対象へ追加した (fun-site の直前情報・近況5節・
    # 枠番別過去10走・得点率早見セクションが GCS 経由でしか読めないため)。
    assert csv_types == [
        "title",
        "race_cards",
        "stt",
        "tkz",
        "sui",
        "original_exhibition",
        "tokuten_hayami",
        "recent_national",
        "recent_local",
        "waku10",
        "motor_stats",
        "racer_st",
        "kimarite",
        "kimarite_picks",
        "index:v1_basic",
        "index:v10_kimarite",
        "results",
        "payouts",
        # 日付パーティションを持たない静的テーブル。2026-08-22 に mirror 対象へ
        # 追加した (fun-site の枠番詳細ページが枠番pt の raw → z → 偏差値 → 寄与
        # を再現するのに、win_rate.csv と場別 μ/σ/w の両方を必要とするため)。
        # sui_params.csv も同じ理由で追加 (気象詳細ページが気象pt の
        # 特徴量 × 係数 → z → 偏差値 → 寄与 を再現する)。
        "waku_table",
        "sui_params",
        "weights:v1_basic",
        "weights:v10_kimarite",
    ]


def test_build_csv_specs_preview_and_program_paths():
    """新しく mirror 対象にした CSV のパスを固定する。"""
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert by_type["tkz"].repo_relative_path == "data/previews/tkz/2026/05/07.csv"
    assert by_type["sui"].repo_relative_path == "data/previews/sui/2026/05/07.csv"
    assert (
        by_type["original_exhibition"].repo_relative_path
        == "data/previews/original_exhibition/2026/05/07.csv"
    )
    assert (
        by_type["tokuten_hayami"].repo_relative_path
        == "data/previews/tokuten_hayami/2026/05/07.csv"
    )
    assert (
        by_type["recent_national"].repo_relative_path
        == "data/programs/recent_national/2026/05/07.csv"
    )
    assert (
        by_type["recent_local"].repo_relative_path
        == "data/programs/recent_local/2026/05/07.csv"
    )
    assert by_type["waku10"].repo_relative_path == "data/programs/waku10/2026/05/07.csv"
    assert (
        by_type["motor_stats"].repo_relative_path
        == "data/programs/motor_stats/2026/05/07.csv"
    )


def test_build_csv_specs_waku_table_path():
    """コース強度テーブルは日付を含まない固定パス。"""
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert (
        by_type["waku_table"].repo_relative_path
        == "data/estimate/stadium/win_rate.csv"
    )


def test_build_csv_specs_sui_params_path():
    """気象回帰係数テーブルも日付を含まない固定パス。"""
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert (
        by_type["sui_params"].repo_relative_path
        == "data/estimate/stadium/sui_params.csv"
    )


def test_build_csv_specs_weights_path_defaults_to_target_month(tmp_path):
    """weights ディレクトリが無い repo では対象月のパスを spec に残す
    (upload 側が local_file_missing でスキップする)。"""
    specs = _build_csv_specs(tmp_path, dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert (
        by_type["weights:v1_basic"].repo_relative_path
        == "data/estimate/stadium/weights/v1_basic/2026-05.csv"
    )


def test_build_csv_specs_weights_falls_back_to_latest_past_month(tmp_path):
    """当月ぶんが未生成なら、build_index が実際に読む直近の過去月を配る。"""
    weights_dir = tmp_path / "data" / "estimate" / "stadium" / "weights" / "v1_basic"
    weights_dir.mkdir(parents=True)
    (weights_dir / "2026-03.csv").write_text("stadium\n", encoding="utf-8")
    (weights_dir / "2026-04.csv").write_text("stadium\n", encoding="utf-8")
    # 対象月より後のファイルは選ばれない (過去日の再ビルドで未来の重みを使わない)。
    (weights_dir / "2026-06.csv").write_text("stadium\n", encoding="utf-8")

    specs = _build_csv_specs(tmp_path, dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert (
        by_type["weights:v1_basic"].repo_relative_path
        == "data/estimate/stadium/weights/v1_basic/2026-04.csv"
    )


def test_build_csv_specs_results_path():
    """results spec must point at ``data/results/realtime/YYYY/MM/DD.csv``."""
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert by_type["results"].repo_relative_path == "data/results/realtime/2026/05/07.csv"


def test_build_csv_specs_payouts_path():
    """payouts spec must point at ``data/results/payouts/YYYY/MM/DD.csv``."""
    specs = _build_csv_specs(Path("/tmp"), dt.date(2026, 5, 7))
    by_type = {s.csv_type: s for s in specs}

    assert by_type["payouts"].repo_relative_path == "data/results/payouts/2026/05/07.csv"


# ---------------------------------------------------------------------------
# assemble_updated_races: result_updated_codes 単独
# ---------------------------------------------------------------------------


def test_assemble_with_result_updated_codes_only(tmp_path):
    """結果のみが更新されたサイクル: realtime_updated_codes が空でも
    result_updated_codes でレースが列挙され、csvTypes に "results" が
    立つこと。これがないと preview-realtime の「結果だけ」サイクルで
    fun-site への通知が一切飛ばない（修正前の挙動）。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(
        tmp_path,
        day,
        [
            ("202605070101", "01", "01"),
            ("202605070102", "01", "02"),
        ],
    )

    upload_results = _make_upload_results("results")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=[],
        result_updated_codes=["202605070101"],
    )

    assert trigger == "realtime"
    assert len(updated) == 1
    entry = updated[0]
    assert entry.race_code == "202605070101"
    assert entry.stadium_id == "01"
    assert entry.race_number == 1
    assert entry.csv_types == {"results"}
    assert entry.index_state is None


def test_assemble_with_realtime_and_result_codes(tmp_path):
    """preview と結果の両方が来たレースは csvTypes に両方の種別が入る。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(
        tmp_path,
        day,
        [
            ("202605070101", "01", "01"),
            ("202605070102", "01", "02"),
            ("202605070201", "02", "01"),
        ],
    )

    upload_results = _make_upload_results("stt", "index:v1_basic", "results")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=["202605070101", "202605070201"],
        result_updated_codes=["202605070101", "202605070102"],
    )

    assert trigger == "realtime"
    by_code = {r.race_code: r for r in updated}

    # 102 は preview なし・結果ありなので results のみ
    assert by_code["202605070102"].csv_types == {"results"}
    # 201 は preview あり・結果なしなので stt/index:v1_basic
    assert by_code["202605070201"].csv_types == {"stt", "index:v1_basic"}
    assert by_code["202605070201"].index_state == "realtime"
    # 101 は両方ある
    assert by_code["202605070101"].csv_types == {"stt", "index:v1_basic", "results"}
    assert by_code["202605070101"].index_state == "realtime"


def test_assemble_results_changed_but_no_codes_yields_empty(tmp_path):
    """changed_types に "results" があっても、result_updated_codes が
    空ならその経由ではレースが立たない（=「結果ファイルは触ったが
    今サイクルで追記されたレースは無い」状況に対する no-op 挙動）。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(tmp_path, day, [("202605070101", "01", "01")])

    upload_results = _make_upload_results("results")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=[],
        result_updated_codes=[],
    )

    assert trigger == "realtime"
    assert updated == []


def test_assemble_with_payout_updated_codes_only(tmp_path):
    """払戻のみが更新されたサイクル: payout_updated_codes でレースが
    列挙され、csvTypes に "payouts" が立つこと。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(tmp_path, day, [("202605070101", "01", "01")])

    upload_results = _make_upload_results("payouts")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=[],
        result_updated_codes=[],
        payout_updated_codes=["202605070101"],
    )

    assert trigger == "realtime"
    assert len(updated) == 1
    entry = updated[0]
    assert entry.race_code == "202605070101"
    assert entry.csv_types == {"payouts"}
    assert entry.index_state is None


def test_assemble_with_result_and_payout_codes(tmp_path):
    """同じレースに対して結果と払戻の両方が来たら csvTypes に両方入る。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(tmp_path, day, [("202605070101", "01", "01")])

    upload_results = _make_upload_results("results", "payouts")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=[],
        result_updated_codes=["202605070101"],
        payout_updated_codes=["202605070101"],
    )

    assert trigger == "realtime"
    assert len(updated) == 1
    assert updated[0].csv_types == {"results", "payouts"}


# ---------------------------------------------------------------------------
# bootstrap path
# ---------------------------------------------------------------------------


def test_bootstrap_includes_results_csv_type(tmp_path):
    """daily-bootstrap (title/race_cards 変更) のサイクルでは、results
    も changed_types に含まれていれば全レースの csvTypes に追加される。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(
        tmp_path,
        day,
        [
            ("202605070101", "01", "01"),
            ("202605070201", "02", "01"),
        ],
    )

    upload_results = _make_upload_results("title", "race_cards", "results")
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=[],
        result_updated_codes=[],
    )

    assert trigger == "daily-bootstrap"
    assert len(updated) == 2
    for entry in updated:
        assert "results" in entry.csv_types
        assert "title" in entry.csv_types
        assert "race_cards" in entry.csv_types


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


def test_assemble_without_result_updated_codes_argument(tmp_path):
    """``result_updated_codes`` は optional。既存の呼び出し
    (preview のみを渡す) との後方互換を保つこと。"""
    day = dt.date(2026, 5, 7)
    _write_race_cards(tmp_path, day, [("202605070101", "01", "01")])

    upload_results = _make_upload_results("stt", "index:v1_basic")
    # Positional / keyword 両方の旧シグネチャで呼べることを確認
    updated, trigger = assemble_updated_races(
        tmp_path,
        day,
        upload_results,
        realtime_updated_codes=["202605070101"],
    )

    assert trigger == "realtime"
    assert len(updated) == 1
    assert updated[0].csv_types == {"stt", "index:v1_basic"}
