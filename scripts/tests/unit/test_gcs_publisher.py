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
    # active 予想者ごとに 1 件ずつ ``index:{predictor_id}`` が挟まる
    # (v1_basic / v2_tenkai / v3_tenkai)。
    assert csv_types == [
        "title",
        "race_cards",
        "stt",
        "index:v1_basic",
        "index:v2_tenkai",
        "index:v3_tenkai",
        "results",
        "payouts",
    ]


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
