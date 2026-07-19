"""Unit tests for the bc_mrireki (モーター履歴) pipeline.

Fixture rows are real lines from Mikuni (jo=10) ``bc_mrireki_20260712_10``
sampled on 2026-07-19 (64 motors x exactly 3 節 each in the full file).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from boatrace.converter import (
    MOTOR_HISTORY_HEADERS,
    motor_history_entry_to_row,
)
from boatrace.models import ScheduleEntry
from boatrace.motor_stats_scraper import parse_mrireki


def _load_motor_stats_module():
    path = Path(__file__).resolve().parents[2] / "motor-stats.py"
    spec = importlib.util.spec_from_file_location("motor_stats_cli", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["motor_stats_cli"] = module
    spec.loader.exec_module(module)
    return module


SAMPLE_MRIREKI = (
    "011\t2026/06/28～2026/07/01\t一般\tディアボート三国リニューアル１１周年記念競走\t"
    "森永　　　隆\t６４２　５４５２　　　　　　　　　　\n"
    "011\t2026/06/21～2026/06/24\t一般\t日本トーター杯\t松田　　真実\t"
    "６６６　６　　　　　　　　　　　　　\n"
    "013\t2026/07/07～2026/07/12\tＧⅢ\tマスターズリーグ第３戦三国レジェンドカップ\t"
    "田頭　　　実\t５２１１１２２　２１[５]　　　　　　　\n"
)


class TestParseMrireki:
    def test_entries(self):
        entries = parse_mrireki(SAMPLE_MRIREKI, 10, "20260712")
        assert entries is not None
        assert len(entries) == 3
        first = entries[0]
        assert first.stadium_code == "10"
        assert first.session_end_key == "2026-07-12"
        assert first.motor_number == 11
        assert first.start_date == "2026-06-28"
        assert first.end_date == "2026-07-01"
        assert first.grade == "一般"
        assert first.racer_name == "森永 隆"

    def test_finish_sequence_keeps_internal_separators(self):
        entries = parse_mrireki(SAMPLE_MRIREKI, 10, "20260712")
        # Trailing full-width padding stripped, internal 日区切り kept.
        assert entries[0].finish_sequence == "６４２　５４５２"
        # 優勝戦N着 marker preserved verbatim.
        assert entries[2].finish_sequence == "５２１１１２２　２１[５]"
        assert entries[2].grade == "ＧⅢ"

    def test_rejects_empty_and_malformed(self):
        assert parse_mrireki("", 10, "20260712") is None
        assert parse_mrireki("garbage\n", 10, "20260712") is None
        assert parse_mrireki(SAMPLE_MRIREKI, 10, "bad-key") is None

    def test_row_aligns_with_headers(self):
        entries = parse_mrireki(SAMPLE_MRIREKI, 10, "20260712")
        row = motor_history_entry_to_row(entries[0])
        assert len(row) == len(MOTOR_HISTORY_HEADERS)
        cell = dict(zip(MOTOR_HISTORY_HEADERS, row))
        assert cell["場コード"] == "10"
        assert cell["基準節終了日"] == "2026-07-12"
        assert cell["モーター番号"] == "11"
        assert cell["使用者名"] == "森永 隆"


class TestPreviousSessionEnd:
    def _entries(self):
        return [
            ScheduleEntry(
                stadium_code="10",
                start_date="2026-06-28",
                end_date="2026-07-01",
                grade="IP",
            ),
            ScheduleEntry(
                stadium_code="10",
                start_date="2026-07-07",
                end_date="2026-07-12",
                grade="G3",
            ),
            ScheduleEntry(
                stadium_code="10",
                start_date="2026-07-18",
                end_date="2026-07-22",
                grade="IP",
            ),
        ]

    def test_picks_latest_completed_session(self):
        mod = _load_motor_stats_module()

        class FakeScheduleScraper:
            def __init__(self, entries):
                self.entries = entries
                self.calls = []

            def scrape_stadium(self, year_month, stadium_code):
                self.calls.append(year_month)
                return self.entries

        scraper = FakeScheduleScraper(self._entries())
        # Mid-session on 07-19: previous 節 ended 07-12.
        assert mod._previous_session_end(scraper, "2026-07-19", 10) == "20260712"
        # On the final day of a 節 (end == today), it is not yet "completed".
        assert mod._previous_session_end(scraper, "2026-07-12", 10) == "20260701"

    def test_falls_back_to_previous_month(self):
        mod = _load_motor_stats_module()

        class FakeScheduleScraper:
            def __init__(self):
                self.calls = []

            def scrape_stadium(self, year_month, stadium_code):
                self.calls.append(year_month)
                if year_month == "202607":
                    return None
                return [
                    ScheduleEntry(
                        stadium_code="10",
                        start_date="2026-06-21",
                        end_date="2026-06-24",
                        grade="IP",
                    )
                ]

        scraper = FakeScheduleScraper()
        assert mod._previous_session_end(scraper, "2026-07-02", 10) == "20260624"
        assert scraper.calls == ["202607", "202606"]


class TestHistoryCsvIO:
    def test_append_and_dedupe(self, tmp_path):
        mod = _load_motor_stats_module()
        path = tmp_path / "2026" / "07" / "12.csv"
        entries = parse_mrireki(SAMPLE_MRIREKI, 10, "20260712")
        rows = [motor_history_entry_to_row(e) for e in entries]

        assert mod.recorded_stadiums(path) == set()
        written = mod.append_history_rows(path, rows)
        assert written == 3
        assert mod.recorded_stadiums(path) == {"10"}

        # Second stadium appends without rewriting the header.
        other = parse_mrireki(SAMPLE_MRIREKI, 3, "20260712")
        mod.append_history_rows(
            path, [motor_history_entry_to_row(e) for e in other]
        )
        content = path.read_text(encoding="utf-8").splitlines()
        assert content[0] == ",".join(MOTOR_HISTORY_HEADERS)
        assert len(content) == 1 + 6
        assert mod.recorded_stadiums(path) == {"10", "03"}
