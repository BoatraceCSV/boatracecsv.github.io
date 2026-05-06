"""Unit tests for the bc_rs1_2 -> CSV realtime result pipeline.

Fixtures are real TSV bodies sampled from race.boatcast.jp on 2026-05-05.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from boatrace.result_realtime import (
    RESULT_HEADERS,
    append_rows,
    build_result_row,
    csv_path_for,
    existing_race_codes,
    parse_rs1_2,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Kiryu (jo=01) Race 01 on 2026-05-05 — normal finish, no F.
SAMPLE_RS_NORMAL = (
    "1\t\t.18\t2\t\t.20\t3\t\t.18\t4\t\t.29\t5\t\t.27\t6\t\t.29\n"
    "１\t3\t鳥居塚　孝博\t1'49\"3\tまくり差し\n"
    "２\t1\t今泉　　　徹\t1'51\"3\t\n"
    "３\t5\t田中　　　堅\t1'53\"0\t\n"
    "４\t4\t外崎　　　悟\t1'54\"2\t\n"
    "５\t2\t川口　　貴久\t1'54\"2\t\n"
    "６\t6\t植竹　　玲奈\t1'55\"3\t\n"
    "1522\t1\t3\t東　　(向い風)\t4\t+21.0\t+13.0\n"
)


# Toda (jo=02) Race 08 on 2026-05-05 — boat 5 flying, boat 3/5 finish but no time.
SAMPLE_RS_FLYING = (
    "2\t\t.08\t1\t\t.14\t6\t\t.05\t3\t\t.02\t4\t\t.03\t5\tF\t.01\n"
    "１\t6\t秋元　　　哲\t1'46\"7\tまくり\n"
    "２\t4\t中澤　咲忍\t1'50\"6\t\n"
    "３\t2\t中里　昌志\t1'51\"9\t\n"
    "４\t1\t小巽　晴光\t1'52\"4\t\n"
    "５\t3\t野田　昌宏\t\t\n"
    "６\t5\t関口　智之\t\t\n"
    "1419\t1\t0\t無風　　(無 風)\t0\t+21.0\t+19.0\n"
)


# ---------------------------------------------------------------------------
# parse_rs1_2
# ---------------------------------------------------------------------------


class TestParseRs12:
    def test_normal_race(self):
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        assert result is not None
        assert result.is_complete

        # Finishes: 1着..6着 in rank order.
        ranks = [f.rank for f in result.finishes]
        assert ranks == [1, 2, 3, 4, 5, 6]
        assert result.finishes[0].boat_number == 3
        assert result.finishes[0].racer_name == "鳥居塚　孝博"
        assert result.finishes[0].race_time == "1'49\"3"
        assert result.finishes[0].kimari_te == "まくり差し"
        assert result.kimari_te == "まくり差し"
        assert result.finishes[5].boat_number == 6

        # Course entries: progressive 進入 (1コース is the first triplet).
        assert [c.course_number for c in result.courses] == [1, 2, 3, 4, 5, 6]
        assert result.courses[0].boat_number == 1
        assert result.courses[0].is_flying is False
        assert result.courses[0].start_timing == pytest.approx(0.18)
        assert result.courses[3].boat_number == 4
        assert result.courses[3].start_timing == pytest.approx(0.29)

        # Weather.
        assert result.weather.observed_at == "1522"
        assert result.weather.weather == 1
        assert result.weather.wave_height == 3.0
        assert result.weather.wind_direction == 3  # 東
        assert result.weather.wind_speed == 4.0
        assert result.weather.air_temperature == 21.0
        assert result.weather.water_temperature == 13.0

    def test_flying_race(self):
        result = parse_rs1_2(SAMPLE_RS_FLYING)
        assert result is not None
        assert result.is_complete

        # 進入: 2,1,6,3,4,5(F)
        boats = [c.boat_number for c in result.courses]
        assert boats == [2, 1, 6, 3, 4, 5]
        assert result.courses[5].is_flying is True
        # F-flagged ST is rendered negative (consistent w/ preview pipeline).
        assert result.courses[5].start_timing == pytest.approx(-0.01)
        # Non-F entries unchanged.
        assert result.courses[0].is_flying is False
        assert result.courses[0].start_timing == pytest.approx(0.08)

        # F'd boats (or 失格扱い) have empty レースタイム but still appear.
        last_two = result.finishes[-2:]
        assert last_two[0].rank == 5
        assert last_two[0].race_time == ""
        assert last_two[1].rank == 6
        assert last_two[1].race_time == ""

    def test_empty_body_returns_none(self):
        assert parse_rs1_2("") is None
        assert parse_rs1_2("\n\n") is None

    def test_only_st_line_is_incomplete(self):
        body = (
            "1\t\t.18\t2\t\t.20\t3\t\t.18\t4\t\t.29\t5\t\t.27\t6\t\t.29\n"
        )
        result = parse_rs1_2(body)
        assert result is not None
        assert not result.is_complete

    def test_weather_line_independent_of_position(self):
        # The weather row is detected by its leading 4-digit time
        # regardless of where it appears. Place it before placement rows
        # (defensive).
        body = (
            "1\t\t.18\t2\t\t.20\t3\t\t.18\t4\t\t.29\t5\t\t.27\t6\t\t.29\n"
            "1522\t1\t3\t東　　(向い風)\t4\t+21.0\t+13.0\n"
            "１\t3\t鳥居塚　孝博\t1'49\"3\tまくり差し\n"
        )
        result = parse_rs1_2(body)
        assert result is not None
        assert result.weather.observed_at == "1522"
        assert result.weather.weather == 1


# ---------------------------------------------------------------------------
# build_result_row
# ---------------------------------------------------------------------------


class TestBuildResultRow:
    def test_columns_match_headers(self):
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        assert result is not None
        row = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:03+09:00",
            result=result,
        )
        assert len(row) == len(RESULT_HEADERS)

    def test_common_identifiers(self):
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        row = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:03+09:00",
            result=result,
        )
        assert row[0] == "202605050101"
        assert row[1] == "2026-05-05"
        assert row[2] == "01"
        assert row[3] == "01R"
        assert row[4] == "15:18"
        assert row[5] == "2026-05-05T15:25:03+09:00"

    def test_kimari_te_and_first_place(self):
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        row = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:03+09:00",
            result=result,
        )
        # Map header -> value for sanity.
        cells = dict(zip(RESULT_HEADERS, row))
        assert cells["決まり手"] == "まくり差し"
        assert cells["1着_艇番"] == "3"
        assert cells["1着_選手名"] == "鳥居塚　孝博"
        assert cells["1着_レースタイム"] == "1'49\"3"
        assert cells["6着_艇番"] == "6"

    def test_flying_marker_in_course_columns(self):
        result = parse_rs1_2(SAMPLE_RS_FLYING)
        row = build_result_row(
            race_code="202605050208",
            date_str="2026-05-05",
            stadium_code=2,
            race_number=8,
            deadline_time="14:13",
            fetched_at_iso="2026-05-05T14:20:00+09:00",
            result=result,
        )
        cells = dict(zip(RESULT_HEADERS, row))
        # 6コース slot holds the F-marked entry (boat 5, F, ST -.01).
        assert cells["6コース_艇番"] == "5"
        assert cells["6コース_F"] == "F"
        assert cells["6コース_スタートタイミング"] == "-0.01"
        # Non-F slot is blank in the F column.
        assert cells["1コース_F"] == ""


# ---------------------------------------------------------------------------
# append_rows / existing_race_codes
# ---------------------------------------------------------------------------


class TestAppendRoundtrip:
    def test_first_write_creates_header(self, tmp_path: Path):
        path = tmp_path / "26" / "05" / "05.csv"
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        row = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:03+09:00",
            result=result,
        )

        n = append_rows(path, RESULT_HEADERS, [row])
        assert n == 1
        text = path.read_text(encoding="utf-8")
        # Header on first line, data on second.
        lines = text.splitlines()
        assert lines[0].split(",")[0] == "レースコード"
        assert lines[1].startswith("202605050101,")

    def test_subsequent_appends_skip_header(self, tmp_path: Path):
        path = tmp_path / "26.csv"
        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        row1 = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:00+09:00",
            result=result,
        )
        row2 = build_result_row(
            race_code="202605050201",
            date_str="2026-05-05",
            stadium_code=2,
            race_number=1,
            deadline_time="14:13",
            fetched_at_iso="2026-05-05T14:20:00+09:00",
            result=result,
        )

        append_rows(path, RESULT_HEADERS, [row1])
        append_rows(path, RESULT_HEADERS, [row2])

        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        # 1 header + 2 data rows.
        assert len(lines) == 3
        assert lines[1].startswith("202605050101,")
        assert lines[2].startswith("202605050201,")

    def test_existing_race_codes(self, tmp_path: Path):
        path = tmp_path / "26.csv"
        assert existing_race_codes(path) == set()

        result = parse_rs1_2(SAMPLE_RS_NORMAL)
        row = build_result_row(
            race_code="202605050101",
            date_str="2026-05-05",
            stadium_code=1,
            race_number=1,
            deadline_time="15:18",
            fetched_at_iso="2026-05-05T15:25:00+09:00",
            result=result,
        )
        append_rows(path, RESULT_HEADERS, [row])
        assert existing_race_codes(path) == {"202605050101"}


class TestCsvPathFor:
    def test_path_layout(self, tmp_path: Path):
        path = csv_path_for(tmp_path, "2026-05-05")
        assert path == tmp_path / "data" / "results" / "realtime" / "2026" / "05" / "05.csv"
