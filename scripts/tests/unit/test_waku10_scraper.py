"""Unit tests for the bc_j_waku10 (枠番別過去10走) parsing + CSV pipeline.

The fixture is the real TSV body for Heiwajima (jo=04) Race 1 on
2026-07-19, cross-checked cell by cell against the SPA's 枠番別過去10走
tab rendering (勝率/平均ST/スタート順 figures, run order 前走→10走前,
and the コース sub-row that only shows non-枠なり entries).
"""

from __future__ import annotations

from boatrace.converter import (
    WAKU10_HEADERS,
    waku10_to_csv,
    waku10_to_row,
)
from boatrace.race_card_scraper import RaceCardScraper


SAMPLE_WAKU10 = (
    "data=\n"
    "1\t0\n"
    "森作　　雄大\t7.30\t0.13\t2.1\t１\t\tIP\t６\t\tIP\t４\t\tIP\t２\t\tIP\t"
    "１\t\tIP\t２\t\tIP\t１\t\tIP\t３\t\tIP\t２\t\tG2\t３\t\tG2\n"
    "杢野　　誓良\t3.60\t0.15\t2.9\t６\t\tIP\t３\t\tIP\t３\t\tIP\t４\t\tIP\t"
    "６\t\tIP\t３\t\tIP\t６\t3\tIP\t６\t\tIP\t３\t\tIP\t４\t\tIP\n"
    "富澤　　祐作\t5.30\t0.13\t1.7\t３\t\tIP\t６\t\tIP\tＦ\t\tIP\t４\t\tIP\t"
    "１\t\tIP\t２\t\tIP\t１\t\tIP\t４\t\tIP\t３\t\tIP\t４\t\tIP\n"
    "片岡　　秀樹\t4.60\t0.13\t2.2\t２\t\tIP\t６\t5\tIP\t２\t\tIP\t４\t\tIP\t"
    "３\t\tIP\t６\t\tIP\t３\t\tIP\t４\t\tIP\t３\t\tIP\t５\t\tIP\n"
    "関　　　浩哉\t6.20\t0.13\t2.9\t３\t\tG2\t３\t\tG2\t５\t6\tSG\t４\t\tSG\t"
    "５\t\tG1\t２\t\tSG\t３\t\tG1\t５\t\tG1\t２\t\tIP\t３\t\tG1\n"
    "近藤　　稔也\t2.10\t0.21\t5.1\t４\t\tIP\t５\t\tIP\t６\t\tIP\t６\t\tIP\t"
    "４\t5\tIP\t５\t\tIP\t６\t\tIP\t６\t\tIP\t４\t\tIP\t６\t\tIP\n"
)


def _parse_sample():
    scraper = RaceCardScraper()
    return scraper._parse_waku10_tsv(SAMPLE_WAKU10, "2026-07-19", 4, 1)


class TestParseWaku10:
    def test_card_meta(self):
        card = _parse_sample()
        assert card is not None
        assert card.race_code == "202607190401"
        assert card.status == "1"
        assert card.is_valid()

    def test_summary_figures(self):
        card = _parse_sample()
        boat1 = card.boats[0]
        assert boat1.racer_name == "森作 雄大"
        assert boat1.win_rate == 7.30
        assert boat1.avg_st == 0.13
        assert boat1.avg_start_order == 2.1
        boat6 = card.boats[5]
        assert boat6.win_rate == 2.10
        assert boat6.avg_st == 0.21
        assert boat6.avg_start_order == 5.1

    def test_runs_newest_first(self):
        card = _parse_sample()
        boat1 = card.boats[0]
        assert len(boat1.runs) == 10
        # SPA renders 前走=１, 2走=６, ..., 10走=３ for boat 1.
        assert boat1.runs[0].finish_position == "1"
        assert boat1.runs[1].finish_position == "6"
        assert boat1.runs[9].finish_position == "3"

    def test_grade_and_special_finish(self):
        card = _parse_sample()
        boat1 = card.boats[0]
        # Oldest two runs are GII in the SPA rendering.
        assert boat1.runs[8].grade == "G2"
        assert boat1.runs[9].grade == "G2"
        assert boat1.runs[0].grade == "IP"
        # 富澤 has an F in the 8走 slot (index 2 newest-first).
        boat3 = card.boats[2]
        assert boat3.runs[2].finish_position == "F"

    def test_entry_course_only_when_not_wakunari(self):
        card = _parse_sample()
        # 杢野 entered course 3 in one run (7th newest); everything else 枠なり.
        boat2 = card.boats[1]
        courses = [r.entry_course for r in boat2.runs]
        assert courses[6] == 3
        assert all(c is None for i, c in enumerate(courses) if i != 6)
        # 片岡 course 5 in 2走 (index 1), 関 course 6 in 3走 (index 2).
        assert card.boats[3].runs[1].entry_course == 5
        assert card.boats[4].runs[2].entry_course == 6

    def test_status_2_returns_empty_card(self):
        scraper = RaceCardScraper()
        card = scraper._parse_waku10_tsv("data=\n2\t0\n", "2026-07-19", 4, 1)
        assert card is not None
        assert card.status == "2"
        assert card.boats == []

    def test_rejects_non_tsv(self):
        scraper = RaceCardScraper()
        assert scraper._parse_waku10_tsv("", "2026-07-19", 4, 1) is None
        assert (
            scraper._parse_waku10_tsv("<html></html>", "2026-07-19", 4, 1)
            is None
        )


class TestWaku10Csv:
    def test_headers_layout(self):
        assert len(WAKU10_HEADERS) == 4 + 6 * (4 + 10 * 3)
        assert WAKU10_HEADERS[4] == "艇1_選手名"
        assert WAKU10_HEADERS[8] == "艇1_過去1走_着順"
        assert WAKU10_HEADERS[4 + 34] == "艇2_選手名"

    def test_row_aligns_with_headers(self):
        card = _parse_sample()
        row = waku10_to_row(card)
        assert len(row) == len(WAKU10_HEADERS)
        cell = dict(zip(WAKU10_HEADERS, row))
        assert cell["レースコード"] == "202607190401"
        assert cell["レース場コード"] == "04"
        assert cell["艇1_枠番別勝率"] == "7.3"
        assert cell["艇1_過去1走_着順"] == "1"
        assert cell["艇2_過去7走_進入"] == "3"
        assert cell["艇1_過去10走_グレード"] == "G2"
        assert cell["艇3_過去3走_着順"] == "F"

    def test_csv_generation(self):
        card = _parse_sample()
        content = waku10_to_csv([card])
        lines = content.splitlines()
        assert len(lines) == 2
        assert lines[0].startswith("レースコード,レース日,")
