"""Unit tests for the bc_mon_2 (月間開催日程) parsing + CSV pipeline.

The fixture is an excerpt of the real body for Mikuni (jo=10), month key
202607, sampled on 2026-07-19. Note the file has no ``data=`` marker —
the status line comes first — and spans several months ahead.
"""

from __future__ import annotations

from boatrace.converter import (
    MONTHLY_SCHEDULE_HEADERS,
    monthly_schedule_to_csv,
    schedule_entry_to_row,
)
from boatrace.monthly_schedule_scraper import MonthlyScheduleScraper, parse_mon2


SAMPLE_MON2 = (
    "1\n"
    "20260628\t20260701\tIP\tディアボート三国リニューアル１１周年記念競走\t12R\n"
    "20260707\t20260712\tG3\tマスターズリーグ第３戦三国レジェンドカップ\t12R\n"
    "20260718\t20260722\tIP\t住信ＳＢＩネット銀行賞\t12R\n"
    "20261004\t20261009\tG1\t開設７３周年記念　北陸艇王決戦\t12R\n"
)


class TestParseMon2:
    def test_entries(self):
        entries = parse_mon2(SAMPLE_MON2, 10)
        assert entries is not None
        assert len(entries) == 4
        first = entries[0]
        assert first.stadium_code == "10"
        assert first.start_date == "2026-06-28"
        assert first.end_date == "2026-07-01"
        assert first.grade == "IP"
        assert first.title == "ディアボート三国リニューアル１１周年記念競走"
        assert first.races == "12R"

    def test_spans_future_months(self):
        entries = parse_mon2(SAMPLE_MON2, 10)
        assert entries[-1].start_date == "2026-10-04"
        assert entries[-1].grade == "G1"

    def test_rejects_bad_status(self):
        assert parse_mon2("0\n20260628\t...\n", 10) is None
        assert parse_mon2("", 10) is None
        assert parse_mon2("<html></html>", 10) is None

    def test_skips_malformed_rows(self):
        body = "1\ngarbage\n20260718\t20260722\tIP\tタイトル\t12R\n"
        entries = parse_mon2(body, 3)
        assert len(entries) == 1
        assert entries[0].stadium_code == "03"


class TestScraper:
    def test_build_url(self):
        scraper = MonthlyScheduleScraper()
        url = scraper._build_url("202607", 10)
        assert url == "https://race.boatcast.jp/hp_txt/10/bc_mon_2_202607_10.txt"

    def test_scrape_stadium_parses(self, monkeypatch):
        scraper = MonthlyScheduleScraper()
        monkeypatch.setattr(scraper, "_fetch_body", lambda url: SAMPLE_MON2)
        entries = scraper.scrape_stadium("202607", 10)
        assert entries is not None and len(entries) == 4

    def test_scrape_stadium_missing(self, monkeypatch):
        scraper = MonthlyScheduleScraper()
        monkeypatch.setattr(scraper, "_fetch_body", lambda url: None)
        assert scraper.scrape_stadium("202607", 10) is None


class TestScheduleCsv:
    def test_row_aligns_with_headers(self):
        entries = parse_mon2(SAMPLE_MON2, 10)
        row = schedule_entry_to_row(entries[0])
        assert len(row) == len(MONTHLY_SCHEDULE_HEADERS)

    def test_csv_sorted_by_stadium_and_date(self):
        a = parse_mon2(SAMPLE_MON2, 10)
        b = parse_mon2(SAMPLE_MON2, 1)
        content = monthly_schedule_to_csv(a + b)
        lines = content.splitlines()
        assert lines[0] == ",".join(MONTHLY_SCHEDULE_HEADERS)
        assert len(lines) == 1 + 8
        # Stadium 01 rows sort before stadium 10 rows.
        assert lines[1].startswith("01,")
        assert lines[5].startswith("10,")
