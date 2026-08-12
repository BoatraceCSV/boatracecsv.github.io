"""Unit tests for TokutenHayamiScraper and its CSV row builder."""

from unittest.mock import MagicMock, patch

from boatrace.preview_csv import (
    TOKUTEN_HAYAMI_HEADERS,
    build_tokuten_hayami_row,
)
from boatrace.tokuten_hayami_scraper import (
    TokutenHayamiScraper,
    _normalize_name,
)


# ---------------------------------------------------------------------------
# Sample TSV bodies mirroring what race.boatcast.jp actually returns.
# 平和島 (04) 2026-08-12 1R. 國分 has raced once and finished 6th (1 point),
# so his 得点率 is 1.00 and "if 1着" is (1 + 10) / 2 = 5.50.
# ---------------------------------------------------------------------------

SAMPLE_TSV = (
    "data=\n"
    "1\n"
    "1\tB1\t5047\t國分　　将太郎\t00\t1.00\t43\t2\t5.50\t2\t4.50\t0"
    "\t3.50\t0\t2.50\t0\t1.50\t0\t1.00\t\t7\n"
    "2\tA2\t4460\t後藤　　翔之\t01\t8.50\t8\t5\t9.00\t5\t8.33\t5"
    "\t7.67\t3\t7.00\t3\t6.33\t3\t6.00\t\t9\n"
    "3\tA2\t5228\t若林　　樹蘭\t01\t6.00\t17\t5\t8.00\t3\t7.00\t3"
    "\t6.00\t2\t5.00\t2\t4.00\t2\t3.50\t\t11\n"
    "4\tB1\t5292\t佐藤　　右京\t00\t2.00\t36\t3\t6.00\t2\t5.00\t2"
    "\t4.00\t0\t3.00\t0\t2.00\t0\t1.50\t\t10\n"
    "5\tB1\t4211\t村田　　　敦\t00\t2.00\t36\t3\t6.00\t2\t5.00\t2"
    "\t4.00\t0\t3.00\t0\t2.00\t0\t1.50\t\t8\n"
    "6\tB2\t5449\t堀井　　涼平\t00\t1.00\t43\t2\t5.50\t2\t4.50\t0"
    "\t3.50\t0\t2.50\t0\t1.50\t0\t1.00\t\t6\n"
    "10\t08\t06\t04\t02\t01\n"
    "18\t\t\n"
    "18\n"
)

# Series without a published table (or after 予選最終日): only the flag line.
SAMPLE_TSV_NOT_READY = "data=\n0\n"

# SPA HTML fallback returned by CloudFront for non-existent races.
SAMPLE_HTML_FALLBACK = (
    '<!doctype html>\n<html lang="ja">\n  <head>\n'
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


def test_normalize_name_collapses_full_width_spaces():
    assert _normalize_name("國分　　将太郎") == "國分 将太郎"
    assert _normalize_name("　村田　　　敦　") == "村田 敦"


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def test_build_url_zero_pads_stadium_and_race():
    scraper = TokutenHayamiScraper()
    assert scraper._build_url("2026-08-12", 4, 1) == (
        "https://race.boatcast.jp/hp_txt/04/"
        "bc_j_tokuten_hayami_20260812_04_01.txt"
    )


def test_build_url_double_digit_stadium_and_race():
    scraper = TokutenHayamiScraper()
    assert scraper._build_url("2026-08-12", 17, 12) == (
        "https://race.boatcast.jp/hp_txt/17/"
        "bc_j_tokuten_hayami_20260812_17_12.txt"
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_tsv_reads_all_six_racers():
    scraper = TokutenHayamiScraper()
    data = scraper._parse_tsv(SAMPLE_TSV, "2026-08-12", 4, 1)

    assert data is not None
    assert data.date == "2026-08-12"
    assert data.stadium_number == 4
    assert data.race_number == 1
    assert data.race_code == "202608120401"
    assert data.status == "1"
    assert data.is_ready()
    assert data.is_valid()
    assert len(data.racers) == 6

    # このレースの着順点 (予選なので 10/8/6/4/2/1) と準優進出ボーダー順位。
    assert data.rank_points == ["10", "08", "06", "04", "02", "01"]
    assert data.border_rank == "18"

    boat1 = data.racers[0]
    assert boat1.boat_number == 1
    assert boat1.class_grade == "B1"
    assert boat1.registration_number == "5047"
    assert boat1.racer_name == "國分 将太郎"
    assert boat1.border_status == "00"
    assert boat1.score_rate == "1.00"
    assert boat1.rank == "43"
    assert boat1.other_race_number == "7"
    # 1着なら 5.50、6着なら 1.00 (= 現在の得点合計 + 着順点) / (出走数 + 1)
    assert boat1.if_rank_score_rates == [
        "5.50",
        "4.50",
        "3.50",
        "2.50",
        "1.50",
        "1.00",
    ]
    assert boat1.if_rank_statuses == ["2", "2", "0", "0", "0", "0"]


def test_parse_tsv_border_status_marks_racers_within_border_rank():
    scraper = TokutenHayamiScraper()
    data = scraper._parse_tsv(SAMPLE_TSV, "2026-08-12", 4, 1)
    assert data is not None

    # ボーダー状態の末尾 1 = 順位がボーダー順位 (18) 以内。
    for racer in data.racers:
        within_border = int(racer.rank) <= int(data.border_rank)
        assert (racer.border_status == "01") is within_border


def test_parse_tsv_not_ready_returns_no_racers():
    scraper = TokutenHayamiScraper()
    data = scraper._parse_tsv(SAMPLE_TSV_NOT_READY, "2026-08-12", 4, 1)

    assert data is not None
    assert data.status == "0"
    assert not data.is_ready()
    assert not data.is_valid()
    assert data.racers == []


def test_parse_tsv_rejects_non_tsv_body():
    scraper = TokutenHayamiScraper()
    assert scraper._parse_tsv(SAMPLE_HTML_FALLBACK, "2026-08-12", 4, 1) is None


# ---------------------------------------------------------------------------
# HTTP handling
# ---------------------------------------------------------------------------


def _response(status_code: int, text: str) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    return response


def test_scrape_race_returns_none_on_403():
    scraper = TokutenHayamiScraper()
    with patch.object(scraper.session, "get", return_value=_response(403, "")):
        assert scraper.scrape_race("2026-08-12", 5, 5) is None


def test_scrape_race_returns_none_on_html_fallback():
    scraper = TokutenHayamiScraper()
    with patch.object(
        scraper.session, "get", return_value=_response(200, SAMPLE_HTML_FALLBACK)
    ):
        assert scraper.scrape_race("2026-08-12", 5, 5) is None


def test_scrape_race_parses_success():
    scraper = TokutenHayamiScraper()
    with patch.object(
        scraper.session, "get", return_value=_response(200, SAMPLE_TSV)
    ):
        data = scraper.scrape_race("2026-08-12", 4, 1)
    assert data is not None
    assert data.is_valid()


# ---------------------------------------------------------------------------
# CSV row
# ---------------------------------------------------------------------------


def test_build_row_matches_header_length_and_values():
    scraper = TokutenHayamiScraper()
    data = scraper._parse_tsv(SAMPLE_TSV, "2026-08-12", 4, 1)
    assert data is not None

    row = build_tokuten_hayami_row(
        race_code=data.race_code,
        date_str=data.date,
        stadium_code=data.stadium_number,
        race_number=data.race_number,
        deadline_time="11:55",
        fetched_at_iso="2026-08-12T11:50:00+09:00",
        border_rank=data.border_rank,
        rank_points=data.rank_points,
        racers=data.racers,
    )

    assert len(row) == len(TOKUTEN_HAYAMI_HEADERS)

    cells = dict(zip(TOKUTEN_HAYAMI_HEADERS, row))
    assert cells["レースコード"] == "202608120401"
    assert cells["レース場"] == "04"
    assert cells["レース回"] == "01R"
    assert cells["ボーダー順位"] == "18"
    assert cells["1着点"] == "10"
    assert cells["6着点"] == "01"
    assert cells["艇1_選手名"] == "國分 将太郎"
    assert cells["艇1_得点率"] == "1.00"
    assert cells["艇1_順位"] == "43"
    assert cells["艇1_早見"] == "7"
    assert cells["艇1_1着時得点率"] == "5.50"
    assert cells["艇1_6着時得点率"] == "1.00"
    assert cells["艇2_ボーダー状態"] == "01"


def test_build_row_fills_blanks_for_missing_boat():
    row = build_tokuten_hayami_row(
        race_code="202608120401",
        date_str="2026-08-12",
        stadium_code=4,
        race_number=1,
        deadline_time="11:55",
        fetched_at_iso="2026-08-12T11:50:00+09:00",
        border_rank=None,
        rank_points=[],
        racers=[],
    )
    assert len(row) == len(TOKUTEN_HAYAMI_HEADERS)
    cells = dict(zip(TOKUTEN_HAYAMI_HEADERS, row))
    assert cells["ボーダー順位"] == ""
    assert cells["1着点"] == ""
    assert cells["艇1_選手名"] == ""
    assert cells["艇6_6着時状態"] == ""
