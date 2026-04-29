"""Unit tests for OriginalExhibitionScraper and CSV conversion."""

from unittest.mock import MagicMock, patch

import pytest

from boatrace.converter import (
    ORIGINAL_EXHIBITION_HEADERS,
    original_exhibition_to_csv,
    original_exhibition_to_row,
)
from boatrace.models import OriginalExhibitionBoat, OriginalExhibitionData
from boatrace.original_exhibition_scraper import (
    OriginalExhibitionScraper,
    _normalize_label,
    _normalize_name,
    _to_float,
)


# ---------------------------------------------------------------------------
# Sample TSV bodies mirroring what race.boatcast.jp actually returns.
# ---------------------------------------------------------------------------

# Miyajima (3 measurement columns)
SAMPLE_TSV_3COL = (
    "data=\n"
    "1\t3\n"
    "一\u3000周\tまわり足\t直\u3000線\n"
    "1\t石渡\u3000\u3000鉄兵\t36.69\t5.49\t7.20\n"
    "2\t田中\u3000信一郎\t36.74\t5.68\t7.17\n"
    "3\t寺田\u3000\u3000千恵\t37.22\t5.74\t7.12\n"
    "4\t服部\u3000\u3000幸男\t37.05\t5.97\t7.26\n"
    "5\t中島\u3000\u3000孝平\t36.72\t5.97\t7.10\n"
    "6\t江口\u3000\u3000晃生\t37.02\t5.78\t7.24\n"
)

# Suminoe (2 measurement columns only)
SAMPLE_TSV_2COL = (
    "data=\n"
    "1\t2\n"
    "一\u3000周\tまわり足\n"
    "1\t庄司\u3000\u3000孝輔\t37.17\t11.53\n"
    "2\t選手\u3000二郎\t37.20\t11.60\n"
    "3\t選手\u3000三郎\t37.25\t11.65\n"
    "4\t選手\u3000四郎\t37.30\t11.70\n"
    "5\t選手\u3000五郎\t37.35\t11.75\n"
    "6\t選手\u3000六郎\t37.40\t11.80\n"
)

# Race that could not be measured — only the meta lines are present.
SAMPLE_TSV_NOT_MEASURABLE = "data=\n2\t3\n"

# SPA HTML fallback returned by CloudFront for non-existent races.
SAMPLE_HTML_FALLBACK = (
    "<!doctype html>\n<html lang=\"ja\">\n  <head>\n"
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


def test_to_float_parses_normal():
    assert _to_float("36.69") == pytest.approx(36.69)


def test_to_float_returns_none_on_blank():
    assert _to_float("") is None
    assert _to_float("   ") is None
    assert _to_float(None) is None


def test_to_float_returns_none_on_garbage():
    assert _to_float("abc") is None


def test_normalize_label_strips_full_width_padding():
    assert _normalize_label("一\u3000周") == "一周"
    assert _normalize_label("直\u3000線") == "直線"
    assert _normalize_label("まわり足") == "まわり足"


def test_normalize_name_collapses_full_width_spaces():
    assert _normalize_name("石渡\u3000\u3000鉄兵") == "石渡 鉄兵"
    assert _normalize_name("\u3000田中\u3000信一郎\u3000") == "田中 信一郎"


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def test_build_url_zero_pads_stadium_and_race():
    scraper = OriginalExhibitionScraper()
    url = scraper._build_url("2026-04-23", 17, 1)
    assert url == (
        "https://race.boatcast.jp/txt/17/bc_oriten_20260423_17_01.txt"
    )


def test_build_url_single_digit_stadium():
    scraper = OriginalExhibitionScraper()
    url = scraper._build_url("2026-04-23", 1, 12)
    assert url == (
        "https://race.boatcast.jp/txt/01/bc_oriten_20260423_01_12.txt"
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_three_column_tsv():
    scraper = OriginalExhibitionScraper()
    data = scraper._parse_tsv(SAMPLE_TSV_3COL, "2026-04-23", 17, 1)
    assert data is not None
    assert data.date == "2026-04-23"
    assert data.stadium_number == 17
    assert data.race_number == 1
    assert data.race_code == "202604231701"
    assert data.status == "1"
    assert data.measure_count == 3
    assert data.measure_labels == ["一周", "まわり足", "直線"]
    assert len(data.boats) == 6
    assert data.is_valid()

    boat1 = data.boats[0]
    assert boat1.boat_number == 1
    assert boat1.racer_name == "石渡 鉄兵"
    assert boat1.value1 == pytest.approx(36.69)
    assert boat1.value2 == pytest.approx(5.49)
    assert boat1.value3 == pytest.approx(7.20)


def test_parse_two_column_tsv_leaves_value3_none():
    scraper = OriginalExhibitionScraper()
    data = scraper._parse_tsv(SAMPLE_TSV_2COL, "2026-04-23", 12, 1)
    assert data is not None
    assert data.measure_count == 2
    assert data.measure_labels == ["一周", "まわり足"]
    assert len(data.boats) == 6
    for boat in data.boats:
        assert boat.value1 is not None
        assert boat.value2 is not None
        assert boat.value3 is None


def test_parse_not_measurable_returns_empty_boats():
    scraper = OriginalExhibitionScraper()
    data = scraper._parse_tsv(SAMPLE_TSV_NOT_MEASURABLE, "2026-04-23", 17, 1)
    assert data is not None
    assert data.status == "2"
    assert data.measure_count == 3
    assert data.boats == []
    assert not data.is_measurable()
    assert not data.is_valid()


def test_parse_rejects_non_data_body():
    scraper = OriginalExhibitionScraper()
    data = scraper._parse_tsv(SAMPLE_HTML_FALLBACK, "2026-04-23", 17, 1)
    assert data is None


# ---------------------------------------------------------------------------
# HTTP flow with mocked requests.Session.get
# ---------------------------------------------------------------------------


def _make_scraper_with_response(status_code: int, body: str):
    scraper = OriginalExhibitionScraper()
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    scraper.session.get = MagicMock(return_value=response)
    # Avoid actual sleep during tests.
    scraper.rate_limiter.wait = MagicMock()
    return scraper


def test_scrape_race_returns_data_on_200_tsv():
    scraper = _make_scraper_with_response(200, SAMPLE_TSV_3COL)
    data = scraper.scrape_race("2026-04-23", 17, 1)
    assert data is not None
    assert data.status == "1"
    assert len(data.boats) == 6


def test_scrape_race_returns_none_on_403():
    scraper = _make_scraper_with_response(403, SAMPLE_HTML_FALLBACK)
    data = scraper.scrape_race("2026-04-20", 17, 1)
    assert data is None


def test_scrape_race_returns_none_on_404():
    scraper = _make_scraper_with_response(404, "")
    data = scraper.scrape_race("2026-04-20", 17, 1)
    assert data is None


def test_scrape_race_returns_none_on_html_200():
    # CloudFront edge case: the SPA HTML is served with 200 for certain URLs.
    scraper = _make_scraper_with_response(200, SAMPLE_HTML_FALLBACK)
    data = scraper.scrape_race("2026-04-20", 17, 1)
    assert data is None


# ---------------------------------------------------------------------------
# CSV conversion
# ---------------------------------------------------------------------------


def _sample_data_three_col():
    return OriginalExhibitionData(
        date="2026-04-23",
        stadium_number=17,
        race_number=1,
        race_code="202604231701",
        status="1",
        measure_count=3,
        measure_labels=["一周", "まわり足", "直線"],
        boats=[
            OriginalExhibitionBoat(
                boat_number=i,
                racer_name=f"選手{i}",
                value1=36.0 + i * 0.1,
                value2=5.0 + i * 0.1,
                value3=7.0 + i * 0.1,
            )
            for i in range(1, 7)
        ],
    )


def test_headers_have_expected_column_count():
    # 9 fixed meta columns + 6 boats * 4 columns each = 33
    assert len(ORIGINAL_EXHIBITION_HEADERS) == 9 + 6 * 4


def test_row_includes_labels_and_boat_values():
    data = _sample_data_three_col()
    row = original_exhibition_to_row(data)
    assert row[0] == "202604231701"
    assert row[1] == "2026-04-23"
    assert row[2] == "17"  # stadium 17 -> "17" (already 2 digits)
    assert row[3] == "01R"
    assert row[4] == "1"  # status
    assert row[5] == "3"  # measure_count
    assert row[6] == "一周"
    assert row[7] == "まわり足"
    assert row[8] == "直線"
    # Boat 1 starts at index 9
    assert row[9] == "選手1"
    assert row[10] == "36.1"
    assert row[11] == "5.1"
    assert row[12] == "7.1"


def test_row_pads_missing_boats_with_blanks():
    data = OriginalExhibitionData(
        date="2026-04-23",
        stadium_number=17,
        race_number=1,
        race_code="202604231701",
        status="2",
        measure_count=3,
        measure_labels=["一周", "まわり足", "直線"],
        boats=[],
    )
    row = original_exhibition_to_row(data)
    # All 6 boats * 4 fields should be blank.
    for i in range(6):
        base = 9 + i * 4
        assert row[base : base + 4] == ["", "", "", ""]


def test_row_zero_pads_single_digit_stadium():
    """レース場 column should be 2-digit zero-padded (e.g. 3 -> '03').

    This matches race_cards / recent_form CSVs whose レース場コード is
    always 2 digits, enabling string-equality joins across files.
    """
    data = OriginalExhibitionData(
        date="2026-04-23",
        stadium_number=3,
        race_number=1,
        race_code="202604230301",
        status="1",
        measure_count=3,
        measure_labels=["一周", "まわり足", "直線"],
        boats=[],
    )
    row = original_exhibition_to_row(data)
    assert row[2] == "03"  # NOT "3"


def test_row_leaves_value3_blank_for_two_column_stadium():
    data = OriginalExhibitionData(
        date="2026-04-23",
        stadium_number=12,
        race_number=1,
        race_code="202604231201",
        status="1",
        measure_count=2,
        measure_labels=["一周", "まわり足"],
        boats=[
            OriginalExhibitionBoat(
                boat_number=i,
                racer_name=f"選手{i}",
                value1=37.0,
                value2=11.5,
                value3=None,
            )
            for i in range(1, 7)
        ],
    )
    row = original_exhibition_to_row(data)
    assert row[8] == ""  # 計測項目3 blank
    for i in range(6):
        value3_idx = 9 + i * 4 + 3
        assert row[value3_idx] == ""


def test_csv_output_has_header_and_rows():
    data = _sample_data_three_col()
    csv_text = original_exhibition_to_csv([data, data])
    lines = csv_text.strip().split("\n")
    assert lines[0].startswith("レースコード,")
    assert len(lines) == 3  # header + 2 rows
