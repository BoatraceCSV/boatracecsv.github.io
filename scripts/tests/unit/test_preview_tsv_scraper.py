"""Unit tests for PreviewTsvScraper.

The fixtures in this module mirror the actual TSV bodies served by
race.boatcast.jp. They were sampled from races on 2026-04-24 (jo=01,
jo=12, jo=17) during Phase 0 endpoint discovery and trimmed to the
shape we rely on. The expected post-processing values were
cross-checked against the existing
``data/previews/daily/2026/04/24.csv`` to ensure parity with the legacy
HTML-based scraper.
"""

from unittest.mock import MagicMock

import pytest

from boatrace.converter import PREVIEWS_HEADERS, race_preview_to_row
from boatrace.preview_tsv_scraper import PreviewTsvScraper


# ---------------------------------------------------------------------------
# Fixtures: real TSV bodies
# ---------------------------------------------------------------------------

# bc_j_tkz for jo=01 race=01 on 2026-04-24 (Kiryu, opening race).
# 6 racer rows, status "1" (normal). Trailing line is the simplified ST
# block; we ignore it in favour of bc_j_stt.
SAMPLE_TKZ_OK = (
    "data=\n"
    "1\n"
    "土井　　歩夢\t6.77\t0\t000\t52.0\t1\t- 0.5\t\t\t\t31\t01\n"
    "大場　　広孝\t6.83\t0\t000\t55.9\t1\t- 0.5\n"
    "谷本　　幸司\t6.90\t0\t000\t52.0\t1\t- 0.5\n"
    "三松　　直美\t6.78\t0\t000\t68.9\t1\t- 0.5\n"
    "菊池　　峰晴\t6.78\t0\t000\t54.0\t1\t- 0.5\n"
    "高木　　茉白\t6.70\t0\t000\t47.5\t1\t- 0.5\n"
    "1\t.18\t\t2\t.12\t\t3\t.21\t\t4\t.29\t\t5\t.18\t\t6\t.15\t\n"
)

# bc_j_tkz for jo=12 race=03: boat 3 has weight under-limit flag and a
# 0.5kg adjustment; boat 6 has +0.5kg adjustment and "+ 0.0" tilt.
SAMPLE_TKZ_MIXED = (
    "data=\n"
    "1\n"
    "谷　　　勝幸\t6.97\t0\t000\t54.0\t1\t- 0.5\n"
    "来田　　衣織\t6.97\t0\t000\t47.0\t1\t- 0.5\t\t\t36\n"
    "杢野　　誓良\t6.92\t1\t005\t51.5\t1\t- 0.5\n"
    "庄司　樹良々\t7.05\t0\t000\t53.7\t0\t+ 0.0\n"
    "井手　　良太\t7.04\t0\t000\t52.0\t1\t- 0.5\n"
    "立間　　充宏\t7.04\t0\t000\t55.4\t0\t+ 0.0\n"
    "1\t.09\tF\t2\t.01\t\t3\t.04\t\t4\t.06\tF\t5\t.06\tF\t6\t.08\tF\n"
)

# Race that could not be measured.
SAMPLE_TKZ_NOT_MEASURABLE = "data=\n2\n"

# bc_j_stt for jo=12 race=03: boat 1 has F flag → ST 0.09 should become -0.09.
SAMPLE_STT_WITH_F = (
    "data=\n"
    "1\n"
    "1\t1\t谷　　　勝幸\t.13\t.09\tF\t5.0\n"
    "2\t2\t来田　　衣織\t.23\t.01\t\t4.0\n"
    "3\t3\t杢野　　誓良\t.16\t.04\t\t1.0\n"
    "4\t4\t庄司　樹良々\t.13\t.06\tF\t1.0\n"
    "5\t5\t井手　　良太\t.14\t.06\tF\t3.0\n"
    "6\t6\t立間　　充宏\t.18\t.08\tF\t1.5\n"
)

# bc_j_stt for jo=01 race=01: clean start (no F flags).
SAMPLE_STT_CLEAN = (
    "data=\n"
    "1\n"
    "1\t1\t土井　　歩夢\t.15\t.18\t\t2.0\n"
    "2\t2\t大場　　広孝\t.15\t.12\t\t2.5\n"
    "3\t3\t谷本　　幸司\t.16\t.21\t\t3.0\n"
    "4\t4\t三松　　直美\t.15\t.29\t\t3.5\n"
    "5\t5\t菊池　　峰晴\t.15\t.18\t\t2.5\n"
    "6\t6\t高木　　茉白\t.16\t.15\t\t3.0\n"
)

# bc_rs1_2 for jo=17 race=08: ST line, then 6 placement rows, then weather.
SAMPLE_RS1_2 = (
    "1\t\t.19\t6\t\t.22\t2\t\t.24\t3\t\t.20\t4\t\t.19\t5\t\t.22\n"
    "１\t1\t森永　　　淳\t1'49\"0\t抜　き\n"
    "２\t6\t西島　　義則\t1'50\"1\t\n"
    "３\t5\t齊藤　　　仁\t1'52\"2\t\n"
    "４\t3\t北村　　征嗣\t1'53\"4\t\n"
    "５\t2\t寺田　　千恵\t1'54\"0\t\n"
    "６\t4\t伊藤　　将吉\t1'56\"0\t\n"
    "1421\t2\t2\t南　　(左横風)\t2\t+17.0\t+15.0\n"
)

# bc_sui for jo=17 on 2026-04-24: single-row latest snapshot.
SAMPLE_SUI = (
    "1641\t2\t2\t南　　(左横風)\t2\t+18.0\t+15.0\t0927\t1437\n"
)

# CloudFront SPA HTML fallback.
SAMPLE_HTML_FALLBACK = (
    "<!doctype html>\n<html lang=\"ja\">\n  <head>\n"
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def test_build_url_per_race_zero_pads():
    scraper = PreviewTsvScraper()
    assert scraper._build_url("hp_txt", "bc_j_tkz", "2026-04-24", 1, 1) == (
        "https://race.boatcast.jp/hp_txt/01/bc_j_tkz_20260424_01_01.txt"
    )
    assert scraper._build_url("hp_txt", "bc_j_stt", "2026-04-24", 17, 12) == (
        "https://race.boatcast.jp/hp_txt/17/bc_j_stt_20260424_17_12.txt"
    )
    assert scraper._build_url("m_txt", "bc_rs1_2", "2026-04-24", 12, 3) == (
        "https://race.boatcast.jp/m_txt/12/bc_rs1_2_20260424_12_03.txt"
    )


def test_build_url_per_stadium_day_when_race_is_none():
    scraper = PreviewTsvScraper()
    assert scraper._build_url("m_txt", "bc_sui", "2026-04-24", 17, None) == (
        "https://race.boatcast.jp/m_txt/17/bc_sui_20260424_17.txt"
    )


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


def test_parse_weight_adjustment_decodes_x10_string():
    assert PreviewTsvScraper._parse_weight_adjustment("000") == 0.0
    assert PreviewTsvScraper._parse_weight_adjustment("005") == 0.5
    assert PreviewTsvScraper._parse_weight_adjustment("010") == 1.0


def test_parse_weight_adjustment_returns_none_on_blank():
    assert PreviewTsvScraper._parse_weight_adjustment("") is None
    assert PreviewTsvScraper._parse_weight_adjustment("   ") is None
    assert PreviewTsvScraper._parse_weight_adjustment("abc") is None


def test_parse_tilt_handles_signed_strings():
    assert PreviewTsvScraper._parse_tilt("- 0.5") == -0.5
    assert PreviewTsvScraper._parse_tilt("+ 0.0") == 0.0
    assert PreviewTsvScraper._parse_tilt("+ 0.5") == 0.5
    assert PreviewTsvScraper._parse_tilt("　- 0.5　") == -0.5
    assert PreviewTsvScraper._parse_tilt("") is None


def test_parse_start_timing_with_flying_flag_returns_negative():
    # ".09" with flag "F" -> -0.09 (matches existing CSV semantics).
    assert PreviewTsvScraper._parse_start_timing(".09", "F") == pytest.approx(-0.09)
    assert PreviewTsvScraper._parse_start_timing(".06", "F") == pytest.approx(-0.06)


def test_parse_start_timing_without_flag_is_positive():
    assert PreviewTsvScraper._parse_start_timing(".18", "") == pytest.approx(0.18)
    assert PreviewTsvScraper._parse_start_timing(".21", "") == pytest.approx(0.21)


def test_parse_start_timing_late_flag_returns_none():
    # "L" indicates 出遅れ — there's no numeric ST to record.
    assert PreviewTsvScraper._parse_start_timing(".15", "L") is None


def test_parse_start_timing_blank_returns_none():
    assert PreviewTsvScraper._parse_start_timing("", "") is None
    assert PreviewTsvScraper._parse_start_timing("   ", "") is None


def test_parse_wind_direction_strips_parenthesised_quality():
    # "南　　(左横風)" -> 5 (=南)
    assert PreviewTsvScraper._parse_wind_direction_string("南　　(左横風)") == 5
    assert PreviewTsvScraper._parse_wind_direction_string("北東") == 2
    assert PreviewTsvScraper._parse_wind_direction_string("北西") == 8


def test_parse_wind_direction_prefers_two_char_match():
    # If the table iterated alphabetically we'd risk matching "北" inside
    # "北西" before checking the longer token. Confirm the 2-char wins.
    assert PreviewTsvScraper._parse_wind_direction_string("北西　　(横風)") == 8
    assert PreviewTsvScraper._parse_wind_direction_string("北東　　(向い風)") == 2


def test_parse_wind_direction_returns_none_on_blank():
    assert PreviewTsvScraper._parse_wind_direction_string("") is None
    assert PreviewTsvScraper._parse_wind_direction_string("　　") is None


def test_parse_temperature_strips_leading_plus():
    assert PreviewTsvScraper._parse_temperature("+18.0") == pytest.approx(18.0)
    assert PreviewTsvScraper._parse_temperature("+0.0") == pytest.approx(0.0)
    assert PreviewTsvScraper._parse_temperature("-1.5") == pytest.approx(-1.5)
    assert PreviewTsvScraper._parse_temperature("") is None


def test_parse_weather_code_accepts_known_values_only():
    # boatcast values: 1晴れ 2くもり 3雨 4雪 5台風 6霧 9その他.
    for code in (1, 2, 3, 4, 5, 6, 9):
        assert PreviewTsvScraper._parse_weather_code(str(code)) == code
    # Out-of-range / garbage rejected.
    assert PreviewTsvScraper._parse_weather_code("0") is None
    assert PreviewTsvScraper._parse_weather_code("7") is None
    assert PreviewTsvScraper._parse_weather_code("abc") is None
    assert PreviewTsvScraper._parse_weather_code("") is None


# ---------------------------------------------------------------------------
# bc_j_tkz parser
# ---------------------------------------------------------------------------


def test_parse_tkz_normal_row():
    scraper = PreviewTsvScraper()
    status, boats = scraper._parse_tkz(SAMPLE_TKZ_OK)
    assert status == "1"
    assert len(boats) == 6
    boat1 = boats[1]
    assert boat1["exhibition_time"] == pytest.approx(6.77)
    assert boat1["weight"] == pytest.approx(52.0)
    assert boat1["weight_adjustment"] == pytest.approx(0.0)
    assert boat1["tilt_adjustment"] == pytest.approx(-0.5)


def test_parse_tkz_decodes_under_weight_adjustment():
    """Boat 3 in race 12-03 had a 0.5kg adjustment ("005")."""
    scraper = PreviewTsvScraper()
    _, boats = scraper._parse_tkz(SAMPLE_TKZ_MIXED)
    boat3 = boats[3]
    assert boat3["weight"] == pytest.approx(51.5)
    assert boat3["weight_adjustment"] == pytest.approx(0.5)
    boat6 = boats[6]
    assert boat6["tilt_adjustment"] == pytest.approx(0.0)
    assert boat6["weight_adjustment"] == pytest.approx(0.0)


def test_parse_tkz_not_measurable_returns_status_with_empty_boats():
    scraper = PreviewTsvScraper()
    status, boats = scraper._parse_tkz(SAMPLE_TKZ_NOT_MEASURABLE)
    assert status == "2"
    assert boats == {}


def test_parse_tkz_rejects_html_body():
    scraper = PreviewTsvScraper()
    status, boats = scraper._parse_tkz(SAMPLE_HTML_FALLBACK)
    assert status is None
    assert boats == {}


# ---------------------------------------------------------------------------
# bc_j_stt parser
# ---------------------------------------------------------------------------


def test_parse_stt_with_f_flags_negates_st():
    scraper = PreviewTsvScraper()
    rows = scraper._parse_stt(SAMPLE_STT_WITH_F)
    assert rows[1]["course_number"] == 1
    assert rows[1]["start_timing"] == pytest.approx(-0.09)
    assert rows[2]["start_timing"] == pytest.approx(0.01)
    assert rows[4]["start_timing"] == pytest.approx(-0.06)


def test_parse_stt_clean_start_keeps_positive():
    scraper = PreviewTsvScraper()
    rows = scraper._parse_stt(SAMPLE_STT_CLEAN)
    assert rows[1]["start_timing"] == pytest.approx(0.18)
    assert rows[6]["start_timing"] == pytest.approx(0.15)
    # All boats have course == waku in a clean entry.
    for boat_num in range(1, 7):
        assert rows[boat_num]["course_number"] == boat_num


def test_parse_stt_skips_unparseable_rows():
    scraper = PreviewTsvScraper()
    body = (
        "data=\n"
        "1\n"
        "?\t?\t欠場\t\t\t\t\n"  # header-style placeholder, no boat info
        "1\t1\t土井　歩夢\t.15\t.18\t\t2.0\n"
    )
    rows = scraper._parse_stt(body)
    assert 1 in rows
    assert rows[1]["course_number"] == 1


# ---------------------------------------------------------------------------
# Weather parsing (bc_rs1_2 + bc_sui)
# ---------------------------------------------------------------------------


def test_last_weather_line_finds_weather_in_rs1_2():
    """The weather row is at the very tail of bc_rs1_2."""
    line = PreviewTsvScraper._last_weather_line(SAMPLE_RS1_2)
    assert line is not None
    assert line.startswith("1421\t")


def test_parse_weather_line_decodes_all_fields():
    parsed = PreviewTsvScraper._parse_weather_line(
        "1421\t2\t2\t南　　(左横風)\t2\t+17.0\t+15.0"
    )
    assert parsed == {
        "weather": 2,
        "wave_height": pytest.approx(2.0),
        "wind_direction": 5,  # 南
        "wind_speed": pytest.approx(2.0),
        "air_temperature": pytest.approx(17.0),
        "water_temperature": pytest.approx(15.0),
    }


def test_parse_weather_line_rejects_non_weather_row():
    # The ST timings line (first line of bc_rs1_2) doesn't start with HHMM.
    parsed = PreviewTsvScraper._parse_weather_line(
        "1\t\t.19\t6\t\t.22\t2"
    )
    assert parsed is None


# ---------------------------------------------------------------------------
# End-to-end with mocked HTTP
# ---------------------------------------------------------------------------


def _build_response(status_code: int, body: str) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = body
    return response


def _make_scraper_with_routes(routes: dict) -> PreviewTsvScraper:
    """Build a scraper whose session.get returns canned bodies per URL.

    ``routes`` maps URL (or URL substring) to (status_code, body). The
    first matching key wins, so put more specific keys first.
    """
    scraper = PreviewTsvScraper()
    scraper.rate_limiter.wait = MagicMock()  # avoid real sleeps

    def fake_get(url, timeout=None, **kwargs):
        for key, (status, body) in routes.items():
            if key in url:
                return _build_response(status, body)
        return _build_response(404, "")

    scraper.session.get = MagicMock(side_effect=fake_get)
    return scraper


def test_scrape_race_preview_end_to_end_with_rs1_2_weather():
    scraper = _make_scraper_with_routes(
        {
            "bc_j_tkz_20260424_12_03": (200, SAMPLE_TKZ_MIXED),
            "bc_j_stt_20260424_12_03": (200, SAMPLE_STT_WITH_F),
            "bc_rs1_2_20260424_12_03": (200, SAMPLE_RS1_2),
        }
    )
    preview = scraper.scrape_race_preview("2026-04-24", 12, 3)
    assert preview is not None
    assert preview.race_code == "202604241203"
    assert preview.stadium == "12"
    assert preview.race_round == "03R"
    assert preview.title is None  # caller is expected to fill it
    # Weather block (from bc_rs1_2 last line)
    assert preview.weather == 2
    assert preview.wind_direction == 5
    assert preview.wind_speed == pytest.approx(2.0)
    assert preview.wave_height == pytest.approx(2.0)
    assert preview.air_temperature == pytest.approx(17.0)
    assert preview.water_temperature == pytest.approx(15.0)
    # Boat 3 has a 0.5kg adjustment; boat 1 has F-flag ST.
    assert preview.boats[2].weight_adjustment == pytest.approx(0.5)
    assert preview.boats[2].weight == pytest.approx(51.5)
    assert preview.boats[0].start_timing == pytest.approx(-0.09)


def test_scrape_race_preview_falls_back_to_bc_sui_when_rs1_2_missing():
    scraper = _make_scraper_with_routes(
        {
            "bc_j_tkz_": (200, SAMPLE_TKZ_OK),
            "bc_j_stt_": (200, SAMPLE_STT_CLEAN),
            # bc_rs1_2 returns 403 (race not yet finished).
            "bc_rs1_2_": (403, SAMPLE_HTML_FALLBACK),
            "bc_sui_": (200, SAMPLE_SUI),
        }
    )
    preview = scraper.scrape_race_preview("2026-04-24", 1, 1)
    assert preview is not None
    # Weather should come from bc_sui (single snapshot, +18.0 temp).
    assert preview.air_temperature == pytest.approx(18.0)
    assert preview.water_temperature == pytest.approx(15.0)
    assert preview.wind_speed == pytest.approx(2.0)


def test_scrape_race_preview_caches_bc_sui_across_races():
    """bc_sui is per-stadium-per-day, so we shouldn't refetch each race."""
    scraper = _make_scraper_with_routes(
        {
            "bc_j_tkz_": (200, SAMPLE_TKZ_OK),
            "bc_j_stt_": (200, SAMPLE_STT_CLEAN),
            "bc_rs1_2_": (403, SAMPLE_HTML_FALLBACK),
            "bc_sui_": (200, SAMPLE_SUI),
        }
    )
    scraper.scrape_race_preview("2026-04-24", 1, 1)
    scraper.scrape_race_preview("2026-04-24", 1, 2)
    sui_calls = [
        call for call in scraper.session.get.call_args_list
        if "bc_sui_" in call.args[0]
    ]
    assert len(sui_calls) == 1, "bc_sui should be fetched only once per stadium"


def test_scrape_race_preview_returns_none_on_missing_tkz():
    scraper = _make_scraper_with_routes(
        {"bc_j_tkz_": (403, SAMPLE_HTML_FALLBACK)}
    )
    preview = scraper.scrape_race_preview("2026-04-24", 1, 1)
    assert preview is None


def test_scrape_race_preview_handles_html_200_fallback():
    """CloudFront sometimes serves the SPA HTML with 200 for missing files."""
    scraper = _make_scraper_with_routes(
        {"bc_j_tkz_": (200, SAMPLE_HTML_FALLBACK)}
    )
    preview = scraper.scrape_race_preview("2026-04-24", 1, 1)
    assert preview is None


def test_scrape_race_preview_works_with_stt_missing():
    """If bc_j_stt is missing, course/ST blanks but other fields populated."""
    scraper = _make_scraper_with_routes(
        {
            "bc_j_tkz_": (200, SAMPLE_TKZ_OK),
            "bc_j_stt_": (403, SAMPLE_HTML_FALLBACK),
            "bc_rs1_2_": (403, SAMPLE_HTML_FALLBACK),
            "bc_sui_": (404, ""),
        }
    )
    preview = scraper.scrape_race_preview("2026-04-24", 1, 1)
    assert preview is not None
    assert preview.boats[0].weight == pytest.approx(52.0)
    assert preview.boats[0].course_number is None
    assert preview.boats[0].start_timing is None


# ---------------------------------------------------------------------------
# Schema parity with PREVIEWS_HEADERS
# ---------------------------------------------------------------------------


def test_csv_row_matches_previews_headers_width():
    """The TSV-source RacePreview must produce a row whose width matches
    the existing PREVIEWS_HEADERS so downstream consumers don't break."""
    scraper = _make_scraper_with_routes(
        {
            "bc_j_tkz_": (200, SAMPLE_TKZ_MIXED),
            "bc_j_stt_": (200, SAMPLE_STT_WITH_F),
            "bc_rs1_2_": (200, SAMPLE_RS1_2),
        }
    )
    preview = scraper.scrape_race_preview("2026-04-24", 12, 3)
    assert preview is not None
    preview.title = "test title"  # caller-filled value
    row = race_preview_to_row(preview)
    assert len(row) == len(PREVIEWS_HEADERS)
