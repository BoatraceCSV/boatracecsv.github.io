"""Unit tests for RecentFormScraper.

Fixtures mirror real ``bc_zensou`` / ``bc_zensou_touchi`` TSV bodies
served by race.boatcast.jp. The sample below is excerpted from
``https://race.boatcast.jp/hp_txt/17/bc_zensou_20260425_17.txt``
(stadium 17, 2026-04-25). The TSV has 50 racer rows in production; the
fixture trims to 3 rows that exercise distinct edge cases:

* Row 1 — full 5 sessions, mixed grades (一般 / ＳＧ-style codes via "ＧⅠ").
* Row 2 — 4 sessions populated, 5th partly empty (newer racer / no 5th
  prior series).
* Row 3 — finish_sequence with 欠 (欠場) and 転 (転覆) tokens.

The expected post-processing values follow the schema decided in
PR-1's design discussion (sessions ordered most-recent-first, dates
normalised to ``YYYY-MM-DD``, full-width spaces collapsed in stadium
names, finish_sequence kept verbatim with trailing padding stripped).
"""

from unittest.mock import MagicMock

import pytest

from boatrace.converter import (
    RECENT_FORM_HEADERS,
    recent_form_to_row,
    recent_forms_to_csv,
)
from boatrace.models import RecentForm, RecentFormBoat, RecentFormSession
from boatrace.recent_form_scraper import (
    RecentFormScraper,
    _format_yyyymmdd_to_iso,
    _normalize_finish_sequence,
    _normalize_stadium_name,
    _parse_session_block,
    _parse_sessions,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Three racers, each with full 5-session blocks. Source: 宮島 2026-04-25.
SAMPLE_ZENSOU = (
    # Racer 3024 (西島　義則): 5 full sessions including 一般 and ＧⅠ grades.
    "3024\t西島　　義則\t20260411\t20260416\t14\t鳴　門\t一般\t１　１２１１６１５２[４]　　　　　　　\t"
    "20260322\t20260325\t13\t尼　崎\t一般\t２３１１１１[１]　　　　　　　　　　　\t"
    "20260311\t20260316\t16\t児　島\t一般\t転６１　３３３２２２２５　　　　　　\t"
    "20260302\t20260305\t15\t丸　亀\t一般\t２４１１６１[１]　　　　　　　　　　　\t"
    "20260220\t20260225\t17\t宮　島\t一般\t５４４６５　Ｆ５２　２　　　　　　　\n"
    # Racer 3159 (江口　晃生): 4 populated sessions, 5th has 欠 token.
    "3159\t江口　　晃生\t20260411\t20260414\t04\t平和島\t一般\t１３２１１２[１]　　　　　　　　　　　\t"
    "20260401\t20260406\t01\t桐　生\t一般\t１１１　１２１６５１[１]　　　　　　　\t"
    "20260312\t20260317\t06\t浜名湖\t一般\t２３３３２１２　Ｆ２３　　　　　　　\t"
    "20260302\t20260305\t15\t丸　亀\t一般\t４６欠欠　　　　　　　　　　　　　　\t"
    "20260221\t20260226\t04\t平和島\t一般\t３１１３２６３　４３１１　　　　　　\n"
    # Racer 0001 (DEBUT): only first 2 sessions populated; sessions 3-5 fully blank.
    "0001\tテスト　　新人\t20260420\t20260425\t05\t多摩川\t一般\t５　　　　　　　　　　　　　　　　　\t"
    "20260401\t20260406\t11\tびわこ\t一般\t６欠　　　　　　　　　　　　　　　　　\t"
    "\t\t\t\t\t\t"
    "\t\t\t\t\t\t"
    "\t\t\t\t\t\n"
)

SAMPLE_HTML_FALLBACK = (
    "<!doctype html>\n<html lang=\"ja\">\n  <head>\n"
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def test_build_url_national():
    scraper = RecentFormScraper()
    assert scraper._build_url("2026-04-25", 17, "national") == (
        "https://race.boatcast.jp/hp_txt/17/bc_zensou_20260425_17.txt"
    )


def test_build_url_local():
    scraper = RecentFormScraper()
    assert scraper._build_url("2026-04-25", 17, "local") == (
        "https://race.boatcast.jp/hp_txt/17/bc_zensou_touchi_20260425_17.txt"
    )


def test_build_url_zero_pads_stadium():
    scraper = RecentFormScraper()
    assert scraper._build_url("2026-01-01", 1, "national") == (
        "https://race.boatcast.jp/hp_txt/01/bc_zensou_20260101_01.txt"
    )


def test_build_url_unknown_variant_raises():
    scraper = RecentFormScraper()
    with pytest.raises(ValueError):
        scraper.scrape_stadium_day("2026-04-25", 17, "bogus")


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("20260411", "2026-04-11"),
        ("20260101", "2026-01-01"),
        ("", ""),
        ("2026", ""),
        ("abc12345", ""),
        ("20260132", "2026-01-32"),  # we do not validate calendar correctness
    ],
)
def test_format_yyyymmdd_to_iso(raw, expected):
    assert _format_yyyymmdd_to_iso(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("鳴　門", "鳴門"),
        ("宮　島", "宮島"),
        ("浜名湖", "浜名湖"),
        ("　　　", ""),
        ("", ""),
    ],
)
def test_normalize_stadium_name(raw, expected):
    assert _normalize_stadium_name(raw) == expected


def test_normalize_finish_sequence_strips_trailing_padding():
    raw = "２３１１１１[１]　　　　　　　　　　　"
    assert _normalize_finish_sequence(raw) == "２３１１１１[１]"


def test_normalize_finish_sequence_keeps_internal_full_width_spaces():
    """Internal 全角 space separates sub-races and must be retained."""
    raw = "５４４６５　Ｆ５２　２　　　　"
    assert _normalize_finish_sequence(raw) == "５４４６５　Ｆ５２　２"


def test_parse_session_block_full():
    block = [
        "20260411",
        "20260416",
        "14",
        "鳴　門",
        "一般",
        "１　１２１１６１５２[４]　　　",
    ]
    session = _parse_session_block(block)
    assert session.start_date == "2026-04-11"
    assert session.end_date == "2026-04-16"
    assert session.stadium_code == "14"
    assert session.stadium_name == "鳴門"
    assert session.grade == "一般"
    assert session.finish_sequence == "１　１２１１６１５２[４]"


def test_parse_session_block_empty():
    block = ["", "", "", "", "", ""]
    session = _parse_session_block(block)
    assert session.start_date is None
    assert session.end_date is None
    assert session.stadium_code is None
    assert session.stadium_name is None
    assert session.grade is None
    assert session.finish_sequence is None


def test_parse_sessions_pads_short_input_to_5():
    """When only 1 block is present, parser still emits 5 sessions (rest empty)."""
    short = ["20260411", "20260416", "14", "鳴　門", "一般", "１"]
    sessions = _parse_sessions(short)
    assert len(sessions) == 5
    assert sessions[0].stadium_code == "14"
    assert sessions[1].stadium_code is None
    assert sessions[4].stadium_code is None


# ---------------------------------------------------------------------------
# TSV body parsing
# ---------------------------------------------------------------------------


def test_scrape_stadium_day_indexes_by_registration_number():
    scraper = RecentFormScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_ZENSOU)
    scraper.session.get = MagicMock(return_value=mock_response)

    index = scraper.scrape_stadium_day("2026-04-25", 17, "national")

    assert index is not None
    assert set(index.keys()) == {"3024", "3159", "0001"}


def test_scrape_stadium_day_first_racer_full_sessions():
    scraper = RecentFormScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_ZENSOU)
    scraper.session.get = MagicMock(return_value=mock_response)

    index = scraper.scrape_stadium_day("2026-04-25", 17, "national")
    name, sessions = index["3024"]
    assert name == "西島 義則"
    assert len(sessions) == 5
    # Most recent session.
    assert sessions[0].start_date == "2026-04-11"
    assert sessions[0].end_date == "2026-04-16"
    assert sessions[0].stadium_code == "14"
    assert sessions[0].stadium_name == "鳴門"
    assert sessions[0].grade == "一般"
    assert sessions[0].finish_sequence == "１　１２１１６１５２[４]"
    # Oldest session has 転覆 token at start of finish_sequence.
    assert sessions[2].finish_sequence.startswith("転６１")
    # 5th session (oldest in the file) populated.
    assert sessions[4].stadium_code == "17"
    assert sessions[4].stadium_name == "宮島"


def test_scrape_stadium_day_newer_racer_has_blank_trailing_sessions():
    scraper = RecentFormScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_ZENSOU)
    scraper.session.get = MagicMock(return_value=mock_response)

    index = scraper.scrape_stadium_day("2026-04-25", 17, "national")
    _name, sessions = index["0001"]
    # First two populated, last three blank.
    assert sessions[0].stadium_code == "05"
    assert sessions[1].stadium_code == "11"
    assert sessions[2].stadium_code is None
    assert sessions[2].stadium_name is None
    assert sessions[2].finish_sequence is None
    assert sessions[3].stadium_code is None
    assert sessions[4].stadium_code is None


def test_scrape_stadium_day_returns_none_on_403():
    scraper = RecentFormScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=403, text=SAMPLE_HTML_FALLBACK)
    scraper.session.get = MagicMock(return_value=mock_response)

    assert scraper.scrape_stadium_day("2026-04-25", 17, "national") is None


def test_scrape_stadium_day_returns_none_on_html_fallback():
    scraper = RecentFormScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_HTML_FALLBACK)
    scraper.session.get = MagicMock(return_value=mock_response)

    assert scraper.scrape_stadium_day("2026-04-25", 17, "national") is None


# ---------------------------------------------------------------------------
# CSV serialisation
# ---------------------------------------------------------------------------


def test_recent_form_headers_have_196_columns():
    """4 race-meta + 6 boats x 32 (2 identity + 5 sessions x 6) = 196."""
    assert len(RECENT_FORM_HEADERS) == 196


def test_recent_form_headers_session_naming():
    assert "艇1_前1節_開始日" in RECENT_FORM_HEADERS
    assert "艇1_前5節_着順列" in RECENT_FORM_HEADERS
    assert "艇6_前5節_着順列" in RECENT_FORM_HEADERS
    # Identity columns precede session columns.
    boat1_first_idx = RECENT_FORM_HEADERS.index("艇1_登録番号")
    boat1_session1_idx = RECENT_FORM_HEADERS.index("艇1_前1節_開始日")
    assert boat1_first_idx < boat1_session1_idx


def test_recent_form_to_row_has_196_cells():
    form = RecentForm(
        date="2026-04-25",
        stadium_number=17,
        race_number=12,
        race_code="202604251712",
        boats=[
            RecentFormBoat(
                boat_number=i,
                sessions=[RecentFormSession() for _ in range(5)],
            )
            for i in range(1, 7)
        ],
    )
    assert len(recent_form_to_row(form)) == 196


def test_recent_form_to_row_first_meta_cells():
    form = RecentForm(
        date="2026-04-25",
        stadium_number=17,
        race_number=12,
        race_code="202604251712",
        boats=[
            RecentFormBoat(
                boat_number=1,
                registration_number="3024",
                racer_name="西島 義則",
                sessions=[
                    RecentFormSession(
                        start_date="2026-04-11",
                        end_date="2026-04-16",
                        stadium_code="14",
                        stadium_name="鳴門",
                        grade="一般",
                        finish_sequence="１",
                    )
                ]
                + [RecentFormSession() for _ in range(4)],
            )
        ]
        + [
            RecentFormBoat(boat_number=i, sessions=[RecentFormSession() for _ in range(5)])
            for i in range(2, 7)
        ],
    )
    row = recent_form_to_row(form)
    assert row[0] == "202604251712"
    assert row[1] == "2026-04-25"
    assert row[2] == "17"
    assert row[3] == "12R"
    # Boat 1 identity.
    assert row[4] == "3024"
    assert row[5] == "西島 義則"
    # Boat 1 前1節 fields (after 4 meta + 2 identity = index 6).
    assert row[6] == "2026-04-11"
    assert row[7] == "2026-04-16"
    assert row[8] == "14"
    assert row[9] == "鳴門"
    assert row[10] == "一般"
    assert row[11] == "１"


def test_recent_forms_to_csv_emits_header_plus_rows():
    forms = [
        RecentForm(
            date="2026-04-25",
            stadium_number=17,
            race_number=r,
            race_code=f"20260425{17:02d}{r:02d}",
            boats=[
                RecentFormBoat(
                    boat_number=i,
                    sessions=[RecentFormSession() for _ in range(5)],
                )
                for i in range(1, 7)
            ],
        )
        for r in (12, 11)
    ]
    csv_text = recent_forms_to_csv(forms, variant="national")
    lines = csv_text.splitlines()
    assert len(lines) == 3
    assert ",11R," in lines[1]
    assert ",12R," in lines[2]


def test_recent_forms_to_csv_local_variant_uses_same_schema():
    """Variant only changes log labelling — header/row layout is identical."""
    forms = [
        RecentForm(
            date="2026-04-25",
            stadium_number=17,
            race_number=1,
            race_code="202604251701",
            boats=[
                RecentFormBoat(
                    boat_number=i,
                    sessions=[RecentFormSession() for _ in range(5)],
                )
                for i in range(1, 7)
            ],
        )
    ]
    nat = recent_forms_to_csv(forms, variant="national")
    loc = recent_forms_to_csv(forms, variant="local")
    # Header line should be identical between variants.
    assert nat.splitlines()[0] == loc.splitlines()[0]
    # Data lines too (same input).
    assert nat.splitlines()[1] == loc.splitlines()[1]
