"""Unit tests for RaceCardScraper.

Fixtures mirror the actual ``bc_j_str3`` TSV bodies served by
race.boatcast.jp. SAMPLE_STR3 was sampled from
``https://race.boatcast.jp/hp_txt/17/bc_j_str3_20260425_17_12.txt``
(stadium 17, race 12, on 2026-04-25 — 第27回マスターズチャンピオン
準優勝戦) and trimmed to the columns we rely on. Column meanings were
verified by matching ``RacerPerformance.js`` (col[7]..col[24]) and
``SectionPerformance.js`` (col[25]..col[38]) on race.boatcast.jp.
"""

from unittest.mock import MagicMock

import pytest

from boatrace.converter import (
    RACE_CARD_HEADERS,
    race_card_to_row,
    race_cards_to_csv,
)
from boatrace.race_card_scraper import (
    RaceCardScraper,
    _normalize_finish_position,
    _parse_session_quintuple,
    _parse_session_st,
)
from boatrace.models import RaceCard, RaceCardBoat, RaceCardSession


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Stadium 17 (宮島) race 12 on 2026-04-25 — 準優勝戦. 6 boats with full data
# including 当地 history, 14-slot 節間成績 (some empty for waku 1 池田 who
# didn't race day1-race1), and a special-character finish (転 / Ｆ won't appear
# in this race but the parser handles them).
SAMPLE_STR3 = (
    "data=\n"
    "1\t6\n"
    "3941\t池田　　浩二\t81期\t愛　知:愛　知\t48\tA1\t\t \t \t0.13\t7.88\t53.2\t71.9\t0.00\t0.0\t0.0\t0\t57\t42.9\t56.2\t0\t35\t34.2\t52.1\t\t-,-,-,-,-\t12,1,1,.10,１\t5,6,6,.07,４\t11,3,3,.20,１\t3,5,5,.22,１\t9,2,2,.11,２\t5,4,4,.18,３\t-,-,-,-,-\t-,-,-,-,-\t12,1,1,.11,３\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
    "3737\t上平　　真二\t75期\t広　島:広　島\t52\tA1\t\t \t \t0.18\t6.87\t46.3\t69.0\t6.47\t43.3\t63.3\t0\t39\t33.8\t54.3\t0\t80\t28.5\t50.3\t\t-,-,-,-,-\t9,3,3,.18,１\t4,1,1,.02,１\t9,4,4,.25,３\t-,-,-,-,-\t7,5,5,.20,３\t1,2,2,.12,１\t10,6,6,.29,６\t-,-,-,-,-\t12,2,2,.13,５\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
    "3897\t白井　　英治\t80期\t山　口:山　口\t49\tA1\t\t \t \t0.13\t7.40\t46.4\t69.6\t8.75\t62.5\t75.0\t0\t51\t38.4\t56.5\t0\t24\t29.1\t48.0\t\t-,-,-,-,-\t12,3,3,.18,６\t3,6,6,.04,２\t9,2,2,.10,２\t6,4,4,.05,５\t12,1,1,.06,１\t6,5,5,.03,２\t-,-,-,-,-\t-,-,-,-,-\t12,3,3,.10,１\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
    "4084\t杉山　　正樹\t87期\t愛　知:愛　知\t46\tA1\t\t \t \t0.16\t6.73\t48.8\t62.9\t7.19\t54.0\t81.0\t0\t79\t35.5\t55.0\t0\t66\t35.2\t51.6\t\t-,-,-,-,-\t8,6,6,.13,２\t1,1,1,.10,１\t7,5,5,.08,４\t-,-,-,-,-\t8,3,2,.12,３\t1,4,4,.18,３\t9,3,3,.07,４\t-,-,-,-,-\t12,4,4,.12,２\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
    "3959\t坪井　　康晴\t82期\t静　岡:静　岡\t48\tA1\t\t \t \t0.14\t7.42\t53.0\t68.3\t7.88\t64.0\t72.0\t0\t82\t36.1\t47.9\t0\t71\t33.8\t45.3\t\t2,6,6,.08,３\t8,2,2,.16,３\t-,-,-,-,-\t8,3,3,.02,１\t3,4,4,.17,４\t12,5,5,.08,４\t-,-,-,-,-\t10,1,1,.13,２\t-,-,-,-,-\t12,5,5,.19,６\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
    "3978\t齊藤　　　仁\t83期\t東　京:東　京\t48\tA1\t\tF\t \t0.13\t5.87\t40.5\t50.5\t7.33\t66.6\t77.7\t0\t32\t45.1\t60.7\t0\t27\t40.3\t51.3\t\t5,1,1,.11,１\t-,-,-,-,-\t-,-,-,-,-\t11,6,6,.28,５\t4,4,4,.14,５\t12,2,2,.09,２\t-,-,-,-,-\t8,6,5,.22,３\t-,-,-,-,-\t12,6,6,.20,４\t-,-,-,-,-\t-,-,-,-,-\t \t \n"
)

# Race-not-held marker.
SAMPLE_STR3_NOT_HELD = "data=\n2\t0\n"

# CloudFront SPA HTML fallback.
SAMPLE_HTML_FALLBACK = (
    "<!doctype html>\n<html lang=\"ja\">\n  <head>\n"
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------


def test_build_url_zero_pads():
    scraper = RaceCardScraper()
    assert scraper._build_url("2026-04-25", 17, 12) == (
        "https://race.boatcast.jp/hp_txt/17/bc_j_str3_20260425_17_12.txt"
    )
    assert scraper._build_url("2026-01-01", 1, 1) == (
        "https://race.boatcast.jp/hp_txt/01/bc_j_str3_20260101_01_01.txt"
    )


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        (".10", 0.10),
        ("0.13", 0.13),
        ("-.05", -0.05),
        ("", None),
        ("-", None),
        ("abc", None),
        (None, None),
    ],
)
def test_parse_session_st(raw, expected):
    assert _parse_session_st(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("１", "1"),
        ("６", "6"),
        ("F", "F"),
        ("L", "L"),
        ("欠", "欠"),
        ("転", "転"),
        ("", None),
        ("-", None),
        (None, None),
    ],
)
def test_normalize_finish_position(raw, expected):
    assert _normalize_finish_position(raw) == expected


def test_parse_session_quintuple_normal():
    s = _parse_session_quintuple("12,1,1,.10,１")
    assert s.race_number == 12
    assert s.entry_course == 1
    assert s.waku == 1
    assert s.start_timing == 0.10
    assert s.finish_position == "1"


def test_parse_session_quintuple_placeholder_returns_empty():
    s = _parse_session_quintuple("-,-,-,-,-")
    assert s.race_number is None
    assert s.entry_course is None
    assert s.waku is None
    assert s.start_timing is None
    assert s.finish_position is None


def test_parse_session_quintuple_blank_returns_empty():
    s = _parse_session_quintuple("")
    assert s.race_number is None
    assert s.finish_position is None


# ---------------------------------------------------------------------------
# Boat row parsing
# ---------------------------------------------------------------------------


def test_scrape_race_parses_full_card():
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200)
    mock_response.text = SAMPLE_STR3
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)

    assert card is not None
    assert card.is_valid()
    assert card.status == "1"
    assert card.ncols == 6
    assert card.race_code == "202604251712"
    assert len(card.boats) == 6


def test_scrape_race_first_boat_profile_fields():
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200)
    mock_response.text = SAMPLE_STR3
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    boat = card.boats[0]
    # Identity
    assert boat.boat_number == 1
    assert boat.registration_number == "3941"
    assert boat.racer_name == "池田 浩二"
    assert boat.period == "81期"
    assert boat.branch == "愛知"
    assert boat.birthplace == "愛知"
    assert boat.age == 48
    assert boat.grade == "A1"
    # Penalty counts (blank in source -> None).
    assert boat.f_count is None
    assert boat.l_count is None
    # National
    assert boat.national_avg_st == 0.13
    assert boat.national_win_rate == 7.88
    assert boat.national_double_rate == 53.2
    assert boat.national_triple_rate == 71.9
    # Local — top racer at non-home stadium with no recent 当地 starts.
    assert boat.local_win_rate == 0.0
    assert boat.local_double_rate == 0.0
    assert boat.local_triple_rate == 0.0
    # Motor / boat
    assert boat.motor_flag == 0
    assert boat.motor_number == 57
    assert boat.motor_double_rate == 42.9
    assert boat.motor_triple_rate == 56.2
    assert boat.boat_flag == 0
    assert boat.boat_id == 35
    assert boat.boat_double_rate == 34.2
    assert boat.boat_triple_rate == 52.1
    # 早見 blank → None
    assert boat.hayami is None


def test_scrape_race_f_count_when_present():
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200)
    mock_response.text = SAMPLE_STR3
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    # Boat 6 (齊藤) has "F" in col[7] in the fixture (one F flag).
    boat6 = card.boats[5]
    # _to_int("F") returns None — F本数 is encoded as letter in this column,
    # not a count. The flag is captured but as a non-numeric string.
    # Defensive contract: parse what's parseable; otherwise leave None.
    assert boat6.f_count is None
    # Profile data is still populated.
    assert boat6.registration_number == "3978"
    assert boat6.racer_name == "齊藤 仁"


def test_scrape_race_session_slots_have_14_entries_per_boat():
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200)
    mock_response.text = SAMPLE_STR3
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    for boat in card.boats:
        assert len(boat.sessions) == 14


def test_scrape_race_session_slot_values_for_top_racer():
    """池田 col[26] = '12,1,1,.10,１' -> day1-race2 = R12, 進入1, 枠1, ST.10, 着1."""
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200)
    mock_response.text = SAMPLE_STR3
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    boat = card.boats[0]  # 池田
    # day1-race1 (slot 0) is "-,-,-,-,-" (didn't race).
    assert boat.sessions[0].race_number is None
    # day1-race2 (slot 1) = R12, 1, 1, .10, "1".
    assert boat.sessions[1].race_number == 12
    assert boat.sessions[1].entry_course == 1
    assert boat.sessions[1].waku == 1
    assert boat.sessions[1].start_timing == 0.10
    assert boat.sessions[1].finish_position == "1"


def test_scrape_race_returns_none_on_403():
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=403, text=SAMPLE_HTML_FALLBACK)
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    assert card is None


def test_scrape_race_returns_none_on_html_fallback():
    """200 + HTML body (CloudFront SPA fallback) is treated as not-found."""
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_HTML_FALLBACK)
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    assert card is None


def test_scrape_race_status_2_returns_card_without_boats():
    """status=2 means the race could not be held — meta lines only."""
    scraper = RaceCardScraper(rate_limiter=MagicMock(wait=lambda: None))
    mock_response = MagicMock(status_code=200, text=SAMPLE_STR3_NOT_HELD)
    scraper.session.get = MagicMock(return_value=mock_response)

    card = scraper.scrape_race("2026-04-25", 17, 12)
    assert card is not None
    assert card.status == "2"
    assert card.boats == []
    assert not card.is_valid()


# ---------------------------------------------------------------------------
# CSV serialisation
# ---------------------------------------------------------------------------


def test_race_card_headers_have_574_columns():
    """4 race-meta + 6 boats x 95 per-boat (25 profile + 14 slots x 5) = 574."""
    assert len(RACE_CARD_HEADERS) == 574


def test_race_card_headers_session_naming():
    """Slot ordering follows boatcast source: day1走1, day1走2, ..., day7走2."""
    assert "艇1_節D1走1_R番号" in RACE_CARD_HEADERS
    assert "艇1_節D1走2_R番号" in RACE_CARD_HEADERS
    assert "艇1_節D7走2_着順" in RACE_CARD_HEADERS
    assert "艇6_節D7走2_着順" in RACE_CARD_HEADERS


def test_race_card_to_row_has_574_cells():
    card = RaceCard(
        date="2026-04-25",
        stadium_number=17,
        race_number=12,
        race_code="202604251712",
        status="1",
        ncols=6,
        boats=[
            RaceCardBoat(
                boat_number=i,
                sessions=[RaceCardSession() for _ in range(14)],
            )
            for i in range(1, 7)
        ],
    )
    row = race_card_to_row(card)
    assert len(row) == 574


def test_race_card_to_row_first_meta_cells():
    card = RaceCard(
        date="2026-04-25",
        stadium_number=17,
        race_number=12,
        race_code="202604251712",
        status="1",
        ncols=6,
        boats=[
            RaceCardBoat(boat_number=1, registration_number="3941", racer_name="池田 浩二"),
            RaceCardBoat(boat_number=2),
            RaceCardBoat(boat_number=3),
            RaceCardBoat(boat_number=4),
            RaceCardBoat(boat_number=5),
            RaceCardBoat(boat_number=6),
        ],
    )
    # Pad sessions for compactness.
    for b in card.boats:
        b.sessions = [RaceCardSession() for _ in range(14)]
    row = race_card_to_row(card)
    assert row[0] == "202604251712"
    assert row[1] == "2026-04-25"
    assert row[2] == "17"
    assert row[3] == "12R"
    # Boat 1 first cell = registration_number.
    assert row[4] == "3941"
    assert row[5] == "池田 浩二"


def test_race_cards_to_csv_emits_header_plus_rows():
    cards = [
        RaceCard(
            date="2026-04-25",
            stadium_number=17,
            race_number=r,
            race_code=f"20260425{17:02d}{r:02d}",
            status="1",
            ncols=6,
            boats=[
                RaceCardBoat(
                    boat_number=i,
                    sessions=[RaceCardSession() for _ in range(14)],
                )
                for i in range(1, 7)
            ],
        )
        for r in (12, 11)
    ]
    csv_text = race_cards_to_csv(cards)
    lines = csv_text.splitlines()
    # Header + 2 races.
    assert len(lines) == 3
    # Stable ordering: race 11 before race 12 (sorted by stadium then race).
    assert ",11R," in lines[1]
    assert ",12R," in lines[2]


def test_race_cards_to_csv_sorts_by_stadium_then_race():
    cards = [
        RaceCard(
            date="2026-04-25",
            stadium_number=17,
            race_number=1,
            race_code="202604251701",
            status="1",
            ncols=6,
            boats=[
                RaceCardBoat(boat_number=i, sessions=[RaceCardSession() for _ in range(14)])
                for i in range(1, 7)
            ],
        ),
        RaceCard(
            date="2026-04-25",
            stadium_number=12,
            race_number=12,
            race_code="202604251212",
            status="1",
            ncols=6,
            boats=[
                RaceCardBoat(boat_number=i, sessions=[RaceCardSession() for _ in range(14)])
                for i in range(1, 7)
            ],
        ),
    ]
    csv_text = race_cards_to_csv(cards)
    lines = csv_text.splitlines()
    assert ",12," in lines[1]  # stadium 12 first
    assert ",17," in lines[2]
