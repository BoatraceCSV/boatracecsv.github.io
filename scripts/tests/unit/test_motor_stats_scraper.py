"""Unit tests for MotorStatsScraper.

Fixtures mirror real ``bc_mst`` and ``bc_mdc`` TSV bodies served by
race.boatcast.jp. SAMPLE_MDC was sampled from
``https://race.boatcast.jp/hp_txt/17/bc_mdc_20251019_17.txt``
(stadium 17, motor period starting 2025-10-19) and trimmed to 3 motors
that exercise the typical ranges:

* row 1 — top-rank motor (motor #25, 勝率 8.10) with 1 優勝 & 1 優出.
* row 2 — mid-rank motor (motor #58, 勝率 6.29) with 0 優勝 / 0 優出.
* row 3 — low-rank motor (motor #12, 勝率 1.38) with all-zero counts.

The expected post-processing values reflect the schema decided in PR-3:
- ★★★/★★ columns are decoded into named fields (rates ÷ 100, dates
  to ISO).
- ★ columns (col[15], col[16], col[21], col[22]) are kept raw as
  ``raw_col_NN`` integers.
"""

from unittest.mock import MagicMock, patch

import pytest

from boatrace.converter import (
    MOTOR_STATS_HEADERS,
    motor_stat_to_row,
    motor_stats_to_csv,
)
from boatrace.models import MotorStat
from boatrace.motor_stats_scraper import (
    MotorStatsScraper,
    _format_stadium,
    _format_yyyymmdd_to_iso,
    _parse_mdc_row,
    _scaled_float,
    _to_int,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# bc_mst: single line containing the motor period start date.
SAMPLE_MST = "20251019\n"

# bc_mdc: 3 motor rows, 33 tab-separated columns each.
SAMPLE_MDC = (
    # Motor 25 — top rank, 1 優勝 / 1 優出, fastest avg lap.
    "20251019\t17\t025\t0810\t001\t08000\t001\t09000\t001\t005\t001\t003\t004\t001\t026\t001\t010\t001\t001\t001\t001\t0677\t008\t1489\t001\t20251022\t000\t003\t002\t000\t000\t000\t20251024\n"
    # Motor 58 — mid rank, 0 優勝 / 0 優出, slower avg lap.
    "20251019\t17\t058\t0629\t008\t07143\t002\t07143\t007\t001\t021\t004\t002\t000\t036\t002\t007\t000\t002\t000\t007\t0684\t030\t1503\t017\t20251019\t000\t000\t000\t000\t001\t000\t20251022\n"
    # Motor 12 — low rank, all zeros.
    "20251019\t17\t012\t0138\t045\t00000\t044\t00000\t045\t000\t036\t000\t036\t000\t036\t008\t008\t000\t002\t000\t007\t0682\t023\t1545\t045\t20251020\t000\t000\t000\t000\t000\t000\t20251024\n"
)

SAMPLE_HTML_FALLBACK = (
    "<!doctype html>\n<html lang=\"ja\">\n  <head>\n"
    "    <title>BOATCAST</title>\n  </head>\n  <body></body>\n</html>\n"
)


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("17", "17"),
        ("1", "01"),
        ("01", "01"),
        ("", None),
        (None, None),
    ],
)
def test_format_stadium(raw, expected):
    assert _format_stadium(raw) == expected


@pytest.mark.parametrize(
    "raw,scale,expected",
    [
        ("0810", 100.0, 8.10),
        ("08000", 100.0, 80.0),
        ("0000", 100.0, 0.0),
        ("", 100.0, None),
        ("-", 100.0, None),
        ("abc", 100.0, None),
    ],
)
def test_scaled_float(raw, scale, expected):
    assert _scaled_float(raw, scale) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("001", 1),
        ("000", 0),
        ("045", 45),
        ("", None),
        ("-", None),
        ("abc", None),
    ],
)
def test_to_int(raw, expected):
    assert _to_int(raw) == expected


def test_format_yyyymmdd_to_iso():
    assert _format_yyyymmdd_to_iso("20251019") == "2025-10-19"
    assert _format_yyyymmdd_to_iso("") == ""
    assert _format_yyyymmdd_to_iso("abc") == ""


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------


def test_parse_mdc_row_top_motor():
    cols = SAMPLE_MDC.splitlines()[0].split("\t")
    stat = _parse_mdc_row(cols, record_date="2026-04-25")
    assert stat is not None
    # Identity
    assert stat.record_date == "2026-04-25"
    assert stat.motor_period_start == "2025-10-19"
    assert stat.stadium_code == "17"
    assert stat.motor_number == 25
    # Rates
    assert stat.win_rate == 8.10
    assert stat.win_rate_rank == 1
    assert stat.double_rate == 80.0
    assert stat.double_rate_rank == 1
    assert stat.triple_rate == 90.0
    assert stat.triple_rate_rank == 1
    # Finishes
    assert stat.first_count == 5
    assert stat.first_rank == 1
    assert stat.second_count == 3
    assert stat.second_rank == 4
    assert stat.third_count == 1
    assert stat.third_rank == 26
    # Confidence ★ raw fields
    assert stat.raw_col_15 == 1
    assert stat.raw_col_16 == 10
    # 優勝 / 優出
    assert stat.championship_count == 1
    assert stat.championship_rank == 1
    assert stat.final_count == 1
    assert stat.final_rank == 1
    # Confidence ★ raw fields
    assert stat.raw_col_21 == 677
    assert stat.raw_col_22 == 8
    # Avg lap
    assert stat.avg_lap_seconds == 14.89
    assert stat.avg_lap_rank == 1
    # Dates
    assert stat.first_use_date == "2025-10-22"
    assert stat.last_maintenance_date == "2025-10-24"
    # Maintenance categories
    assert stat.maintenance_type1_count == 0
    assert stat.maintenance_type2_count == 3
    assert stat.maintenance_type3_count == 2
    assert stat.maintenance_type4_count == 0
    assert stat.maintenance_type5_count == 0
    assert stat.maintenance_type6_count == 0


def test_parse_mdc_row_mid_motor_zero_championship():
    cols = SAMPLE_MDC.splitlines()[1].split("\t")
    stat = _parse_mdc_row(cols, record_date="2026-04-25")
    assert stat.motor_number == 58
    assert stat.win_rate == 6.29
    assert stat.win_rate_rank == 8
    assert stat.championship_count == 0
    assert stat.championship_rank == 2
    assert stat.final_count == 0
    assert stat.final_rank == 7  # tied with 38 other motors at rank 7


def test_parse_mdc_row_low_motor():
    cols = SAMPLE_MDC.splitlines()[2].split("\t")
    stat = _parse_mdc_row(cols, record_date="2026-04-25")
    assert stat.motor_number == 12
    assert stat.win_rate == 1.38
    assert stat.win_rate_rank == 45
    assert stat.first_count == 0
    assert stat.second_count == 0
    assert stat.third_count == 0
    # Avg lap is slowest in the file.
    assert stat.avg_lap_seconds == 15.45
    assert stat.avg_lap_rank == 45


def test_parse_mdc_row_short_returns_none():
    """Defensive: rows shorter than 33 columns are rejected."""
    cols = ["20251019", "17", "025", "0810"]
    assert _parse_mdc_row(cols, record_date="2026-04-25") is None


def test_parse_mdc_row_missing_motor_number_returns_none():
    cols = SAMPLE_MDC.splitlines()[0].split("\t")
    cols[2] = ""
    assert _parse_mdc_row(cols, record_date="2026-04-25") is None


# ---------------------------------------------------------------------------
# Stadium-level fetch
# ---------------------------------------------------------------------------


def _make_scraper_with_responses(mst_status, mst_text, mdc_status, mdc_text):
    """Create a scraper whose ``session.get`` returns mst then mdc."""
    scraper = MotorStatsScraper(rate_limiter=MagicMock(wait=lambda: None))
    mst_resp = MagicMock(status_code=mst_status, text=mst_text)
    mdc_resp = MagicMock(status_code=mdc_status, text=mdc_text)

    call_order = {"i": 0}

    def fake_get(url, timeout=None):
        i = call_order["i"]
        call_order["i"] += 1
        return [mst_resp, mdc_resp][i]

    scraper.session.get = fake_get
    return scraper


def test_scrape_stadium_returns_3_motors():
    scraper = _make_scraper_with_responses(200, SAMPLE_MST, 200, SAMPLE_MDC)
    motors = scraper.scrape_stadium("2026-04-25", 17)
    assert motors is not None
    assert len(motors) == 3
    assert [m.motor_number for m in motors] == [25, 58, 12]
    # All carry the record_date passed in.
    assert all(m.record_date == "2026-04-25" for m in motors)
    # All share the same period start.
    assert all(m.motor_period_start == "2025-10-19" for m in motors)


def test_scrape_stadium_returns_none_when_mst_missing():
    scraper = _make_scraper_with_responses(403, SAMPLE_HTML_FALLBACK, 200, SAMPLE_MDC)
    assert scraper.scrape_stadium("2026-04-25", 17) is None


def test_scrape_stadium_returns_none_when_mdc_missing():
    scraper = _make_scraper_with_responses(200, SAMPLE_MST, 403, SAMPLE_HTML_FALLBACK)
    assert scraper.scrape_stadium("2026-04-25", 17) is None


def test_scrape_stadium_returns_none_when_mst_unparseable():
    scraper = _make_scraper_with_responses(200, "not a date\n", 200, SAMPLE_MDC)
    assert scraper.scrape_stadium("2026-04-25", 17) is None


# ---------------------------------------------------------------------------
# CSV serialisation
# ---------------------------------------------------------------------------


def test_motor_stats_headers_have_34_columns():
    """1 record_date + 33 source columns (with raw_col_15/16/21/22 kept) = 34."""
    assert len(MOTOR_STATS_HEADERS) == 34


def test_motor_stats_headers_first_columns():
    assert MOTOR_STATS_HEADERS[0] == "記録日"
    assert MOTOR_STATS_HEADERS[1] == "モーター期起算日"
    assert MOTOR_STATS_HEADERS[2] == "場コード"
    assert MOTOR_STATS_HEADERS[3] == "モーター番号"


def test_motor_stats_headers_include_raw_columns():
    assert "raw_col_15" in MOTOR_STATS_HEADERS
    assert "raw_col_16" in MOTOR_STATS_HEADERS
    assert "raw_col_21" in MOTOR_STATS_HEADERS
    assert "raw_col_22" in MOTOR_STATS_HEADERS


def test_motor_stat_to_row_has_34_cells():
    stat = MotorStat(record_date="2026-04-25", motor_number=25)
    assert len(motor_stat_to_row(stat)) == 34


def test_motor_stat_to_row_top_motor_full():
    cols = SAMPLE_MDC.splitlines()[0].split("\t")
    stat = _parse_mdc_row(cols, record_date="2026-04-25")
    row = motor_stat_to_row(stat)
    assert row[0] == "2026-04-25"
    assert row[1] == "2025-10-19"
    assert row[2] == "17"
    assert row[3] == "25"
    # 勝率 8.10 — Python's str(8.1) yields "8.1", so we tolerate both.
    assert row[4] in ("8.1", "8.10")
    # 1着回数 = 5
    assert row[10] == "5"
    # raw_col_15 (★) is preserved as int.
    assert row[16] == "1"
    # 優勝回数 = 1
    assert row[18] == "1"
    # 整備種別2回数 = 3
    assert row[28] == "3"
    # 直近メンテ日
    assert row[33] == "2025-10-24"


def test_motor_stats_to_csv_emits_header_plus_3_rows():
    cols = SAMPLE_MDC.splitlines()
    stats = [_parse_mdc_row(line.split("\t"), record_date="2026-04-25") for line in cols]
    csv_text = motor_stats_to_csv(stats)
    lines = csv_text.splitlines()
    assert len(lines) == 4  # header + 3 motors
    # Sorted by (stadium_code, motor_number) → 12, 25, 58.
    assert lines[1].split(",")[3] == "12"
    assert lines[2].split(",")[3] == "25"
    assert lines[3].split(",")[3] == "58"


def test_motor_stats_to_csv_orders_across_stadiums():
    stats = [
        MotorStat(
            record_date="2026-04-25",
            stadium_code="22",
            motor_number=10,
        ),
        MotorStat(
            record_date="2026-04-25",
            stadium_code="03",
            motor_number=99,
        ),
        MotorStat(
            record_date="2026-04-25",
            stadium_code="03",
            motor_number=1,
        ),
    ]
    csv_text = motor_stats_to_csv(stats)
    lines = csv_text.splitlines()
    # stadium 03 motors first, then stadium 22.
    assert lines[1].split(",")[2:4] == ["03", "1"]
    assert lines[2].split(",")[2:4] == ["03", "99"]
    assert lines[3].split(",")[2:4] == ["22", "10"]
