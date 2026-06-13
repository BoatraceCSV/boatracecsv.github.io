"""Unit tests for the bc_rs2 -> CSV realtime payout pipeline.

Fixtures are real TSV bodies sampled from race.boatcast.jp on 2026-05-16.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from boatrace.payout_realtime import (
    PAYOUT_HEADERS,
    Payout,
    RacePayouts,
    append_rows,
    build_payout_row,
    csv_path_for,
    existing_race_codes,
    parse_rs2,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Ashiya (jo=21) Race 12 on 2026-05-16 — 1着=1, 2着=4, 3着=2 (Ikeda Koji etc.)
# Reference: corresponding bc_rs1_2 shows 池田/馬場/瓜生.
SAMPLE_RS2_NORMAL = (
    "1\t0\t\n"
    "1\t-\t4\t460\t円\t2\n"
    "\n"
    "1\t>\t4\t490\t円\t2\n"
    "\n"
    "1\t4\t2\t2,180\t円\t8\n"
    "\n"
    "1\t2\t4\t940\t円\t4\n"
    "\n"
    "1\t=\t4\t230\t円\t2\n"
    "1\t=\t2\t270\t円\t5\n"
    "2\t=\t4\t300\t円\t7\n"
    "\n"
    "\n"
    "\n"
    "1\t130\t円\n"
    "\n"
    "1\t100\t円\n"
    "4\t210\t円\n"
)


# Toda (jo=02) Race 01 on 2026-05-16 — 1着=4, 2着=2, 3着=3.
# Uses ``<`` separator for 2連複 (orientation pointing at 1着=4).
SAMPLE_RS2_LT_SEPARATOR = (
    "1\t0\t\n"
    "4\t-\t2\t950\t円\t4\n"
    "\n"
    "2\t<\t4\t500\t円\t3\n"
    "\n"
    "4\t2\t3\t2,220\t円\t6\n"
    "\n"
    "2\t3\t4\t430\t円\t2\n"
    "\n"
    "2\t=\t4\t160\t円\t2\n"
    "3\t=\t4\t150\t円\t1\n"
    "2\t=\t3\t180\t円\t3\n"
    "\n"
    "\n"
    "\n"
    "4\t310\t円\n"
    "\n"
    "4\t230\t円\n"
    "2\t300\t円\n"
)


# Synthetic: 拡連複 section with only 2 rows (e.g. payout missing for 2-3着).
# Validates that positional invariants are preserved.
SAMPLE_RS2_PARTIAL_KAKUREN = (
    "1\t0\t\n"
    "1\t-\t4\t460\t円\t2\n"
    "\n"
    "1\t>\t4\t490\t円\t2\n"
    "\n"
    "1\t4\t2\t2,180\t円\t8\n"
    "\n"
    "1\t2\t4\t940\t円\t4\n"
    "\n"
    "1\t=\t4\t230\t円\t2\n"
    "1\t=\t2\t270\t円\t5\n"
    "\n"
    "\n"
    "\n"
    "1\t130\t円\n"
    "\n"
    "1\t100\t円\n"
    "4\t210\t円\n"
)


# Synthetic: an unparseable / empty body.
SAMPLE_RS2_EMPTY = ""

SAMPLE_RS2_BLANK = "   \n  \n"

# Synthetic: SPA fallback (caller should reject via _fetch_body before
# parsing, but defensive: parse_rs2 still returns *something* — assert we
# don't crash).
SAMPLE_RS2_HTML = "<!doctype html>\n<html><body>404</body></html>\n"


# ---------------------------------------------------------------------------
# parse_rs2
# ---------------------------------------------------------------------------


class TestParseRs2:
    def test_normal_race_ashiya_r12(self):
        p = parse_rs2(SAMPLE_RS2_NORMAL)
        assert p is not None
        assert p.is_complete

        # 単勝
        assert p.tansho == Payout(combination="1", payout=130, popularity=None)

        # 複勝 (1着 boat1=¥100, 2着 boat4=¥210)
        assert len(p.fukusho) == 2
        assert p.fukusho[0] == Payout(combination="1", payout=100, popularity=None)
        assert p.fukusho[1] == Payout(combination="4", payout=210, popularity=None)

        # 2連単 (1着→2着 1-4)
        assert p.nirentan == Payout(combination="1-4", payout=460, popularity=2)
        # 2連複 (boatcast の `>` を `=` に正規化、組番は smaller-first)
        assert p.nirenpuku == Payout(combination="1=4", payout=490, popularity=2)
        # 3連単 (1-4-2)
        assert p.sanrentan == Payout(combination="1-4-2", payout=2180, popularity=8)
        # 3連複 (1=2=4)
        assert p.sanrenpuku == Payout(combination="1=2=4", payout=940, popularity=4)

        # 拡連複 (1-2着, 1-3着, 2-3着) — file order is invariant.
        assert p.kakurenfuku[0] == Payout(
            combination="1=4", payout=230, popularity=2
        )
        assert p.kakurenfuku[1] == Payout(
            combination="1=2", payout=270, popularity=5
        )
        assert p.kakurenfuku[2] == Payout(
            combination="2=4", payout=300, popularity=7
        )

    def test_normal_race_toda_r1_lt_separator(self):
        """The ``<`` separator for 2連複 should still parse and normalize
        the combination to ascending order ``2=4``.
        """
        p = parse_rs2(SAMPLE_RS2_LT_SEPARATOR)
        assert p is not None
        assert p.is_complete

        assert p.nirentan == Payout(combination="4-2", payout=950, popularity=4)
        assert p.nirenpuku == Payout(combination="2=4", payout=500, popularity=3)
        assert p.sanrentan == Payout(combination="4-2-3", payout=2220, popularity=6)
        assert p.sanrenpuku == Payout(combination="2=3=4", payout=430, popularity=2)

        assert p.tansho == Payout(combination="4", payout=310, popularity=None)
        assert p.fukusho == [
            Payout(combination="4", payout=230, popularity=None),
            Payout(combination="2", payout=300, popularity=None),
        ]

        # 拡連複 positional invariant: 1-2着 (2-4), 1-3着 (3-4), 2-3着 (2-3).
        assert p.kakurenfuku[0] == Payout(
            combination="2=4", payout=160, popularity=2
        )
        assert p.kakurenfuku[1] == Payout(
            combination="3=4", payout=150, popularity=1
        )
        assert p.kakurenfuku[2] == Payout(
            combination="2=3", payout=180, popularity=3
        )

    def test_partial_kakurenfuku_keeps_positional_slots(self):
        """When 拡連複 has fewer than 3 rows, the missing slots stay None
        (do not shift)."""
        p = parse_rs2(SAMPLE_RS2_PARTIAL_KAKUREN)
        assert p is not None
        assert p.is_complete
        assert p.kakurenfuku[0] is not None
        assert p.kakurenfuku[1] is not None
        assert p.kakurenfuku[2] is None  # missing 2-3着 slot

    def test_empty_body_returns_none(self):
        assert parse_rs2(SAMPLE_RS2_EMPTY) is None

    def test_blank_body_returns_none(self):
        assert parse_rs2(SAMPLE_RS2_BLANK) is None

    def test_html_fallback_is_incomplete(self):
        """The SPA fallback shouldn't crash. It will not be ``is_complete``
        because 3連単 is unparseable."""
        p = parse_rs2(SAMPLE_RS2_HTML)
        # Either None or a stub that is not complete.
        assert p is None or not p.is_complete


# ---------------------------------------------------------------------------
# build_payout_row
# ---------------------------------------------------------------------------


class TestBuildPayoutRow:
    def test_normal_race_row(self):
        p = parse_rs2(SAMPLE_RS2_NORMAL)
        assert p is not None
        row = build_payout_row(
            race_code="202605162112",
            date_str="2026-05-16",
            stadium_code=21,
            race_number=12,
            deadline_time="20:53",
            fetched_at_iso="2026-05-16T20:55:00+09:00",
            payouts=p,
        )

        # Header / row length must match.
        assert len(row) == len(PAYOUT_HEADERS)

        # First 6 columns are common fields.
        assert row[:6] == [
            "202605162112",
            "2026-05-16",
            "21",
            "12R",
            "20:53",
            "2026-05-16T20:55:00+09:00",
        ]

        # Spot-check each bet type ends up in the right column index.
        idx = {h: i for i, h in enumerate(PAYOUT_HEADERS)}
        assert row[idx["単勝_艇番"]] == "1"
        assert row[idx["単勝_払戻金"]] == "130"
        assert row[idx["複勝_1着_艇番"]] == "1"
        assert row[idx["複勝_1着_払戻金"]] == "100"
        assert row[idx["複勝_2着_艇番"]] == "4"
        assert row[idx["複勝_2着_払戻金"]] == "210"
        # 複勝 3着 slot empty when only top-2 paid.
        assert row[idx["複勝_3着_艇番"]] == ""
        assert row[idx["複勝_3着_払戻金"]] == ""

        assert row[idx["2連単_組番"]] == "1-4"
        assert row[idx["2連単_払戻金"]] == "460"
        assert row[idx["2連単_人気"]] == "2"
        assert row[idx["2連複_組番"]] == "1=4"
        assert row[idx["2連複_払戻金"]] == "490"
        assert row[idx["2連複_人気"]] == "2"

        assert row[idx["拡連複_1-2着_組番"]] == "1=4"
        assert row[idx["拡連複_1-2着_払戻金"]] == "230"
        assert row[idx["拡連複_1-2着_人気"]] == "2"
        assert row[idx["拡連複_1-3着_組番"]] == "1=2"
        assert row[idx["拡連複_1-3着_払戻金"]] == "270"
        assert row[idx["拡連複_1-3着_人気"]] == "5"
        assert row[idx["拡連複_2-3着_組番"]] == "2=4"
        assert row[idx["拡連複_2-3着_払戻金"]] == "300"
        assert row[idx["拡連複_2-3着_人気"]] == "7"

        assert row[idx["3連単_組番"]] == "1-4-2"
        assert row[idx["3連単_払戻金"]] == "2180"
        assert row[idx["3連単_人気"]] == "8"
        assert row[idx["3連複_組番"]] == "1=2=4"
        assert row[idx["3連複_払戻金"]] == "940"
        assert row[idx["3連複_人気"]] == "4"

    def test_partial_kakurenfuku_empty_cells(self):
        p = parse_rs2(SAMPLE_RS2_PARTIAL_KAKUREN)
        assert p is not None
        row = build_payout_row(
            race_code="202605162112",
            date_str="2026-05-16",
            stadium_code=21,
            race_number=12,
            deadline_time="20:53",
            fetched_at_iso="2026-05-16T20:55:00+09:00",
            payouts=p,
        )
        idx = {h: i for i, h in enumerate(PAYOUT_HEADERS)}
        # 2-3着 slot stays empty (positional invariant).
        assert row[idx["拡連複_2-3着_組番"]] == ""
        assert row[idx["拡連複_2-3着_払戻金"]] == ""
        assert row[idx["拡連複_2-3着_人気"]] == ""


# ---------------------------------------------------------------------------
# CSV path / dedup
# ---------------------------------------------------------------------------


class TestCsvPathFor:
    def test_resolves_data_results_payouts_path(self, tmp_path: Path):
        path = csv_path_for(tmp_path, "2026-05-16")
        assert path == tmp_path / "data" / "results" / "payouts" / "2026" / "05" / "16.csv"


class TestExistingRaceCodes:
    def test_returns_empty_when_file_missing(self, tmp_path: Path):
        assert existing_race_codes(tmp_path / "missing.csv") == set()

    def test_returns_codes_from_first_column(self, tmp_path: Path):
        path = tmp_path / "p.csv"
        path.write_text(
            "レースコード,foo\n202605160101,a\n202605160102,b\n",
            encoding="utf-8",
        )
        assert existing_race_codes(path) == {"202605160101", "202605160102"}


class TestAppendRows:
    def test_writes_header_on_first_call_then_appends(self, tmp_path: Path):
        path = tmp_path / "p.csv"
        n1 = append_rows(path, PAYOUT_HEADERS, [["202605160101"] + [""] * (len(PAYOUT_HEADERS) - 1)])
        n2 = append_rows(path, PAYOUT_HEADERS, [["202605160102"] + [""] * (len(PAYOUT_HEADERS) - 1)])
        assert n1 == 1 and n2 == 1
        lines = path.read_text(encoding="utf-8").splitlines()
        # 1 header + 2 data rows
        assert len(lines) == 3
        assert lines[0].startswith("レースコード,")
        assert lines[1].startswith("202605160101")
        assert lines[2].startswith("202605160102")

    def test_no_op_when_rows_empty(self, tmp_path: Path):
        path = tmp_path / "p.csv"
        assert append_rows(path, PAYOUT_HEADERS, []) == 0
        assert not path.exists()
