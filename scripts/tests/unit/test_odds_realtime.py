"""Unit tests for the bc_smt_od{1,2,3} -> CSV realtime odds pipeline.

Fixtures are real TSV bodies sampled from race.boatcast.jp on 2026-07-19:

* od1 / od2: Heiwajima (jo=04) Race 1 — cross-checked cell by cell
  against the SPA's オッズ tab rendering the same files.
* od3: Mikuni (jo=10) Race 6 — cross-checked against
  ``bc_smt_best_worst20`` (人気順上位20) for the same race.
"""

from __future__ import annotations

import pytest

from boatrace.odds_realtime import (
    OD1_HEADERS,
    OD2_HEADERS,
    OD3_HEADERS,
    ODDS_HEADERS,
    ODDS_SOURCES,
    OddsRealtimeFetcher,
    build_odds_row,
    exacta_combos,
    pair_combos,
    parse_od1,
    parse_od2,
    parse_od3,
    trifecta_combos,
    trio_combos,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# Heiwajima (jo=04) Race 1 on 2026-07-19 (deadline 11:55, sampled ~11:25).
SAMPLE_OD1 = (
    "data=\n"
    "1\n"
    "3.0\t20.2\t9.3\t24.3\t40.5\t4.6\t60.7\t20.2\t243.0\t40.5\t48.6\t3.8\t"
    "243.0\t34.7\t81.0\t40.5\t17.3\t243.0\t30.3\t34.7\n"
    "1.0\t～\t1.1\t1.8\t～\t12.7\t4.6\t～\t25.5\t1.4\t～\t6.3\t0.0\t～\t0.0\t"
    "2.8\t～\t25.5\t0.0\t～\t0.0\t0.0\t～\t0.0\t0.0\t～\t0.0\t16.0\t～\t51.0\t"
    "2.8\t～\t8.5\t0.0\t～\t0.0\t0.0\t～\t0.0\t0.0\t～\t0.0\t0.0\t～\t0.0\n"
    "2.3\t7.1\t0.0\t0.0\t1.2\t0.0\t0\t0\t0\t0\t0\t0\n"
    "1.0\t0.0\t3.1\t0.0\t1.0\t0.0\t0\t0\t0\t0\t0\t0\n"
    "森作　　雄大\t杢野　　誓良\t富澤　　祐作\t片岡　　秀樹\t関　　　浩哉\t近藤　　稔也\n"
    "1.0\t～\t1.6\t0.0\t～\t0.0\t3.1\t～\t10.5\t0.0\t～\t0.0\t1.0\t～\t1.6\t"
    "0.0\t～\t0.0\n"
)

SAMPLE_OD2 = (
    "data=\n"
    "1\n"
    "森作　　雄大\t12.5\t15.4\t33.3\t4.0\t50.0\t0\n"
    "杢野　　誓良\t14.3\t100.1\t100.1\t40.0\t66.7\t0\n"
    "富澤　　祐作\t15.4\t33.3\t66.7\t18.2\t66.7\t0\n"
    "片岡　　秀樹\t50.0\t100.1\t200.2\t40.0\t66.7\t0\n"
    "関　　　浩哉\t6.6\t12.5\t10.5\t13.3\t18.2\t0\n"
    "近藤　　稔也\t200.2\t100.1\t100.1\t100.1\t66.7\t0\n"
    "3.6\t2.4\t4.1\t4.8\t0.0\t29.2\t0.0\t0.0\t14.6\t0.0\t14.6\t0.0\t29.2\t"
    "0.0\t0.0\n"
)

# Mikuni (jo=10) Race 6 on 2026-07-19. bc_smt_best_worst20 for the same
# moment ranked 4-2-3 (11.7) first and 4-3-2 (12.4) second.
SAMPLE_OD3 = (
    "data=\n"
    "1\n"
    "泉　　　具巳\t64.7\t67.6\t210.2\t140.8\t64.7\t71.1\t145.3\t235.6\t46.0\t"
    "54.6\t177.4\t82.3\t231.6\t168.7\t198.0\t253.0\t325.3\t278.8\t200.9\t"
    "359.6\t0\t0\t0\t0\t0\t\n"
    "是澤　　孝宏\t89.9\t75.5\t195.2\t177.4\t102.7\t33.2\t140.8\t146.9\t68.6\t"
    "32.0\t166.6\t55.1\t179.8\t284.7\t216.9\t310.5\t220.4\t257.8\t106.7\t"
    "414.1\t0\t0\t0\t0\t0\t\n"
    "岩崎　　正哉\t101.2\t62.4\t92.3\t123.1\t104.3\t31.8\t146.9\t106.7\t82.8\t"
    "32.0\t136.6\t66.0\t177.4\t155.2\t168.7\t284.7\t369.3\t244.0\t148.5\t"
    "390.4\t0\t0\t0\t0\t0\t\n"
    "白井　　英治\t29.8\t36.3\t61.5\t43.3\t29.9\t11.7\t59.1\t17.1\t41.4\t12.4\t"
    "52.1\t19.2\t99.0\t86.4\t91.1\t62.1\t56.7\t22.3\t26.4\t58.4\t0\t0\t0\t0\t"
    "0\t\n"
    "内田　　　壮\t390.4\t182.2\t213.5\t414.1\t414.1\t216.9\t390.4\t427.0\t"
    "216.9\t160.7\t333.3\t401.9\t341.6\t369.3\t379.6\t248.4\t650.7\t803.8\t"
    "488.0\t401.9\t0\t0\t0\t0\t0\t\n"
    "西川　新太郎\t350.4\t525.6\t401.9\t621.1\t525.6\t341.6\t162.6\t650.7\t"
    "594.1\t284.7\t216.9\t427.0\t325.3\t123.1\t216.9\t317.8\t650.7\t594.1\t"
    "683.2\t455.5\t0\t0\t0\t0\t0\t\n"
)

HTML_FALLBACK = "<!doctype html><html><body>SPA</body></html>"


# ---------------------------------------------------------------------------
# Combination enumerations
# ---------------------------------------------------------------------------


class TestCombos:
    def test_counts(self):
        assert len(trifecta_combos()) == 120
        assert len(exacta_combos()) == 30
        assert len(pair_combos()) == 15
        assert len(trio_combos()) == 20

    def test_trifecta_order(self):
        combos = trifecta_combos()
        assert combos[0] == (1, 2, 3)
        assert combos[19] == (1, 6, 5)
        # Row for 1着=4 starts at 60; (2,3) is its 6th entry (index 5).
        assert combos[60] == (4, 1, 2)
        assert combos[65] == (4, 2, 3)
        assert combos[-1] == (6, 5, 4)

    def test_exacta_order(self):
        combos = exacta_combos()
        assert combos[:5] == [(1, 2), (1, 3), (1, 4), (1, 5), (1, 6)]
        assert combos[5] == (2, 1)
        assert combos[-1] == (6, 5)

    def test_pair_and_trio_ascending(self):
        assert pair_combos()[0] == (1, 2)
        assert pair_combos()[-1] == (5, 6)
        assert trio_combos()[0] == (1, 2, 3)
        assert trio_combos()[-1] == (4, 5, 6)


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


class TestHeaders:
    def test_lengths(self):
        assert len(OD3_HEADERS) == 6 + 120
        assert len(OD2_HEADERS) == 6 + 30 + 15
        assert len(OD1_HEADERS) == 6 + 20 + 30 + 6 + 12

    def test_headers_dict_covers_sources(self):
        assert set(ODDS_HEADERS) == set(ODDS_SOURCES)

    def test_spot_labels(self):
        assert OD3_HEADERS[6] == "3連単_1-2-3"
        assert OD3_HEADERS[6 + 65] == "3連単_4-2-3"
        assert OD2_HEADERS[6] == "2連単_1-2"
        assert OD2_HEADERS[6 + 30] == "2連複_1=2"
        assert OD1_HEADERS[6] == "3連複_1=2=3"
        assert OD1_HEADERS[6 + 20] == "拡連複_1=2_下限"
        assert OD1_HEADERS[6 + 20 + 30] == "単勝_1"
        assert OD1_HEADERS[6 + 20 + 30 + 6] == "複勝_1_下限"


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


class TestParseOd3:
    def test_values_and_order(self):
        values = parse_od3(SAMPLE_OD3)
        assert values is not None
        assert len(values) == 120
        combos = trifecta_combos()
        by_combo = dict(zip(combos, values))
        # Cross-checked against bc_smt_best_worst20 (人気1位/2位).
        assert by_combo[(4, 2, 3)] == "11.7"
        assert by_combo[(4, 3, 2)] == "12.4"
        assert by_combo[(1, 2, 3)] == "64.7"
        assert by_combo[(6, 5, 4)] == "455.5"

    def test_rejects_bad_status(self):
        body = SAMPLE_OD3.replace("data=\n1\n", "data=\n0\n", 1)
        assert parse_od3(body) is None

    def test_rejects_missing_header(self):
        assert parse_od3("1\n" + SAMPLE_OD3.split("\n", 2)[2]) is None
        assert parse_od3("") is None

    def test_rejects_truncated(self):
        truncated = "\n".join(SAMPLE_OD3.splitlines()[:5])
        assert parse_od3(truncated) is None


class TestParseOd2:
    def test_values_and_order(self):
        values = parse_od2(SAMPLE_OD2)
        assert values is not None
        assert len(values) == 45
        exacta = dict(zip(exacta_combos(), values[:30]))
        quinella = dict(zip(pair_combos(), values[30:]))
        # Cross-checked against the オッズ tab rendering.
        assert exacta[(1, 2)] == "12.5"
        assert exacta[(1, 5)] == "4.0"
        assert exacta[(5, 1)] == "6.6"
        assert exacta[(6, 5)] == "66.7"
        assert quinella[(1, 3)] == "2.4"
        assert quinella[(2, 3)] == "29.2"
        assert quinella[(5, 6)] == "0.0"

    def test_rejects_truncated(self):
        # Drop the trailing 2連複 line.
        truncated = "\n".join(SAMPLE_OD2.splitlines()[:8])
        assert parse_od2(truncated) is None


class TestParseOd1:
    def test_values_and_order(self):
        values = parse_od1(SAMPLE_OD1)
        assert values is not None
        assert len(values) == 68
        trio = dict(zip(trio_combos(), values[:20]))
        wide_flat = values[20:50]
        win = values[50:56]
        place_flat = values[56:68]
        # Cross-checked against the オッズ tab rendering.
        assert trio[(1, 2, 3)] == "3.0"
        assert trio[(3, 4, 5)] == "17.3"
        assert trio[(4, 5, 6)] == "34.7"
        wide = {
            combo: (wide_flat[i * 2], wide_flat[i * 2 + 1])
            for i, combo in enumerate(pair_combos())
        }
        assert wide[(1, 2)] == ("1.0", "1.1")
        assert wide[(3, 4)] == ("16.0", "51.0")
        assert win == ["2.3", "7.1", "0.0", "0.0", "1.2", "0.0"]
        assert place_flat[0:2] == ["1.0", "1.6"]  # 複勝 艇1
        assert place_flat[4:6] == ["3.1", "10.5"]  # 複勝 艇3

    def test_rejects_truncated(self):
        truncated = "\n".join(SAMPLE_OD1.splitlines()[:6])
        assert parse_od1(truncated) is None


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


class TestBuildOddsRow:
    def test_row_aligns_with_headers(self):
        values = parse_od1(SAMPLE_OD1)
        row = build_odds_row(
            race_code="202607190401",
            date_str="2026-07-19",
            stadium_code=4,
            race_number=1,
            deadline_time="11:55",
            fetched_at_iso="2026-07-19T11:50:00+09:00",
            values=values,
        )
        assert len(row) == len(OD1_HEADERS)
        assert row[0] == "202607190401"
        assert row[2] == "04"
        assert row[3] == "01R"
        cell = dict(zip(OD1_HEADERS, row))
        assert cell["3連複_1=2=3"] == "3.0"
        assert cell["拡連複_3=4_上限"] == "51.0"
        assert cell["単勝_1"] == "2.3"
        assert cell["複勝_3_下限"] == "3.1"


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class TestFetcher:
    def test_build_url(self):
        fetcher = OddsRealtimeFetcher()
        url = fetcher._build_url("od3", "2026-07-19", 4, 1)
        assert url == (
            "https://race.boatcast.jp/txt/04/bc_smt_od3_20260719_04_01.txt"
        )

    def test_fetch_values_html_fallback(self, monkeypatch):
        fetcher = OddsRealtimeFetcher()
        monkeypatch.setattr(
            fetcher, "_fetch_body", lambda url: None
        )
        assert fetcher.fetch_values("od3", "2026-07-19", 4, 1) is None

    def test_fetch_values_parses(self, monkeypatch):
        fetcher = OddsRealtimeFetcher()
        monkeypatch.setattr(
            fetcher, "_fetch_body", lambda url: SAMPLE_OD2
        )
        values = fetcher.fetch_values("od2", "2026-07-19", 4, 1)
        assert values is not None and len(values) == 45

    def test_fetch_values_unknown_source(self):
        fetcher = OddsRealtimeFetcher()
        with pytest.raises(ValueError):
            fetcher.fetch_values("od9", "2026-07-19", 4, 1)
