"""Unit tests for preview-realtime.py's select_finished_races.

Regression tests for the 2026-07-28 びわこ SG (オーシャンカップ初日) miss:
live progress drifted 30-50 minutes behind the scheduled deadlines
(7R deadline 13:48 → result recorded 14:01+, 8R deadline 14:20 → result
recorded 15:08), so the legacy fixed ``[deadline+3, deadline+30]`` window
closed before bc_rs1_2 was published and 7R-12R were permanently skipped.
Catch-up mode (``finished_max=None``) keeps unrecorded races eligible for
the rest of the day.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "preview_realtime", SCRIPTS_DIR / "preview-realtime.py"
)
preview_realtime = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(preview_realtime)

from boatrace.holding_list import HoldingRace, build_race_code  # noqa: E402

JST = timezone(timedelta(hours=9))
DATE = "2026-07-28"


def _race(stadium: int, number: int, deadline: str, cancel: str = "") -> HoldingRace:
    return HoldingRace(
        stadium_code=stadium,
        race_number=number,
        deadline_time=deadline,
        cancel_status=cancel,
        title=None,
    )


def _now(hhmm: str) -> datetime:
    hh, mm = hhmm.split(":")
    return datetime(2026, 7, 28, int(hh), int(mm), tzinfo=JST)


def _codes(races):
    return [build_race_code(DATE, r.stadium_code, r.race_number) for r in races]


class TestFixedWindow:
    """Legacy behaviour with an explicit finished_max is unchanged."""

    def test_race_inside_window_selected(self):
        races = [_race(11, 6, "13:17")]
        got = preview_realtime.select_finished_races(
            races, _now("13:30"), DATE, 3, 30, set()
        )
        assert _codes(got) == ["202607281106"]

    def test_race_older_than_max_dropped(self):
        # 8R deadline 14:20 checked at 15:10 → 50 min old > 30 → dropped.
        races = [_race(11, 8, "14:20")]
        got = preview_realtime.select_finished_races(
            races, _now("15:10"), DATE, 3, 30, set()
        )
        assert got == []

    def test_race_newer_than_min_dropped(self):
        races = [_race(11, 8, "14:20")]
        got = preview_realtime.select_finished_races(
            races, _now("14:21"), DATE, 3, 30, set()
        )
        assert got == []


class TestCatchupMode:
    """finished_max=None keeps unrecorded races eligible all day."""

    def test_delayed_race_recovered(self):
        # The exact 2026-07-28 miss: at 16:00 the fixed window had long
        # closed for 7R/8R, catch-up mode still selects them.
        races = [_race(11, 7, "13:48"), _race(11, 8, "14:20")]
        got = preview_realtime.select_finished_races(
            races, _now("16:00"), DATE, 3, None, set()
        )
        assert _codes(got) == ["202607281107", "202607281108"]

    def test_already_recorded_not_retried(self):
        races = [_race(11, 7, "13:48"), _race(11, 8, "14:20")]
        got = preview_realtime.select_finished_races(
            races, _now("16:00"), DATE, 3, None, {"202607281107"}
        )
        assert _codes(got) == ["202607281108"]

    def test_lower_bound_still_applied(self):
        # Deadline not yet finished_min minutes in the past → excluded.
        races = [_race(11, 9, "15:58")]
        got = preview_realtime.select_finished_races(
            races, _now("16:00"), DATE, 3, None, set()
        )
        assert got == []

    def test_cancelled_race_skipped(self):
        races = [_race(9, 11, "15:49", cancel="途中中止")]
        got = preview_realtime.select_finished_races(
            races, _now("17:00"), DATE, 3, None, set()
        )
        assert got == []

    def test_unparseable_deadline_skipped(self):
        # Live holding list rewrites deadline to 締切/確定 post-deadline.
        races = [_race(11, 7, "確定")]
        got = preview_realtime.select_finished_races(
            races, _now("16:00"), DATE, 3, None, set()
        )
        assert got == []


class TestCatchupLimit:
    def test_truncates_oldest_first(self):
        races = [
            _race(11, 9, "14:52"),
            _race(11, 7, "13:48"),
            _race(11, 8, "14:20"),
        ]
        got = preview_realtime.select_finished_races(
            races, _now("17:00"), DATE, 3, None, set(), catchup_limit=2
        )
        assert _codes(got) == ["202607281107", "202607281108"]

    def test_no_truncation_when_under_limit(self):
        races = [_race(11, 7, "13:48")]
        got = preview_realtime.select_finished_races(
            races, _now("17:00"), DATE, 3, None, set(), catchup_limit=15
        )
        assert _codes(got) == ["202607281107"]

    def test_sorted_by_deadline_even_without_limit(self):
        races = [_race(11, 8, "14:20"), _race(11, 7, "13:48")]
        got = preview_realtime.select_finished_races(
            races, _now("17:00"), DATE, 3, None, set()
        )
        assert _codes(got) == ["202607281107", "202607281108"]


class TestCliDefaults:
    def test_default_is_catchup_with_limit(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["preview-realtime.py"])
        args = preview_realtime.parse_args()
        assert args.result_window_max is None
        assert args.result_catchup_limit == 15

    def test_explicit_window_restores_legacy(self, monkeypatch):
        monkeypatch.setattr(
            sys, "argv", ["preview-realtime.py", "--result-window-max", "30"]
        )
        args = preview_realtime.parse_args()
        assert args.result_window_max == 30
