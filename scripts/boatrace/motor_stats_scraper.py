"""Scrape motor period statistics (モーター当期成績) from race.boatcast.jp.

Two TSV endpoints are used per stadium:

* ``/hp_txt/{jo}/bc_mst_{jo}.txt``
    Single-line file containing the current motor-period start date as
    ``YYYYMMDD`` (e.g. ``"20251019"``). Used as a key for the bc_mdc URL.

* ``/hp_txt/{jo}/bc_mdc_{period_start}_{jo}.txt``
    33 tab-separated columns × ~60 motors. Layout (empirically decoded):

      [0]  モーター期起算日 (YYYYMMDD; same in every row of the file)
      [1]  場コード (zero-padded 2-digit)
      [2]  モーター番号
      [3]  勝率 ×100      [4]  勝率順位
      [5]  2連対率 ×100   [6]  2連対率順位
      [7]  3連対率 ×100   [8]  3連対率順位
      [9]  1着回数        [10] 1着順位
      [11] 2着回数        [12] 2着順位
      [13] 3着回数        [14] 3着順位
      [15] (★ unknown — kept raw)
      [16] (★ unknown — kept raw)
      [17] 優勝回数       [18] 優勝順位      ← JS-confirmed
      [19] 優出回数       [20] 優出順位      ← JS-confirmed
      [21] (★ unknown — kept raw)
      [22] (★ unknown — kept raw)
      [23] 平均ラップ秒×100  [24] 平均ラップ順位
      [25] 期内初使用日 (YYYYMMDD)
      [26..31] 整備種別1..6回数 (カテゴリ名は要解明)
      [32] 直近メンテ日 (YYYYMMDD)

Confidence levels for each column are documented in the README's
*Motor Stats* section. ★ rows are stored as ``raw_col_NN`` so that the
schema does not need to break when meanings are determined.

Caveat: ``bc_mdc`` only carries the **current** motor period for each
stadium — historical periods are not retained server-side. Backfill is
therefore not possible; only forward-going daily snapshots accumulate
useful time-series data. This is reflected in the ``record_date`` field
of :class:`MotorStat`, which captures the date the snapshot was taken.
"""

from __future__ import annotations

from typing import List, Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import MotorStat


_BC_MDC_NCOLS = 33


class MotorStatsScraperError(Exception):
    """Motor stats scraping failed."""

    pass


class MotorStatsScraper:
    """Fetch ``bc_mst`` + ``bc_mdc`` for a stadium and parse into MotorStat rows."""

    def __init__(
        self,
        base_url: str = "https://race.boatcast.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                )
            }
        )

    # ---- Public API -----------------------------------------------------

    def scrape_stadium(
        self,
        record_date: str,
        stadium_code: int,
    ) -> Optional[List[MotorStat]]:
        """Fetch the motor stats snapshot for one stadium.

        Args:
            record_date: ``YYYY-MM-DD`` — used as the ``record_date`` of
                each emitted :class:`MotorStat`. Caller passes the script's
                date argument (typically yesterday-JST).
            stadium_code: 1..24.

        Returns:
            List of :class:`MotorStat` (one per motor at the stadium), or
            ``None`` when ``bc_mst`` or ``bc_mdc`` is unavailable / unparseable.
        """
        period_start = self._fetch_motor_period(stadium_code)
        if period_start is None:
            logging_module.debug(
                "motor_stats_period_unavailable",
                stadium=stadium_code,
                record_date=record_date,
            )
            return None

        rows = self._fetch_motor_data(stadium_code, period_start)
        if rows is None:
            return None

        period_start_iso = _format_yyyymmdd_to_iso(period_start)

        motors: List[MotorStat] = []
        for cols in rows:
            stat = _parse_mdc_row(
                cols, record_date=record_date, fallback_period_iso=period_start_iso
            )
            if stat is not None:
                motors.append(stat)
        return motors

    # ---- Internal fetches ----------------------------------------------

    def _fetch_motor_period(self, stadium_code: int) -> Optional[str]:
        """Fetch ``bc_mst`` and return the ``YYYYMMDD`` period start, or None."""
        url = f"{self.base_url}/hp_txt/{stadium_code:02d}/bc_mst_{stadium_code:02d}.txt"
        body = self._fetch(url)
        if body is None:
            return None
        for line in body.splitlines():
            cleaned = line.strip()
            if len(cleaned) == 8 and cleaned.isdigit():
                return cleaned
        logging_module.warning(
            "motor_stats_bc_mst_unrecognised", url=url, body_excerpt=body[:80]
        )
        return None

    def _fetch_motor_data(
        self,
        stadium_code: int,
        period_start: str,
    ) -> Optional[List[List[str]]]:
        """Fetch ``bc_mdc`` and return tokenised rows (each a list of strings)."""
        url = (
            f"{self.base_url}/hp_txt/{stadium_code:02d}/"
            f"bc_mdc_{period_start}_{stadium_code:02d}.txt"
        )
        body = self._fetch(url)
        if body is None:
            return None

        rows: List[List[str]] = []
        for raw in body.splitlines():
            if not raw.strip():
                continue
            cols = raw.split("\t")
            if len(cols) < _BC_MDC_NCOLS:
                # Source row malformed; skip silently.
                continue
            rows.append(cols)
        return rows

    def _fetch(self, url: str) -> Optional[str]:
        """Generic GET with the same 403/HTML-fallback handling as siblings."""
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code in (403, 404):
                logging_module.debug(
                    "motor_stats_not_found",
                    url=url,
                    status_code=response.status_code,
                )
                return None
            if response.status_code != 200:
                logging_module.warning(
                    "motor_stats_http_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text
            if body.lstrip().startswith("<"):
                logging_module.debug("motor_stats_body_is_html", url=url)
                return None
            return body

        except requests.Timeout:
            logging_module.warning("motor_stats_timeout", url=url)
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "motor_stats_connection_error", url=url, error=str(e)
            )
            return None
        except Exception as e:  # pragma: no cover - defensive
            logging_module.warning(
                "motor_stats_unexpected_error",
                url=url,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None


# ---------------------------------------------------------------------------
# Module-level helpers (also used by tests)
# ---------------------------------------------------------------------------


def _parse_mdc_row(
    cols: List[str],
    record_date: str,
    fallback_period_iso: Optional[str] = None,
) -> Optional[MotorStat]:
    """Parse one ``bc_mdc`` row into a :class:`MotorStat`.

    ``fallback_period_iso`` is used when col[0] cannot be parsed (which
    should not happen — ``bc_mdc`` always carries the period date — but
    we guard defensively).
    """
    if len(cols) < _BC_MDC_NCOLS:
        return None

    motor_number = _to_int(cols[2])
    if motor_number is None:
        # A row without a parseable motor number is useless.
        return None

    period_iso = _format_yyyymmdd_to_iso(cols[0]) or fallback_period_iso

    return MotorStat(
        record_date=record_date,
        motor_period_start=period_iso,
        stadium_code=_format_stadium(cols[1]),
        motor_number=motor_number,
        win_rate=_scaled_float(cols[3], scale=100.0),
        win_rate_rank=_to_int(cols[4]),
        double_rate=_scaled_float(cols[5], scale=100.0),
        double_rate_rank=_to_int(cols[6]),
        triple_rate=_scaled_float(cols[7], scale=100.0),
        triple_rate_rank=_to_int(cols[8]),
        first_count=_to_int(cols[9]),
        first_rank=_to_int(cols[10]),
        second_count=_to_int(cols[11]),
        second_rank=_to_int(cols[12]),
        third_count=_to_int(cols[13]),
        third_rank=_to_int(cols[14]),
        raw_col_15=_to_int(cols[15]),
        raw_col_16=_to_int(cols[16]),
        championship_count=_to_int(cols[17]),
        championship_rank=_to_int(cols[18]),
        final_count=_to_int(cols[19]),
        final_rank=_to_int(cols[20]),
        raw_col_21=_to_int(cols[21]),
        raw_col_22=_to_int(cols[22]),
        avg_lap_seconds=_scaled_float(cols[23], scale=100.0),
        avg_lap_rank=_to_int(cols[24]),
        first_use_date=_format_yyyymmdd_to_iso(cols[25]) or None,
        maintenance_type1_count=_to_int(cols[26]),
        maintenance_type2_count=_to_int(cols[27]),
        maintenance_type3_count=_to_int(cols[28]),
        maintenance_type4_count=_to_int(cols[29]),
        maintenance_type5_count=_to_int(cols[30]),
        maintenance_type6_count=_to_int(cols[31]),
        last_maintenance_date=_format_yyyymmdd_to_iso(cols[32]) or None,
    )


def _format_stadium(raw: str) -> Optional[str]:
    """Zero-pad to 2 digits so values stay text-stable across stadiums 1..9."""
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    if cleaned.isdigit():
        return cleaned.zfill(2)
    return cleaned


def _to_int(raw) -> Optional[int]:
    if raw is None:
        return None
    cleaned = str(raw).strip()
    if not cleaned or cleaned == "-":
        return None
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return None


def _scaled_float(raw, scale: float) -> Optional[float]:
    """Treat ``"0810"`` as 8.10 when ``scale=100``; ``""``/non-numeric -> None."""
    if raw is None:
        return None
    cleaned = str(raw).strip()
    if not cleaned or cleaned == "-":
        return None
    try:
        return float(cleaned) / scale
    except ValueError:
        return None


def _format_yyyymmdd_to_iso(raw: str) -> str:
    """``20251019`` -> ``2025-10-19``. Returns ``""`` for malformed input."""
    if not raw:
        return ""
    cleaned = raw.strip()
    if len(cleaned) != 8 or not cleaned.isdigit():
        return ""
    return f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]}"


__all__ = [
    "MotorStatsScraper",
    "MotorStatsScraperError",
    "_parse_mdc_row",
    "_scaled_float",
    "_to_int",
    "_format_yyyymmdd_to_iso",
    "_format_stadium",
]
