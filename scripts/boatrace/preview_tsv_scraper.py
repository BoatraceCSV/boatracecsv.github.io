"""Scrape preview (直前情報) data from race.boatcast.jp via plain TSV files.

This is the TSV-based replacement for the old ``preview_scraper`` (which
parsed HTML from ``www.boatrace.jp``). The data is composed from up to four
sibling files served by CloudFront, all UTF-8 plain TSV with no auth:

* ``/hp_txt/{jo}/bc_j_tkz_{ymd}_{jo}_{race}.txt``
    直前情報。選手別の展示タイム / 体重 / 体重調整 / チルト 等。
* ``/hp_txt/{jo}/bc_j_stt_{ymd}_{jo}_{race}.txt``
    スタート展示。進入コース / 枠番 / ST 展示 + F/L フラグ。
* ``/m_txt/{jo}/bc_rs1_2_{ymd}_{jo}_{race}.txt``
    レース確定後の水面気象（最終行）。終了済みのレースで採用。
* ``/m_txt/{jo}/bc_sui_{ymd}_{jo}.txt``
    会場 × 日付の最新水面気象 1 行。bc_rs1_2 が未生成の場合のフォールバック。

Each fetch tolerates the CloudFront 403 + HTML fallback (returned for
non-existent files) the same way as ``original_exhibition_scraper.py``.

The shape of each TSV was reverse-engineered from the SPA bundle on
race.boatcast.jp (``StartDisplay.js`` / ``OriginalDisplayData.js``) and
verified against ``data/previews/2026/04/24.csv``. See
``specs/0XX-preview-tsv-migration/research.md`` for the column-by-column
mapping.

The returned :class:`RacePreview` does **not** populate ``title`` — that
field has no equivalent in boatcast.jp's TSVs. The caller is expected to
post-fill it from the B-file program data.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import PreviewBoatInfo, RacePreview
from .original_exhibition_scraper import _normalize_name, _to_float


class PreviewTsvScraperError(Exception):
    """Preview TSV scraping failed."""

    pass


# --- 風向: 文字列 → 既存 CSV のコード (1〜8) -------------------------------
# 既存 CSV と互換に取るため、boatcast の文字列表現を以下の数値に正規化する。
# 順序は重要: 部分一致回避のため 2 文字方位を先にチェックする。
_WIND_DIRECTION_TO_CODE: List[Tuple[str, int]] = [
    ("北西", 8),
    ("北東", 2),
    ("南西", 6),
    ("南東", 4),
    ("北", 1),
    ("東", 3),
    ("南", 5),
    ("西", 7),
]


# --- 天候: boatcast (1〜9) を CSV に書き出すときの値 ------------------------
# 案 A: 1〜3 は現行 CSV と完全互換。4=雪 / 5=台風 / 6=霧 / 9=その他 は
# boatcast 値を生で書き出す（過去データの 4=大雨 / 5=霧 と意味が衝突する点
# は specs/research.md に記載）。
_VALID_WEATHER_CODES = {1, 2, 3, 4, 5, 6, 9}


class PreviewTsvScraper:
    """Scraper for race preview data composed from boatcast.jp TSV files."""

    def __init__(
        self,
        base_url: str = "https://race.boatcast.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        """Initialize the scraper.

        Args:
            base_url: Base URL for race.boatcast.jp.
            timeout_seconds: HTTP request timeout in seconds.
            rate_limiter: Optional shared :class:`RateLimiter`.
        """
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
        # Cache bc_sui per (date, stadium_code) — same content for all races
        # of the day at a stadium. None marks a confirmed-missing file.
        self._sui_cache: Dict[Tuple[str, int], Optional[List[str]]] = {}

    # ---- Public API -----------------------------------------------------

    def scrape_race_preview(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RacePreview]:
        """Fetch and assemble preview data for a single race.

        Args:
            date: Date in ``YYYY-MM-DD`` form.
            stadium_code: 1..24
            race_number: 1..12

        Returns:
            :class:`RacePreview` (without ``title``), or ``None`` when the
            race file does not exist or essential parsing fails.
        """
        try:
            tkz_body = self._fetch(
                self._build_url("hp_txt", "bc_j_tkz", date, stadium_code, race_number)
            )
            if tkz_body is None:
                return None

            tkz_status, boat_partial = self._parse_tkz(tkz_body)
            if tkz_status is None:
                # The file existed but its body wasn't recognisable.
                return None

            # Start display: required for 進入コース / ST 展示, but if
            # missing we still return weight/exhibition data with those
            # fields blank (matches existing CSV semantics).
            stt_body = self._fetch(
                self._build_url("hp_txt", "bc_j_stt", date, stadium_code, race_number)
            )
            stt_rows = self._parse_stt(stt_body) if stt_body else {}

            # Weather: prefer per-race terminal record, fall back to
            # per-stadium daily snapshot.
            weather = self._fetch_weather(date, stadium_code, race_number)

            boats = self._compose_boats(boat_partial, stt_rows)
            if len(boats) != 6:
                return None

            preview = RacePreview(
                date=date,
                stadium=str(stadium_code),
                race_round=f"{race_number:02d}R",
                title=None,  # Populated by caller from program (B-file).
                race_code=self._race_code(date, stadium_code, race_number),
                stadium_number=stadium_code,
                wind_speed=weather.get("wind_speed"),
                wind_direction=weather.get("wind_direction"),
                wave_height=weather.get("wave_height"),
                weather=weather.get("weather"),
                air_temperature=weather.get("air_temperature"),
                water_temperature=weather.get("water_temperature"),
                boats=boats,
            )
            return preview

        except Exception as e:
            logging_module.warning(
                "preview_tsv_unexpected_error",
                date=date,
                stadium=stadium_code,
                race=race_number,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    # ---- URL & HTTP -----------------------------------------------------

    def _build_url(
        self,
        prefix: str,
        kind: str,
        date: str,
        stadium_code: int,
        race_number: Optional[int],
    ) -> str:
        """Compose a TSV URL.

        ``race_number=None`` produces a per-stadium daily file URL (used
        for ``bc_sui``).
        """
        ymd = date.replace("-", "")
        jo = f"{stadium_code:02d}"
        if race_number is None:
            return f"{self.base_url}/{prefix}/{jo}/{kind}_{ymd}_{jo}.txt"
        rno = f"{race_number:02d}"
        return f"{self.base_url}/{prefix}/{jo}/{kind}_{ymd}_{jo}_{rno}.txt"

    def _fetch(self, url: str) -> Optional[str]:
        """Fetch a TSV body, returning ``None`` for missing files / errors.

        Treats CloudFront 403/404 and the HTML fallback (200 with HTML body)
        all as "not found".
        """
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code in (403, 404):
                logging_module.debug(
                    "preview_tsv_not_found",
                    url=url,
                    status_code=response.status_code,
                )
                return None
            if response.status_code != 200:
                logging_module.warning(
                    "preview_tsv_http_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text

            # CloudFront sometimes returns 200 with an HTML SPA fallback
            # for non-existent paths. Detect by inspecting the first
            # non-blank character.
            stripped = body.lstrip()
            if stripped.startswith("<"):
                logging_module.debug(
                    "preview_tsv_body_is_html", url=url
                )
                return None

            return body

        except requests.Timeout:
            logging_module.warning("preview_tsv_timeout", url=url)
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "preview_tsv_connection_error", url=url, error=str(e)
            )
            return None

    # ---- Parsers --------------------------------------------------------

    def _parse_tkz(
        self, body: str
    ) -> Tuple[Optional[str], Dict[int, Dict[str, Optional[float]]]]:
        """Parse ``bc_j_tkz``.

        Returns ``(status, {boat_number: {weight, weight_adjustment,
        exhibition_time, tilt_adjustment}})``. ``status`` is ``"1"`` for
        normal, ``"2"`` for "could not measure", ``"0"`` for "measuring",
        or ``None`` when the body is unparseable.

        The TSV layout (per row, after the ``data=`` and status lines):
            [0] 選手名 (full-width-space padded; not used here)
            [1] 展示タイム (e.g., ``"6.77"``)
            [2] 体重制限フラグ (``"1"`` = under-weight)
            [3] 体重調整 × 10 (e.g., ``"000"`` -> 0.0kg, ``"005"`` -> 0.5kg)
            [4] 体重 kg
            [5] (unused flag)
            [6] チルト (``"- 0.5"`` / ``"+ 0.0"`` / ``"+ 0.5"``)
            [7..] 当日成績フリー形式
        """
        lines = body.splitlines()
        if not lines or not lines[0].lstrip().startswith("data="):
            return None, {}
        if len(lines) < 2:
            return None, {}

        status_field = lines[1].split("\t")[0].strip() or None
        boats: Dict[int, Dict[str, Optional[float]]] = {}

        # Up to 6 boat rows starting from line 3 (index 2).
        boat_rows = []
        for raw in lines[2:]:
            if not raw.strip():
                continue
            boat_rows.append(raw)
            if len(boat_rows) >= 6:
                break

        for boat_num, raw in enumerate(boat_rows, start=1):
            cols = raw.split("\t")
            if len(cols) < 5:
                continue
            entry: Dict[str, Optional[float]] = {
                "exhibition_time": _to_float(cols[1]) if len(cols) > 1 else None,
                "weight": _to_float(cols[4]) if len(cols) > 4 else None,
                "weight_adjustment": self._parse_weight_adjustment(
                    cols[3] if len(cols) > 3 else ""
                ),
                "tilt_adjustment": self._parse_tilt(
                    cols[6] if len(cols) > 6 else ""
                ),
            }
            boats[boat_num] = entry

        return status_field, boats

    def _parse_stt(self, body: str) -> Dict[int, Dict[str, Optional[float]]]:
        """Parse ``bc_j_stt`` into ``{boat_number: {course, start_timing}}``.

        Per-row layout:
            [0] 進入コース (1..6 or non-numeric for 欠場/不明)
            [1] 枠番
            [2] 選手名
            [3] 今節平均ST
            [4] ST 展示数値 (e.g., ``".08"``)
            [5] F/L フラグ (``"F"`` フライング / ``"L"`` 出遅れ / 空)
            [6] 今節スタート順
        """
        result: Dict[int, Dict[str, Optional[float]]] = {}
        lines = body.splitlines()
        if not lines or not lines[0].lstrip().startswith("data="):
            return result
        # Skip data=, status; rest is up to 6 boats.
        for raw in lines[2:]:
            if not raw.strip():
                continue
            cols = raw.split("\t")
            if len(cols) < 5:
                continue
            try:
                boat_num = int(cols[1].strip())
            except ValueError:
                continue
            if boat_num < 1 or boat_num > 6:
                continue
            try:
                course = int(cols[0].strip())
                if course < 1 or course > 6:
                    course = None
            except ValueError:
                course = None

            st_value = cols[4] if len(cols) > 4 else ""
            st_flag = cols[5].strip() if len(cols) > 5 else ""
            result[boat_num] = {
                "course_number": course,
                "start_timing": self._parse_start_timing(st_value, st_flag),
            }
        return result

    def _fetch_weather(
        self, date: str, stadium_code: int, race_number: int
    ) -> Dict[str, Optional[float]]:
        """Resolve the weather block for a race.

        Strategy:
        1. Fetch ``bc_rs1_2`` (per-race, post-race terminal weather) — its
           **last non-blank line** is the weather row. Used when the race
           is finished.
        2. Fall back to ``bc_sui`` (per-stadium daily latest snapshot) when
           the race hasn't been finalised yet. ``bc_sui`` is cached per
           (date, stadium) so we don't refetch for every race.
        """
        # --- Step 1: bc_rs1_2 ---
        rs_body = self._fetch(
            self._build_url("m_txt", "bc_rs1_2", date, stadium_code, race_number)
        )
        if rs_body:
            weather_line = self._last_weather_line(rs_body)
            if weather_line:
                parsed = self._parse_weather_line(weather_line)
                if parsed:
                    return parsed

        # --- Step 2: bc_sui ---
        cache_key = (date, stadium_code)
        if cache_key not in self._sui_cache:
            sui_body = self._fetch(
                self._build_url("m_txt", "bc_sui", date, stadium_code, None)
            )
            self._sui_cache[cache_key] = (
                sui_body.splitlines() if sui_body else None
            )
        lines = self._sui_cache[cache_key]
        if lines:
            for raw in reversed(lines):
                if raw.strip():
                    parsed = self._parse_weather_line(raw)
                    if parsed:
                        return parsed

        return {}

    @staticmethod
    def _last_weather_line(body: str) -> Optional[str]:
        """Return the last weather-shaped line of a ``bc_rs1_2`` body.

        Weather lines start with a 4-digit time (``HHMM``) and have at
        least 7 tab-separated fields. We scan from the end so that a
        well-formed weather row at the file tail is picked up regardless
        of how many start-timing / placement rows precede it.
        """
        for raw in reversed(body.splitlines()):
            if not raw.strip():
                continue
            fields = raw.split("\t")
            if len(fields) >= 7 and re.fullmatch(r"\d{4}", fields[0].strip()):
                return raw
        return None

    @classmethod
    def _parse_weather_line(cls, line: str) -> Optional[Dict[str, Optional[float]]]:
        """Parse a weather TSV row into the preview's weather block.

        Layout (matching ``StartDisplay.js`` reading of bc_sui /
        bc_rs1_2 last line):
            [0] 時刻 HHMM (unused)
            [1] 天候 (1..9)
            [2] 波高 (cm)
            [3] 風向(全角空白) + ``"(風質)"``
            [4] 風速 (m)
            [5] 気温 ``"+18.0"`` / ``"-1.0"``
            [6] 水温 ``"+15.0"``
        """
        cols = line.split("\t")
        if len(cols) < 7:
            return None
        if not re.fullmatch(r"\d{4}", cols[0].strip()):
            return None
        try:
            return {
                "weather": cls._parse_weather_code(cols[1]),
                "wave_height": _to_float(cols[2]),
                "wind_direction": cls._parse_wind_direction_string(cols[3]),
                "wind_speed": _to_float(cols[4]),
                "air_temperature": cls._parse_temperature(cols[5]),
                "water_temperature": cls._parse_temperature(cols[6]),
            }
        except Exception:
            return None

    # ---- Field-level helpers -------------------------------------------

    @staticmethod
    def _parse_weight_adjustment(raw: str) -> Optional[float]:
        """``"000"`` -> 0.0, ``"005"`` -> 0.5, ``"010"`` -> 1.0.

        The boatcast TSV stores 体重調整 multiplied by 10 as a zero-padded
        integer string. Empty / non-numeric -> ``None``.
        """
        cleaned = raw.strip()
        if not cleaned:
            return None
        try:
            return int(cleaned) / 10.0
        except ValueError:
            return None

    @staticmethod
    def _parse_tilt(raw: str) -> Optional[float]:
        """``"- 0.5"`` -> -0.5, ``"+ 0.0"`` -> 0.0, ``"+ 0.5"`` -> 0.5.

        Internal whitespace (regular or full-width) is stripped before
        ``float()``.
        """
        if raw is None:
            return None
        cleaned = re.sub(r"[\s　]+", "", raw)
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _parse_start_timing(value: str, flag: str) -> Optional[float]:
        """Combine ``[".08"]`` and ``"F"``/``"L"``/``""`` into a float.

        Conventions (matching the existing CSV):
            * ``F`` -> negate. ``F.08`` becomes ``-0.08``.
            * ``L`` -> the racer was 出遅れ (no numeric ST recorded). We
              return ``None`` rather than synthesising a value.
            * empty / other -> use the value as-is, treated as positive.

        Leading dot values (``".08"``) are treated as ``"0.08"``.
        """
        if flag.upper() == "L":
            return None
        cleaned = value.strip() if value else ""
        if not cleaned:
            return None
        if cleaned.startswith("."):
            cleaned = "0" + cleaned
        try:
            num = float(cleaned)
        except ValueError:
            return None
        if flag.upper() == "F":
            num = -num
        return num

    @staticmethod
    def _parse_weather_code(raw: str) -> Optional[int]:
        """Return the weather code as an int, restricted to known values."""
        cleaned = raw.strip()
        if not cleaned:
            return None
        try:
            code = int(cleaned)
        except ValueError:
            return None
        if code in _VALID_WEATHER_CODES:
            return code
        return None

    @staticmethod
    def _parse_wind_direction_string(raw: str) -> Optional[int]:
        """``"南　　(左横風)"`` -> 5 (=南).

        Splits off any parenthesised suffix, strips full-width and ASCII
        whitespace, then matches against the 8-direction table preferring
        2-character directions to avoid partial matches.
        """
        if raw is None:
            return None
        # Trim trailing parenthesised 風質 such as "(左横風)".
        primary = raw.split("　(")[0]
        primary = primary.split("(")[0]
        primary = re.sub(r"[\s　]+", "", primary)
        if not primary:
            return None
        for token, code in _WIND_DIRECTION_TO_CODE:
            if token in primary:
                return code
        return None

    @staticmethod
    def _parse_temperature(raw: str) -> Optional[float]:
        """Strip leading ``+`` (kept by boatcast for positive values).

        Negative values keep their ``-`` sign because we want the actual
        signed temperature.
        """
        if raw is None:
            return None
        cleaned = raw.strip().lstrip("+")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    # ---- Composition ----------------------------------------------------

    @staticmethod
    def _compose_boats(
        tkz_data: Dict[int, Dict[str, Optional[float]]],
        stt_data: Dict[int, Dict[str, Optional[float]]],
    ) -> List[PreviewBoatInfo]:
        """Merge per-boat data from bc_j_tkz and bc_j_stt into a 6-boat list.

        Always returns 6 entries (boats 1..6). Missing values are left as
        ``None`` and the existing CSV writer renders them as blanks.
        """
        boats: List[PreviewBoatInfo] = []
        for boat_num in range(1, 7):
            tkz = tkz_data.get(boat_num) or {}
            stt = stt_data.get(boat_num) or {}
            boats.append(
                PreviewBoatInfo(
                    boat_number=boat_num,
                    course_number=stt.get("course_number"),
                    weight=tkz.get("weight"),
                    weight_adjustment=tkz.get("weight_adjustment"),
                    exhibition_time=tkz.get("exhibition_time"),
                    tilt_adjustment=tkz.get("tilt_adjustment"),
                    start_timing=stt.get("start_timing"),
                )
            )
        return boats

    @staticmethod
    def _race_code(date: str, stadium_code: int, race_number: int) -> str:
        return f"{date.replace('-', '')}{stadium_code:02d}{race_number:02d}"


# Convenience alias so callers reading "_normalize_name" still find it via
# the new module without importing the old one.
__all__ = [
    "PreviewTsvScraper",
    "PreviewTsvScraperError",
    "_normalize_name",
]
