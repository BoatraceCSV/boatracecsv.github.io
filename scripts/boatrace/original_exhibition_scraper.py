"""Scrape original exhibition data (オリジナル展示データ) from race.boatcast.jp.

The site exposes per-race TSV files at:
    https://race.boatcast.jp/txt/{jo:02d}/bc_oriten_{YYYYMMDD}_{jo:02d}_{race:02d}.txt

Format (tab-separated, UTF-8):
    line 1: "data="                (literal marker)
    line 2: "{status}\t{ncols}"    (status: "1" measured, "2" not measurable,
                                    "0" measuring. ncols: 2 or 3)
    line 3: column labels          (e.g. "一　周\tまわり足\t直　線")
    line 4..9: one row per boat    "{boat}\t{racer_name}\t{v1}\t{v2}[\t{v3}]"

A non-existent race returns HTTP 403 with an HTML body (CloudFront error page),
so we detect validity by checking the "data=" prefix rather than the status code.
"""

import re
from typing import Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import OriginalExhibitionBoat, OriginalExhibitionData


class OriginalExhibitionScraperError(Exception):
    """Original exhibition scraping failed."""

    pass


class OriginalExhibitionScraper:
    """Scraper for original exhibition data (オリジナル展示データ)."""

    def __init__(
        self,
        base_url: str = "https://race.boatcast.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        """Initialize scraper.

        Args:
            base_url: Base URL for the boatcast site.
            timeout_seconds: HTTP request timeout.
            rate_limiter: Optional shared RateLimiter.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    # ---- Public API -----------------------------------------------------

    def scrape_race(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[OriginalExhibitionData]:
        """Fetch and parse one race.

        Args:
            date: YYYY-MM-DD
            stadium_code: 1..24
            race_number: 1..12

        Returns:
            OriginalExhibitionData, or None when the race file does not exist
            (non-boat-race day / race not held) or fetch/parse failed.
        """
        url = self._build_url(date, stadium_code, race_number)

        try:
            logging_module.debug(
                "original_exhibition_fetch_start",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )

            self.rate_limiter.wait()

            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code == 403 or response.status_code == 404:
                # Race file does not exist (no race held, or before the date).
                logging_module.debug(
                    "original_exhibition_not_found",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            if response.status_code != 200:
                logging_module.warning(
                    "original_exhibition_http_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text

            # CloudFront sometimes returns 200 with an HTML body when the race
            # does not exist (SPA fallback). Detect via the "data=" marker.
            if not body.lstrip().startswith("data="):
                logging_module.debug(
                    "original_exhibition_body_not_tsv",
                    url=url,
                )
                return None

            return self._parse_tsv(body, date, stadium_code, race_number)

        except requests.Timeout:
            logging_module.warning(
                "original_exhibition_timeout",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "original_exhibition_connection_error",
                url=url,
                error=str(e),
            )
            return None
        except Exception as e:  # pragma: no cover - defensive
            logging_module.warning(
                "original_exhibition_unexpected_error",
                url=url,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    # ---- Helpers --------------------------------------------------------

    def _build_url(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> str:
        """Compose the TSV URL for a single race.

        Args:
            date: YYYY-MM-DD
            stadium_code: 1..24
            race_number: 1..12
        """
        date_yyyymmdd = date.replace("-", "")
        jo = f"{stadium_code:02d}"
        rno = f"{race_number:02d}"
        return f"{self.base_url}/txt/{jo}/bc_oriten_{date_yyyymmdd}_{jo}_{rno}.txt"

    def _parse_tsv(
        self,
        body: str,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[OriginalExhibitionData]:
        """Parse the TSV body into OriginalExhibitionData.

        Returns None when the body is malformed.
        """
        try:
            lines = body.splitlines()
            if len(lines) < 2:
                return None

            # Line 1 is the "data=" marker; sanity-check it.
            if not lines[0].startswith("data="):
                return None

            # Line 2: "{status}\t{ncols}"
            meta_parts = lines[1].split("\t")
            status = meta_parts[0].strip() if meta_parts and meta_parts[0] else None
            measure_count: Optional[int] = None
            if len(meta_parts) >= 2 and meta_parts[1].strip().isdigit():
                measure_count = int(meta_parts[1].strip())

            race_code = self._race_code(date, stadium_code, race_number)

            data = OriginalExhibitionData(
                date=date,
                stadium_number=stadium_code,
                race_number=race_number,
                race_code=race_code,
                status=status if status else None,
                measure_count=measure_count,
            )

            # When the race cannot be measured ("2") or no data yet (status
            # missing / null), the file typically has only the two meta lines.
            if len(lines) < 3:
                return data

            # Line 3: labels for each measurement column.
            label_parts = lines[2].split("\t")
            if measure_count is not None:
                data.measure_labels = [
                    _normalize_label(p) for p in label_parts[:measure_count]
                ]
            else:
                data.measure_labels = [_normalize_label(p) for p in label_parts]

            # Lines 4..9: one per boat. We accept any number up to 6.
            for raw in lines[3:]:
                if not raw.strip():
                    continue
                parts = raw.split("\t")
                if len(parts) < 2:
                    continue
                try:
                    boat_no = int(parts[0].strip())
                except ValueError:
                    continue
                if boat_no < 1 or boat_no > 6:
                    continue

                boat = OriginalExhibitionBoat(
                    boat_number=boat_no,
                    racer_name=_normalize_name(parts[1]) if len(parts) > 1 else None,
                    value1=_to_float(parts[2]) if len(parts) > 2 else None,
                    value2=_to_float(parts[3]) if len(parts) > 3 else None,
                    value3=_to_float(parts[4]) if len(parts) > 4 else None,
                )
                data.boats.append(boat)

                if len(data.boats) >= 6:
                    break

            return data

        except Exception as e:
            logging_module.debug(
                "original_exhibition_parse_error",
                date=date,
                stadium=stadium_code,
                race=race_number,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    @staticmethod
    def _race_code(date: str, stadium_code: int, race_number: int) -> str:
        return f"{date.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _normalize_label(label: str) -> str:
    """Normalize a column label: collapse full-width spaces used for padding."""
    if label is None:
        return ""
    # The source pads labels with full-width spaces, e.g. "一　周".
    return label.replace("\u3000", "").strip()


def _normalize_name(name: str) -> str:
    """Normalize racer name: strip leading/trailing whitespace.

    Full-width spaces inside the name are used as padding by the source
    (e.g. "石渡　　鉄兵"). We collapse them to a single space to keep the
    field readable in CSV output while preserving separation.
    """
    if name is None:
        return ""
    stripped = name.strip().strip("\u3000")
    # Collapse consecutive full-width spaces into a single half-width space.
    return re.sub(r"\u3000+", " ", stripped)


def _to_float(value: str) -> Optional[float]:
    """Parse a float; return None on failure."""
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


__all__ = [
    "OriginalExhibitionScraper",
    "OriginalExhibitionScraperError",
]
