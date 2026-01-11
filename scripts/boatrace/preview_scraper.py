"""Scrape preview (直前情報) data from boatrace website."""

import requests
from typing import Optional, List
from bs4 import BeautifulSoup
from . import logger as logging_module
from .models import RacePreview, PreviewBoatInfo
from .downloader import RateLimiter


class PreviewScraperError(Exception):
    """Preview scraping failed."""

    pass


class PreviewScraper:
    """Scraper for race preview (直前情報) data from HTML."""

    def __init__(
        self,
        base_url: str = "https://www.boatrace.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        """Initialize preview scraper.

        Args:
            base_url: Base URL for boatrace website
            timeout_seconds: Request timeout
            rate_limiter: Optional RateLimiter instance
        """
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    def scrape_race_preview(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RacePreview]:
        """Scrape preview data for a single race.

        Args:
            date: Date string (YYYY-MM-DD format)
            stadium_code: Stadium code (1-24)
            race_number: Race number (1-12)

        Returns:
            RacePreview object or None if data not found
        """
        url = self._build_url(date, stadium_code, race_number)

        try:
            self.rate_limiter.wait()

            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code == 404:
                logging_module.debug(
                    "preview_not_found",
                    url=url,
                    date=date,
                    stadium=stadium_code,
                    race=race_number,
                )
                return None

            if response.status_code != 200:
                logging_module.warning(
                    "preview_scrape_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            html = response.text

            preview = self._parse_preview_html(html, date, stadium_code, race_number)
            if preview:
                logging_module.debug(
                    "preview_scraped",
                    date=date,
                    stadium=stadium_code,
                    race=race_number,
                )
            return preview

        except requests.Timeout:
            logging_module.warning(
                "preview_scrape_timeout",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "preview_scrape_connection_error",
                url=url,
                error=str(e),
            )
            return None
        except Exception as e:
            logging_module.warning(
                "preview_scrape_unexpected_error",
                url=url,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    def _build_url(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> str:
        """Build preview URL.

        Args:
            date: Date string (YYYY-MM-DD format)
            stadium_code: Stadium code (1-24)
            race_number: Race number (1-12)

        Returns:
            URL string
        """
        # Convert date YYYY-MM-DD to YYYYMMDD
        date_yyyymmdd = date.replace("-", "")

        # Format: https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=YYYYMMDD&jcd=NN&rno=N
        return (
            f"{self.base_url}/owpc/pc/race/beforeinfo"
            f"?hd={date_yyyymmdd}&jcd={stadium_code:02d}&rno={race_number}"
        )

    def _parse_preview_html(
        self,
        html: str,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RacePreview]:
        """Parse preview data from HTML.

        Args:
            html: HTML content
            date: Date string (YYYY-MM-DD format)
            stadium_code: Stadium code (1-24)
            race_number: Race number (1-12)

        Returns:
            RacePreview object or None if parsing fails
        """
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Extract weather information
            weather_data = self._parse_weather_info(soup)
            if weather_data is None:
                return None

            # Extract boat information
            boats = self._parse_boat_info(soup)
            if not boats or len(boats) != 6:
                return None

            # Generate race code: YYYYMMDDRRNN (RR=stadium, NN=race#)
            race_code = f"{date.replace('-', '')}{stadium_code:02d}{race_number:02d}"

            preview = RacePreview(
                date=date,
                stadium=str(stadium_code),
                race_round=f"{race_number:02d}R",
                title=self._extract_race_title(soup),
                race_code=race_code,
                stadium_number=stadium_code,
                wind_speed=weather_data.get("wind_speed"),
                wind_direction=weather_data.get("wind_direction"),
                wave_height=weather_data.get("wave_height"),
                weather=weather_data.get("weather"),
                air_temperature=weather_data.get("air_temperature"),
                water_temperature=weather_data.get("water_temperature"),
                boats=boats,
            )

            return preview

        except Exception as e:
            logging_module.debug(
                "preview_parse_error",
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    def _extract_race_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract race title from HTML.

        Args:
            soup: BeautifulSoup object

        Returns:
            Race title or None
        """
        try:
            # Try to find race title in heading tags
            h1 = soup.find("h1")
            if h1:
                return h1.get_text(strip=True)

            h2 = soup.find("h2")
            if h2:
                return h2.get_text(strip=True)

            return None
        except Exception:
            return None

    def _parse_weather_info(self, soup: BeautifulSoup) -> Optional[dict]:
        """Parse weather information from HTML.

        Args:
            soup: BeautifulSoup object

        Returns:
            Dict with weather data or None if parsing fails
        """
        try:
            weather_data = {}

            # Find main content area (similar to PHP scraper's baseXPath)
            main_div = soup.find("main")
            if not main_div:
                return None

            # Extract temperature information
            air_temp_text = self._find_text_by_label(main_div, "気温")
            weather_data["air_temperature"] = self._parse_float(air_temp_text)

            weather_text = self._find_text_by_label(main_div, "天候")
            weather_data["weather"] = self._parse_weather_code(weather_text)

            wind_speed_text = self._find_text_by_label(main_div, "風速")
            weather_data["wind_speed"] = self._parse_float(wind_speed_text)

            wind_dir_text = self._find_text_by_label(main_div, "風向")
            weather_data["wind_direction"] = self._parse_wind_direction(wind_dir_text)

            wave_height_text = self._find_text_by_label(main_div, "波高")
            weather_data["wave_height"] = self._parse_float(wave_height_text)

            water_temp_text = self._find_text_by_label(main_div, "水温")
            weather_data["water_temperature"] = self._parse_float(water_temp_text)

            return weather_data

        except Exception as e:
            logging_module.debug(
                "weather_parse_error",
                error=str(e),
            )
            return None

    def _parse_boat_info(self, soup: BeautifulSoup) -> Optional[List[PreviewBoatInfo]]:
        """Parse boat information from HTML.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of PreviewBoatInfo objects (6 boats) or None if parsing fails
        """
        try:
            boats: List[PreviewBoatInfo] = []

            main_div = soup.find("main")
            if not main_div:
                return None

            # Find table containing racer information
            tables = main_div.find_all("table")
            if not tables:
                return None

            # Process each boat (should be 6)
            for boat_number in range(1, 7):
                boat_info = self._extract_boat_data(soup, boat_number)
                if boat_info:
                    boats.append(boat_info)

            # Return None if we don't have exactly 6 boats
            if len(boats) != 6:
                return None

            return boats

        except Exception as e:
            logging_module.debug(
                "boat_parse_error",
                error=str(e),
            )
            return None

    def _extract_boat_data(
        self,
        soup: BeautifulSoup,
        boat_number: int,
    ) -> Optional[PreviewBoatInfo]:
        """Extract data for a single boat.

        Args:
            soup: BeautifulSoup object
            boat_number: Boat number (1-6)

        Returns:
            PreviewBoatInfo object or None if parsing fails
        """
        try:
            # Find table rows containing boat data
            tables = soup.find_all("table")
            if not tables:
                return None

            # Initialize with boat number
            boat_info = PreviewBoatInfo(boat_number=boat_number)

            # Try to find course number
            # This would typically be extracted from table cells
            course_cell = self._find_boat_cell_value(soup, boat_number, "course")
            boat_info.course_number = self._parse_int(course_cell)

            # Extract weight
            weight_text = self._find_boat_cell_value(soup, boat_number, "weight")
            boat_info.weight = self._parse_float(weight_text)

            # Extract weight adjustment
            weight_adj_text = self._find_boat_cell_value(soup, boat_number, "weight_adj")
            boat_info.weight_adjustment = self._parse_float(weight_adj_text)

            # Extract exhibition time
            exhibit_text = self._find_boat_cell_value(soup, boat_number, "exhibition")
            boat_info.exhibition_time = self._parse_float(exhibit_text)

            # Extract tilt adjustment
            tilt_text = self._find_boat_cell_value(soup, boat_number, "tilt")
            boat_info.tilt_adjustment = self._parse_float(tilt_text)

            # Extract start timing
            start_text = self._find_boat_cell_value(soup, boat_number, "start_timing")
            boat_info.start_timing = self._parse_float(start_text)

            return boat_info

        except Exception:
            return None

    def _find_text_by_label(
        self,
        element,
        label: str,
    ) -> Optional[str]:
        """Find text by associated label.

        Args:
            element: BeautifulSoup element to search in
            label: Label text to search for

        Returns:
            Associated value text or None
        """
        try:
            # Search for text containing the label
            for elem in element.find_all(string=True):
                if label in str(elem):
                    # Get parent element
                    parent = elem.parent
                    if parent:
                        # Try to find value in next sibling or nearby elements
                        next_elem = parent.find_next_sibling()
                        if next_elem:
                            return next_elem.get_text(strip=True)

                        # Try to find in parent's next elements
                        text = parent.get_text(strip=True)
                        # Extract value after label
                        parts = text.split(label)
                        if len(parts) > 1:
                            return parts[1].strip()

            return None
        except Exception:
            return None

    def _find_boat_cell_value(
        self,
        soup: BeautifulSoup,
        boat_number: int,
        field_name: str,
    ) -> Optional[str]:
        """Find boat cell value from tables.

        Args:
            soup: BeautifulSoup object
            boat_number: Boat number (1-6)
            field_name: Field name (course, weight, etc.)

        Returns:
            Cell value text or None
        """
        try:
            # This is a simplified extraction - in production would need more specific selectors
            # based on actual HTML structure analysis
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for idx, row in enumerate(rows):
                    cells = row.find_all(["td", "th"])
                    # Look for boat number in first cell, then extract corresponding field
                    if cells and str(boat_number) in cells[0].get_text():
                        # Return value based on field position/name
                        if len(cells) > 1:
                            # Simplified: return appropriate cell value
                            if field_name == "course" and len(cells) > 1:
                                return cells[1].get_text(strip=True)
                            elif field_name == "weight" and len(cells) > 3:
                                return cells[3].get_text(strip=True)
                            elif field_name == "exhibition" and len(cells) > 4:
                                return cells[4].get_text(strip=True)

            return None
        except Exception:
            return None

    # Utility parsing methods

    @staticmethod
    def _parse_float(value: Optional[str]) -> Optional[float]:
        """Parse float value from string.

        Args:
            value: String value to parse

        Returns:
            Float or None if parsing fails
        """
        if not value:
            return None
        try:
            # Remove any whitespace and non-numeric characters except decimal point
            cleaned = "".join(c for c in value if c.isdigit() or c in ".-")
            if not cleaned:
                return None
            return float(cleaned)
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _parse_int(value: Optional[str]) -> Optional[int]:
        """Parse integer value from string.

        Args:
            value: String value to parse

        Returns:
            Integer or None if parsing fails
        """
        if not value:
            return None
        try:
            # Extract first integer found
            import re

            match = re.search(r"\d+", str(value))
            if match:
                return int(match.group())
            return None
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _parse_weather_code(weather_text: Optional[str]) -> Optional[int]:
        """Parse weather code from weather text.

        Args:
            weather_text: Weather description text

        Returns:
            Weather code (0-5) or None
        """
        if not weather_text:
            return None

        weather_map = {
            "晴": 1,
            "曇": 2,
            "雨": 3,
            "小雨": 3,
            "大雨": 4,
            "霧": 5,
        }

        for key, code in weather_map.items():
            if key in weather_text:
                return code

        return None

    @staticmethod
    def _parse_wind_direction(wind_text: Optional[str]) -> Optional[int]:
        """Parse wind direction code from text.

        Args:
            wind_text: Wind direction text

        Returns:
            Wind direction code (1-4) or None
        """
        if not wind_text:
            return None

        direction_map = {
            "北": 1,
            "東": 2,
            "南": 3,
            "西": 4,
        }

        for key, code in direction_map.items():
            if key in wind_text:
                return code

        return None
