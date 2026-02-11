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
            logging_module.debug(
                "preview_scrape_start",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )

            self.rate_limiter.wait()

            logging_module.debug(
                "preview_http_request",
                url=url,
                timeout=self.timeout_seconds,
            )

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
            # Method 1: Try heading tags
            for tag in ["h1", "h2", "h3"]:
                elem = soup.find(tag)
                if elem:
                    title = elem.get_text(strip=True)
                    if title:
                        return title

            # Method 2: Look for div/span with class containing title
            main_div = soup.find("main")
            if main_div:
                # Check for common title patterns
                for container in main_div.find_all(['div', 'p', 'span']):
                    cls = container.get('class', [])
                    if any('title' in str(c).lower() or 'race' in str(c).lower() for c in cls):
                        text = container.get_text(strip=True)
                        if text and len(text) > 5:
                            return text

            # Method 3: Return None if not found
            return None
        except Exception:
            return None

    def _parse_weather_info(self, soup: BeautifulSoup) -> Optional[dict]:
        """Parse weather information from HTML.

        HTML structure uses weather1_bodyUnit divs with class indicators:
        - is-direction: 気温 (air temperature)
        - is-weather: 天候 (weather code in image class)
        - is-wind: 風速 (wind speed)
        - is-windDirection: 風向 (wind direction code in image class)
        - is-waterTemperature: 水温 (water temperature)
        - is-wave: 波高 (wave height)

        Args:
            soup: BeautifulSoup object

        Returns:
            Dict with weather data or None if parsing fails
        """
        try:
            weather_data = {}

            # Find main content area
            main_div = soup.find("main")
            if not main_div:
                return None

            # Find weather section (weather1_body container)
            weather_body = main_div.find("div", class_="weather1_body")
            if not weather_body:
                return None

            # Get all weather unit divs
            units = weather_body.find_all("div", class_="weather1_bodyUnit")

            # Extract each weather element by class indicator
            for unit in units:
                unit_class = " ".join(unit.get("class", []))

                if "is-direction" in unit_class:
                    # Air temperature
                    label_data = unit.find("span", class_="weather1_bodyUnitLabelData")
                    if label_data:
                        weather_data["air_temperature"] = self._parse_float(label_data.get_text(strip=True))

                elif "is-weather" in unit_class:
                    # Weather code - extract from image class or text
                    img = unit.find("p", class_="weather1_bodyUnitImage")
                    if img:
                        # Get class to determine weather code
                        img_class = " ".join(img.get("class", []))
                        # is-weather1 = 晴, is-weather2 = 曇, etc.
                        weather_data["weather"] = self._extract_weather_code_from_class(img_class)
                    else:
                        # Fallback: get text from span
                        span = unit.find("span", class_="weather1_bodyUnitLabelTitle")
                        if span:
                            weather_data["weather"] = self._parse_weather_code(span.get_text(strip=True))

                elif "is-wind" in unit_class and "is-windDirection" not in unit_class:
                    # Wind speed
                    label_data = unit.find("span", class_="weather1_bodyUnitLabelData")
                    if label_data:
                        wind_text = label_data.get_text(strip=True)
                        # Parse "2m" -> 2.0
                        weather_data["wind_speed"] = self._parse_float(wind_text.replace("m", "").strip())

                elif "is-windDirection" in unit_class:
                    # Wind direction code - extract from image class
                    img = unit.find("p", class_="weather1_bodyUnitImage")
                    if img:
                        # Get class to determine wind direction code
                        img_class = " ".join(img.get("class", []))
                        # is-wind1 = 北, is-wind2 = 北東, etc.
                        weather_data["wind_direction"] = self._extract_wind_code_from_class(img_class)

                elif "is-waterTemperature" in unit_class:
                    # Water temperature
                    label_data = unit.find("span", class_="weather1_bodyUnitLabelData")
                    if label_data:
                        weather_data["water_temperature"] = self._parse_float(label_data.get_text(strip=True))

                elif "is-wave" in unit_class:
                    # Wave height
                    label_data = unit.find("span", class_="weather1_bodyUnitLabelData")
                    if label_data:
                        wave_text = label_data.get_text(strip=True)
                        # Parse "3cm" -> 3.0
                        weather_data["wave_height"] = self._parse_float(wave_text.replace("cm", "").strip())

            return weather_data if weather_data else None

        except Exception as e:
            logging_module.debug(
                "weather_parse_error",
                error=str(e),
            )
            return None

    def _parse_boat_info(self, soup: BeautifulSoup) -> Optional[List[PreviewBoatInfo]]:
        """Parse boat information from HTML.

        HTML structure based on PHP XPath analysis:
        - Course/boat layout table: Contains 6 rows (one per course), each row has boat info
          XPath: div[2]/div[level]/div[2]/div[1]/table/tbody/tr[courseNum]
        - Boat detail table: Contains 6 tbody elements (one per boat), each with weight/adjustment data
          XPath: div[2]/div[level]/div[1]/div[1]/table/tbody[boatNum]

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

            # Initialize boats with boat_number only
            boats_dict = {i: PreviewBoatInfo(boat_number=i) for i in range(1, 7)}

            # Find all tables in main content
            all_tables = main_div.find_all("table", recursive=True)
            if not all_tables:
                return None

            # Strategy: Find the course/boat table and boat detail table by content analysis
            # Look for table that has 6 rows (one per course) with boat numbers
            course_table = None
            boat_detail_table = None

            for table in all_tables:
                tbody = table.find("tbody")
                if not tbody:
                    continue

                rows = tbody.find_all("tr")
                if len(rows) == 6:
                    # Likely course table (6 courses)
                    if not course_table:
                        course_table = table

            # Find boat detail tables (should have 6 tbody elements)
            for table in all_tables:
                tbodies = table.find_all("tbody")
                if len(tbodies) == 6:
                    # Likely boat detail table
                    if not boat_detail_table:
                        boat_detail_table = table

            # === Extract from course table ===
            if course_table:
                tbody = course_table.find("tbody")
                rows = tbody.find_all("tr")
                for course_idx, row in enumerate(rows, 1):  # course_idx: 1-6
                    cells = row.find_all("td")
                    if cells:
                        # First cell usually contains boat number and start timing
                        spans = cells[0].find_all("span")
                        if spans:
                            try:
                                boat_num = int(spans[0].get_text(strip=True))
                                if 1 <= boat_num <= 6:
                                    boats_dict[boat_num].course_number = course_idx

                                    # Start timing may be in span[2] or span[3]
                                    if len(spans) >= 3:
                                        timing_text = spans[2].get_text(strip=True)
                                        boats_dict[boat_num].start_timing = self._parse_float(
                                            timing_text
                                        )
                            except (ValueError, IndexError):
                                pass

            # === Extract from boat detail table ===
            if boat_detail_table:
                tbodies = boat_detail_table.find_all("tbody")
                for boat_num, tbody in enumerate(tbodies, 1):  # boat_num: 1-6
                    if boat_num > 6:
                        break

                    rows = tbody.find_all("tr")
                    if len(rows) >= 3:
                        # First row contains weight and timing data
                        row1_cells = rows[0].find_all("td")
                        if len(row1_cells) >= 6:
                            # Weight is usually in td[3] or td[4]
                            boats_dict[boat_num].weight = self._parse_float(
                                row1_cells[3].get_text(strip=True)
                            )
                            # Exhibition time in td[4] or td[5]
                            boats_dict[boat_num].exhibition_time = self._parse_float(
                                row1_cells[4].get_text(strip=True)
                            )
                            # Tilt adjustment in td[5] or td[6]
                            boats_dict[boat_num].tilt_adjustment = self._parse_float(
                                row1_cells[5].get_text(strip=True)
                            )

                        # Third row contains weight adjustment
                        row3_cells = rows[2].find_all("td")
                        if row3_cells:
                            boats_dict[boat_num].weight_adjustment = self._parse_float(
                                row3_cells[0].get_text(strip=True)
                            )

            # === Extract start timing from ST display table ===
            # Look for table with class "is-w238" which contains ST data
            st_table = None
            for table in all_tables:
                table_class = " ".join(table.get("class", []))
                if "is-w238" in table_class:
                    st_table = table
                    break

            if st_table:
                tbody = st_table.find("tbody", class_="is-p10-0")
                if tbody:
                    rows = tbody.find_all("tr")
                    for course_idx, row in enumerate(rows, 1):  # course_idx: 1-6
                        if course_idx > 6:
                            break

                        # Each row has a div.table1_boatImage1 with boat number and ST
                        boat_image_div = row.find("div", class_="table1_boatImage1")
                        if boat_image_div:
                            # Boat number is in span.table1_boatImage1Number
                            boat_num_span = boat_image_div.find("span", class_="table1_boatImage1Number")
                            if boat_num_span:
                                try:
                                    boat_num = int(boat_num_span.get_text(strip=True))
                                    if 1 <= boat_num <= 6:
                                        # ST value is in span.table1_boatImage1Time
                                        st_span = boat_image_div.find("span", class_="table1_boatImage1Time")
                                        if st_span:
                                            st_text = st_span.get_text(strip=True)
                                            # "F.XX" means false start with negative timing (e.g., "F.01" = -0.01)
                                            if st_text.startswith("F."):
                                                # Convert "F.01" to "-0.01"
                                                st_text = "-0." + st_text[2:]
                                            boats_dict[boat_num].start_timing = self._parse_float(st_text)
                                except (ValueError, AttributeError):
                                    pass

            # Collect boats in order
            for boat_num in range(1, 7):
                boats.append(boats_dict[boat_num])

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

        Searches for label text in div/span elements and extracts associated value.
        Handles both inline and sibling element patterns.

        Args:
            element: BeautifulSoup element to search in
            label: Label text to search for

        Returns:
            Associated value text or None
        """
        try:
            # Method 1: Search for label in all strings (direct match)
            for elem in element.find_all(string=True):
                if label in str(elem):
                    parent = elem.parent
                    if parent:
                        # Try pattern: label and value in same element
                        text = parent.get_text(strip=True)
                        if label in text:
                            # Extract value after label
                            parts = text.split(label)
                            if len(parts) > 1:
                                value = parts[1].strip()
                                # Clean up: remove next label if present
                                for next_label in ["気温", "天候", "風速", "風向", "波高", "水温"]:
                                    if next_label in value:
                                        value = value.split(next_label)[0].strip()
                                        break
                                if value:
                                    return value

                        # Try pattern: label in element, value in next sibling
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            value = next_sibling.get_text(strip=True)
                            if value:
                                return value

                        # Try pattern: label in element, value in next span/div
                        next_elem = parent.find_next()
                        if next_elem:
                            value = next_elem.get_text(strip=True)
                            if value:
                                return value

            # Method 2: Look for div/span containing label, check nearby for value
            for container in element.find_all(['div', 'span']):
                text = container.get_text(strip=True)
                if label in text:
                    # Try to extract value from same container
                    parts = text.split(label)
                    if len(parts) > 1:
                        value = parts[1].strip()
                        # Clean up
                        for next_label in ["気温", "天候", "風速", "風向", "波高", "水温"]:
                            if next_label in value:
                                value = value.split(next_label)[0].strip()
                                break
                        if value:
                            return value

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
            Wind direction code (1-8) or None
        """
        if not wind_text:
            return None

        # Match longer strings first to avoid partial matches
        # (e.g., "北西" before "北")
        direction_map = {
            "北西": 8, "北東": 2, "南西": 6, "南東": 4,
            "北": 1, "東": 3, "南": 5, "西": 7,
        }

        for key, code in direction_map.items():
            if key in wind_text:
                return code

        return None

    @staticmethod
    def _extract_weather_code_from_class(img_class: str) -> Optional[int]:
        """Extract weather code from image class.

        HTML structure uses is-weather1/2/3/4/5 to indicate weather type:
        - is-weather1: 晴 (clear)
        - is-weather2: 曇 (cloudy)
        - is-weather3: 雨 (rain)
        - is-weather4: 大雨 (heavy rain)
        - is-weather5: 霧 (fog)

        Args:
            img_class: Image element class string

        Returns:
            Weather code (1-5) or None
        """
        if not img_class:
            return None

        # Extract numeric code from class like "weather1_bodyUnitImage is-weather1"
        for i in range(1, 6):
            if f"is-weather{i}" in img_class:
                return i

        return None

    @staticmethod
    def _extract_wind_code_from_class(img_class: str) -> Optional[int]:
        """Extract wind direction code from image class.

        HTML structure uses is-wind1..8 to indicate wind direction:
        - is-wind1: 北 (north)
        - is-wind2: 北東 (northeast)
        - is-wind3: 東 (east)
        - is-wind4: 南東 (southeast)
        - is-wind5: 南 (south)
        - is-wind6: 南西 (southwest)
        - is-wind7: 西 (west)
        - is-wind8: 北西 (northwest)

        Args:
            img_class: Image element class string

        Returns:
            Wind direction code (1-8) or None
        """
        if not img_class:
            return None

        wind_map = {
            "is-wind1": 1,  # 北
            "is-wind2": 2,  # 北東
            "is-wind3": 3,  # 東
            "is-wind4": 4,  # 南東
            "is-wind5": 5,  # 南
            "is-wind6": 6,  # 南西
            "is-wind7": 7,  # 西
            "is-wind8": 8,  # 北西
        }

        for wind_class, code in wind_map.items():
            if wind_class in img_class:
                return code

        return None
