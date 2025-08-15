"""
PDGA Web Scraper
"""

import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TournamentInfo:
    """Data class for tournament information"""
    tournament_id: str
    name: str
    start_date: str
    end_date: str
    location: str
    city: str
    state: str
    country: str
    tournament_type: str
    total_players: int
    url: str


@dataclass
class PlayerRating:
    """Data class for player rating information"""
    player_id: str
    name: str
    current_rating: int
    rating_change: int
    rounds_rated: int
    last_updated: str


@dataclass
class CourseInfo:
    """Data class for course information"""
    course_id: str
    name: str
    city: str
    state: str
    country: str
    holes: int
    par: int
    length: str
    rating: float
    url: str


class PDGAScraper:
    """Main scraper class for PDGA website"""

    def __init__(self, base_url: str = "https://www.pdga.com", delay: float = 1.0):
        """
        Initialize the PDGA scraper

        Args:
            base_url: Base URL for PDGA website
            delay: Delay between requests in seconds (default: 1.0)
        """
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()

        # Set up headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        logger.info(
            f"Initialized PDGA scraper with {delay}s delay between requests")

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """
        Make a request with rate limiting and error handling

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Response object or None if failed
        """
        try:
            logger.debug(f"Making request to: {url}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Rate limiting
            time.sleep(self.delay)

            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    def _parse_html(self, html_content: str) -> BeautifulSoup:
        """Parse HTML content with BeautifulSoup"""
        return BeautifulSoup(html_content, 'html.parser')

    def search_tournaments(self,
                           year: Optional[int] = None,
                           state: Optional[str] = None,
                           tournament_type: Optional[str] = None,
                           limit: int = 50) -> List[TournamentInfo]:
        """
        Search for tournaments based on criteria

        Args:
            year: Tournament year (default: current year)
            state: State abbreviation (e.g., 'CA', 'TX')
            tournament_type: Type of tournament
            limit: Maximum number of tournaments to return

        Returns:
            List of TournamentInfo objects
        """
        if year is None:
            year = datetime.now().year

        # Build search URL - PDGA uses /tour/event/ format
        search_url = f"{self.base_url}/tour/event/search"
        params = {
            'year': year,
            'limit': limit
        }

        if state:
            params['state'] = state
        if tournament_type:
            params['type'] = tournament_type

        response = self._make_request(search_url, params)
        if not response:
            return []

        soup = self._parse_html(response.text)
        tournaments = []

        # Parse tournament results
        # Note: This is a simplified parser - you'll need to adjust based on actual HTML structure
        tournament_elements = soup.find_all('div', class_='tournament-item')

        for element in tournament_elements[:limit]:
            try:
                tournament = self._parse_tournament_element(element)
                if tournament:
                    tournaments.append(tournament)
            except Exception as e:
                logger.warning(f"Failed to parse tournament element: {e}")
                continue

        logger.info(f"Found {len(tournaments)} tournaments")
        return tournaments

    def _parse_tournament_element(self, element) -> Optional[TournamentInfo]:
        """Parse individual tournament element from search results"""
        try:
            # Extract tournament ID from URL
            link = element.find('a', href=True)
            if not link:
                return None

            url = link['href']
            tournament_id = url.split('/')[-1] if '/' in url else url

            # Extract tournament name
            name = link.get_text(strip=True)

            # Extract dates (this will need adjustment based on actual HTML)
            date_element = element.find('span', class_='date')
            start_date = date_element.get_text(
                strip=True) if date_element else ""

            # Extract location
            location_element = element.find('span', class_='location')
            location = location_element.get_text(
                strip=True) if location_element else ""

            # Extract other fields as needed
            # This is a placeholder - you'll need to adjust based on actual HTML structure

            return TournamentInfo(
                tournament_id=tournament_id,
                name=name,
                start_date=start_date,
                end_date="",  # Extract if available
                location=location,
                city="",      # Extract if available
                state="",     # Extract if available
                country="",   # Extract if available
                tournament_type="",  # Extract if available
                total_players=0,     # Extract if available
                url=url
            )

        except Exception as e:
            logger.warning(f"Failed to parse tournament element: {e}")
            return None

    def get_tournament_results(self, tournament_id: str) -> pd.DataFrame:
        """
        Get detailed results for a specific tournament

        Args:
            tournament_id: PDGA tournament ID

        Returns:
            DataFrame with tournament results
        """
        url = f"{self.base_url}/tour/event/{tournament_id}"
        response = self._make_request(url)

        if not response:
            return pd.DataFrame()

        soup = self._parse_html(response.text)

        # Parse results tables - PDGA has multiple tables for different divisions
        all_results = []

        # Look for all tables that contain results
        tables = soup.find_all('table')

        for table in tables:
            # Check if this table contains results data
            headers = table.find_all('th')
            if not headers:
                continue

            header_text = ' '.join(
                [h.get_text(strip=True).lower() for h in headers])

            # Check if this looks like a results table
            if any(keyword in header_text for keyword in ['place', 'points', 'name', 'pdga', 'rating']):
                logger.info(
                    f"Found results table with headers: {[h.get_text(strip=True) for h in headers]}")

                # Extract headers and handle empty ones
                columns = []
                for h in headers:
                    header_text = h.get_text(strip=True)
                    if header_text:
                        columns.append(header_text)
                    else:
                        # Generate a name for empty headers
                        columns.append(f"Col_{len(columns)}")

                # Extract rows for this table
                rows = table.find_all('tr')[1:]  # Skip header row
                table_results = []

                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= len(columns):
                        row_data = [cell.get_text(strip=True)
                                    for cell in cells]
                        # Ensure row_data has same length as columns
                        if len(row_data) > len(columns):
                            row_data = row_data[:len(columns)]
                        elif len(row_data) < len(columns):
                            # Pad with empty strings if row is too short
                            row_data.extend(
                                [''] * (len(columns) - len(row_data)))
                        table_results.append(row_data)

                # Create DataFrame for this table
                if table_results:
                    table_df = pd.DataFrame(table_results, columns=columns)
                    all_results.append(table_df)
                    logger.info(
                        f"Extracted {len(table_df)} results from division table")

        if all_results:
            # Combine all division results
            df = pd.concat(all_results, ignore_index=True)
            logger.info(
                f"Total results extracted: {len(df)} from {len(all_results)} divisions")
        else:
            df = pd.DataFrame()
            logger.warning(
                f"No results data found for tournament {tournament_id}")

        return df

    def get_player_rating(self, player_id: str) -> Optional[PlayerRating]:
        """
        Get current rating for a specific player

        Args:
            player_id: PDGA player ID

        Returns:
            PlayerRating object or None if not found
        """
        url = f"{self.base_url}/player/{player_id}"
        response = self._make_request(url)

        if not response:
            return None

        soup = self._parse_html(response.text)

        try:
            # Extract player name from H1 tags (PDGA uses multiple H1 tags)
            name_elements = soup.find_all('h1')
            name = ""
            for element in name_elements:
                text = element.get_text(strip=True)
                if text and "#" in text:  # Look for "Name #Number" format
                    name = text.split('#')[0].strip()
                    break

            # Extract location from Player Info section
            location = ""
            location_elements = soup.find_all(
                text=re.compile(r'Location:', re.I))
            if location_elements:
                for element in location_elements:
                    parent = element.parent
                    if parent:
                        location_text = parent.get_text()
                        if 'Location:' in location_text:
                            location = location_text.replace(
                                'Location:', '').strip()
                            break

            # Extract current rating (this will need to be updated based on actual structure)
            current_rating = 0
            # Look for rating in the page content
            rating_patterns = [
                r'Current Rating:\s*(\d+)',
                r'Rating:\s*(\d+)',
                r'(\d{3,4})\s*rating'
            ]
            for pattern in rating_patterns:
                matches = re.findall(pattern, response.text, re.I)
                if matches:
                    try:
                        current_rating = int(matches[0])
                        break
                    except ValueError:
                        continue

            # Extract other fields as needed
            # This will need adjustment based on actual HTML structure

            return PlayerRating(
                player_id=player_id,
                name=name,
                current_rating=current_rating,
                rating_change=0,  # Extract if available
                rounds_rated=0,   # Extract if available
                last_updated=""   # Extract if available
            )

        except Exception as e:
            logger.warning(
                f"Failed to parse player rating for {player_id}: {e}")
            return None

    def search_courses(self,
                       city: Optional[str] = None,
                       state: Optional[str] = None,
                       limit: int = 50) -> List[CourseInfo]:
        """
        Search for courses based on criteria

        Args:
            city: City name
            state: State abbreviation
            limit: Maximum number of courses to return

        Returns:
            List of CourseInfo objects
        """
        # Build search URL - PDGA uses /courses/ format
        search_url = f"{self.base_url}/courses/search"
        params = {'limit': limit}

        if city:
            params['city'] = city
        if state:
            params['state'] = state

        response = self._make_request(search_url, params)
        if not response:
            return []

        soup = self._parse_html(response.text)
        courses = []

        # Parse course results
        # This will need adjustment based on actual HTML structure
        course_elements = soup.find_all('div', class_='course-item')

        for element in course_elements[:limit]:
            try:
                course = self._parse_course_element(element)
                if course:
                    courses.append(course)
            except Exception as e:
                logger.warning(f"Failed to parse course element: {e}")
                continue

        logger.info(f"Found {len(courses)} courses")
        return courses

    def _parse_course_element(self, element) -> Optional[CourseInfo]:
        """Parse individual course element from search results"""
        try:
            # Extract course ID from URL
            link = element.find('a', href=True)
            if not link:
                return None

            url = link['href']
            course_id = url.split('/')[-1] if '/' in url else url

            # Extract course name
            name = link.get_text(strip=True)

            # Extract other fields as needed
            # This will need adjustment based on actual HTML structure

            return CourseInfo(
                course_id=course_id,
                name=name,
                city="",      # Extract if available
                state="",     # Extract if available
                country="",   # Extract if available
                holes=0,      # Extract if available
                par=0,        # Extract if available
                length="",    # Extract if available
                rating=0.0,   # Extract if available
                url=url
            )

        except Exception as e:
            logger.warning(f"Failed to parse course element: {e}")
            return None

    def get_course_details(self, course_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific course

        Args:
            course_id: PDGA course ID

        Returns:
            Dictionary with course details
        """
        url = f"{self.base_url}/course/{course_id}"
        response = self._make_request(url)

        if not response:
            return {}

        soup = self._parse_html(response.text)

        # Extract course details
        # This will need adjustment based on actual HTML structure
        details = {}

        # Extract course name
        name_element = soup.find('h1', class_='course-name')
        if name_element:
            details['name'] = name_element.get_text(strip=True)

        # Extract other details as needed
        # This is a placeholder - you'll need to adjust based on actual HTML structure

        logger.info(f"Extracted details for course {course_id}")
        return details

    def close(self):
        """Close the session"""
        self.session.close()
        logger.info("PDGA scraper session closed")

# Convenience functions for common operations


def get_tournament_results(tournament_id: str, delay: float = 1.0) -> pd.DataFrame:
    """Quick function to get tournament results"""
    scraper = PDGAScraper(delay=delay)
    try:
        return scraper.get_tournament_results(tournament_id)
    finally:
        scraper.close()


def get_player_rating(player_id: str, delay: float = 1.0) -> Optional[PlayerRating]:
    """Quick function to get player rating"""
    scraper = PDGAScraper(delay=delay)
    try:
        return scraper.get_player_rating(player_id)
    finally:
        scraper.close()


def search_tournaments(year: Optional[int] = None,
                       state: Optional[str] = None,
                       limit: int = 50,
                       delay: float = 1.0) -> List[TournamentInfo]:
    """Quick function to search tournaments"""
    scraper = PDGAScraper(delay=delay)
    try:
        return scraper.search_tournaments(year=year, state=state, limit=limit)
    finally:
        scraper.close()
