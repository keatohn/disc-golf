"""
PDGA Tournament Scraper
"""

import time
import logging
from typing import Dict, List, Optional, Any, Tuple
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
class RoundScore:
    """Data class for individual round scores"""
    round_number: int
    score: int
    rating: int
    course: str
    date: str


@dataclass
class PlayerResult:
    """Data class for player tournament result"""
    player_id: str
    player_name: str
    pdga_number: str
    division: str
    place: int
    total_score: int
    total_rating: int
    rounds: List[RoundScore]
    cash_winnings: float
    points: int


@dataclass
class TournamentDetails:
    """Data class for detailed tournament information"""
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
    divisions: List[str]
    courses: List[str]
    url: str


class PDGATournamentScraper:
    """Specialized scraper for PDGA tournament data"""

    def __init__(self, base_url: str = "https://www.pdga.com", delay: float = 2.0):
        """
        Initialize the tournament scraper

        Args:
            base_url: Base URL for PDGA website
            delay: Delay between requests in seconds (default: 2.0)
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

        logger.info(f"Initialized PDGA tournament scraper with {delay}s delay")

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """Make a request with rate limiting and error handling"""
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

    def get_tournament_details(self, tournament_id: str) -> Optional[TournamentDetails]:
        """
        Get detailed information about a specific tournament

        Args:
            tournament_id: PDGA tournament ID

        Returns:
            TournamentDetails object or None if not found
        """

        url = f"{self.base_url}/tour/event/{tournament_id}"
        response = self._make_request(url)

        if not response:
            return None

        soup = self._parse_html(response.text)

        try:
            # Extract tournament name
            name_element = soup.find('h1') or soup.find('title')
            name = name_element.get_text(
                strip=True) if name_element else "Unknown Tournament"

            # Extract dates (look for common date patterns)
            date_patterns = [
                r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
                r'(\d{4}-\d{2}-\d{2})',      # YYYY-MM-DD
                r'(\w+ \d{1,2},? \d{4})',    # Month DD, YYYY
            ]

            start_date = ""
            end_date = ""
            for pattern in date_patterns:
                dates = re.findall(pattern, response.text)
                if dates:
                    if len(dates) >= 2:
                        start_date, end_date = dates[0], dates[1]
                    elif len(dates) == 1:
                        start_date = dates[0]
                    break

            # Extract location information
            location = ""
            city = ""
            state = ""
            country = ""

            # Look for location patterns in the HTML
            location_elements = soup.find_all(
                text=re.compile(r'Location|Venue|Address', re.I))
            if location_elements:
                for element in location_elements:
                    parent = element.parent
                    if parent:
                        location_text = parent.get_text()
                        # Try to extract city, state from location text
                        state_match = re.search(
                            r'([A-Z]{2})\s*$', location_text)
                        if state_match:
                            state = state_match.group(1)
                        location = location_text.strip()
                        break

            # Extract divisions (look for division headers or lists)
            divisions = []
            division_elements = soup.find_all(
                text=re.compile(r'Division|Div', re.I))
            for element in division_elements:
                parent = element.parent
                if parent:
                    div_text = parent.get_text()
                    # Look for division codes like MPO, FPO, MA1, etc.
                    div_codes = re.findall(
                        r'\b(MPO|FPO|MA[1-4]|FA[1-4]|MP[4-5]|FP[4-5]|MJ[1-8]|FJ[1-8])\b', div_text)
                    divisions.extend(div_codes)

            # Remove duplicates and sort
            divisions = sorted(list(set(divisions)))

            # Extract courses
            courses = []
            course_elements = soup.find_all(
                text=re.compile(r'Course|Hole|Tee', re.I))
            for element in course_elements:
                parent = element.parent
                if parent:
                    course_text = parent.get_text()
                    # Look for course names (this is a simplified approach)
                    if 'course' in course_text.lower() or 'hole' in course_text.lower():
                        courses.append(course_text.strip())

            # Remove duplicates
            courses = list(set(courses))

            return TournamentDetails(
                tournament_id=tournament_id,
                name=name,
                start_date=start_date,
                end_date=end_date,
                location=location,
                city=city,
                state=state,
                country=country,
                tournament_type="",  # Extract if available
                total_players=0,     # Extract if available
                divisions=divisions,
                courses=courses,
                url=url
            )

        except Exception as e:
            logger.warning(
                f"Failed to parse tournament details for {tournament_id}: {e}")
            return None

    def get_tournament_results(self, tournament_id: str, division: Optional[str] = None) -> pd.DataFrame:
        """
        Get detailed results for a specific tournament

        Args:
            tournament_id: PDGA tournament ID
            division: Specific division to get results for (optional)

        Returns:
            DataFrame with tournament results
        """
        # Use the main tournament page - this has the embedded results for completed tournaments
        # The live scores endpoint only works for active tournaments
        result_urls = [
            f"{self.base_url}/tour/event/{tournament_id}",
            f"{self.base_url}/tour/event/{tournament_id}/results",
            f"{self.base_url}/tour/event/{tournament_id}/final-results",
            f"{self.base_url}/tour/event/{tournament_id}/leaderboard",
        ]

        results_df = pd.DataFrame()

        for url in result_urls:
            response = self._make_request(url)
            if response:
                soup = self._parse_html(response.text)
                results_df = self._parse_results_page(soup, division)
                if not results_df.empty:
                    logger.info(f"Successfully parsed results from {url}")
                    break

        if results_df.empty:
            logger.warning(
                f"Could not find results for tournament {tournament_id}")

        return results_df

    def _parse_results_page(self, soup: BeautifulSoup, division: Optional[str] = None) -> pd.DataFrame:
        """Parse the results page HTML to extract tournament results"""
        results_data = []

        # Look for results tables
        tables = soup.find_all('table')

        for table in tables:
            # Check if this looks like a results table
            headers = table.find_all('th')
            if not headers:
                continue

            header_text = [h.get_text(strip=True) for h in headers]
            header_lower = ' '.join(header_text).lower()

            # Skip status/info tables - look for actual player results
            if any(keyword in header_lower for keyword in ['status', 'total players', 'event complete']):
                logger.debug("Skipping status/info table")
                continue

            # Check if this table contains results data
            if any(keyword in header_lower for keyword in ['place', 'player', 'pdga', 'score', 'rating', 'name']):
                logger.info("Found results table")

                # Extract headers
                columns = [h.get_text(strip=True) for h in headers]

                # Extract rows
                rows = table.find_all('tr')[1:]  # Skip header row

                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= len(columns):
                        row_data = [cell.get_text(strip=True)
                                    for cell in cells]

                        # Ensure we have the right number of columns
                        if len(row_data) == len(columns):
                            # Skip rows that look like status/info
                            if any(keyword in ' '.join(row_data).lower() for keyword in ['event complete', 'total players', 'status']):
                                continue
                            results_data.append(row_data)

                # If we found data, break out of table loop
                if results_data:
                    break

        if results_data:
            # Create DataFrame
            df = pd.DataFrame(results_data, columns=columns)

            # Clean up the data
            df = self._clean_results_dataframe(df)

            logger.info(f"Extracted {len(df)} results")
            return df

        return pd.DataFrame()

    def _clean_results_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the results DataFrame"""
        # Remove empty rows
        df = df.dropna(how='all')

        # Clean up column names
        df.columns = df.columns.str.strip()

        # Try to convert numeric columns
        numeric_columns = ['Place', 'Score', 'Rating', 'Total', 'Final']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean up player names
        if 'Player' in df.columns:
            df['Player'] = df['Player'].str.strip()

        return df

    def get_player_round_scores(self, tournament_id: str, player_id: str) -> List[RoundScore]:
        """
        Get round-by-round scores for a specific player in a tournament

        Args:
            tournament_id: PDGA tournament ID
            player_id: PDGA player ID

        Returns:
            List of RoundScore objects
        """
        # This would require accessing individual player result pages
        # Implementation depends on PDGA's URL structure
        url = f"{self.base_url}/tournament/{tournament_id}/player/{player_id}"
        response = self._make_request(url)

        if not response:
            return []

        soup = self._parse_html(response.text)
        rounds = []

        # Look for round-by-round score data
        # This will need to be implemented based on actual HTML structure

        return rounds

    def get_player_tournament_history(self, player_id: str,
                                      start_date: Optional[str] = None,
                                      end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get tournament history for a specific player

        Args:
            player_id: PDGA player ID
            start_date: Start date for search (optional)
            end_date: End date for search (optional)

        Returns:
            List of tournament result dictionaries
        """
        # Use the correct PDGA URL structure: /player/{id}
        url = f"{self.base_url}/player/{player_id}"
        response = self._make_request(url)

        if not response:
            logger.warning(f"Could not access player page for {player_id}")
            return []

        soup = self._parse_html(response.text)
        tournament_history = []

        try:
            # Look for tournament results tables on the player page
            # Based on the actual HTML structure, results are in tables with these headers:
            # ['Place', 'Points', 'Tournament', 'Tier', 'Dates']
            tables = soup.find_all('table')

            for table in tables:
                headers = table.find_all('th')
                if not headers:
                    continue

                header_text = [h.get_text(strip=True) for h in headers]
                header_lower = ' '.join(header_text).lower()

                # Check if this looks like a tournament results table
                if any(keyword in header_lower for keyword in ['place', 'points', 'tournament', 'dates']):
                    logger.info(
                        f"Found tournament results table for player {player_id}")

                    # Parse the table rows
                    rows = table.find_all('tr')[1:]  # Skip header row

                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 3:  # Need at least place, points, tournament
                            try:
                                # Extract tournament information based on actual structure
                                place_text = cells[0].get_text(strip=True)
                                points_text = cells[1].get_text(
                                    strip=True) if len(cells) > 1 else ""
                                tournament_link = cells[2].find(
                                    'a', href=True) if len(cells) > 2 else None
                                tier_text = cells[3].get_text(
                                    strip=True) if len(cells) > 3 else ""
                                dates_text = cells[4].get_text(
                                    strip=True) if len(cells) > 4 else ""

                                if tournament_link and place_text.isdigit():
                                    tournament_url = tournament_link['href']
                                    tournament_name = tournament_link.get_text(
                                        strip=True)

                                    # Extract tournament ID from URL
                                    tournament_id_match = re.search(
                                        r'/tour/event/(\d+)', tournament_url)
                                    if tournament_id_match:
                                        tournament_id = tournament_id_match.group(
                                            1)

                                        # Extract division from the URL fragment
                                        division = ""
                                        if '#' in tournament_url:
                                            division_match = re.search(
                                                r'#([A-Z0-9]+)', tournament_url)
                                            if division_match:
                                                division = division_match.group(
                                                    1)

                                        # Parse points
                                        points = 0.0
                                        try:
                                            points = float(points_text)
                                        except ValueError:
                                            pass

                                        tournament_result = {
                                            'tournament_id': tournament_id,
                                            'tournament_name': tournament_name,
                                            'division': division,
                                            'place': int(place_text),
                                            'points': points,
                                            'tier': tier_text,
                                            'dates': dates_text,
                                            'url': tournament_url
                                        }

                                        tournament_history.append(
                                            tournament_result)

                            except (ValueError, IndexError) as e:
                                logger.debug(
                                    f"Error parsing tournament row: {e}")
                                continue

                    # If we found results, break out of table loop
                    if tournament_history:
                        break

            logger.info(
                f"Found {len(tournament_history)} tournament results for player {player_id}")

        except Exception as e:
            logger.warning(
                f"Error parsing tournament history for player {player_id}: {e}")

        return tournament_history

    def search_tournaments_by_date_range(self,
                                         start_date: str,
                                         end_date: str,
                                         state: Optional[str] = None,
                                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for tournaments within a date range

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            state: State abbreviation (optional)
            limit: Maximum number of tournaments to return

        Returns:
            List of tournament dictionaries
        """
        # Convert dates to datetime objects
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return []

        tournaments = []
        current_date = start_dt

        while current_date <= end_dt and len(tournaments) < limit:
            year = current_date.year
            month = current_date.month

            # Search for tournaments in this month/year
            month_tournaments = self._search_tournaments_by_month(
                year, month, state)
            tournaments.extend(month_tournaments)

            # Move to next month
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            current_date = current_date.replace(year=year, month=month)

        return tournaments[:limit]

    def _search_tournaments_by_month(self, year: int, month: int, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for tournaments in a specific month"""
        # PDGA doesn't have a simple search endpoint, so we'll use a different approach
        # For now, return an empty list - tournaments will be found through player history
        # or by manually providing tournament IDs

        logger.info(
            f"Tournament search for {year}-{month:02d} - using player history approach")

        # TODO: Implement tournament discovery through:
        # 1. Player tournament history pages
        # 2. Event schedule pages
        # 3. Course event listings

        return []

    def get_live_round_scores(self, tournament_id: str, division: str, round_number: int) -> pd.DataFrame:
        """
        Get round scores for a specific tournament, division, and round

        Args:
            tournament_id: PDGA tournament ID
            division: Division code (e.g., 'MPO', 'MA1', 'MA2')
            round_number: Round number (1, 2, 3, etc.)

        Returns:
            DataFrame with round scores
        """
        # First try the live scores endpoint (for active tournaments)
        live_url = f"{self.base_url}/live/event/{tournament_id}/{division}/scores"
        params = {'round': round_number}

        response = self._make_request(live_url, params)
        if response:
            soup = self._parse_html(response.text)
            scores_data = self._parse_live_scores_table(
                soup, tournament_id, division, round_number)

            if not scores_data.empty:
                logger.info(
                    f"Found live scores for tournament {tournament_id}, division {division}, round {round_number}")
                return scores_data

        # If live scores not available, try to get from main tournament page
        logger.info(
            f"Live scores not available, trying main tournament page for {tournament_id}")
        return self._get_round_scores_from_main_page(tournament_id, division, round_number)

    def _parse_live_scores_table(self, soup: BeautifulSoup, tournament_id: str, division: str, round_number: int) -> pd.DataFrame:
        """Parse the live scores table HTML to extract round scores"""
        scores_data = []

        # Look for the scores table
        tables = soup.find_all('table')

        for table in tables:
            headers = table.find_all('th')
            if not headers:
                continue

            header_text = [h.get_text(strip=True) for h in headers]
            header_lower = ' '.join(header_text).lower()

            # Check if this looks like a scores table
            if any(keyword in header_lower for keyword in ['place', 'player', 'pdga', 'score', 'rating']):
                logger.info(
                    f"Found scores table for tournament {tournament_id}, division {division}, round {round_number}")

                # Extract headers
                columns = [h.get_text(strip=True) for h in headers]

                # Extract rows
                rows = table.find_all('tr')[1:]  # Skip header row

                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= len(columns):
                        row_data = [cell.get_text(strip=True)
                                    for cell in cells]

                        # Ensure we have the right number of columns
                        if len(row_data) == len(columns):
                            # Add metadata columns
                            row_data.extend(
                                [tournament_id, division, round_number])
                            scores_data.append(row_data)

                # If we found data, break out of table loop
                if scores_data:
                    break

        if scores_data:
            # Add metadata columns to headers
            full_columns = columns + \
                ['tournament_id', 'division', 'round_number']
            df = pd.DataFrame(scores_data, columns=full_columns)

            # Clean up the data
            df = self._clean_scores_dataframe(df)

            logger.info(
                f"Extracted {len(df)} scores for tournament {tournament_id}, division {division}, round {round_number}")
            return df

        return pd.DataFrame()

    def _get_round_scores_from_main_page(self, tournament_id: str, division: str, round_number: int) -> pd.DataFrame:
        """Get round scores from the main tournament page (for completed tournaments)"""
        # This is a placeholder - would need to implement parsing of the main tournament page
        # to extract round-by-round scores for specific divisions
        logger.info(
            f"Getting round {round_number} scores from main page for {tournament_id}, division {division}")

        # For now, return empty DataFrame - this would need more complex parsing
        # of the tournament page structure to extract round scores by division
        return pd.DataFrame()

    def get_all_round_scores(self, tournament_id: str, division: str) -> Dict[int, pd.DataFrame]:
        """
        Get all available rounds for a tournament and division

        Args:
            tournament_id: PDGA tournament ID
            division: Division code (e.g., 'MPO', 'FPO', 'MA1', 'MA2')

        Returns:
            Dictionary mapping round numbers to score DataFrames
        """
        all_rounds = {}

        # Try common round numbers (most tournaments have 1-4 rounds)
        for round_num in range(1, 5):
            round_scores = self.get_live_round_scores(
                tournament_id, division, round_num)
            if not round_scores.empty:
                all_rounds[round_num] = round_scores
                logger.info(
                    f"Found round {round_num} with {len(round_scores)} scores")
            else:
                logger.debug(f"No scores found for round {round_num}")

        return all_rounds

    def get_tournament_divisions(self, tournament_id: str) -> List[str]:
        """
        Get all divisions available for a tournament

        Args:
            tournament_id: PDGA tournament ID

        Returns:
            List of division codes
        """
        # Only the divisions we need
        common_divisions = [
            'MPO',      # Open division
            'MA1', 'MA2', 'MA3',  # Mixed Amateur divisions
            'MA40', 'MA50'        # Masters divisions
        ]

        available_divisions = []

        # Check which divisions have data by trying to access round 1
        for division in common_divisions:
            round_scores = self.get_live_round_scores(
                tournament_id, division, 1)
            if not round_scores.empty:
                available_divisions.append(division)
                logger.info(
                    f"Division {division} has data for tournament {tournament_id}")

        return available_divisions

    def close(self):
        """Close the session"""
        self.session.close()
        logger.info("PDGA tournament scraper session closed")

# Convenience functions


def get_tournament_results_quick(tournament_id: str, delay: float = 2.0) -> pd.DataFrame:
    """Quick function to get tournament results"""
    scraper = PDGATournamentScraper(delay=delay)
    try:
        return scraper.get_tournament_results(tournament_id)
    finally:
        scraper.close()


def get_tournament_details_quick(tournament_id: str, delay: float = 2.0) -> Optional[TournamentDetails]:
    """Quick function to get tournament details"""
    scraper = PDGATournamentScraper(delay=delay)
    try:
        return scraper.get_tournament_details(tournament_id)
    finally:
        scraper.close()

    def _clean_scores_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize the scores DataFrame"""
        # Remove empty rows
        df = df.dropna(how='all')

        # Clean up column names
        df.columns = df.columns.str.strip()

        # Try to convert numeric columns
        numeric_columns = ['Place', 'Score',
                           'Rating', 'Total', 'Final', 'Round']
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                except (ValueError, TypeError):
                    pass

        # Clean up player names and PDGA numbers
        if 'Player' in df.columns:
            df['Player'] = df['Player'].str.strip()

        if 'PDGA' in df.columns:
            # Extract just the PDGA number
            df['PDGA'] = df['PDGA'].astype(str).str.extract(r'(\d+)')[0]

        return df
