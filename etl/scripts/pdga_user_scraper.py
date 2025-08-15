#!/usr/bin/env python3
"""
PDGA User Scraper
"""

import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time

# Add path for imports from etl/airflow/lib
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'airflow', 'lib'))

from user_manager import get_user_manager
from pdga_scraper import PDGAScraper
from pdga_tournament_scraper import PDGATournamentScraper


class PDGAUserDataFetcher:
    """Fetches comprehensive PDGA data for users with PDGA IDs"""

    def __init__(self, delay: float = 2.0):
        """
        Initialize the data fetcher

        Args:
            delay: Delay between requests in seconds (default: 2.0)
        """
        self.delay = delay
        self.player_scraper = PDGAScraper(delay=delay)
        self.tournament_scraper = PDGATournamentScraper(delay=delay)
        self.user_manager = get_user_manager()

        # Data storage
        self.player_data = {}
        self.tournament_data = {}
        self.course_data = {}
        self.player_tournament_history = {}

        print(f"🏌️  Initialized PDGA User Data Fetcher with {delay}s delay")

    def fetch_all_user_data(self) -> Dict[str, Any]:
        """
        Fetch data for all users with PDGA IDs

        Returns:
            Dictionary containing all fetched data
        """
        print("\n🔍 Fetching data for all users with PDGA IDs...")
        print("=" * 60)

        # Get users with PDGA IDs
        users_with_pdga = []
        for name, user in self.user_manager.users.items():
            if user.pdga_id:
                users_with_pdga.append(user)
                print(f"  ✓ {user.display_name} (PDGA: {user.pdga_id})")

        if not users_with_pdga:
            print("  ❌ No users found with PDGA IDs")
            return {}

        print(f"\n📊 Found {len(users_with_pdga)} users with PDGA IDs")

        # Fetch data for each user
        for user in users_with_pdga:
            print(
                f"\n👤 Fetching data for {user.display_name} (PDGA: {user.pdga_id})")
            self._fetch_user_data(user)

        # Compile summary
        summary = self._compile_summary()

        return {
            'summary': summary,
            'player_data': self.player_data,
            'tournament_data': self.tournament_data,
            'course_data': self.course_data,
            'player_tournament_history': self.player_tournament_history,
            'fetched_at': datetime.now().isoformat()
        }

    def _fetch_user_data(self, user):
        """Fetch data for a specific user"""
        try:
            pdga_id = user.pdga_id

            # 1. Fetch player data
            print(f"  📊 Fetching player data...")
            player_info = self.player_scraper.get_player_rating(pdga_id)
            if player_info:
                self.player_data[pdga_id] = {
                    'user_name': user.name,
                    'display_name': user.display_name,
                    'pdga_id': pdga_id,
                    'player_name': player_info.name,
                    'current_rating': player_info.current_rating,
                    'rating_change': player_info.rating_change,
                    'rounds_rated': player_info.rounds_rated,
                    'last_updated': player_info.last_updated,
                    'location': getattr(player_info, 'location', ''),
                    'fetched_at': datetime.now().isoformat()
                }
                print(
                    f"    ✓ Player data: {player_info.name} (Rating: {player_info.current_rating})")
            else:
                print(
                    f"    ❌ Could not fetch player data for PDGA ID: {pdga_id}")

            # 2. Search for recent tournaments (last 2 years)
            print(f"  🏆 Searching for recent tournaments...")
            current_year = datetime.now().year
            tournaments_found = []

            # Use tournament scraper for tournament search
            for year in [current_year, current_year - 1]:
                year_tournaments = self.tournament_scraper.search_tournaments_by_date_range(
                    f"{year}-01-01",
                    f"{year}-12-31"
                )
                tournaments_found.extend(year_tournaments)

            print(
                f"    ✓ Found {len(tournaments_found)} tournaments in last 2 years")

            # 3. For each tournament, get details and check for user participation
            user_tournaments = []
            # Limit to 30 for performance
            for tournament in tournaments_found[:30]:
                tournament_id = tournament.get('tournament_id')
                if tournament_id:
                    print(
                        f"    🔍 Checking tournament: {tournament.get('name', 'Unknown')}")

                    # Get tournament details using tournament scraper
                    tournament_details = self.tournament_scraper.get_tournament_details(
                        tournament_id)
                    if tournament_details:
                        # Store tournament data
                        self.tournament_data[tournament_id] = {
                            'tournament_id': tournament_id,
                            'name': tournament_details.name,
                            'start_date': tournament_details.start_date,
                            'end_date': tournament_details.end_date,
                            'location': tournament_details.location,
                            'city': tournament_details.city,
                            'state': tournament_details.state,
                            'country': tournament_details.country,
                            'divisions': tournament_details.divisions,
                            'courses': tournament_details.courses,
                            'url': tournament_details.url,
                            'fetched_at': datetime.now().isoformat()
                        }

                        # 4. Extract course data from tournaments
                        for course_name in tournament_details.courses:
                            if course_name and course_name not in self.course_data:
                                print(f"      🏟️  Found course: {course_name}")
                                self.course_data[course_name] = {
                                    'name': course_name,
                                    'tournaments': [tournament_id],
                                    'first_seen': datetime.now().isoformat(),
                                    'last_seen': datetime.now().isoformat()
                                }
                            elif course_name in self.course_data:
                                # Update existing course data
                                if tournament_id not in self.course_data[course_name]['tournaments']:
                                    self.course_data[course_name]['tournaments'].append(
                                        tournament_id)
                                self.course_data[course_name]['last_seen'] = datetime.now(
                                ).isoformat()

                        user_tournaments.append(tournament_id)

            print(f"    ✓ Processed {len(user_tournaments)} tournaments")

            # 5. Try to get player's tournament history
            print(f"  📈 Fetching tournament history...")
            try:
                tournament_history = self.tournament_scraper.get_player_tournament_history(
                    pdga_id)
                if tournament_history:
                    self.player_tournament_history[pdga_id] = [
                        {
                            'tournament_id': result.tournament_id,
                            'tournament_name': result.tournament_name,
                            'division': result.division,
                            'place': result.place,
                            'total_score': result.total_score,
                            'total_rating': result.total_rating,
                            'rounds_played': result.rounds_played,
                            'cash_winnings': result.cash_winnings,
                            'points': result.points,
                            'tournament_date': result.tournament_date
                        }
                        for result in tournament_history
                    ]
                    print(
                        f"    ✓ Found {len(tournament_history)} tournament results")
                else:
                    print(
                        "⚠️  No tournament history found (may need HTML structure updates)")
            except Exception as e:
                print(f"    ⚠️  Could not fetch tournament history: {e}")

        except Exception as e:
            print(f"    ❌ Error fetching data for {user.display_name}: {e}")

    def _compile_summary(self) -> Dict[str, Any]:
        """Compile a summary of all fetched data"""
        return {
            'total_users_processed': len([u for u in self.user_manager.users.values() if u.pdga_id]),
            'total_players_found': len(self.player_data),
            'total_tournaments_found': len(self.tournament_data),
            'total_courses_found': len(self.course_data),
            'total_tournament_results': len(self.player_tournament_history),
            'data_breakdown': {
                'player_data': {pdga_id: data['player_name'] for pdga_id, data in self.player_data.items()},
                'tournament_count': len(self.tournament_data),
                'course_count': len(self.course_data),
                'tournament_results_count': sum(len(results) for results in self.player_tournament_history.values())
            }
        }

    def save_data_to_files(self, output_dir: str = "pdga_data"):
        """Save all fetched data to JSON and CSV files"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save complete data as JSON
        complete_data = {
            'summary': self._compile_summary(),
            'player_data': self.player_data,
            'tournament_data': self.tournament_data,
            'course_data': self.course_data,
            'player_tournament_history': self.player_tournament_history,
            'fetched_at': datetime.now().isoformat()
        }

        json_file = output_path / f"pdga_complete_data_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(complete_data, f, indent=2)
        print(f"\n💾 Saved complete data to: {json_file}")

        # Save individual data as CSV files
        if self.player_data:
            player_df = pd.DataFrame.from_dict(
                self.player_data, orient='index')
            player_df = player_df.reset_index()
            player_csv = output_path / f"pdga_player_data_{timestamp}.csv"
            player_df.to_csv(player_csv, index=False)
            print(f"💾 Saved player data to: {player_csv}")

        if self.tournament_data:
            tournament_df = pd.DataFrame.from_dict(
                self.tournament_data, orient='index')
            tournament_df = tournament_df.reset_index()
            tournament_csv = output_path / \
                f"pdga_tournament_data_{timestamp}.csv"
            tournament_df.to_csv(tournament_csv, index=False)
            print(f"💾 Saved tournament data to: {tournament_csv}")

        if self.course_data:
            course_df = pd.DataFrame.from_dict(
                self.course_data, orient='index')
            course_df = course_df.reset_index()
            course_csv = output_path / f"pdga_course_data_{timestamp}.csv"
            course_df.to_csv(course_csv, index=False)
            print(f"💾 Saved course data to: {course_csv}")

        if self.player_tournament_history:
            # Flatten tournament history for CSV
            history_rows = []
            for pdga_id, results in self.player_tournament_history.items():
                for result in results:
                    result['pdga_id'] = pdga_id
                    history_rows.append(result)

            if history_rows:
                history_df = pd.DataFrame(history_rows)
                history_csv = output_path / \
                    f"pdga_tournament_history_{timestamp}.csv"
                history_df.to_csv(history_csv, index=False)
                print(f"💾 Saved tournament history to: {history_csv}")

        return str(output_path)

    def close(self):
        """Close all scrapers"""
        self.player_scraper.close()
        self.tournament_scraper.close()
        print("\n🔒 Closed all scrapers")


def main():
    """Main function to demonstrate the PDGA User Data Fetcher"""
    print("🏌️  PDGA User Data Fetcher")
    print("=" * 60)
    print("This script will fetch PDGA data for all users with PDGA IDs")
    print("including player data, tournaments, and courses.")
    print()

    # Initialize the fetcher
    fetcher = PDGAUserDataFetcher(delay=2.0)

    try:
        # Fetch all data
        data = fetcher.fetch_all_user_data()

        if data:
            # Display summary
            summary = data['summary']
            print(f"\n📊 Data Fetching Summary")
            print("=" * 40)
            print(f"Users Processed: {summary['total_users_processed']}")
            print(f"Players Found: {summary['total_players_found']}")
            print(f"Tournaments Found: {summary['total_tournaments_found']}")
            print(f"Courses Found: {summary['total_courses_found']}")
            print(f"Tournament Results: {summary['total_tournament_results']}")

            # Show player details
            if data['player_data']:
                print(f"\n👤 Player Data:")
                print("-" * 20)
                for pdga_id, player in data['player_data'].items():
                    print(f"  {player['display_name']} (PDGA: {pdga_id})")
                    print(f"    Rating: {player['current_rating']}")
                    print(f"    Location: {player.get('location', 'N/A')}")

                    # Show tournament history if available
                    if pdga_id in data['player_tournament_history']:
                        history = data['player_tournament_history'][pdga_id]
                        print(
                            f"    Tournament Results: {len(history)} tournaments")
                        if history:
                            recent_result = history[0]  # Show most recent
                            print(
                                f"    Recent: {recent_result['tournament_name']} - {recent_result['place']} place")
                    print()

            # Save data to files
            output_dir = fetcher.save_data_to_files()
            print(f"\n✅ All data saved to: {output_dir}")

        else:
            print("❌ No data was fetched")

    except Exception as e:
        print(f"❌ Error during data fetching: {e}")
        import traceback
        traceback.print_exc()

    finally:
        fetcher.close()


if __name__ == "__main__":
    main()
