"""
Page modules for the Disc Golf Analytics Dashboard
"""

from .title_page import show_title_page
from .monthly_summary import show_monthly_summary
from .all_rounds import show_all_rounds
from .record_sheet import show_record_sheet
from .player_profile import show_player_profile
from .course_profile import show_course_profile
from .hole_analysis import show_hole_analysis
from .stats_tables import show_stats_tables
from .historic_ratings import show_historic_ratings
from .head_to_head import show_head_to_head
from .power_scores import show_power_scores
from .turkeys_bounce_backs import show_turkeys_bounce_backs
from .hole_streaks import show_hole_streaks
from .golden_birdies import show_golden_birdies

__all__ = [
    'show_title_page',
    'show_monthly_summary',
    'show_all_rounds',
    'show_record_sheet',
    'show_player_profile',
    'show_course_profile',
    'show_hole_analysis',
    'show_stats_tables',
    'show_historic_ratings',
    'show_head_to_head',
    'show_power_scores',
    'show_turkeys_bounce_backs',
    'show_hole_streaks',
    'show_golden_birdies',
]
