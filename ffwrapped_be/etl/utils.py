from ratelimit import RateLimitException, sleep_and_retry
import logging
import time
from typing import Dict

logging.basicConfig(level=logging.INFO)


def custom_sleep_and_retry(func):
    @sleep_and_retry
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RateLimitException as e:
            logging.info(
                f"Rate limit reached. Sleeping for {e.period_remaining} seconds."
            )
            time.sleep(e.period_remaining)
            return func(*args, **kwargs)

    return wrapper


WEEKLY_PLAYER_EXTRACTOR_HEADER_COLS = [
    "Player",
    "FantPt",
    "Att",
    "Att",
    "Tgt",
    "FGM",
    "Fmb",
    "Day",
    "G#",
    "Week",
    "Date",
    "Age",
    "Team",
    "",
    "Opp",
    "Result",
    "Pass_Cmp",
    "Pass_Att",
    "Inc",
    "Cmp%",
    "Pass_Yds",
    "Pass_TD",
    "Pass_Int",
    "Pick6",
    "TD%",
    "Int%",
    "Rate",
    "Sk",
    "Sk_Yds",
    "Sk%",
    "Y/A",
    "AY/A",
    "ANY/A",
    "Y/C",
    "Succ%",
    "Rush_Att",
    "Rush_Yds",
    "Y/A",
    "Rush_TD",
    "Rush_1D",
    "Succ%",
    "Tgt",
    "Rec",
    "Rec_Yds",
    "Y/R",
    "Rec_TD",
    "Ctch%",
    "Y/Tgt",
    "Rec_1D",
    "Succ%",
    "TD",
    "XPM",
    "XPA",
    "XP%",
    "FGM",
    "FGA",
    "FG%",
    "2PM",
    "Sfty",
    "Pts",
    "FantPt",
    "PPR",
    "DKPt",
    "FDPt",
    "Fmb",
    "FR",
    "Yds",
    "FRTD",
    "FF",
    "Pos.",
]

ESPN_SCORING_FIELDS = [
    "PY",
    "PTD",
    "INTT",
    "2PC",
    "P300",
    "P400",
    "RY",
    "RTD",
    "2PR",
    "RY100",
    "RY200",
    "REC",
    "REY",
    "RETD",
    "2PRE",
    "REY100",
    "REY200",
    "FG0",
    "FG40",
    "FG50",
    "FG60",
    "FGM",
    "PAT",
    "KRTD",
    "PRTD",
    "INTTD",
    "FRTD",
    "BLKKRTD",
    "2PRET",
    "1PSF",
    "SK",
    "BLKK",
    "INT",
    "FR",
    "SF",
    "PA0",
    "PA1",
    "PA7",
    "PA14",
    "PA28",
    "PA35",
    "PA46",
    "YA100",
    "YA199",
    "YA299",
    "YA399",
    "YA449",
    "YA499",
    "YA549",
    "YA550",
    "FUML",
    "FTD",
]

STANDARDIZED_SCORING_RULES = {
    # Passing
    "pass_yds": "Passing yards",
    "pass_td": "Passing touchdown",
    "pass_int": "Passing interception",
    "pass_2pt": "Passing 2-point conversion",
    "pass_yds_bonus_300_399": "Passing yards bonus (300-399)",
    "pass_yds_bonus_400": "Passing yards bonus (400+)",
    # Rushing
    "rush_yds": "Rushing yards",
    "rush_td": "Rushing touchdown",
    "rush_2pt": "Rushing 2-point conversion",
    "rush_yds_bonus_100_199": "Rushing yards bonus (100-199)",
    "rush_yds_bonus_200": "Rushing yards bonus (200+)",
    # Receiving
    "rec": "Reception",
    "rec_yds": "Receiving yards",
    "rec_td": "Receiving touchdown",
    "rec_2pt": "Receiving 2-point conversion",
    "rec_yds_bonus_100_199": "Receiving yards bonus (100-199)",
    "rec_yds_bonus_200": "Receiving yards bonus (200+)",
    # Kicking
    "fg_made_0_39": "Field goal made (0-39 yards)",
    "fg_made_40_49": "Field goal made (40-49 yards)",
    "fg_made_50_59": "Field goal made (50-59 yards)",
    "fg_made_60_plus": "Field goal made (60+ yards)",
    "fg_missed": "Field goal missed",
    "pat_made": "Point after touchdown made",
    # Defense
    "def_ko_td": "Defense kickoff return touchdown",
    "def_punt_td": "Defense punt return touchdown",
    "def_int_td": "Defense interception return touchdown",
    "def_fum_td": "Defense fumble return touchdown",
    "def_blk_kick_td": "Defense blocked kick return touchdown",
    "def_2pt_return": "Defense 2-point return",
    "def_1pt_safety": "Defense 1-point safety",
    "def_sack": "Defense sack",
    "def_blk_kick": "Defense blocked kick",
    "def_int": "Defense interception",
    "def_fum_rec": "Defense fumble recovery",
    "def_safety": "Defense safety",
    "def_pa_0": "Defense points allowed (0)",
    "def_pa_1_6": "Defense points allowed (1-6)",
    "def_pa_7_13": "Defense points allowed (7-13)",
    "def_pa_14_17": "Defense points allowed (14-17)",
    "def_pa_28_34": "Defense points allowed (28-34)",
    "def_pa_35_45": "Defense points allowed (35-45)",
    "def_pa_46_plus": "Defense points allowed (46+)",
    "def_ya_0_99": "Defense yards allowed (0-99)",
    "def_ya_100_199": "Defense yards allowed (100-199)",
    "def_ya_200_299": "Defense yards allowed (200-299)",
    "def_ya_350_399": "Defense yards allowed (350-399)",
    "def_ya_400_449": "Defense yards allowed (400-449)",
    "def_ya_450_499": "Defense yards allowed (450-499)",
    "def_ya_500_549": "Defense yards allowed (500-549)",
    "def_ya_550_plus": "Defense yards allowed (550+)",
    # Misc
    "fum_lost": "Fumble lost",
    "fum_rec_td": "Fumble recovery touchdown",
}

ESPN_TO_STANDARDIZED_SCORING_MAP = dict(
    zip(ESPN_SCORING_FIELDS, STANDARDIZED_SCORING_RULES.keys())
)


def validate_scoring_format(scoring_format: Dict) -> bool:
    """
    Validate the scoring format dictionary to ensure all keys are valid standardized fields.
    """
    for key in scoring_format.keys():
        if key not in STANDARDIZED_SCORING_RULES:
            return False
    return True


STATS_TO_DB_COLUMNS = {
    # Passing Stats
    "passingCompletions": "pass_cmp",
    "passingAttempts": "pass_att",
    "passingYards": "pass_yds",
    "passingTouchdowns": "pass_td",
    "passingInterceptions": "pass_int",
    "passingTimesSacked": "sacks",
    # Rushing Stats
    "rushingAttempts": "rush_att",
    "rushingYards": "rush_yds",
    "rushingTouchdowns": "rush_td",
    # Receiving Stats
    "receivingTargets": "targets",
    "receivingReceptions": "receptions",
    "receivingYards": "rec_yds",
    "receivingTouchdowns": "rec_td",
    # Miscellaneous
    "fumbles": "fumbles",
    # Kicking Stats
    "madeExtraPoints": "xpm",
    "attemptedExtraPoints": "xpa",
    "madeFieldGoals": "fgm",
    "attemptedFieldGoals": "fga",
}
