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
    """
    This is name of rules as stored in the scoring config in database.
        - In the league_season table
    """
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


ESPN_PLAYER_STATS_TO_SCORING_CONFIG = {
    # Passing Stats
    "passingAttempts": None,  # PA
    "passingCompletions": None,  # PC
    "passingYards": "pass_yds",  # PY
    "passingTouchdowns": "pass_td",  # PTD
    "passing40PlusYardTD": None,  # PTD40
    "passing50PlusYardTD": None,  # PTD50
    "passing300To399YardGame": "pass_yds_bonus_300_399",  # P300
    "passing400PlusYardGame": "pass_yds_bonus_400",  # P400
    "passing2PtConversions": "pass_2pt",  # 2PC
    "passingInterceptions": "pass_int",  # INT
    # Rushing Stats
    "rushingAttempts": None,  # RA
    "rushingYards": "rush_yds",  # RY
    "rushingTouchdowns": "rush_td",  # RTD
    "rushing2PtConversions": "rush_2pt",  # 2PR
    "rushing40PlusYardTD": None,  # RTD40
    "rushing50PlusYardTD": None,  # RTD50
    "rushing100To199YardGame": "rush_yds_bonus_100_199",  # RY100
    "rushing200PlusYardGame": "rush_yds_bonus_200",  # RY200
    # Receiving Stats
    "receivingReceptions": "rec",  # REC
    "receivingYards": "rec_yds",  # REY
    "receivingTouchdowns": "rec_td",  # RETD
    "receiving2PtConversions": "rec_2pt",  # 2PRE
    "receiving40PlusYardTD": None,  # RETD40
    "receiving50PlusYardTD": None,  # RETD50
    "receiving100To199YardGame": "rec_yds_bonus_100_199",  # REY100
    "receiving200PlusYardGame": "rec_yds_bonus_200",  # REY200
    "receivingTargets": None,  # RET
    "fumbleRecoveredForTD": "fum_rec_td",  # FTD
    "passingTimesSacked": None,  # SK
    "fumbles": None,  # FUM
    "lostFumbles": "fum_lost",  # FUML
    # Kicking Stats
    "madeFieldGoalsFrom60Plus": "fg_made_60_plus",  # FG60
    "attemptedFieldGoalsFrom60Plus": None,  # FGA60
    "madeFieldGoalsFrom50Plus": "fg_made_50_59",  # FG50 (does not map directly to FG50 as FG50 does not include 60+)
    "attemptedFieldGoalsFrom50Plus": None,  # FGA50 (does not map directly to FGA50 as FG50 does not include 60+)
    "madeFieldGoalsFrom40To49": "fg_made_40_49",  # FG40
    "attemptedFieldGoalsFrom40To49": None,  # FGA40
    "madeFieldGoalsFromUnder40": "fg_made_0_39",  # FG0
    "attemptedFieldGoalsFromUnder40": None,  # FGA0
    "madeExtraPoints": "pat_made",  # PAT
    "attemptedExtraPoints": None,  # PATA
    "missedFieldGoals": "fg_missed",  # FG
    # Defensive Stats
    "defensive0PointsAllowed": "def_pa_0",  # PA0
    "defensive1To6PointsAllowed": "def_pa_1_6",  # PA1
    "defensive7To13PointsAllowed": "def_pa_7_13",  # PA7
    "defensive14To17PointsAllowed": "def_pa_14_17",  # PA14
    "defensiveBlockedKickForTouchdowns": "def_blk_kick_td",  # BLKKRTD
    "defensiveInterceptions": "def_int",  # INT
    "defensiveFumbles": "def_fum_rec",  # FR
    "defensiveBlockedKicks": "def_blk_kick",  # BLKK
    "defensiveSafeties": "def_safety",  # SF
    "defensiveSacks": "def_sack",  # SK
    "kickoffReturnTouchdowns": "def_ko_td",  # KRTD
    "puntReturnTouchdowns": "def_punt_td",  # PRTD
    "interceptionReturnTouchdowns": "def_int_td",  # INTTD
    "fumbleReturnTouchdowns": "def_fum_td",  # FRTD
    "defensiveForcedFumbles": None,  # FF
    "defensiveAssistedTackles": None,  # TKA
    "defensiveSoloTackles": None,  # TKS
    "defensiveTotalTackles": None,  # TK
    "defensivePassesDefensed": None,  # PD
    "kickoffReturnYards": None,  # KR
    "puntReturnYards": None,  # PR
    "puntsReturned": None,  # PTR
    "defensivePointsAllowed": None,  # PA
    "defensive18To21PointsAllowed": None,  # PA18
    "defensive22To27PointsAllowed": None,  # PA22
    "defensive28To34PointsAllowed": "def_pa_28_34",  # PA28
    "defensive35To45PointsAllowed": "def_pa_35_45",  # PA35
    "defensive45PlusPointsAllowed": "def_pa_46_plus",  # PA46
    "defensiveYardsAllowed": None,  # YA
    "defensiveLessThan100YardsAllowed": "def_ya_0_99",  # YA100
    "defensive100To199YardsAllowed": "def_ya_100_199",  # YA199
    "defensive200To299YardsAllowed": "def_ya_200_299",  # YA299
    "defensive300To349YardsAllowed": None,  # YA349
    "defensive350To399YardsAllowed": "def_ya_350_399",  # YA399
    "defensive400To449YardsAllowed": "def_ya_400_449",  # YA449
    "defensive450To499YardsAllowed": "def_ya_450_499",  # YA499
    "defensive500To549YardsAllowed": "def_ya_500_549",  # YA549
    "defensive550PlusYardsAllowed": "def_ya_550_plus",  # YA550
    "defensive2PtReturns": "def_2pt_return",  # 2PTRET
}

ESPN_PLAYER_STATS_TO_DB = {
    # Passing Stats
    "passingAttempts": "passing_attempts",  # PA
    "passingCompletions": "passing_completions",  # PC
    "passingYards": "passing_yards",  # PY
    "passingTouchdowns": "passing_touchdowns",  # PTD
    "passing40PlusYardTD": "passing_40yard_tds",  # PTD40
    "passing50PlusYardTD": "passing_50yard_tds",  # PTD50
    "passing2PtConversions": "passing_2pt_conversions",  # 2PC
    "passingInterceptions": "passing_interceptions",  # INT
    # "passingCompletionPercentage": None,
    # Rushing Stats
    "rushingAttempts": "rushing_attempts",  # RA
    "rushingYards": "rushing_yards",  # RY
    "rushingTouchdowns": "rushing_touchdowns",  # RTD
    "rushing2PtConversions": "rushing_2pt_conversions",  # 2PR
    "rushing40PlusYardTD": "rushing_40yard_tds",  # RTD40
    "rushing50PlusYardTD": "rushing_50yard_tds",  # RTD50
    # Receiving Stats
    "receivingReceptions": "receiving_receptions",  # REC
    "receivingYards": "receiving_yards",  # REY
    "receivingTouchdowns": "receiving_touchdowns",  # RETD
    "receiving2PtConversions": "receiving_2pt_conversions",  # 2PRE
    "receiving40PlusYardTD": "receiving_40yard_tds",  # RETD40
    "receiving50PlusYardTD": "receiving_50yard_tds",  # RETD50
    "receivingTargets": "receiving_targets",  # RET
    # General offensive stats
    "fumbleRecoveredForTD": "fumbles_recovered_for_td",  # FTD
    "passingTimesSacked": "passing_sacks",  # SK
    "fumbles": "fumbles",  # FUM
    "lostFumbles": "fumbles_lost",  # FUML
    # Kicking Stats
    "madeFieldGoalsFrom60Plus": "kicking_fgm_60_plus",  # FG60
    "attemptedFieldGoalsFrom60Plus": "kicking_fga_60_plus",  # FGA60
    "madeFieldGoalsFrom50Plus": "kicking_fgm_50_59",  # FG50 (does not map directly to FG50 as FG50 does not include 60+)
    "attemptedFieldGoalsFrom50Plus": "kicking_fga_50_59",  # FGA50 (does not map directly to FGA50 as FG50 does not include 60+)
    "madeFieldGoalsFrom40To49": "kicking_fgm_40_49",  # FG40
    "attemptedFieldGoalsFrom40To49": "kicking_fga_40_49",  # FGA40
    "madeFieldGoalsFromUnder40": "kicking_fgm_0_39",  # FG0
    "attemptedFieldGoalsFromUnder40": "kicking_fga_0_39",  # FGA0
    "madeExtraPoints": "kicking_xpm",  # PAT
    "attemptedExtraPoints": "kicking_xpa",  # PATA
    # Defensive Stats
    "defensiveBlockedKickForTouchdowns": "defensive_blocked_kick_return_tds",  # BLKKRTD
    "defensiveInterceptions": "defensive_interceptions",  # INT
    "defensiveFumbles": "defensive_fumble_recoveries",  # FR
    "defensiveBlockedKicks": "defensive_blocked_kicks",  # BLKK
    "defensiveSafeties": "defensive_safeties",  # SF
    "defensiveSacks": "defensive_sacks",  # SK
    "kickoffReturnTouchdowns": "kickoff_return_touchdowns",  # KRTD
    "puntReturnTouchdowns": "punt_return_touchdowns",  # PRTD
    "interceptionReturnTouchdowns": "interception_return_touchdowns",  # INTTD
    "fumbleReturnTouchdowns": "fumble_return_touchdowns",  # FRTD
    "defensiveForcedFumbles": "defensive_forced_fumbles",  # FF
    "defensiveAssistedTackles": "defensive_assisted_tackles",  # TKA
    "defensiveSoloTackles": "defensive_solo_tackles",  # TKS
    "defensivePassesDefensed": "defensive_passes_defended",  # PD
    "kickoffReturnYards": "kickoff_return_yards",  # KR
    "puntReturnYards": "punt_return_yards",  # PR
    "puntsReturned": "punts_returned",  # PTR
    "defensivePointsAllowed": "defensive_points_allowed",  # PA
    "defensiveYardsAllowed": "defensive_yards_allowed",  # YA
    "defensive2PtReturns": "defensive_2pt_return",  # 2PTRET
}


def generate_derived_espn_statistics(input_dict: Dict[str, int]) -> Dict[str, int]:
    """
    Takes basic ESPN statistics from dictionary
      (Set of stats is those in DB, but name is ESPN-given name)
    Returns a dictionary of derived statistics to be added to the input dictionary
    """
    derived_stats = {}
    # Passing Stats
    if input_dict.get("passingYards"):
        derived_stats["passing300To399YardGame"] = (
            1
            if input_dict["passingYards"] >= 300 and input_dict["passingYards"] < 400
            else 0
        )
        derived_stats["passing400PlusYardGame"] = (
            1 if input_dict["passingYards"] >= 400 else 0
        )
    # Rushing Stats
    if input_dict.get("rushingYards"):
        derived_stats["rushing100To199YardGame"] = (
            1
            if input_dict["rushingYards"] >= 100 and input_dict["rushingYards"] < 200
            else 0
        )
        derived_stats["rushing200PlusYardGame"] = (
            1 if input_dict["rushingYards"] >= 200 else 0
        )
    # Receiving Stats
    if input_dict.get("receivingYards"):
        derived_stats["receiving100To199YardGame"] = (
            1
            if input_dict["receivingYards"] >= 100
            and input_dict["receivingYards"] < 200
            else 0
        )
        derived_stats["receiving200PlusYardGame"] = (
            1 if input_dict["receivingYards"] >= 200 else 0
        )
    # Special Teams
    if (
        input_dict.get("attemptedFieldGoalsFrom60Plus")
        or input_dict.get("attemptedFieldGoalsFrom50Plus")
        or input_dict.get("attemptedFieldGoalsFrom40To49")
        or input_dict.get("attemptedFieldGoalsFromUnder40")
    ):
        derived_stats["missedFieldGoals"] = (
            (
                input_dict.get("attemptedFieldGoalsFrom60Plus", 0)
                - input_dict.get("madeFieldGoalsFrom60Plus", 0)
            )
            + (
                input_dict.get("attemptedFieldGoalsFrom50Plus", 0)
                - input_dict.get("madeFieldGoalsFrom50Plus", 0)
            )
            + (
                input_dict.get("attemptedFieldGoalsFrom40To49", 0)
                - input_dict.get("madeFieldGoalsFrom40To49", 0)
            )
            + (
                input_dict.get("attemptedFieldGoalsFromUnder40", 0)
                - input_dict.get("madeFieldGoalsFromUnder40", 0)
            )
        )
    # Defense
    if input_dict.get("defensivePointsAllowed"):
        derived_stats["defensive0PointsAllowed"] = (
            1 if input_dict["defensivePointsAllowed"] == 0 else 0
        )
        derived_stats["defensive1To6PointsAllowed"] = (
            1
            if input_dict["defensivePointsAllowed"] >= 1
            and input_dict["defensivePointsAllowed"] <= 6
            else 0
        )
        derived_stats["defensive7To13PointsAllowed"] = (
            1
            if input_dict["defensivePointsAllowed"] >= 7
            and input_dict["defensivePointsAllowed"] <= 13
            else 0
        )
        derived_stats["defensive14To17PointsAllowed"] = (
            1
            if input_dict["defensivePointsAllowed"] >= 14
            and input_dict["defensivePointsAllowed"] <= 17
            else 0
        )
        derived_stats["defensive28To34PointsAllowed"] = (
            1
            if input_dict["defensivePointsAllowed"] >= 28
            and input_dict["defensivePointsAllowed"] <= 34
            else 0
        )
        derived_stats["defensive35To45PointsAllowed"] = (
            1
            if input_dict["defensivePointsAllowed"] >= 35
            and input_dict["defensivePointsAllowed"] <= 45
            else 0
        )
        derived_stats["defensive46PlusPointsAllowed"] = (
            1 if input_dict["defensivePointsAllowed"] >= 46 else 0
        )
    if input_dict.get("defensiveYardsAllowed"):
        derived_stats["defensiveLessThan100YardsAllowed"] = (
            1 if input_dict["defensiveYardsAllowed"] < 100 else 0
        )
        derived_stats["defensive100To199YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 100
            and input_dict["defensiveYardsAllowed"] < 200
            else 0
        )
        derived_stats["defensive200To299YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 200
            and input_dict["defensiveYardsAllowed"] < 300
            else 0
        )
        derived_stats["defensive300To349YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 300
            and input_dict["defensiveYardsAllowed"] < 350
            else 0
        )
        derived_stats["defensive350To399YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 350
            and input_dict["defensiveYardsAllowed"] < 400
            else 0
        )
        derived_stats["defensive400To449YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 400
            and input_dict["defensiveYardsAllowed"] < 450
            else 0
        )
        derived_stats["defensive450To499YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 450
            and input_dict["defensiveYardsAllowed"] < 500
            else 0
        )
        derived_stats["defensive500To549YardsAllowed"] = (
            1
            if input_dict["defensiveYardsAllowed"] >= 500
            and input_dict["defensiveYardsAllowed"] < 550
            else 0
        )
        derived_stats["defensive550PlusYardsAllowed"] = (
            1 if input_dict["defensiveYardsAllowed"] >= 550 else 0
        )
    input_dict.update(derived_stats)
    return input_dict


DB_PLAYER_STATS_TO_ESPN = {v: k for k, v in ESPN_PLAYER_STATS_TO_DB.items()}
