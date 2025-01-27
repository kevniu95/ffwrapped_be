from ratelimit import RateLimitException, sleep_and_retry
import logging
import time

logging.basicConfig(level=logging.INFO)

def custom_sleep_and_retry(func):
    @sleep_and_retry
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RateLimitException as e:
            logging.info(f"Rate limit reached. Sleeping for {e.period_remaining} seconds.")
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