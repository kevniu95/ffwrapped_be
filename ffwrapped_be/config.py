# filepath: /Users/kniu91/Documents/projects/ffwrapped_be/ffwrapped_be/config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    pfref_base = os.getenv("PFREF_BASE")
    stathead_base = os.getenv("STATHEAD_BASE")

    stathead_username = os.getenv("STATHEAD_USERNAME")
    stathead_password = os.getenv("STATHEAD_PASSWORD")

    espn_swid = os.getenv("ESPN_SWID")
    espn_s2 = os.getenv("ESPN_S2")
    espn_league_id = os.getenv("ESPN_LEAGUE_ID")

    railway_db_url = os.getenv("RAILWAY_DB_URL")
    railway_db_user = os.getenv("RAILWAY_DB_USER")
    railway_db_password = os.getenv("RAILWAY_DB_PASSWORD")


config = Config()
