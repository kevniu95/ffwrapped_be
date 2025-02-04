import logging
from typing import List, Dict
from datetime import datetime

from espn_api.football import League, Team
from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.db import databases as db
from ffwrapped_be.config import config
from ffwrapped_be.app.data_models.orm import LeagueSeason, LeagueTeam
from ffwrapped_be.etl import utils

logger = logging.getLogger(__name__)


class ESPNTransformLoader:
    def __init__(self, league_id: int, season: int, espn_s2: str, swid: str):
        self.extractor = ESPNExtractor(league_id, season, espn_s2, swid)
        self.db = db.SessionLocal()
        self.espn_league: League = self.extractor.extract_league()

    def _get_existing_db_league(self, espn_league: League) -> LeagueSeason:
        try:
            league_db: LeagueSeason = db.get_league_season_by_platform_league_id(
                espn_league.league_id, self.db
            )
        except:
            logger.error(
                f"Error querying db for ESPN League with id: {espn_league.league_id}"
            )
        if not league_db:
            logger.error(
                f"Error querying db for ESPN League with id: {espn_league.league_id}"
            )
        return league_db

    def _process_league_scoring_format(self, league_scoring_format: List) -> Dict:
        """
        Takes list of scoring format info from ESPN and returns a JSON object for entry to db
        """
        # Turn below into a dict comprehension
        espn_scoring_dict = {}
        for row in league_scoring_format:
            espn_scoring_dict[row["abbr"]] = row["points"]

        standardized_dict = {}
        for key in espn_scoring_dict.keys():
            if key not in utils.ESPN_TO_STANDARDIZED_SCORING_MAP.keys():
                logger.warning(
                    f"Scoring category {key} not found in map to standardized scoring rules"
                )
                continue

            standardized_dict[utils.ESPN_TO_STANDARDIZED_SCORING_MAP[key]] = (
                espn_scoring_dict[key]
            )

        if not utils.validate_scoring_format(standardized_dict):
            logger.error("Scoring rules are not valid")
            raise ValueError("Scoring rules are not valid")
        return standardized_dict

    def transform_load_league(self):
        platform = db.get_platform_by_name("ESPN", self.db)
        logger.info("Successfully retrieved ESPN platform_id from DB")

        league = self.espn_league
        logger.info("Successfully extracted league info from ESPN API")

        standardized_scoring_rules = self._process_league_scoring_format(
            league.settings.scoring_format
        )
        logger.info("Successfully standardized scoring rules for league")

        league_object = LeagueSeason(
            platform_id=platform.platform_id,
            platform_league_id=league.league_id,
            season=league.year,
            scoring_config=standardized_scoring_rules,
        )

        db.insert_record(league_object, db=self.db)
        db.commit(self.db)

        logger.info(
            f"Successfully loaded league {league.league_id}, season {league.year} into DB"
        )

    def transform_load_teams(self):
        # Get league season id from DB b/c it should exist
        db_league = self._get_existing_db_league(self.espn_league)
        if not db_league:
            logger.error(
                f"Error in retrieving league {self.espn_league.league_id} from db"
            )
            raise ValueError(
                f"Error in retrieving league {self.espn_league.league_id}from db"
            )
        db_league_id = db_league.league_season_id
        logger.info(
            f"Successfully retrieved league {self.espn_league.league_id}'s db id, value of: {db_league_id}"
        )

        league: League = self.espn_league
        league_team_entries = []
        for team in league.teams:
            league_team_entry = {
                "league_season_id": db_league_id,
                "platform_team_id": team.team_id,
                "team_name": team.team_name,
                "team_abbreviation": team.team_abbrev,
            }
            league_team_entries.append(league_team_entry)

        db.bulk_insert(
            league_team_entries, record_type=LeagueTeam, flush=True, db=self.db
        )
        db.commit(self.db)

        logger.info(
            f"Successfully inserted league teams into db for league {self.espn_league.league_id}"
        )


if __name__ == "__main__":
    espnTransformLoader = ESPNTransformLoader(
        config.espn_league_id, 2024, config.espn_s2, config.espn_swid
    )

    espnTransformLoader.transform_load_league()
    espnTransformLoader.transform_load_teams()
