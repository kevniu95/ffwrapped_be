import logging
from typing import List, Dict
from datetime import datetime

from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.db import databases as db
from ffwrapped_be.config import config
from ffwrapped_be.app.data_models.orm import Platform, LeagueSeason
from ffwrapped_be.etl import utils

logger = logging.getLogger(__name__)


class ESPNTransformLoader:
    def __init__(self, league_id: int, season: int, espn_s2: str, swid: str):
        self.extractor = ESPNExtractor(league_id, season, espn_s2, swid)
        self.db = db.SessionLocal()

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

        league = self.extractor.extract_league()
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
        logger.info(
            f"Successfully loaded league {league.league_id}, season {league.year} into DB"
        )

        db.commit(self.db)


if __name__ == "__main__":
    espnTransformLoader = ESPNTransformLoader(
        config.espn_league_id, 2024, config.espn_s2, config.espn_swid
    )

    espnTransformLoader.transform_load_league()
