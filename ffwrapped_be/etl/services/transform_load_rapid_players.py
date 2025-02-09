import logging
from typing import List, Dict

from ffwrapped_be.etl.extractors.rapid_tank_extractor import RapidTankExtractor
from ffwrapped_be.app.data_models.orm import Player
from ffwrapped_be.db import databases as db

logger = logging.getLogger(__name__)


class RapidPlayerTransformLoader:
    def __init__(self):
        self.extractor = RapidTankExtractor()
        self.db = db.SessionLocal()

    def load_players(self) -> List[Dict]:
        players_json = self.extractor.get_players()
        existing_players = db.get_players_by_pfref_id(
            [player.get("fRefID", None) for player in players_json], self.db
        )
        existing_players_set = set([player.pfref_id for player in existing_players])
        logger.info(f"Found {len(existing_players)} existing players in the database")

        update_mappings = []
        for player_data in players_json:
            pfref_id = player_data.get("fRefID", None)
            if pfref_id in existing_players_set:
                player_entry = {
                    "pfref_id": player_data.get("fRefID", None),
                    "espn_id": player_data.get("espnID", None),
                    "sleeper_bot_id": player_data.get("sleeperBotID", None),
                    "fantasy_pros_id": player_data.get("fantasyProsPlayerID", None),
                    "yahoo_id": player_data.get("yahooPlayerID", None),
                    "cbs_player_id": player_data.get("cbsPlayerID", None),
                }
                update_mappings.append(player_entry)
        logger.info("Starting bulk update of player data")
        db.bulk_upsert_players_with_ids(update_mappings, self.db)
        logger.info("Bulk update of player data complete")
        return


if __name__ == "__main__":
    rapidPlayerTransformLoader = RapidPlayerTransformLoader()
    players = rapidPlayerTransformLoader.load_players()
