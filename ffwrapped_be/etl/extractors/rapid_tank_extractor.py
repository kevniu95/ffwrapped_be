import logging
import requests
import json
import os
from abc import ABC, abstractmethod
from typing import List, Dict

from ffwrapped_be.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RapidTankExtractor:
    def __init__(self):
        self.base_url = config.rapid_api_tank_url

    def get_players(self) -> List[Dict]:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        json_file_path = os.path.join(dir_path, "../data/nfl_player_data.json")

        logger.info(
            f"Searching for json file of nfl player data at path {json_file_path}..."
        )
        if not os.path.exists(json_file_path):
            logger.info("JSON of nfl player data not found, retrieving now...")
            player_list_endpoint = "/getNFLPlayerList"
            url = self.base_url + player_list_endpoint
            headers = {
                "x-rapidapi-host": config.rapid_api_host,
                "x-rapidapi-key": config.rapid_api_key,
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 200 and response.json():
                json_data = response.json()
                with open(json_file_path, "w") as f:
                    json.dump(json_data, f)
                logger.info("JSON of NFL player data retrieved and saved.")
        else:
            logger.info("JSON of nfl player data already exists, importing.")
            json_data = json.load(open(json_file_path, "r"))
        return json_data["body"]


if __name__ == "__main__":
    rapidExtractor = RapidTankExtractor()
    rapidExtractor.get_players()
