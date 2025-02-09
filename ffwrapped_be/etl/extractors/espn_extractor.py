import logging
from abc import ABC, abstractmethod
from typing import List, Dict

import espn_api
from espn_api.football import League
from ffwrapped_be.config import config

logger = logging.getLogger(__name__)


class ESPNExtractor:
    def __init__(self, league_id: int, year: int, espn_s2: str, swid: str):
        self.league = League(
            league_id=league_id,
            year=year,
            espn_s2=config.espn_s2,
            swid=config.espn_swid,
        )

    def extract_league(self) -> League:
        return self.league

    def extract_teams(self) -> List[Dict]:
        return self.league.teams


if __name__ == "__main__":
    extractor = ESPNExtractor(
        config.espn_league_id, 2024, config.espn_s2, config.espn_swid
    )

    for row in extractor.league.settings.scoring_format:
        print(row["label"])
        print(row["points"])
        print()
    # print(extractor.league.settings.scoring_format)
    # teams = extractor.extract_teams()
    # vs = vars(teams[0])
    # for k, v in vs.items():
    #     print(k, v)
    # vars(team_)
    # [print(team) for team in extractor.extract_teams()]
    # league = extractor.league

    # print(league.extract_teams())
