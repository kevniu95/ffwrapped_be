import logging
from typing import List, Dict
from datetime import datetime

from ffwrapped_be.etl.extractors.weekly_extractor import WeeklyGameExtractor
from ffwrapped_be.app.data_models.orm import Team, Game
from ffwrapped_be.db import databases as db

logger = logging.getLogger(__name__)


class GameTransformLoader():
  def __init__(self):
      self.extractor = WeeklyGameExtractor()
      self.db = db.SessionLocal()
  

  def _get_season(self, date: str) -> str:
      # Round down date to get season
      year = date.split('-')[0]
      month = date.split('-')[1]
      if month in ['01', '02', '03', '04']:
          return int(year) - 1
      else:
          return int(year)

  def _process_home_away(self, ha_str: str) -> str:
      if ha_str == '@':
          return 'away'
      else:
          return 'home'

  def _process_date(self, date: str) -> datetime:
      return datetime.strptime(date, '%Y-%m-%d')

  def get_team_mapping(self) -> Dict[str, int]:
      team_mapping = {}
      teams = db.get_all_records(Team, self.db)
      for team in teams:
          team_mapping[team.team_pfref_id] = team.team_id
      return team_mapping
  
  def transform_load(self, year: int):
      game_data: List[Dict] = self.extractor.extract(year)
      logger.info(f'Extracted weekly game data for all active NFL teams for year {year}')

      team_mapping = self.get_team_mapping()

      game_entries = []
      for row in game_data:
          season = self._get_season(row['Date'])
          game_entry = {"season": season, 
                        "week": int(row['Week']),
                        "home_way": self._process_home_away(row["home_away"]),
                        "tm_id": team_mapping[row["Team_id"]],
                        "opp_id": team_mapping[row["Opp_id"]],
                        "game_date": self._process_date(row["Date"])
                        }
          
          game_entries.append(game_entry)
      insertion_results = db.bulk_insert(game_entries, record_type = Game, flush = True, db = self.db)
      logger.info(f'Successfully inserted weekly game info in bulk!')

      db.commit(self.db)
      self.db.close()
      logger.info('Committed transaction and closed database session')

if __name__ == '__main__':
    game_transform_loader = GameTransformLoader()
    game_transform_loader.transform_load()