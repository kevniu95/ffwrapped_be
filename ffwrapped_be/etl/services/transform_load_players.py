import logging
from typing import List, Dict
from datetime import datetime

from ffwrapped_be.etl.extractors.weekly_extractor import WeeklyPlayerExtractor
from ffwrapped_be.app.data_models.orm import Team, PlayerWeek, Player, PlayerWeekMetadata
from ffwrapped_be.db import databases as db

logger = logging.getLogger(__name__)

class PlayerWeekTransformLoader():
    def __init__(self):
        self.extractor = WeeklyPlayerExtractor()
        self.db = db.SessionLocal()
        self.stathead_obs_per_page = 200

    def _clear_data(self):
        logger.info("Clearing all existing player data")
        db.delete_all_rows(PlayerWeek, self.db)
        db.execute_text_command("ALTER SEQUENCE player_week_player_week_id_seq RESTART WITH 1", self.db)
        logger.info('Deleted all existing player weekly data')
        
        db.delete_all_rows(Player, self.db)
        db.execute_text_command("ALTER SEQUENCE players_player_id_seq RESTART WITH 1", self.db)
        logger.info('Deleted all existing player data')
        
        db.delete_all_rows(PlayerWeekMetadata, self.db)
        db.execute_text_command("ALTER SEQUENCE player_week_metadata_player_week_metadata_id_seq RESTART WITH 1", self.db)
        logger.info('Deleted all existing player weekly metadata')
    
    def get_existing_player_ids(self, pfref_ids: List[str]) -> Dict[str, int]:
        existing_players = db.get_players_by_id(pfref_ids, db = self.db)
        return {player.pfref_id: player.player_id for player in existing_players}

    def insert_new_player_ids(self, new_players: List[Dict]) -> Dict[str, int]:
        db.bulk_insert(new_players, record_type = Player, flush = True, db=self.db)
        return self.get_existing_player_ids([player['pfref_id'] for player in new_players])

    def get_team_mapping(self) -> Dict[str, int]:
      team_mapping = {}
      teams = db.get_all_records(Team, self.db)
      for team in teams:
          team_mapping[team.team_pfref_id] = team.team_id
      return team_mapping
    
    
    def etl_season(self, year: int):
        offset = 0
        while True:
            logger.info(f'Extracting weekly player data for the year {year} at offset {offset}')
            if not self.etl_chunk(year, offset):
                logger.info(f"Didn't find any more data to extract at offset {offset}, so stopping...")
                break
            offset += self.stathead_obs_per_page
    
    def etl_chunk(self, year: int, offset: int) -> bool:
        # Check if we've already processed this chunk
        metadata = db.get_player_metadata_by_season_chunk(year, offset, db = self.db)
        if metadata and metadata.completed:
            logger.info(f'Chunk at offset {offset} for year {year} already processed, skipping')
            return True
        
        weekly_player_data: List[Dict] = self.extractor.extract_offset(year, offset)
        if not weekly_player_data:
            logger.info(f'No data found for year {year} at offset {offset}')
            return False
        logger.info(f'Extracted weekly player data for the year {year} at offset {offset}')

        pfref_ids = [row['Player_id'] for row in weekly_player_data]
        existing_player_ids : Dict[str, int] = self.get_existing_player_ids(pfref_ids)
        logger.info(f'Found {len(existing_player_ids)} existing players from this set of weekly data')

        new_players: List[Dict[str, str]] = []
        for row in weekly_player_data:
            if row['Player_id'] not in existing_player_ids:
                new_players.append({
                    "pfref_id": row["Player_id"],
                    "first_name": row["Player"].split()[0],
                    "last_name": row["Player"].split()[-1]
                })
                # Add new players here, but set their ID to -1 for now
                existing_player_ids[row['Player_id']] = -1

        if new_players:
            logger.info(f'Detected {len(new_players)} new players to insert')
            new_player_ids = self.insert_new_player_ids(new_players)
            existing_player_ids.update(new_player_ids)
            logger.info('Successfully inserted new players')
        else:
            logger.info('No new players to insert, moving on to insertion of weekly player data')

        team_mapping = self.get_team_mapping()
        player_week_entries = []
        for row in weekly_player_data:
            player_week_entry = {
                'player_id': existing_player_ids[row['Player_id']],
                'season': year,
                'week': int(row['Week']),
                'tm_id': team_mapping[row['Team_id']],
                'pass_cmp': int(row['Pass_Cmp']),
                'pass_att': int(row['Pass_Att']),
                'pass_yds': int(row['Pass_Yds']),
                'pass_td': int(row['Pass_TD']),
                'pass_int': int(row['Pass_Int']),
                'sacks': int(row['Sk']),
                'sack_yds': int(row['Sk_Yds']),
                'rush_att': int(row['Rush_Att']),
                'rush_yds': int(row['Rush_Yds']),
                'rush_td': int(row['Rush_TD']),
                'targets': int(row['Tgt']),
                'receptions': int(row['Rec']),
                'rec_yds': int(row['Rec_Yds']),
                'rec_td': int(row['Rec_TD']),
                'fumbles': int(row['Fmb']),
                'xpm': int(row['XPM']),
                'xpa': int(row['XPA']),
                'fgm': int(row['FGM']),
                'fga': int(row['FGA']),
                'points': float(row['FantPt'])
            }
            player_week_entries.append(player_week_entry)
        
        # TODO: Let's use upsert with logic for on_conflict_do_update() later on
        db.bulk_insert(player_week_entries, record_type = PlayerWeek, flush = True, db = self.db)
        logger.info(f'Successfully inserted player weekly data for offset {offset}. Added {len(player_week_entries)} entries')
        db.commit(self.db)

        # Add metadata for the chunk
        # TODO: Let's eventually use same insert method for bulk and single records
        # TODO: Let's use upsert with logic for on_conflict_do_update() later on
        metadata_entry = PlayerWeekMetadata(season = year, 
                                            chunk_start_value = offset, 
                                            chunk_size = len(player_week_entries), 
                                            completed = True)
        db.insert_record(metadata_entry, db = self.db)
        logger.info(f'Successfully inserted metadata for chunk at offset {offset}')
        
        return True

if __name__ == '__main__':
    player_week_transform_loader = PlayerWeekTransformLoader()
    player_week_transform_loader._clear_data()
    player_week_transform_loader.etl_season(2024)
    # player_week_transform_loader.etl_chunk(2024, 0)
    # player_week_transform_loader.etl_chunk(2024, 200)
