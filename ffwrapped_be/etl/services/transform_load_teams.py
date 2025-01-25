import logging
from typing import List, Dict
from ffwrapped_be.etl.extractors.team_extractor import TeamExtractor, TeamDetailExtractor
from ffwrapped_be.app.data_models.orm import Team, TeamName
from ffwrapped_be.db import databases as db

logger = logging.getLogger(__name__)

class TeamTransformLoader():
    def __init__(self):
        self.team_extractor = TeamExtractor()
        self.db = db.SessionLocal()
        
    def transform_load(self):
        team_data: List[Dict] = self.team_extractor.extract()
        logger.info('Extracted generic data for all active NFL teams')
        
        # Load basic team data into the database
        team_entries = []
        for row in team_data:
            team_entry = {'team_pfref_id': row['team_abbreviation']}
            teams.append(team_entry)
            # teams[row['team_abbreviation']] = team_row_entry
        insertion_results = db.bulk_insert(team_entries, record_type = Team, flush = True, db= self.db)
        teams: List[Team] = insertion_results.all()
        logger.info(f'Successfully inserted teams in bulk!')
            
        # Load team names into the database
        for team in teams:
            logger.info(f'Extracting team detail data for team {team.team_pfref_id}')
            team_detail_extractor = TeamDetailExtractor(team.team_pfref_id)
            team_detail_data = team_detail_extractor.extract()

            team_name_entries = {}
            for row in team_detail_data:
                team_name_entry = {'season': int(row['Year']), 'tm_id': team.team_id, 'team_name': row['Tm'].strip('*')}
                team_names[row['Year']] = team_name_entry
            insertion_results = db.bulk_insert(team_name_entries, record_type = TeamName, flush = True, db=self.db)
            team_names: List[TeamName] = insertion_results.all()
            logger.info(f'Successfully inserted team names for team {team.team_pfref_id} in bulk!')
        
        db.commit(self.db)
        self.db.close()
        logger.info('Committed transaction and closed database session')

if __name__ == '__main__':
    team_transform_loader = TeamTransformLoader()
    team_transform_loader.transform_load()