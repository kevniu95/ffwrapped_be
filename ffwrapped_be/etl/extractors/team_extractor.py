import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict
import requests
from ffwrapped_be.config import config
from bs4 import BeautifulSoup
from ratelimit import limits
from ffwrapped_be.etl.utils import custom_sleep_and_retry

logger = logging.getLogger(__name__)

@custom_sleep_and_retry
@limits(calls=4, period=20)
def limited_pfref_request(url):
    time.sleep(2)
    return requests.get(url)

class Extractor(ABC):
    @abstractmethod
    def extract(self) -> List[Dict]:
        """
        Extract data from the source and return it as a list of dictionaries.
        Each dictionary represents a record or item of data.
        """
        pass

class TeamExtractor(Extractor):
    def __init__(self):
        self.url = config.pfref_base + '/teams/'
        
    def extract(self) -> List[Dict]:
        page = limited_pfref_request(self.url)
        
        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
        elif page.status_code == 429:
            logger.error('Rate limit reached')
            raise Exception('Rate limit reached')
        
        table = soup.find_all('table', id = 'teams_active')
        
        all_rows = table[0].find_all('tr')

        header_row = all_rows[1]
        header_cols = header_row.find_all('th')
        header_cols = [ele.text.strip() for ele in header_cols]

        data_rows = table[0].find_all('tr')[2:]
        team_data = []
   
        for row in data_rows:
            # Skip rows with class "partial_table"
            if 'partial_table' in row.get('class', []):
                continue

            # Extract data from 'th' and 'td' elements
            th_element = row.find('th')
            th = th_element.text.strip() if th_element else ''
            team_link = th_element.find('a')['href'] if th_element and th_element.find('a') else ''

            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            
            # Combine 'th' and 'td' data
            row_data = [th] + cols
            row_dict = dict(zip(header_cols, row_data))
            row_dict['team_link'] = team_link  # Add the team link to the dictionary
            row_dict['team_abbreviation']  = team_link.split('/')[-2]  # Add the team abbreviation to the dictionary
            if (len(row_dict) + 1) >= len(header_cols):
                team_data.append(row_dict)
            
        return team_data

class TeamDetailExtractor(Extractor):
    def __init__(self, team_abbrv: str):
        self.url = config.pfref_base + '/teams/' + team_abbrv
    
    def extract(self) -> List[Dict]:
        page = limited_pfref_request(self.url)
        
        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
        elif page.status_code == 429:
            logger.error('Rate limit reached')
            raise Exception('Rate limit reached')
        
        table = soup.find_all('table', id = 'team_index')
        
        all_rows = table[0].find_all('tr')

        header_row = all_rows[1]
        header_cols = header_row.find_all('th')
        header_cols = [ele.text.strip() for ele in header_cols]

        data_rows = table[0].find_all('tr')[2:]
        team_data = []
   
        for row in data_rows:
            # Extract data from 'th' and 'td' elements
            th_element = row.find('th')
            th = th_element.text.strip() if th_element else ''
            
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            
            # Combine 'th' and 'td' data
            row_data = [th] + cols
            row_dict = dict(zip(header_cols, row_data))
            
            # NOTE: the columns 'pts' and 'yds' for off and def not distinguished
            # Rather they are overwritten
            # Come back to if it matters
            if (len(row_dict) + 2) >= len(header_cols):
                team_data.append(row_dict)
        return team_data
        

if __name__ == '__main__':
    # team_extractor = TeamExtractor()
    # team_data = team_extractor.extract()
    # [print(row) for row in team_data]

    team_detail_extractor = TeamDetailExtractor('was')
    team_detail_data = team_detail_extractor.extract()
    [print(row) for row in team_detail_data]