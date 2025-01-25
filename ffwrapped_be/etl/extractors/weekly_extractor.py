import logging
import requests
from typing import List, Dict
from bs4 import BeautifulSoup

from ratelimit import limits
from ffwrapped_be.config import config
from ffwrapped_be.etl.utils import custom_sleep_and_retry
from ffwrapped_be.etl.extractors.team_extractor import Extractor

logger = logging.getLogger(__name__)

@custom_sleep_and_retry
@limits(calls=9, period=30)
def limited_pfref_request(session, url):
    logger.info(f"Session is {session}")
    return session.get(url)

class WeeklyGameExtractor(Extractor):
    def __init__(self):
        stathead_base_url = config.stathead_base
        weekly_games_url = "/football/team-game-finder.cgi"
        query_params = {
            'request': 1,
            'order_by': 'team_name_abbr',
            'comp_type': 'reg',
            'order_by_asc': 1,
            'year_max': 2024,
            'team_game_max': 17,
            'match': 'team_game',
            'year_min': 2024,
            'week_num_season_min': 1,
            'timeframe': 'seasons',
            'week_num_season_max': 22,
            'team_game_min': 1,
            # 'offset': 0
        }
        self.login_url = stathead_base_url + '/users/login.cgi'
        self.username = config.stathead_username
        self.password = config.stathead_password
        self.session = requests.Session()
        self.login()
        self.url = stathead_base_url + weekly_games_url + '?' + '&'.join([f'{k}={v}' for k, v in query_params.items()])
        self.offset_increment = 200


    def login(self):
        payload = {
            'username': self.username,
            'password': self.password
        }
        response = self.session.post(self.login_url, data=payload)
        logger.info(f"Login response was {response}")
        if response.status_code == 200:
            logger.info("Logged in successfully")
        else:
            logger.error("Failed to log in")
            raise Exception("Login failed")
    

    def extract(self) -> List[Dict]:
        """
        Call extract_offset multiple times to get all the data
        """

        offset = 0
        all_data = []
        while True:
            logger.info(f"Extracting data with offset {offset}")
            data = self.extract_offset(offset)
            if not data:
                break
            all_data.extend(data)
            offset += self.offset_increment
        return all_data
        
        
    def extract_offset(self, offset: int) -> List[Dict]:
        offset_url = self.url + f'&offset={offset}'
        page = limited_pfref_request(self.session, offset_url)
        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'html.parser')
        else:
            logger.error(f"Failed to extract data from {self.url}")
            raise Exception(f"Failed to extract data from {self.url}")
        
        table = soup.find_all('table', id = 'stats')
        if not table:
            logger.info("No table found at offset {offset}")
            return []
        all_rows = table[0].find_all('tr')

        if len(all_rows) <= 25:
            logger.warning("Only 25 rows found, make sure you're logged in!")

        header_row = all_rows[0]
        header_cols = header_row.find_all('th')
        header_cols = [ele.text.strip() for ele in header_cols if ele.text.strip() != 'Rk']
        header_cols = [i if i != '' else 'home_away' for i in header_cols ]

        data_rows = table[0].find_all('tr')[1:]
        game_data = []
        for row in data_rows:
            cols = row.find_all('td')
            row_data = {}
            for header, col in zip(header_cols, cols):
                row_data[header] = col.text.strip()
                # Check for 'a' tag and extract link
                a_tag = col.find('a')
                if a_tag:
                    row_data[f"{header}_id"] = a_tag['href'].split('/')[-2]
            game_data.append(row_data)
        logger.info(f"Successfully extracted {len(game_data)} rows for offset {offset}")
        return game_data


if __name__ == '__main__':
    weekly_game_extractor = WeeklyGameExtractor()
    # print(weekly_game_extractor.url)
    game_data = weekly_game_extractor.extract()
    [print(a) for a in game_data]
    print(len(game_data))