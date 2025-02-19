import logging
from enum import Enum
from typing import List, Dict

from espn_api.base_pick import BasePick
from espn_api.football.box_score import BoxScore
from espn_api.football import Team, League
from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.db import databases as db
from ffwrapped_be.config import config
from ffwrapped_be.app.data_models.orm import (
    LeagueSeason,
    LeagueTeam,
    DraftTeam,
    WeeklyStarter,
    PlayerSeason,
    PlayerWeekESPN,
)
from ffwrapped_be.etl import utils

logger = logging.getLogger(__name__)


class FantasyPosition(Enum):
    QB = "QB"
    RB = "RB"
    WR = "WR"
    TE = "TE"
    FLEX = "FLEX"
    K = "K"
    DST = "DST"


# TODO: Update relationships between tables for simplified queries
# like the one you have in load_draft_teams
class ESPNTransformLoader:
    def __init__(self, league_id: int, season: int, espn_s2: str, swid: str):
        self.extractor = ESPNExtractor(league_id, season, espn_s2, swid)
        self.db = db.SessionLocal()
        self.espn_league: League = self.extractor.extract_league()
        self.espn_to_db_map = {}
        self._platform_to_league_id_mapping = None

    def _get_existing_db_league(self, espn_league: League) -> LeagueSeason:
        try:
            league_db: LeagueSeason = db.get_league_season_by_platform_league_id(
                espn_league.league_id, espn_league.year, self.db
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
            lineup_config=league.settings.position_slot_counts,
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

    def transform_load_draft_teams(self) -> None:
        # TODO: refactor to use methods and properties defined
        league = self.espn_league
        league_teams: List[LeagueTeam] = self._get_existing_db_league(
            league
        ).league_teams
        logger.info(
            f"Successfully extracted league teams for espn league {self.espn_league.league_id}"
        )

        pick_dict = {}
        draft: List[BasePick] = league.draft
        for pick in draft:
            league_team_id = self.platform_to_league_id_mapping[pick.team.team_id]
            draft_pick_number = pick.round_num * len(league_teams) + pick.round_pick
            pick_dict[pick.playerId] = {
                "league_team_id": league_team_id,
                "draft_pick_number": draft_pick_number,
            }
        logger.info("Extracted pick and league team id info from draft picks")

        player_espn_ids = [str(i) for i in list(pick_dict.keys())]
        db_players = db.get_players_by_espn_id(player_espn_ids, self.db)
        espn_to_db_map = {
            int(db_player.espn_id): {"player_id": db_player.player_id}
            for db_player in db_players
        }
        # TODO: Not all espn_player_ids have an entry in players table (defense, kickers)- fix this?
        logger.info(
            f"Extracted {len(espn_to_db_map)} player ids from db based on espn_ids"
        )

        draft_pick_entries = {
            k: pick_dict.get(k, {}) | espn_to_db_map.get(k, {})
            for k in espn_to_db_map.keys()
        }
        db.bulk_insert(draft_pick_entries.values(), DraftTeam, db=self.db)
        logger.info(
            f"Successfully inserted draft picks for league {self.espn_league.league_id}"
        )

    def _update_espn_to_db_map(self, player_espn_ids: List[str]) -> None:
        requested_players = [
            espn_id
            for espn_id in player_espn_ids
            if espn_id not in self.espn_to_db_map.keys()
        ]
        if requested_players:
            db_players = db.get_players_by_espn_id(requested_players, self.db)
            espn_to_db_map = {
                int(db_player.espn_id): {"db_player_id": db_player.player_id}
                for db_player in db_players
            }
            self.espn_to_db_map.update(espn_to_db_map)
        else:
            logger.info("All requested players already in espn_to_db_map")
        return

    @property
    def platform_to_league_id_mapping(self) -> Dict[int, int]:
        if not self._platform_to_league_id_mapping:
            league_teams: List[LeagueTeam] = self._get_existing_db_league(
                self.espn_league
            ).league_teams
            platform_to_league_id_mapping = {
                int(team.platform_team_id): int(team.league_team_id)
                for team in league_teams
            }
            logger.info("Successfully extracted league teams for espn league")
            self._platform_to_league_id_mapping = platform_to_league_id_mapping
        return self._platform_to_league_id_mapping

    def _transform_load_box_score_team(
        self, box_score: BoxScore, week: int, home_team: bool
    ):
        team = box_score.home_team if home_team else box_score.away_team
        lineup = box_score.home_lineup if home_team else box_score.away_lineup
        box_team_desc = "home" if home_team else "away"
        if not lineup:
            logger.info(
                f"No lineup info found for {box_team_desc} team in box score for week {week}"
            )
            return

        player_espn_ids = [str(player.playerId) for player in lineup]
        self._update_espn_to_db_map(player_espn_ids)
        weekly_starter_entries = []
        for player in lineup:
            if player.lineupSlot not in ["K", "D/ST"]:
                player_id = self.espn_to_db_map.get(player.playerId, {}).get(
                    "db_player_id", None
                )
                if player_id is not None:
                    position = (
                        player.lineupSlot if player.lineupSlot != "RB/WR/TE" else "FLEX"
                    )
                    weekly_starter = {
                        "league_team_id": self.platform_to_league_id_mapping[
                            team.team_id
                        ],
                        "week": week,
                        "player_id": player_id,
                        "lineup_position": position,
                    }
                    weekly_starter_entries.append(weekly_starter)
        logger.debug(
            "About to insert %s weekly starters for week %s", box_team_desc, week
        )
        db.bulk_insert(weekly_starter_entries, record_type=WeeklyStarter, db=self.db)

    def transform_load_weekly_starters(self):
        league = self.espn_league
        for week in range(1, 18):
            logger.info("Extracting box scores for week %s", week)
            box_scores = league.box_scores(week)
            for box_score in box_scores:
                self._transform_load_box_score_team(
                    box_score, week=week, home_team=True
                )
                self._transform_load_box_score_team(
                    box_score, week=week, home_team=False
                )

    def transform_load_player_week(self):
        """
        - Picks players off `players` table and uses ESPN API to determine weekly statistics
        - After this is done, you still need a separate ETL for D/ST and Kickers
        """
        BATCH_SIZE = 100
        league = self.espn_league
        players = db.get_players_with_espn_id(offset=0, db=self.db)
        logger.info(f"Successfully {len(players)} retrieved players from db")

        player_week_entries = []
        for index, player in enumerate(players, 1):
            espn_id = int(player.espn_id)
            player_info = league.player_info(playerId=espn_id)

            for week in range(1, 19):
                mapped_data = {}
                if week not in player_info.stats:
                    continue
                for key, value in player_info.stats[week]["breakdown"].items():
                    if key in utils.ESPN_PLAYER_STATS_TO_DB.keys():
                        mapped_data[utils.ESPN_PLAYER_STATS_TO_DB[key]] = value
                mapped_data.update(
                    {
                        "player_id": player.player_id,
                        "season": league.year,
                        "week": week,
                        "tm_id": player_info.proTeam,
                    }
                )
                player_week_entries.append(mapped_data)

            if index % BATCH_SIZE == 0:
                logger.info(f"Inserting weekly data for last {BATCH_SIZE} players")
                db.bulk_insert(player_week_entries, PlayerWeekESPN, db=self.db)
                player_week_entries = []

        if player_week_entries:
            remaining_count = len(player_week_entries)
            logger.info(
                f"Processing weekly data for final batch of {remaining_count} player weeks"
            )
            db.bulk_insert(player_week_entries, PlayerWeekESPN, db=self.db)

    def transform_load_player_season(self):
        """
        - Picks players off `players` table and uses ESPN API to determine position
        - Then populates `player_season` table with player positions
        """
        BATCH_SIZE = 100
        league = self.espn_league
        players = db.get_players_with_espn_id(self.db)
        players = [player for player in players if len(player.seasons) == 0]
        logger.info(f"Successfully {len(players)} retrieved players from db")

        player_season_entries = []
        for index, player in enumerate(players, 1):
            espn_id = int(player.espn_id)
            player_info = league.player_info(playerId=espn_id)
            if (
                player_info.position
                and player_info.position in FantasyPosition.__members__
            ):
                player_season_entries.append(
                    {
                        "player_id": player.player_id,
                        "season": league.year,
                        "position": player_info.position,
                    }
                )
            if index % BATCH_SIZE == 0:
                logger.info(
                    f"Inserting {BATCH_SIZE} player season entries (processed {index} of {len(players)} total)"
                )
                db.bulk_insert(player_season_entries, PlayerSeason, db=self.db)
                player_season_entries = []

        if player_season_entries:
            remaining_count = len(player_season_entries)
            logger.info(f"Processing final batch of {remaining_count} players")
            db.bulk_insert(player_season_entries, PlayerSeason, db=self.db)


if __name__ == "__main__":
    espnTransformLoader = ESPNTransformLoader(
        config.espn_league_id, 2024, config.espn_s2, config.espn_swid
    )

    # league = espnTransformLoader.espn_league
    # a = league.player_info(playerId=-16001)
    # print(a.name)
    # print(a.stats[1]["breakdown"])
    espnTransformLoader.transform_load_player_week()
    # espnTransformLoader.transform_load_player_season()

    # espnTransformLoader.transform_load_league()
    # espnTransformLoader.transform_load_teams()
    # espnTransformLoader.transform_load_draft_teams()
    # espnTransformLoader.transform_load_weekly_starters()
    # starters = espnTransformLoader.transform_load_weekly_starters()
    # league = espnTransformLoader.espn_league
    # print(league.settings.lineup_slot_counts)
    # print(league.settings.position_slot_counts)

    # espnTransformLoader.transform_load_league()
    # espnTransformLoader.transform_load_teams()
    # draft = espnTransformLoader.transform_load_draft_teams()
    # for i in draft:
    #     print(i.team)
    #     print(i.playerId)
    #     print(i.playerName)
    #     print()
