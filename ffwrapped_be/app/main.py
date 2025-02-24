from collections import defaultdict
import logging
import cachetools
from typing import List, Optional, Dict
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session, joinedload
from ffwrapped_be.db import databases as db
from ffwrapped_be.db.databases import get_db
from ffwrapped_be.etl import utils
from ffwrapped_be.config import config
from ffwrapped_be.app.service.best_lineup import (
    LeagueLineupSettings,
    Player,
    get_best_weekly_lineup,
    LineupResponse,
)


app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

cache = cachetools.LRUCache(maxsize=128)


@app.get("/")
def read_root():
    return {"Hello": "World"}


def update_weekly_stat_names():
    update_dict = {"receptions": "rec", "fumbles": "fum_lost"}
    for key, value in update_dict.items():
        print(key, value)


@app.get(
    "/leagues/{league_id}/teams/lineups/best-drafted",
    response_model=Dict[int, LineupResponse],
    response_model_exclude_unset=True,
)
def get_best_lineup_drafted(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: Optional[int] = Query(None, alias="week"),
    db_session: Session = Depends(get_db),
):
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    league_lineup = LeagueLineupSettings(**league.lineup_config)
    scoring_config = league.scoring_config

    # Get the ESPN-based player_week rows for the team
    players = db.get_draft_team_players(league_id, str(teamId), 2024, db_session)
    for player in players:
        # TODO: Include D/ST and K to draft team
        if not player.seasons:
            logger.error(f"Player {player.player_id} has no player_seasons")
            raise Exception(f"Player {player.player_id} has no player_seasons")
        if not player.seasons[0].espn_weeks_dict:
            logger.error(f"Player {player.player_id} has no player_weeks")
            raise Exception(f"Player {player.player_id} has no player_weeks")

    bestLineupResponses: Dict[int, LineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player in players:
            points = 0
            player_season = player.seasons[0]
            player_week = player_season.espn_weeks_dict.get(week, None)
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player_week).items()
                if v is not None
            }
            newDict = utils.generate_derived_espn_statistics(newDict)
            newDict = {
                utils.ESPN_PLAYER_STATS_TO_SCORING_CONFIG.get(k, k): v
                for k, v in newDict.items()
                if v is not None
            }
            for k, v in newDict.items():
                if k in scoring_config.keys():
                    points += v * scoring_config[k]
            new_players.append(
                Player(
                    name=player.first_name + " " + player.last_name,
                    id=player.player_id,
                    position=player_season.position,
                    points=round(points, 2),
                )
            )
        bestLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week
        )
    return bestLineupResponses


@app.get(
    "/leagues/{league_id}/teams/lineups/actual",
    response_model=Dict[int, LineupResponse],
    response_model_exclude_unset=True,
)
def get_actual_lineup(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: Optional[int] = Query(None, alias="week"),
    db_session: Session = Depends(get_db),
):
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    league_lineup = LeagueLineupSettings(**league.lineup_config)
    scoring_config = league.scoring_config

    weekly_players = db.get_weekly_team_players(
        league_id, str(teamId), 2024, db_session
    )
    for player in weekly_players:
        if not player.seasons:
            logger.error(f"Player {player.player_id} has no player_seasons")
            raise Exception(f"Player {player.player_id} has no player_seasons")
        if not player.seasons[0].espn_weeks_dict:
            logger.error(f"Player {player.player_id} has no player_weeks")
            raise Exception(f"Player {player.player_id} has no player_weeks")

    actualLineupResponses: Dict[int, LineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player in weekly_players:
            points = 0
            player_season = player.seasons[0]
            player_week = player_season.espn_weeks_dict.get(week, None)
            if not player_week:
                logger.info(
                    f"{player.first_name} {player.last_name} has no player_week for week: {week}. Moving to next player"
                )
                continue
            league_weekly_team = player_week.league_weekly_team
            if not league_weekly_team:
                logger.info(
                    f"{player.first_name} {player.last_name} has no lineup_position on roster for week: {week}. Moving to next player"
                )
                continue
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player_week).items()
                if v is not None
            }
            newDict = utils.generate_derived_espn_statistics(newDict)
            newDict = {
                utils.ESPN_PLAYER_STATS_TO_SCORING_CONFIG.get(k, k): v
                for k, v in newDict.items()
                if v is not None
            }
            for k, v in newDict.items():
                if k in scoring_config.keys():
                    points += v * scoring_config[k]
            new_players.append(
                Player(
                    name=player.first_name + " " + player.last_name,
                    id=player.player_id,
                    position=player_season.position,
                    points=round(points, 2),
                    rank=league_weekly_team[0].lineup_position not in ["BE", "IR"],
                )
            )
        actualLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week, sortby=["rank", "points"]
        )

    return actualLineupResponses


@app.get(
    "/leagues/{league_id}/teams/lineups/best-actual",
    response_model=Dict[int, LineupResponse],
    response_model_exclude_unset=True,
)
def get_best_possible_lineup(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: Optional[int] = Query(None, alias="week"),
    db_session: Session = Depends(get_db),
):
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    league_lineup = LeagueLineupSettings(**league.lineup_config)
    scoring_config = league.scoring_config

    weekly_players = db.get_weekly_team_players(
        league_id, str(teamId), 2024, db_session
    )
    for player in weekly_players:
        if not player.seasons:
            logger.error(f"Player {player.player_id} has no player_seasons")
            raise Exception(f"Player {player.player_id} has no player_seasons")
        if not player.seasons[0].espn_weeks_dict:
            logger.error(f"Player {player.player_id} has no player_weeks")
            raise Exception(f"Player {player.player_id} has no player_weeks")

    bestLineupResponses: Dict[int, LineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player in weekly_players:
            points = 0
            player_season = player.seasons[0]
            player_week = player_season.espn_weeks_dict.get(week, None)
            if not player_week:
                logger.info(
                    f"{player.first_name} {player.last_name} has no player_week for week: {week}. Moving to next player"
                )
                continue
            league_weekly_team = player_week.league_weekly_team
            if not league_weekly_team:
                logger.info(
                    f"{player.first_name} {player.last_name} has no lineup_position on roster for week: {week}. Moving to next player"
                )
                continue
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player_week).items()
                if v is not None
            }
            newDict = utils.generate_derived_espn_statistics(newDict)
            newDict = {
                utils.ESPN_PLAYER_STATS_TO_SCORING_CONFIG.get(k, k): v
                for k, v in newDict.items()
                if v is not None
            }
            for k, v in newDict.items():
                if k in scoring_config.keys():
                    points += v * scoring_config[k]
            new_players.append(
                Player(
                    name=player.first_name + " " + player.last_name,
                    id=player.player_id,
                    position=player_season.position,
                    points=round(points, 2),
                )
            )
        bestLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week
        )
    return bestLineupResponses
