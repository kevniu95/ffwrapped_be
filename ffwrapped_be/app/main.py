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
    BestLineupResponse,
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


@app.get("/leagues/{league_id}/teams/lineups/best-drafted")
def get_best_lineup_drafted(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: Optional[int] = Query(None, alias="week"),
    db_session: Session = Depends(get_db),
):
    # TODO: Include D/ST and K
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    lineup_config = league.lineup_config
    league_lineup = LeagueLineupSettings(**lineup_config)

    scoring_config = league.scoring_config

    # Get set of drafted players for the team
    missing = db.get_draft_team_missing("ESPN", league_id, str(teamId), db_session)
    if missing:
        # TODO: Include D/ST and K to draft team
        logger.error(f"{len(missing)} players are missing from player_weeks table")
        raise Exception(f"ETL for {len(missing)} missing player_weeks not loaded yet")

    # Get the ESPN-based player_week rows for the team
    player_week_rows = db.get_draft_team_weekly_espn_rows(
        league_id, str(teamId), db_session
    )

    week_rows = defaultdict(list)
    for player_week_row in player_week_rows:
        week_rows[player_week_row.week].append(player_week_row)

    bestLineupResponses: Dict[int, BestLineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player_week_row in week_rows[week]:
            points = 0
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player_week_row).items()
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
                    name=player_week_row.player_season.player.first_name
                    + " "
                    + player_week_row.player_season.player.last_name,
                    id=player_week_row.player_season.player.player_id,
                    position=player_week_row.player_season.position,
                    points=round(points, 2),
                )
            )
        print(f"Adding to bestLineupResponses for week {week}")
        bestLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week
        )
    return bestLineupResponses


@app.get("/leagues/{league_id}/teams/lineups/actual")
def get_actual_lineup(
    league_season_id: int,
    teamId: int = Query(..., alias="teamId"),
    week: int = Query(..., alias="week"),
    db: Session = Depends(get_db),
):
    # Logic to get the actual lineup for the team for the given week
    # ...
    return {"team_id": teamId, "week": week, "actual_lineup": "data"}


@app.get("/leagues/{league_id}/teams/lineups/best-actual")
def get_best_possible_lineup(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: Optional[int] = Query(None, alias="week"),
    db_session: Session = Depends(get_db),
):
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    linup_config = league.lineup_config
    league_lineup = LeagueLineupSettings(**linup_config)

    scoring_config = league.scoring_config

    missing = db.get_weekly_league_team_missing(
        "ESPN", league_id, str(teamId), db_session
    )

    if missing:
        # TODO: Include D/ST and K to draft team
        logger.error(f"{len(missing)} players are missing from player_weeks table")
        raise Exception(f"ETL for {len(missing)} missing player_weeks not loaded yet")

    player_week_rows = db.get_weekly_espn_rows(league_id, str(teamId), db_session, week)

    week_rows = defaultdict(list)
    for player_week_row in player_week_rows:
        week_rows[player_week_row.week].append(player_week_row)

    bestLineupResponses: Dict[int, BestLineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player_week_row in week_rows[week]:
            points = 0
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player_week_row).items()
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
                    name=player_week_row.player_season.player.first_name
                    + " "
                    + player_week_row.player_season.player.last_name,
                    id=player_week_row.player_season.player.player_id,
                    position=player_week_row.player_season.position,
                    points=round(points, 2),
                )
            )
        bestLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week
        )
    return bestLineupResponses
