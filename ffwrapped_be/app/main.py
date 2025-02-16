import logging
import time
import re
import cachetools
from typing import Dict, List
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session, joinedload
from ffwrapped_be.db import databases as db
from ffwrapped_be.db.databases import get_db
from ffwrapped_be.app.data_models.orm import LeagueSeason, LeagueTeam, DraftTeam
from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.config import config
from ffwrapped_be.app.service.best_lineup import (
    LeagueLineupSettings,
    Player,
    get_best_weekly_lineup,
)
from fastapi.exceptions import HTTPException


from espn_api.football import League, BoxPlayer

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
    week: int = Query(..., alias="week"),
    db_session: Session = Depends(get_db),
):
    # TODO: Need to do ETL for all players anyway
    # 1. Change fumbles to fumbles lost
    # 2. Maybe just wholesale adopt ESPN scoring settings and save them in the database
    # 3. Include D/ST and K
    # 4.For extra-weird stat categories - should we calculate during ETL and save in the database?

    # TODO: Also need to consider if we want to directly return all weeks

    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    lineup_config = league.lineup_config
    league_lineup = LeagueLineupSettings(**lineup_config)

    scoring_config = league.scoring_config

    # Get set of drafted players for the team
    missing = db.get_draft_team_missing("ESPN", league_id, str(teamId), db_session)

    if missing:
        # TODO: Implement ETL for missing players, including DST and K
        # This should be irrelevant with new plan
        logger.error(f"{len(missing)} players are missing from player_weeks table")
        raise Exception(f"ETL for {len(missing)} missing player_weeks not loaded yet")

    player_week_rows = db.get_draft_team_weekly_rows(
        "ESPN", league_id, str(teamId), db_session, week
    )

    scoring_config["receptions"] = scoring_config.pop("rec")
    scoring_config["fumbles"] = scoring_config.pop("fum_lost")

    new_players: List[Player] = []
    for player_week_row in player_week_rows:
        player = player_week_row.PlayerWeek
        points = 0
        for k, v in vars(player).items():
            if k in scoring_config.keys():
                points += v * scoring_config[k]
        new_players.append(
            Player(
                name=player_week_row.first_name + " " + player_week_row.last_name,
                id=player.player_id,
                position=player_week_row.position,
                points=round(points, 2),
            )
        )
    league_lineup = LeagueLineupSettings(**lineup_config)
    print(new_players)
    return get_best_weekly_lineup(league_lineup, new_players, week)


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


@app.get("/leagues/{leauge_id}/teams/lineups/best-actual")
def get_best_possible_lineup(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: int = Query(..., alias="week"),
    db: Session = Depends(get_db),
):
    # TODO:
    # 1. Consider replacing ESPN data with database data ALTHOUGH - can we alleviate need for db just by caching ESPN data intelligently?
    #  - Run tests to see if faster to pull from cached espn league than DB
    cache_key = (league_id, 2024)
    if cache_key in cache:
        espn_league = cache[cache_key]
        logger.info(f"Cache hit. Retrieved league {league_id} from cache")
    else:
        extractor = ESPNExtractor(league_id, 2024, config.espn_s2, config.espn_swid)
        espn_league: League = extractor.extract_league()
        cache[cache_key] = espn_league
        logger.info(f"Cache miss. Extracted league {league_id} from ESPN")

    # Get box scores
    box_scores = espn_league.box_scores(week)

    # Find the team and lineup
    for box_score in box_scores:
        if (
            not isinstance(box_score.home_team, int)
            and box_score.home_team.team_id == teamId
        ):
            lineup = box_score.home_lineup
        elif (
            not isinstance(box_score.away_team, int)
            and box_score.away_team.team_id == teamId
        ):
            lineup = box_score.away_lineup

    # Clean keys in position_Slot_counts of extra punctuation
    new_players: List[Player] = []
    for player in lineup:
        points = player.stats.get(week, {}).get("points", 0)
        new_players.append(
            Player(
                name=player.name,
                id=player.playerId,
                position=player.position,
                points=points,
            )
        )
    league_lineup = LeagueLineupSettings(**espn_league.settings.position_slot_counts)
    return get_best_weekly_lineup(league_lineup, new_players, week)
