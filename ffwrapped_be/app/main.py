import time
from collections import defaultdict
import logging
import cachetools
from typing import List, Optional, Dict
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session, joinedload
from ffwrapped_be.db import databases as db
from ffwrapped_be.db.databases import get_db
from ffwrapped_be.etl import utils
from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.config import config
from ffwrapped_be.app.service.best_lineup import (
    LeagueLineupSettings,
    Player,
    get_best_weekly_lineup,
    BestLineupResponse,
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
        league_id, str(teamId), db_session, week
    )

    week_rows = defaultdict(list)
    for player_week_row in player_week_rows:
        week_rows[player_week_row.PlayerWeekESPN.week].append(player_week_row)

    bestLineupResponses: Dict[int, BestLineupResponse] = {}
    for week in range(1, 18):
        new_players: List[Player] = []
        for player_week_row in week_rows[week]:
            player = player_week_row.PlayerWeekESPN
            points = 0
            newDict = {
                utils.DB_PLAYER_STATS_TO_ESPN.get(k, k): v
                for k, v in vars(player).items()
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
                    name=player_week_row.first_name + " " + player_week_row.last_name,
                    id=player.player_id,
                    position=player_week_row.position,
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
    # TODO:
    # 1. Run tests to see if faster to pull from cached espn league than DB
    # 2. Save weekly lineups to DB to reduce time on this endpoint

    # Timing start
    start_time = time.time()

    # Get league from DB
    db_start_time = time.time()
    league = db.get_league_season_by_platform_league_id(league_id, 2024, db_session)
    db_end_time = time.time()
    logger.info(
        f"Time to get league from DB: {db_end_time - db_start_time:.2f} seconds"
    )

    scoring_config = league.scoring_config
    cache_key = (league_id, 2024)

    espn_start_time = time.time()
    if cache_key in cache:
        espn_league = cache[cache_key]
        logger.info(f"Cache hit. Retrieved league {league_id} from cache")
    else:
        extractor = ESPNExtractor(league_id, 2024, config.espn_s2, config.espn_swid)
        espn_league: League = extractor.extract_league()
        cache[cache_key] = espn_league
        logger.info(f"Cache miss. Extracted league {league_id} from ESPN")
    espn_end_time = time.time()
    logger.info(
        f"Time to get ESPN league: {espn_end_time - espn_start_time:.2f} seconds"
    )

    total_time_for_box_scores = 0

    # Clean keys in position_Slot_counts of extra punctuation
    bestLineupResponses: Dict[int, BestLineupResponse] = {}
    for week in range(1, 18):
        week_start_time = time.time()

        # Get box scores
        box_scores_start_time = time.time()
        box_scores = espn_league.box_scores(week)
        box_scores_end_time = time.time()
        total_time_for_box_scores += box_scores_end_time - box_scores_start_time
        logger.info(
            f"Time to get box scores for week {week}: {box_scores_end_time - box_scores_start_time:.2f} seconds"
        )

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

        new_players: List[Player] = []

        players_start_time = time.time()
        for player in lineup:
            newDict = player.stats.get(week, {}).get("breakdown", {})
            newDict = {
                utils.ESPN_PLAYER_STATS_TO_SCORING_CONFIG.get(k, k): v
                for k, v in newDict.items()
                if v is not None
            }
            points = 0
            for k, v in newDict.items():
                if k in scoring_config.keys():
                    points += v * scoring_config[k]
            if (points - player.stats.get(week, {}).get("points", 0)) > 0.001:
                logger.error(
                    f"Player {player.name} has a discrepancy between calculated points and ESPN points in week {week}"
                )
            new_player = Player(
                name=player.name,
                id=player.playerId,
                position=player.position,
                points=round(points, 2),
            )
            new_players.append(new_player)
        players_end_time = time.time()
        logger.info(
            f"Time to instantiate new players and calculate points for week {week}: {players_end_time - players_start_time:.2f} seconds"
        )

        lineup_start_time = time.time()
        league_lineup = LeagueLineupSettings(
            **espn_league.settings.position_slot_counts
        )
        bestLineupResponses[int(week)] = get_best_weekly_lineup(
            league_lineup, new_players, week
        )
        lineup_end_time = time.time()
        logger.info(
            f"Time to calculate best weekly lineup for week {week}: {lineup_end_time - lineup_start_time:.2f} seconds"
        )

        week_end_time = time.time()
        logger.info(
            f"Total time for week {week}: {week_end_time - week_start_time:.2f} seconds"
        )
    logger.info("Total time for box scores: %.2f seconds", total_time_for_box_scores)
    total_end_time = time.time()
    logger.info(
        f"Total time for get_best_possible_lineup: {total_end_time - start_time:.2f} seconds"
    )

    return bestLineupResponses
