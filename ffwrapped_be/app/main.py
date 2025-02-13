import logging
import time
import cachetools
from typing import Dict, List
from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session, joinedload
from ffwrapped_be.db import databases as db
from ffwrapped_be.db.databases import get_db
from ffwrapped_be.app.data_models.orm import LeagueSeason, LeagueTeam, DraftTeam, Player
from ffwrapped_be.etl.extractors.espn_extractor import ESPNExtractor
from ffwrapped_be.config import config
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

# Overall TODO:
# 1. Make configurable with different league scoring settings


@app.get("/")
def read_root():
    return {"Hello": "World"}


def update_weekly_stat_names():
    update_dict = {"receptions": "rec", "fumbles": "fum_lost"}
    for key, value in update_dict.items():
        print(key, value)


def convert_player(player: BoxPlayer, week: int):
    return {
        "name": player.name,
        "id": player.playerId,
        "position": player.position,
        "points": player.stats[week].get("points", 0),
    }


def _get_best_possible_lineup(team_config: Dict, players: List[BoxPlayer], week: int):
    starters = [player for player in players if player.position != "BE"]

    # Group players by position
    position_groups = {}
    for player in starters:
        if player.position not in position_groups:
            position_groups[player.position] = []
        position_groups[player.position].append(player)

    # Sort players within each position group by points scored
    for position in position_groups:
        position_groups[position].sort(
            key=lambda x: x.stats[week].get("points", 0), reverse=True
        )

    # Select the best players for each position based on the team configuration
    best_lineup = {}
    used_players = set()

    for position, count in team_config.items():
        if position in position_groups and position not in [
            "RB/WR",
            "WR/TE",
            "RB/WR/TE",
        ]:
            for i in range(count):
                if i < len(position_groups[position]):
                    player = position_groups[position].pop(0)
                    if position not in best_lineup:
                        best_lineup[position] = []
                    best_lineup[position].append(convert_player(player, week))
                    used_players.add(player)

    flex_positions = ["RB/WR/TE"]
    for flex_position in flex_positions:
        if flex_position in team_config:
            flex_count = team_config[flex_position]
            logger.info("Flex count: %s", flex_count)
            if flex_count == 0:
                continue
            eligible_players = []
            if flex_position == "RB/WR/TE":
                eligible_players = (
                    position_groups.get("RB", [])
                    + position_groups.get("WR", [])
                    + position_groups.get("TE", [])
                )

            # Remove already used players
            eligible_players = [
                player for player in eligible_players if player not in used_players
            ]
            eligible_players.sort(
                key=lambda x: x.stats[week].get("points", 0), reverse=True
            )
            for i in range(flex_count):
                if i < len(eligible_players):
                    player = eligible_players.pop(0)
                    position_groups[player.position].pop(0)
                    if "FLEX" not in best_lineup:
                        best_lineup["FLEX"] = []
                    best_lineup["FLEX"].append(convert_player(player, week))
                    used_players.add(player)
    # Convert all position_group lists so players converted players
    for position in position_groups:
        position_groups[position] = [
            convert_player(player, week) for player in position_groups[position]
        ]
    return {"best_lineup": best_lineup, "bench": position_groups}


@app.get("/leagues/{league_id}/teams/lineups/best-drafted")
def get_best_lineup_drafted(
    league_id: str,
    teamId: int = Query(..., alias="teamId"),
    week: int = Query(..., alias="week"),
    db_session: Session = Depends(get_db),
):
    # Already loaded drafted players for this league
    # Go through them, and use player_info() method to create Player object
    # Fill out new version of player_week table using this data

    # Should then be able to fill out this endpoint

    # How are we going to get stats for players not on team during week of matchup

    # Get league season from db
    # Add new column to draft_team to show if weekly player stats loaded
    # Add new table to show weekly player stats

    cache_key = (league_id, 2024)
    if cache_key in cache:
        espn_league = cache[cache_key]
        logger.info
    else:
        extractor = ESPNExtractor(league_id, 2024, config.espn_s2, config.espn_swid)
        espn_league: League = extractor.extract_league()
        print(cache_key)
        cache[cache_key] = espn_league
        logger.info(f"Cache miss. Extracted league {league_id} from ESPN")

    info = espn_league.player_info("Patrick Mahomes")
    print(espn_league.finalScoringPeriod)
    print(info.stats.keys())


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
    # 1. Consider replacing ESPN data with database data
    # ALTHOUGH - can we alleviate need for db just by caching ESPN data intelligently?

    start_time = time.time()
    extractor_start = time.time()

    cache_key = (league_id, 2024)
    print(cache_key)
    if cache_key in cache:
        espn_league = cache[cache_key]
        logger.info
    else:
        extractor = ESPNExtractor(league_id, 2024, config.espn_s2, config.espn_swid)
        espn_league: League = extractor.extract_league()
        print(cache_key)
        cache[cache_key] = espn_league
        logger.info(f"Cache miss. Extracted league {league_id} from ESPN")
    extractor_end = time.time()
    logger.info(f"Extractor time: {extractor_end - extractor_start:.4f} seconds")

    # Get box scores
    box_scores_start = time.time()
    box_scores = espn_league.box_scores(week)
    box_scores_end = time.time()
    logger.info(f"Box scores time: {box_scores_end - box_scores_start:.4f} seconds")

    # Find the team and lineup
    team_start = time.time()
    for box_score in box_scores:
        if (
            not isinstance(box_score.home_team, int)
            and box_score.home_team.team_id == teamId
        ):
            team = box_score.home_team
            lineup = box_score.home_lineup
        elif (
            not isinstance(box_score.away_team, int)
            and box_score.away_team.team_id == teamId
        ):
            team = box_score.away_team
            lineup = box_score.away_lineup
    team_end = time.time()
    logger.info(f"Team and lineup time: {team_end - team_start:.4f} seconds")

    # Get the best possible lineup
    best_lineup_start = time.time()
    bpl = _get_best_possible_lineup(
        espn_league.settings.position_slot_counts, lineup, week
    )
    best_lineup_end = time.time()
    logger.info(
        f"Best lineup calculation time: {best_lineup_end - best_lineup_start:.4f} seconds"
    )

    end_time = time.time()
    logger.info(f"Total time: {end_time - start_time:.4f} seconds")
    return bpl
