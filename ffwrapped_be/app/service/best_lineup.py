import logging
from typing import List, Dict, Optional
from collections import defaultdict
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class LeagueLineupSettings(BaseModel):
    QB: int = Field(description="Number of QBs")
    RB: int = Field(description="Number of RBs")
    WR: int = Field(description="Number of WRs")
    TE: int = Field(description="Number of TEs")

    class Config:
        allow_population_by_field_name = True
        extra = "allow"  # Allow extra fields


class Player(BaseModel):
    name: str = Field(description="Player's name")
    id: Optional[int] = Field(description="Player's ID")
    position: str = Field(description="Player's position")
    points: float = Field(description="Player's stats for each week")


class ESPNPlayer(Player):
    espn_id: int = Field(description="Player's ESPN ID")


# ========
# Response models
# ========
class BestLineupResponse(BaseModel):
    best_lineup: Dict[str, List[Player]]
    bench: Dict[str, List[Player]]


def _assemble_sorted_position_groups(
    players: List[Player], sortby: str
) -> Dict[str, List[Player]]:
    position_groups = defaultdict(list)
    for player in players:
        position_groups[player.position].append(player)

    for position in position_groups:
        position_groups[position].sort(
            key=lambda player: getattr(player, sortby), reverse=True
        )
    return position_groups


def _assemble_sorted_flex_group(
    flex_position_name: str,
    position_groups: Dict[str, List[Player]],
    sortby: str,
) -> List[Player]:
    flex_positions = [i.strip() for i in flex_position_name.split("/")]
    logger.debug("Here are flex positions: %s", flex_positions)
    eligible_players = []
    for flex_position in flex_positions:
        eligible_players.extend(position_groups.get(flex_position, []))

    eligible_players.sort(key=lambda player: getattr(player, sortby), reverse=True)
    return eligible_players


def _get_flex_positions(
    league_lineup_dict: Dict[str, int], sorted_position_groups: Dict[str, List[Player]]
) -> List[str]:
    return [
        position
        for position in league_lineup_dict.keys()
        if "/" in position
        and league_lineup_dict[position] > 0
        and position not in ["QB", "RB", "WR", "TE", "D/ST", "K"]
    ]


def get_best_weekly_lineup(
    league_lineup: LeagueLineupSettings, lineup: List[Player], week: int
):
    # Create initial position groups and fill best lineup with them
    league_lineup_dict = league_lineup.model_dump()
    sorted_position_groups = _assemble_sorted_position_groups(lineup, "points")
    logger.info(f"Sorted following position groups: {sorted_position_groups.keys()}")

    best_lineup = defaultdict(list)
    for position in sorted_position_groups.keys():
        position_count = league_lineup_dict.get(position, 0)
        for _ in range(position_count):
            player = sorted_position_groups[position].pop(0)
            best_lineup[position].append(player)

    # Fill best lineup with players from flex positions
    flex_positions = _get_flex_positions(league_lineup_dict, sorted_position_groups)
    logger.info("Flex positions: %s", flex_positions)

    for flex_position_number, flex_position in enumerate(flex_positions, 1):
        flex_count = league_lineup_dict.get(flex_position, 0)
        eligible_players = _assemble_sorted_flex_group(
            flex_position, sorted_position_groups, "points"
        )
        for _ in range(flex_count):
            player = eligible_players.pop(0)
            sorted_position_groups[player.position].pop(0)
            best_lineup[f"FLEX-{flex_position_number}"].append(player)
    return BestLineupResponse(best_lineup=best_lineup, bench=sorted_position_groups)
