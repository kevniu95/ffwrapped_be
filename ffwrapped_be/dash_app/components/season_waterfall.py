import numpy as np
import logging
import plotly.graph_objects as go
from ffwrapped_be.dash_app.utils.data_fetcher import fetch_lineup_data
from ffwrapped_be.dash_app.utils.data_processor import process_weekly_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_season_waterfall(team_id, view_mode="all"):
    """Creates the season performance waterfall chart with toggle options"""
    logger.info(
        f"Creating season overview for team {team_id} with view mode: {view_mode}"
    )

    # Fetch data for all three scenarios
    draft_data = fetch_lineup_data(
        "leagues/47097656/teams/lineups/best-drafted", team_id
    )
    actual_best_data = fetch_lineup_data(
        "leagues/47097656/teams/lineups/best-actual", team_id
    )
    actual_lineup_data = fetch_lineup_data(
        "leagues/47097656/teams/lineups/actual", team_id
    )

    # Process data for all three scenarios
    draft_weeks, draft_points, draft_hover = process_weekly_data(draft_data)
    actual_best_weeks, actual_best_points, actual_best_hover = process_weekly_data(
        actual_best_data
    )
    actual_lineup_weeks, actual_lineup_points, actual_lineup_hover = (
        process_weekly_data(actual_lineup_data)
    )

    draft_points_avg = np.mean(draft_points)
    actual_best_points_avg = np.mean(actual_best_points)
    actual_lineup_points_avg = np.mean(actual_lineup_points)
    transaction_impact = actual_best_points_avg - draft_points_avg
    lineup_decision_impact = actual_lineup_points_avg - actual_best_points_avg

    waterfall_fig = go.Figure(
        go.Waterfall(
            name="Decision Impact",
            orientation="v",
            measure=["absolute", "relative", "relative", "total"],
            x=["Draft Value", "Transactions", "Lineup Decisions", "Actual Points"],
            textposition="outside",
            text=[
                f"{draft_points_avg:.1f}",
                f"{transaction_impact:+.1f}",
                f"{lineup_decision_impact:+.1f}",
                f"{actual_lineup_points_avg:.1f}",
            ],
            y=[
                draft_points_avg,
                transaction_impact,
                lineup_decision_impact,
                actual_lineup_points_avg,
            ],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            decreasing={"marker": {"color": "red"}},
            increasing={"marker": {"color": "green"}},
            totals={"marker": {"color": "blue"}},
        )
    )

    waterfall_fig.update_layout(
        title=f"Performance Components Breakdown",
        xaxis_title="Components",
        yaxis_title="Points",
        showlegend=False,
    )
    return waterfall_fig
