import plotly.graph_objs as go
import numpy as np
import logging
from ffwrapped_be.dash_app.utils.data_fetcher import fetch_lineup_data
from ffwrapped_be.dash_app.utils.data_processor import process_weekly_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_season_overview(team_id, view_mode="all"):
    """Creates the season overview chart with toggle options"""
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

    # Create the figure
    fig = go.Figure()

    # Add traces based on view mode
    if view_mode == "all" or view_mode == "roster_comparison":
        # Add drafted team line
        fig.add_trace(
            go.Scatter(
                x=draft_weeks,
                y=draft_points,
                mode="lines+markers",
                name="Best lineup - originally drafted",
                # customdata=draft_hover,
                # hovertemplate="<b>Week %{x}</b><br>%{customdata}<extra></extra>",
                line=dict(color="green", width=2),
                marker=dict(size=8),
            )
        )

        # Add actual best roster line
        fig.add_trace(
            go.Scatter(
                x=actual_best_weeks,
                y=actual_best_points,
                mode="lines+markers",
                name="Best lineup - actual roster",
                # customdata=actual_best_hover,
                # hovertemplate="<b>Week %{x}</b><br>%{customdata}<extra></extra>",
                line=dict(color="blue", width=2),
                marker=dict(size=8),
            )
        )

        # Fill between best drafted and best actual (transactions)
        fig.add_trace(
            go.Scatter(
                x=draft_weeks + draft_weeks[::-1],
                y=draft_points + actual_best_points[::-1],
                fill="toself",
                fillcolor="rgba(30, 144, 255, 0.3)",  # Blue for transactions
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    if view_mode == "all" or view_mode == "lineup_comparison":
        # Add actual lineup performance
        fig.add_trace(
            go.Scatter(
                x=actual_lineup_weeks,
                y=actual_lineup_points,
                mode="lines+markers",
                name="Actual lineup performance",
                # customdata=actual_lineup_hover,
                # hovertemplate="<b>Week %{x}</b><br>%{customdata}<extra></extra>",
                line=dict(color="orange", width=2),
                marker=dict(size=8),
            )
        )

        # Add actual best roster line if it's not already there
        if view_mode != "all":
            fig.add_trace(
                go.Scatter(
                    x=actual_best_weeks,
                    y=actual_best_points,
                    mode="lines+markers",
                    name="Best lineup - actual roster",
                    # customdata=actual_best_hover,
                    # hovertemplate="<b>Week %{x}</b><br>%{customdata}<extra></extra>",
                    line=dict(color="blue", width=2),
                    marker=dict(size=8),
                )
            )

        # Fill between best actual and actual lineup (lineup decisions)
        fig.add_trace(
            go.Scatter(
                x=actual_best_weeks + actual_best_weeks[::-1],
                y=actual_best_points + actual_lineup_points[::-1],
                fill="toself",
                fillcolor="rgba(255, 165, 0, 0.3)",  # Orange for lineup decisions
                line=dict(color="rgba(255,255,255,0)"),
                hoverinfo="skip",
                showlegend=False,
            )
        )

    # Calculate averages
    avg_draft = np.mean(draft_points)
    avg_actual_best = np.mean(actual_best_points)
    avg_actual_lineup = np.mean(actual_lineup_points)

    # Add horizontal average lines based on view mode
    if view_mode == "all" or view_mode == "roster_comparison":
        fig.add_trace(
            go.Scatter(
                x=[min(draft_weeks), max(draft_weeks)],
                y=[avg_draft, avg_draft],
                mode="lines",
                name="Avg draft potential",
                line=dict(color="green", dash="dash"),
            )
        )

    if (
        view_mode == "all"
        or view_mode == "roster_comparison"
        or view_mode == "lineup_comparison"
    ):
        fig.add_trace(
            go.Scatter(
                x=[min(actual_best_weeks), max(actual_best_weeks)],
                y=[avg_actual_best, avg_actual_best],
                mode="lines",
                name="Avg best possible",
                line=dict(color="blue", dash="dash"),
            )
        )

    if view_mode == "all" or view_mode == "lineup_comparison":
        fig.add_trace(
            go.Scatter(
                x=[min(actual_lineup_weeks), max(actual_lineup_weeks)],
                y=[avg_actual_lineup, avg_actual_lineup],
                mode="lines",
                name="Avg actual score",
                line=dict(color="orange", dash="dash"),
            )
        )

    # Set appropriate title based on view mode
    title_text = "Season Performance Overview"
    if view_mode == "roster_comparison":
        title_text = "Season Performance: Drafted vs. Actual Roster Comparison"
    elif view_mode == "lineup_comparison":
        title_text = "Season Performance: Lineup Decision Comparison"

    # Add click data annotation
    fig.update_layout(
        title=title_text,
        xaxis=dict(
            title="Week",
            tickmode="linear",
            tick0=1,
            dtick=1,
        ),
        yaxis_title="Points",
        hovermode="closest",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        clickmode="event+select",
        template="plotly_white",
    )

    return fig
