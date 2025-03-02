import plotly.graph_objs as go
from dash import html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import logging
from ffwrapped_be.dash_app.utils.data_fetcher import fetch_lineup_data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_week_analysis(team_id, week):
    """Creates the weekly analysis components including lineup comparison and waterfall chart"""
    logger.info(f"Creating week analysis for team {team_id}, week {week}")

    # Fetch data for all three scenarios for the selected week
    draft_best = fetch_lineup_data(
        "leagues/47097656/teams/lineups/best-drafted", team_id
    )
    actual_best = fetch_lineup_data(
        "leagues/47097656/teams/lineups/best-actual", team_id
    )
    actual_lineup = fetch_lineup_data("leagues/47097656/teams/lineups/actual", team_id)

    # Extract data for the selected week
    week_str = str(week)

    # Check if data is available for the selected week
    if (
        week_str not in draft_best
        or week_str not in actual_best
        or week_str not in actual_lineup
    ):
        logger.warning(f"Data not available for team {team_id}, week {week}")
        return html.Div(
            [html.H4(f"Week {week} data not available", className="text-danger")]
        )

    # Calculate totals
    draft_best_total = sum(
        player["points"]
        for position in draft_best[week_str]["starters"].values()
        for player in position
    )

    actual_best_total = sum(
        player["points"]
        for position in actual_best[week_str]["starters"].values()
        for player in position
    )

    actual_lineup_total = sum(
        player["points"]
        for position in actual_lineup[week_str]["starters"].values()
        for player in position
    )

    # Create lineup comparison tables for actual vs optimal
    actual_starters = []
    for position, players in actual_lineup[week_str]["starters"].items():
        for player in players:
            actual_starters.append(
                {
                    "position": position,
                    "name": player["name"],
                    "points": player["points"],
                }
            )

    best_starters = []
    for position, players in actual_best[week_str]["starters"].items():
        for player in players:
            best_starters.append(
                {
                    "position": position,
                    "name": player["name"],
                    "points": player["points"],
                }
            )

    # Create a mapping of positions to optimize comparison
    actual_by_position = {}
    for player in actual_starters:
        pos = player["position"]
        if pos not in actual_by_position:
            actual_by_position[pos] = []
        actual_by_position[pos].append(player)

    best_by_position = {}
    for player in best_starters:
        pos = player["position"]
        if pos not in best_by_position:
            best_by_position[pos] = []
        best_by_position[pos].append(player)

    # Create lineup comparison visualization
    lineup_comp_fig = go.Figure()

    # Get all unique positions
    all_positions = sorted(
        set(list(actual_by_position.keys()) + list(best_by_position.keys()))
    )

    # Position colors
    position_colors = {
        "QB": "#E41A1C",
        "RB": "#377EB8",
        "WR": "#4DAF4A",
        "TE": "#984EA3",
        "FLEX": "#FF7F00",
        "D/ST": "#FFFF33",
        "K": "#A65628",
    }

    # Initialize lists for comparison data
    positions = []
    actual_names = []
    actual_points = []
    best_names = []
    best_points = []
    colors = []

    # Compile data for each position
    for pos in all_positions:
        actual_players = actual_by_position.get(pos, [])
        best_players = best_by_position.get(pos, [])

        # Match up players by index (this assumes same number in each position)
        max_players = max(len(actual_players), len(best_players))

        for i in range(max_players):
            positions.append(pos + (f"-{i+1}" if max_players > 1 else ""))

            # Actual player info
            if i < len(actual_players):
                actual_names.append(actual_players[i]["name"])
                actual_points.append(actual_players[i]["points"])
            else:
                actual_names.append("")
                actual_points.append(0)

            # Best player info
            if i < len(best_players):
                best_names.append(best_players[i]["name"])
                best_points.append(best_players[i]["points"])
            else:
                best_names.append("")
                best_points.append(0)

            # Determine color based on if there's a difference
            if i < len(actual_players) and i < len(best_players):
                if actual_players[i]["name"] != best_players[i]["name"]:
                    colors.append("red")  # Different players
                else:
                    colors.append("green")  # Same player
            else:
                colors.append("gray")

    # Calculate point differences
    point_diffs = [b - a for a, b in zip(actual_points, best_points)]

    # Create a figure to highlight differences between actual and optimal
    lineup_diff_table = go.Figure()

    # Add the data
    lineup_diff_table.add_trace(
        go.Table(
            header=dict(
                values=[
                    "Position",
                    "Actual Starter",
                    "Points",
                    "Optimal Starter",
                    "Points",
                    "Difference",
                ],
                fill_color="#f0f0f0",
                align="left",
                font=dict(size=14),
            ),
            cells=dict(
                values=[
                    positions,
                    actual_names,
                    [f"{p:.1f}" for p in actual_points],
                    best_names,
                    [f"{p:.1f}" for p in best_points],
                    [f"{d:+.1f}" for d in point_diffs],
                ],
                fill_color=[
                    ["white"] * len(positions),
                    ["white"] * len(positions),
                    ["white"] * len(positions),
                    ["white"] * len(positions),
                    ["white"] * len(positions),
                    [
                        ["#ffcccc" if d < 0 else "#ccffcc" if d > 0 else "white"]
                        for d in point_diffs
                    ],
                ],
                align="left",
                font=dict(size=13),
            ),
        )
    )

    lineup_diff_table.update_layout(
        title=f"Week {week} - Lineup Decisions Analysis",
        height=len(positions) * 35 + 100,  # Adjust height based on number of rows
        margin=dict(l=0, r=0, t=40, b=0),
    )

    # Create waterfall chart to show decision impact
    transaction_impact = actual_best_total - draft_best_total
    lineup_decision_impact = actual_lineup_total - actual_best_total

    waterfall_fig = go.Figure(
        go.Waterfall(
            name="Decision Impact",
            orientation="v",
            measure=["absolute", "relative", "relative", "total"],
            x=["Draft Value", "Transactions", "Lineup Decisions", "Actual Points"],
            textposition="outside",
            text=[
                f"{draft_best_total:.1f}",
                f"{transaction_impact:+.1f}",
                f"{lineup_decision_impact:+.1f}",
                f"{actual_lineup_total:.1f}",
            ],
            y=[
                draft_best_total,
                transaction_impact,
                lineup_decision_impact,
                actual_lineup_total,
            ],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            decreasing={"marker": {"color": "red"}},
            increasing={"marker": {"color": "green"}},
            totals={"marker": {"color": "blue"}},
        )
    )

    waterfall_fig.update_layout(
        title=f"Week {week} - Performance Components Breakdown",
        xaxis_title="Components",
        yaxis_title="Points",
        showlegend=False,
    )

    # Return a div with both visualizations
    return html.Div(
        [
            html.H3(f"Week {week} Detailed Analysis", className="mb-4"),
            dbc.Row(
                [
                    dbc.Col(
                        [dcc.Graph(figure=waterfall_fig, id="waterfall-chart")], width=6
                    ),
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardHeader(f"Week {week} Summary"),
                                    dbc.CardBody(
                                        [
                                            html.P(
                                                [
                                                    html.Strong("Week Performance: "),
                                                    html.Span(
                                                        f"{actual_lineup_total:.1f} points"
                                                    ),
                                                ]
                                            ),
                                            html.P(
                                                [
                                                    html.Strong(
                                                        "Potential with Optimal Lineup: "
                                                    ),
                                                    html.Span(
                                                        f"{actual_best_total:.1f} points"
                                                    ),
                                                ]
                                            ),
                                            html.P(
                                                [
                                                    html.Strong(
                                                        "Points Left on Bench: "
                                                    ),
                                                    html.Span(
                                                        f"{actual_best_total - actual_lineup_total:.1f} points"
                                                    ),
                                                ]
                                            ),
                                            html.P(
                                                [
                                                    html.Strong(
                                                        "Original Draft Potential: "
                                                    ),
                                                    html.Span(
                                                        f"{draft_best_total:.1f} points"
                                                    ),
                                                ]
                                            ),
                                            html.P(
                                                [
                                                    html.Strong("Transaction Impact: "),
                                                    html.Span(
                                                        f"{transaction_impact:+.1f} points",
                                                        style={
                                                            "color": (
                                                                "green"
                                                                if transaction_impact
                                                                > 0
                                                                else "red"
                                                            )
                                                        },
                                                    ),
                                                ]
                                            ),
                                            html.P(
                                                [
                                                    html.Strong(
                                                        "Lineup Decision Impact: "
                                                    ),
                                                    html.Span(
                                                        f"{lineup_decision_impact:+.1f} points",
                                                        style={
                                                            "color": (
                                                                "green"
                                                                if lineup_decision_impact
                                                                > 0
                                                                else "red"
                                                            )
                                                        },
                                                    ),
                                                ]
                                            ),
                                        ]
                                    ),
                                ]
                            )
                        ],
                        width=6,
                    ),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [dcc.Graph(figure=lineup_diff_table, id="lineup-diff-table")],
                        width=12,
                        className="mt-4",
                    )
                ]
            ),
        ]
    )
