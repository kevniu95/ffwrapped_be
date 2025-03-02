import dash_bootstrap_components as dbc
from dash import html
from ffwrapped_be.dash_app.utils.data_fetcher import fetch_lineup_data


def create_season_summary_cards(team_id, stat_type="weekly"):
    """Creates the summary cards for season-long performance"""
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

    # Calculate total points for each scenario
    draft_total = sum(
        sum(player["points"] for pos in data["starters"].values() for player in pos)
        for week, data in draft_data.items()
    )

    actual_best_total = sum(
        sum(player["points"] for pos in data["starters"].values() for player in pos)
        for week, data in actual_best_data.items()
    )

    actual_lineup_total = sum(
        sum(player["points"] for pos in data["starters"].values() for player in pos)
        for week, data in actual_lineup_data.items()
    )

    # Count weeks for averaging
    num_weeks = len(draft_data.keys())

    # Convert to weekly averages if requested
    if stat_type == "weekly" and num_weeks > 0:
        draft_total /= num_weeks
        actual_best_total /= num_weeks
        actual_lineup_total /= num_weeks

    # Calculate impact metrics
    transaction_value = actual_best_total - draft_total
    lineup_efficiency = (
        (actual_lineup_total / actual_best_total) * 100 if actual_best_total > 0 else 0
    )
    points_left = actual_best_total - actual_lineup_total
    stat_label = "Weekly Total" if stat_type == "weekly" else "Season Total"

    # Create the summary cards - now with two larger sections
    baseline_section = dbc.Col(
        dbc.Card(
            [
                dbc.CardHeader("Baseline"),
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{draft_total:.1f}",
                                                    className="card-title",
                                                ),
                                                html.P(
                                                    f"Possible Points (Drafted)",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card",
                                ),
                                width=12,
                            ),
                        ],
                    ),
                ),
            ],
            className="mb-4",
        ),
        width=2,
    )

    # Create the summary cards - now with two larger sections
    transaction_section = dbc.Col(
        dbc.Card(
            [
                dbc.CardHeader("Transactions"),
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{actual_best_total:.1f}",
                                                    className="card-title",
                                                ),
                                                html.P(
                                                    "Total Possible Points (Actual)",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card",
                                ),
                                width=6,
                            ),
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{transaction_value:+.1f}",
                                                    className="card-title",
                                                    style={
                                                        "color": (
                                                            "green"
                                                            if transaction_value > 0
                                                            else "red"
                                                        )
                                                    },
                                                ),
                                                html.P(
                                                    "Points from Transactions",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card",
                                ),
                                width=6,
                            ),
                        ],
                    ),
                ),
            ],
            className="mb-4",
        ),
        width=4,
    )

    lineup_section = dbc.Col(
        dbc.Card(
            [
                dbc.CardHeader("Lineup Decisions"),
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{actual_lineup_total:.1f}",
                                                    className="card-title",
                                                ),
                                                html.P(
                                                    "Total Season Points",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card",
                                ),
                                width=4,
                            ),
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{lineup_efficiency:.1f}%",
                                                    className="card-title",
                                                    style={
                                                        "color": (
                                                            "green"
                                                            if lineup_efficiency > 90
                                                            else (
                                                                "orange"
                                                                if lineup_efficiency
                                                                > 75
                                                                else "red"
                                                            )
                                                        )
                                                    },
                                                ),
                                                html.P(
                                                    "Lineup Efficiency",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card",
                                ),
                                width=4,
                            ),
                            dbc.Col(
                                dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                html.H4(
                                                    f"{points_left:.1f}",
                                                    className="card-title",
                                                    style={
                                                        "color": (
                                                            "red"
                                                            if points_left > 10
                                                            else (
                                                                "orange"
                                                                if points_left > 5
                                                                else "green"
                                                            )
                                                        )
                                                    },
                                                ),
                                                html.P(
                                                    "Points Left on Bench",
                                                    className="card-text",
                                                ),
                                            ]
                                        )
                                    ],
                                    className="summary-card h-100",
                                ),
                                width=4,
                            ),
                        ]
                    )
                ),
            ],
            className="mb-4",
        ),
        width=5,
    )

    return dbc.Row([baseline_section, transaction_section, lineup_section])


# Create a function for weekly summary cards
# def create_weekly_summary_cards(team_id, week):
#     """Creates summary cards for a specific week"""
#     # Fetch data for all three scenarios for the selected week
#     draft_best = fetch_lineup_data(
#         "leagues/47097656/teams/lineups/best-drafted", team_id
#     )
#     actual_best = fetch_lineup_data(
#         "leagues/47097656/teams/lineups/best-actual", team_id
#     )
#     actual_lineup = fetch_lineup_data("leagues/47097656/teams/lineups/actual", team_id)

#     # Extract data for the selected week
#     week_str = str(week)

#     # Check if data is available for the selected week
#     if (
#         week_str not in draft_best
#         or week_str not in actual_best
#         or week_str not in actual_lineup
#     ):
#         return html.Div(
#             [html.H4(f"Week {week} data not available", className="text-danger")]
#         )

#     # Calculate totals for the week
#     draft_best_total = sum(
#         player["points"]
#         for position in draft_best[week_str]["starters"].values()
#         for player in position
#     )

#     actual_best_total = sum(
#         player["points"]
#         for position in actual_best[week_str]["starters"].values()
#         for player in position
#     )

#     actual_lineup_total = sum(
#         player["points"]
#         for position in actual_lineup[week_str]["starters"].values()
#         for player in position
#     )

#     # Calculate impact metrics
#     transaction_value = actual_best_total - draft_best_total
#     lineup_efficiency = (
#         (actual_lineup_total / actual_best_total) * 100 if actual_best_total > 0 else 0
#     )
#     points_left = actual_best_total - actual_lineup_total

#     return dbc.Row(
#         [
#             dbc.Col(
#                 dbc.Card(
#                     [
#                         dbc.CardHeader(f"Week {week} Summary"),
#                         dbc.CardBody(
#                             [
#                                 dbc.Row(
#                                     [
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{draft_best_total:.1f}",
#                                                                 className="card-title",
#                                                             ),
#                                                             html.P(
#                                                                 "Possible Points (Drafted)",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{actual_best_total:.1f}",
#                                                                 className="card-title",
#                                                             ),
#                                                             html.P(
#                                                                 "Possible Points (Actual)",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{transaction_value:+.1f}",
#                                                                 className="card-title",
#                                                                 style={
#                                                                     "color": (
#                                                                         "green"
#                                                                         if transaction_value
#                                                                         > 0
#                                                                         else "red"
#                                                                     )
#                                                                 },
#                                                             ),
#                                                             html.P(
#                                                                 "Transaction Impact",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{lineup_efficiency:.1f}%",
#                                                                 className="card-title",
#                                                                 style={
#                                                                     "color": (
#                                                                         "green"
#                                                                         if lineup_efficiency
#                                                                         > 90
#                                                                         else (
#                                                                             "orange"
#                                                                             if lineup_efficiency
#                                                                             > 75
#                                                                             else "red"
#                                                                         )
#                                                                     )
#                                                                 },
#                                                             ),
#                                                             html.P(
#                                                                 "Lineup Efficiency",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{actual_lineup_total:.1f}",
#                                                                 className="card-title",
#                                                             ),
#                                                             html.P(
#                                                                 "Actual Points",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                         dbc.Col(
#                                             dbc.Card(
#                                                 [
#                                                     dbc.CardBody(
#                                                         [
#                                                             html.H4(
#                                                                 f"{points_left:.1f}",
#                                                                 className="card-title",
#                                                                 style={
#                                                                     "color": (
#                                                                         "red"
#                                                                         if points_left
#                                                                         > 10
#                                                                         else (
#                                                                             "orange"
#                                                                             if points_left
#                                                                             > 5
#                                                                             else "green"
#                                                                         )
#                                                                     )
#                                                                 },
#                                                             ),
#                                                             html.P(
#                                                                 "Points Left on Bench",
#                                                                 className="card-text",
#                                                             ),
#                                                         ]
#                                                     )
#                                                 ],
#                                                 className="summary-card h-100",
#                                             ),
#                                             width=2,
#                                         ),
#                                     ]
#                                 )
#                             ]
#                         ),
#                     ]
#                 ),
#                 width=12,
#             )
#         ],
#         className="mb-4",
#     )
