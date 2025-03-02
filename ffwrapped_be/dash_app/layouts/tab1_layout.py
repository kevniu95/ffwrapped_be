from dash import html, dcc
import dash_bootstrap_components as dbc


def get_tab1_layout():
    """
    Layout for Tab 1: Season Performance Overview.
    """
    return dbc.Container(
        [
            html.H3("Season Performance Dashboard", className="mt-4"),
            html.P(
                "Select a team to view performance metrics and click on any week for detailed analysis."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label("Select Team:"),
                            dcc.Dropdown(
                                id="team-dropdown",
                                options=[
                                    {"label": f"Team {i}", "value": i}
                                    for i in range(1, 11)
                                ],
                                value=1,
                                clearable=False,
                            ),
                        ],
                        width=4,
                    )
                ]
            ),
            html.Div(id="season-summary-cards", className="mt-4"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label("Select View:"),
                            dbc.RadioItems(
                                id="view-toggle",
                                options=[
                                    {"label": "Show All", "value": "all"},
                                    {
                                        "label": "Roster Comparison",
                                        "value": "roster_comparison",
                                    },
                                    {
                                        "label": "Lineup Decisions",
                                        "value": "lineup_comparison",
                                    },
                                ],
                                value="all",
                                inline=True,
                                className="mb-3",
                            ),
                        ],
                        width=12,
                    )
                ]
            ),
            dcc.Graph(id="season-overview-chart"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dcc.Graph(
                                id="season-waterfall",
                            )
                        ],
                        width=6,
                    ),
                ]
            ),
        ],
        fluid=True,
    )
