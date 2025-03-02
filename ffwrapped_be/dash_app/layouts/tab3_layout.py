from dash import html, dcc
import dash_bootstrap_components as dbc


def get_tab3_layout():
    """
    Layout for Tab 3: General League Breakdown.
    """
    return dbc.Container(
        [
            html.H3("League-Wide Breakdown", className="mt-4"),
            html.P(
                "Illustrate multiple teams (1–10) over weeks (1–18), using line charts."
            ),
            # Example placeholders for the three line charts
            dcc.Graph(id="league-drafted-overview-graph"),
            dcc.Graph(id="league-actual-overview-graph"),
            dcc.Graph(id="league-selected-overview-graph"),
            # Stacked bar chart or waterfall-like analysis for each team
            html.Div(id="league-breakdown-chart", className="mt-4"),
        ],
        fluid=True,
    )
