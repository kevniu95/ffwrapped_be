from dash import html, dcc
import dash_bootstrap_components as dbc


def get_tab2_layout():
    """
    Layout for Tab 2: Weekly Analysis.
    """
    return dbc.Container(
        [
            html.H3("Weekly Analysis", className="mt-4"),
            # Show the same summary cards but for a specific week
            html.Div(id="weekly-summary-cards", className="mt-4"),
            # Toggle for comparing originally drafted vs actual or best possible vs actual
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label("Comparison Type:"),
                            dbc.RadioItems(
                                id="weekly-comparison-toggle",
                                options=[
                                    {
                                        "label": "Drafted vs Actual",
                                        "value": "drafted_vs_actual",
                                    },
                                    {
                                        "label": "Best vs Actual",
                                        "value": "best_vs_actual",
                                    },
                                ],
                                value="drafted_vs_actual",
                                inline=True,
                                className="mb-3",
                            ),
                        ],
                        width=12,
                    )
                ]
            ),
            # Table or graph to show side-by-side diffs with green/red highlighting
            html.Div(id="weekly-analysis-container", className="mt-4"),
        ],
        fluid=True,
    )
