import dash_bootstrap_components as dbc
from dash import html

from ffwrapped_be.dash_app.app import app
from ffwrapped_be.dash_app.layouts.tab1_layout import get_tab1_layout
from ffwrapped_be.dash_app.layouts.tab2_layout import get_tab2_layout
from ffwrapped_be.dash_app.layouts.tab3_layout import get_tab3_layout

# App layout
app.layout = dbc.Container(
    [
        html.H1("Fantasy Football Wrapped", className="my-4 text-center"),
        dbc.Tabs(
            [
                dbc.Tab(get_tab1_layout(), label="Season Overview"),
                dbc.Tab(get_tab2_layout(), label="Weekly Analysis"),
                dbc.Tab(get_tab3_layout(), label="League Breakdown"),
            ]
        ),
        # Footer with credits
        html.Footer(
            [
                html.Hr(),
                html.P(
                    "Fantasy Football Wrapped - 2024",
                    className="text-center text-muted",
                ),
            ],
            className="mt-5",
        ),
    ],
    fluid=True,
    className="p-4",
)

from ffwrapped_be.dash_app.callbacks import tab1_callbacks

if __name__ == "__main__":
    app.run_server(debug=True)

# # Callbacks for Tab 1
# @app.callback(
#     [
#         Output("season-overview-chart", "figure"),
#         Output("season-summary-cards", "children"),
#         Output("season-waterfall", "figure"),
#     ],
#     [
#         Input("team-dropdown", "value"),
#         Input("view-toggle", "value"),
#         # Input("stat-type-toggle", "value"),
#     ],
# )
# def update_season_overview(
#     team_id,
#     view_mode,
#     # stat_type
# ):
#     """Updates the season overview chart and summary cards"""
#     season_chart = create_season_overview(team_id, view_mode)
#     summary_cards = create_season_summary_cards(team_id)
#     season_waterfall = create_season_waterfall(team_id, view_mode)
#     return season_chart, summary_cards, season_waterfall


# @app.callback(
#     [
#         Output("weekly-summary-cards", "children"),
#         Output("weekly-analysis-container", "children"),
#     ],
#     [Input("week-dropdown", "value"), Input("season-overview-chart", "clickData")],
#     [State("team-dropdown", "value")],
# )
# def update_weekly_analysis(selected_week, click_data, team_id):
#     """Updates the weekly analysis section based on dropdown or chart click"""
#     ctx = callback_context

#     # Determine if the callback was triggered by dropdown or chart click
#     if not ctx.triggered:
#         # No trigger, return empty div
#         return html.Div(), html.Div()

#     trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

#     if trigger_id == "week-dropdown" and selected_week is not None:
#         # Dropdown selection
#         week = selected_week
#     elif trigger_id == "season-overview-chart" and click_data is not None:
#         # Chart click - extract the week from click data
#         week = click_data["points"][0]["x"]
#     else:
#         # No valid selection
#         return html.Div(), html.Div()

#     # Create the weekly summary cards
#     weekly_summary = create_weekly_summary_cards(team_id, week)

#     # Create the weekly analysis components
#     weekly_analysis = create_week_analysis(team_id, week)

#     return weekly_summary, weekly_analysis


# # Callback to update week dropdown when chart is clicked
# @app.callback(
#     Output("week-dropdown", "value"),
#     [Input("season-overview-chart", "clickData")],
#     [State("week-dropdown", "value")],
# )
# def sync_week_dropdown(click_data, current_value):
#     """Updates the week dropdown when a point on the chart is clicked"""
#     if click_data is not None:
#         return click_data["points"][0]["x"]
#     return current_value


# # Callbacks for Tab 2 (League Comparison)
# @app.callback(
#     [Output("league-radar", "figure"), Output("league-comparison", "figure")],
#     [Input("team-dropdown", "value")],
# )
# def update_league_comparison(team_id):
#     """Updates the league comparison charts"""
#     # This is a placeholder that will use the existing implementation
#     # Here we're just creating empty figures to avoid errors
#     # In a real implementation, you would use your existing league comparison code

#     # Create placeholder radar chart
#     radar_fig = go.Figure()
#     radar_fig.update_layout(
#         title="League-wide Team Performance Comparison (Coming Soon)", height=500
#     )

#     # Create placeholder bar chart
#     bar_fig = go.Figure()
#     bar_fig.update_layout(
#         title="League-wide Performance Metrics (Coming Soon)", height=500
#     )

#     return radar_fig, bar_fig


# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
