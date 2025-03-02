import dash
import dash_bootstrap_components as dbc

# Initialize the app - this makes the app instance available for import elsewhere
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # For production deployment
