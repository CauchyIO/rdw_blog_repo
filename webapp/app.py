"""Main Dash application entry point for the RDW Vehicle Dashboard."""

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

# Utility imports
from utils.styles import get_custom_css
from utils.data_loader import load_car_data, get_spark

# Callback imports
from callbacks.filter_callbacks import register_filter_callbacks
from callbacks.results_callbacks import register_results_callbacks
from callbacks.chat_callbacks import register_chat_callbacks
from callbacks.routing_callbacks import register_routing_callbacks

# Tab imports
from tabs import (
    register_license_plate_callbacks,
    register_prompt_feedback_callbacks,
)

# Backend imports
from ai_chat import AIChat


# Initialize Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css",
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
        "https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;500;600;700;800&display=swap",
    ],
    suppress_callback_exceptions=True,
)

# Add custom CSS
app.index_string = get_custom_css()

# Initialize AI Chat
ai_chat = AIChat()

# Load data using data_loader utility
df_pandas, brands, vehicle_types, colors = load_car_data(limit=5000)

# Get Spark session for direct queries (license plate lookup, etc.)
spark = get_spark()


# Set up main layout
app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="view-mode-store", data="grid"),
        html.Div(id="page-content", style={"flex": "1"}),
        # Copyright footer
        html.Div(
            [
                html.Span(
                    "© 2025 Cauchy.io - All Rights Reserved",
                    style={
                        "fontSize": "10px",
                        "color": "#636e72",
                    },
                ),
            ],
            style={
                "textAlign": "center",
                "padding": "8px 4px",
                "backgroundColor": "#f8f9fc",
                "borderTop": "1px solid #e3e8ee",
                "marginTop": "auto",
            },
        ),
    ],
    style={
        "display": "flex",
        "flexDirection": "column",
        "minHeight": "100vh",
    },
)

# Register all callbacks
register_filter_callbacks(app, df_pandas)
register_results_callbacks(app, df_pandas)
register_chat_callbacks(app, ai_chat)
register_routing_callbacks(app, df_pandas, brands, vehicle_types, colors)
register_license_plate_callbacks(app, spark)
register_prompt_feedback_callbacks(app, spark)


# Expose the server for Databricks Apps deployment
server = app.server

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
