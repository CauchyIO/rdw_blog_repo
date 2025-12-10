"""Callbacks for page routing."""

from urllib.parse import unquote
import dash
from dash import Input, Output, html
import dash_bootstrap_components as dbc
import pandas as pd

from components.layouts import create_main_layout, create_car_detail_layout, create_overview_tab
from tabs import create_license_plate_tab, create_prompt_feedback_tab


def register_routing_callbacks(
    app: dash.Dash,
    df_pandas: pd.DataFrame,
    brands: list[str],
    vehicle_types: list[str],
    colors: list[str],
):
    """Register routing callbacks for page navigation and tab switching."""

    @app.callback(Output("page-content", "children"), Input("url", "pathname"))
    def display_page(pathname):
        """Route to appropriate page based on URL pathname."""
        if pathname is None or pathname == "/":
            return create_main_layout(brands, vehicle_types, colors)
        elif pathname.startswith("/car/"):
            encoded_kenteken = pathname.split("/car/")[1]
            kenteken = unquote(encoded_kenteken)  # Decode URL-encoded license plate
            return create_car_detail_layout(kenteken, df_pandas)
        else:
            return html.Div(
                [
                    html.H1("404 - Page Not Found", style={"textAlign": "center"}),
                    dbc.Button(
                        "Back",
                        href="/",
                        color="primary",
                        style={"margin": "20px auto", "display": "block"},
                    ),
                ]
            )

    @app.callback(
        Output("tabs-content", "children"),
        Input("tabs", "active_tab"),
    )
    def render_tab_content(active_tab):
        """Render content based on selected tab."""
        if active_tab == "overview":
            return create_overview_tab(brands, vehicle_types, colors)
        elif active_tab == "license-plate":
            return create_license_plate_tab()
        elif active_tab == "prompt-feedback":
            return create_prompt_feedback_tab()
        return html.Div("Select a tab")