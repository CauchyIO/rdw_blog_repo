"""Results section component for the webapp."""

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_results_section() -> dbc.Col:
    """
    Create the main results section in the center of the page.

    Returns:
        Dash Bootstrap Column component with results display
    """
    return dbc.Col(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.I(
                                className="fas fa-list-ul",
                                style={
                                    "marginRight": "10px",
                                    "color": "#E05842",
                                    "fontSize": "24px",
                                },
                            ),
                            html.H2(
                                "Search Results",
                                style={
                                    "display": "inline",
                                    "color": "#2d3436",
                                    "fontWeight": "600",
                                },
                            ),
                        ],
                        style={"display": "inline-block"}
                    ),
                    html.Div(
                        [
                            dbc.ButtonGroup(
                                [
                                    dbc.Button(
                                        html.I(className="fas fa-list"),
                                        id="list-view-btn",
                                        color="primary",
                                        size="sm",
                                        outline=False,
                                        style={"padding": "6px 10px"}
                                    ),
                                    dbc.Button(
                                        html.I(className="fas fa-th"),
                                        id="grid-view-btn",
                                        color="primary",
                                        size="sm",
                                        outline=True,
                                        style={"padding": "6px 10px"}
                                    ),
                                ],
                                size="sm"
                            )
                        ],
                        style={"float": "right", "marginTop": "5px"}
                    ),
                ],
                style={
                    "marginBottom": "25px",
                    "paddingBottom": "15px",
                    "borderBottom": "2px solid #E05842",
                },
            ),
            dcc.Loading(
                id="loading-results-main",
                type="dot",
                color="#E05842",
                children=[
                    html.Div(
                        id="results-content",
                        children=_create_empty_results_message(),
                    )
                ]
            ),
        ],
        width=10,
        style={
            "background": "#ffffff",
            "padding": "25px 30px",
            "height": "100vh",
            "overflow": "auto",
            "borderRight": "1px solid #e3e8ee",
        },
    )


def _create_empty_results_message() -> html.Div:
    """Create the initial empty results message."""
    return html.Div(
        [
            html.Div(
                [
                    html.I(
                        className="fas fa-info-circle",
                        style={"marginRight": "8px", "color": "#fd79a8"}
                    ),
                    "No filters applied"
                ],
                style={
                    "padding": "10px 15px",
                    "backgroundColor": "#fd79a815",
                    "border": "1px solid #fd79a830",
                    "borderRadius": "8px",
                    "marginBottom": "25px",
                    "fontSize": "14px",
                    "color": "#636e72",
                    "textAlign": "center",
                }
            ),
            html.I(
                className="fas fa-search",
                style={
                    "fontSize": "48px",
                    "color": "#ddd",
                    "marginBottom": "15px",
                },
            ),
            html.H4(
                "Ready to Search",
                style={"color": "#636e72", "marginBottom": "10px"},
            ),
            html.P(
                "Adjust the filters on the left or use the AI assistant to find your perfect car",
                style={"color": "#b2bec3", "fontSize": "16px"},
            ),
        ],
        style={
            "textAlign": "center",
            "padding": "60px 20px",
            "color": "#636e72",
        },
    )