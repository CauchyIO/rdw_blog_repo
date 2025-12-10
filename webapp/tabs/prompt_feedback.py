"""Prompt Feedback tab layout and callbacks."""
from dash import html, Input, Output
import dash_bootstrap_components as dbc

from .common import (
    format_kenteken,
    create_category_section,
    organize_car_data,
)


def create_prompt_feedback_tab():
    """Create the Prompt Feedback tab layout."""
    return html.Div(
        [
            dbc.Container(
                [
                    html.Div(
                        [
                            html.H2(
                                "Prompt Feedback",
                                style={
                                    "color": "#2d3436",
                                    "marginBottom": "20px",
                                    "fontWeight": "600",
                                    "marginTop": "30px",
                                    "textAlign": "center",
                                },
                            ),
                            html.P(
                                "Click the button below to view a random vehicle",
                                style={"color": "#636e72", "marginBottom": "40px", "textAlign": "center"},
                            ),
                            html.Div(
                                dbc.Button(
                                    [
                                        html.I(className="fas fa-random", style={"marginRight": "8px"}),
                                        "Randomize Vehicle"
                                    ],
                                    id="randomize-btn",
                                    size="lg",
                                    style={
                                        "background": "linear-gradient(135deg, rgb(224, 88, 66) 0%, rgb(200, 70, 50) 100%)",
                                        "border": "none",
                                        "padding": "12px 40px",
                                        "fontSize": "18px",
                                        "fontWeight": "600",
                                    },
                                ),
                                style={"textAlign": "center", "marginBottom": "40px"}
                            ),
                            html.Div(id="random-vehicle-results"),
                        ],
                        style={"maxWidth": "800px", "margin": "0 auto"},
                    )
                ],
                fluid=True,
                style={"padding": "40px"},
            )
        ]
    )


def register_callbacks(app, spark):
    """Register prompt feedback tab callbacks."""

    @app.callback(
        Output("random-vehicle-results", "children"),
        [Input("randomize-btn", "n_clicks")],
        prevent_initial_call=True
    )
    def show_random_vehicle(_n_clicks):
        """Display a random vehicle from the database."""
        try:
            if spark is None:
                return html.Div(
                    "Database connection not available",
                    style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
                )

            # Get a random vehicle from the table using rand() for proper random sampling
            from pyspark.sql import functions as F
            spark_df = spark.table("gold_catalog.rdw_etl.gekentekende_voertuigen")
            car_data = spark_df.orderBy(F.rand()).limit(1).toPandas()

            if len(car_data) == 0:
                return html.Div(
                    "No vehicles found in database",
                    style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
                )

            # Display car details
            car = car_data.iloc[0]
            car_name = f"{car.get('merk', 'Unknown')} {car.get('handelsbenaming', 'Unknown Model')}"

            # Organize data into categories
            categorized_details = organize_car_data(car)

            # Return comprehensive detailed view
            return html.Div(
                [
                    # Car title section with header
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.H1(
                                        [
                                            html.I(
                                                className="fas fa-car-side",
                                                style={
                                                    "marginRight": "15px",
                                                    "color": "#ffffff",
                                                },
                                            ),
                                            car_name,
                                        ],
                                        style={
                                            "textAlign": "center",
                                            "margin": "0",
                                            "padding": "25px 0",
                                            "color": "#ffffff",
                                            "fontWeight": "600",
                                            "fontSize": "2.5rem",
                                            "textShadow": "0 2px 4px rgba(0,0,0,0.3)",
                                        },
                                    )
                                ],
                                width=12,
                            )
                        ],
                        style={
                            "background": "linear-gradient(135deg, rgb(224, 88, 66) 0%, rgb(200, 70, 50) 100%)",
                            "boxShadow": "0 4px 6px rgba(0,0,0,0.1)",
                            "marginBottom": "30px",
                        },
                    ),
                    # License plate display
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-id-card",
                                                style={
                                                    "marginRight": "10px",
                                                    "color": "#00b894",
                                                    "fontSize": "24px",
                                                },
                                            ),
                                            html.H3(
                                                format_kenteken(car.get("kenteken", "Unknown")),
                                                style={
                                                    "display": "inline",
                                                    "color": "#00b894",
                                                    "fontWeight": "600",
                                                    "margin": "0",
                                                },
                                            ),
                                        ],
                                        style={
                                            "textAlign": "center",
                                            "marginBottom": "30px",
                                        },
                                    )
                                ],
                                width=12,
                            )
                        ]
                    ),
                    # Detailed information organized by categories
                    html.Div(
                        [
                            html.H3(
                                "Complete Vehicle Information",
                                style={
                                    "color": "#2d3436",
                                    "marginBottom": "30px",
                                    "fontWeight": "600",
                                    "textAlign": "center",
                                },
                            ),
                            # Create sections for each category
                            html.Div(
                                [
                                    create_category_section(category, details)
                                    for category, details in categorized_details.items()
                                ]
                            ),
                        ]
                    ),
                ],
                style={"padding": "30px"},
            )

        except Exception as e:
            print(f"Error fetching random vehicle: {e}")
            import traceback
            traceback.print_exc()
            return html.Div(
                f"Error fetching random vehicle: {str(e)}",
                style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
            )
