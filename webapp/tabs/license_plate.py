"""License Plate search tab layout and callbacks."""
from dash import html, Input, Output, State, dcc
import dash_bootstrap_components as dbc

from .common import (
    format_kenteken,
    create_category_section,
    organize_car_data,
)


def create_license_plate_tab():
    """Create the License Plate search tab layout with floating AI assistant."""
    return html.Div(
        [
            dbc.Container(
                [
                    html.Div(
                        [
                            html.H2(
                                "License Plate Search",
                                style={
                                    "color": "#2d3436",
                                    "marginBottom": "20px",
                                    "fontWeight": "600",
                                    "marginTop": "30px",
                                    "textAlign": "center",
                                },
                            ),
                            html.P(
                                "Enter a Dutch license plate to view detailed vehicle information",
                                style={"color": "#636e72", "marginBottom": "40px", "textAlign": "center"},
                            ),
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Div(
                                                "NL",
                                                className="eu-badge"
                                            ),
                                            dbc.Input(
                                                id="kenteken-input",
                                                placeholder="AB-12-CD",
                                                type="text",
                                                maxLength=8,
                                                className="license-plate-input",
                                            ),
                                        ],
                                        className="license-plate-container",
                                    ),
                                ],
                                style={"display": "flex", "justifyContent": "center", "marginBottom": "30px"}
                            ),
                            html.Div(
                                dbc.Button(
                                    [
                                        html.I(className="fas fa-search", style={"marginRight": "8px"}),
                                        "Search"
                                    ],
                                    id="kenteken-search-btn",
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
                            html.Div(id="kenteken-results"),
                            # Hidden store for car data
                            dcc.Store(id="car-data-store"),
                        ],
                        style={"maxWidth": "800px", "margin": "0 auto"},
                    )
                ],
                fluid=True,
                style={"padding": "20px"},
            ),
            # Floating AI assistant for car details (hidden by default)
            html.Div(
                id="car-assistant-container",
                children=[
                    # Toggle button
                    html.Button(
                        [
                            html.I(
                                className="fas fa-robot",
                                style={"fontSize": "24px"},
                            ),
                        ],
                        id="car-chat-toggle-btn",
                        style={
                            "position": "fixed",
                            "bottom": "20px",
                            "right": "20px",
                            "width": "60px",
                            "height": "60px",
                            "borderRadius": "50%",
                            "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                            "border": "none",
                            "color": "#ffffff",
                            "cursor": "pointer",
                            "boxShadow": "0 4px 12px rgba(224, 88, 66, 0.4)",
                            "zIndex": "1001",
                            "transition": "all 0.3s ease",
                            "display": "none",  # Hidden by default
                        },
                    ),
                    # Chat panel
                    html.Div(
                        id="car-chat-panel",
                        children=[
                            # Header with close button
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-robot",
                                                style={
                                                    "marginRight": "10px",
                                                    "color": "#ffffff",
                                                    "fontSize": "20px",
                                                },
                                            ),
                                            html.H4(
                                                "Ask About This Car",
                                                style={
                                                    "display": "inline",
                                                    "color": "#ffffff",
                                                    "fontWeight": "600",
                                                    "margin": "0",
                                                    "fontSize": "18px",
                                                },
                                            ),
                                        ],
                                        style={"display": "inline-block"},
                                    ),
                                    html.Button(
                                        html.I(className="fas fa-times"),
                                        id="car-chat-close-btn",
                                        style={
                                            "float": "right",
                                            "background": "transparent",
                                            "border": "none",
                                            "color": "#ffffff",
                                            "fontSize": "20px",
                                            "cursor": "pointer",
                                            "padding": "0",
                                        },
                                    ),
                                ],
                                style={
                                    "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                                    "padding": "15px 20px",
                                    "borderRadius": "12px 12px 0 0",
                                },
                            ),
                            # Chat messages area
                            html.Div(
                                id="car-chat-messages",
                                children=[
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-robot",
                                                style={"marginRight": "8px", "color": "#E05842"},
                                            ),
                                            html.Span("Hi! I can answer questions about this vehicle."),
                                        ],
                                        style={
                                            "padding": "15px",
                                            "background": "linear-gradient(135deg, #E0584210 0%, #C7443010 100%)",
                                            "borderRadius": "12px",
                                            "marginBottom": "12px",
                                            "fontSize": "14px",
                                            "color": "#2d3436",
                                            "border": "1px solid #E0584220",
                                        },
                                    )
                                ],
                                style={
                                    "height": "350px",
                                    "overflowY": "auto",
                                    "padding": "15px",
                                    "backgroundColor": "#fafbfc",
                                },
                            ),
                            # Chat input area
                            html.Div(
                                [
                                    dcc.Textarea(
                                        id="car-chat-input",
                                        placeholder="Ask about specs, history, etc...",
                                        style={
                                            "width": "100%",
                                            "height": "80px",
                                            "marginBottom": "10px",
                                            "resize": "none",
                                            "border": "1px solid #e3e8ee",
                                            "borderRadius": "8px",
                                            "padding": "10px",
                                            "fontSize": "14px",
                                            "backgroundColor": "#ffffff",
                                            "outline": "none",
                                        },
                                    ),
                                    dbc.Button(
                                        [
                                            html.I(
                                                className="fas fa-paper-plane",
                                                style={"marginRight": "8px"},
                                            ),
                                            "Send",
                                        ],
                                        id="send-car-chat-btn",
                                        style={
                                            "width": "100%",
                                            "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                                            "border": "none",
                                            "borderRadius": "8px",
                                            "padding": "10px 15px",
                                            "fontSize": "14px",
                                            "fontWeight": "500",
                                        },
                                    ),
                                ],
                                style={
                                    "padding": "15px",
                                    "backgroundColor": "#ffffff",
                                    "borderRadius": "0 0 12px 12px",
                                },
                            ),
                        ],
                        style={
                            "position": "fixed",
                            "bottom": "90px",
                            "right": "20px",
                            "width": "400px",
                            "maxHeight": "600px",
                            "backgroundColor": "#ffffff",
                            "borderRadius": "12px",
                            "boxShadow": "0 8px 24px rgba(0,0,0,0.15)",
                            "zIndex": "1000",
                            "display": "none",  # Hidden by default
                        },
                    ),
                ],
                style={"display": "none"},  # Container hidden by default
            ),
        ]
    )


def register_callbacks(app, spark):
    """Register license plate tab callbacks."""

    @app.callback(
        [
            Output("kenteken-results", "children"),
            Output("car-data-store", "data"),
            Output("car-assistant-container", "style"),
        ],
        [Input("kenteken-search-btn", "n_clicks")],
        [State("kenteken-input", "value")],
        prevent_initial_call=True
    )
    def search_kenteken(n_clicks, kenteken_value):
        """Search for vehicle by license plate."""
        print(f"Search triggered with kenteken_value: {kenteken_value}")

        if not kenteken_value or not kenteken_value.strip():
            print("Empty kenteken value")
            return (
                html.Div(
                    "Please enter a license plate number",
                    style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
                ),
                None,
                {"display": "none"}
            )

        # Clean up kenteken input (remove spaces and dashes)
        kenteken_clean = kenteken_value.strip().replace("-", "").replace(" ", "").upper()
        print(f"Cleaned kenteken: {kenteken_clean}")

        try:
            if spark is None:
                return (
                    html.Div(
                        "Database connection not available",
                        style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
                    ),
                    None,
                    {"display": "none"}
                )

            # Query the non-deduplicated table for kenteken lookup
            spark_df = spark.table("gold_catalog.rdw_etl.gekentekende_voertuigen")
            car_data = spark_df.filter(spark_df.kenteken == kenteken_clean).limit(1).toPandas()

            if len(car_data) == 0:
                return (
                    html.Div(
                        [
                            html.I(className="fas fa-exclamation-triangle", style={"fontSize": "48px", "color": "#e74c3c", "marginBottom": "15px"}),
                            html.H4(f"No vehicle found with license plate: {format_kenteken(kenteken_clean)}", style={"color": "#e74c3c"}),
                        ],
                        style={"padding": "40px", "textAlign": "center"}
                    ),
                    None,
                    {"display": "none"}
                )

            # Display car details using the same comprehensive format as car detail page
            car = car_data.iloc[0]
            car_name = f"{car.get('merk', 'Unknown')} {car.get('handelsbenaming', 'Unknown Model')}"

            # Map Dutch colors to hex codes
            color_map = {
                "PAARS": "#9b59b6",      # Purple
                "ORANJE": "#e67e22",     # Orange
                "BEIGE": "#d4a574",      # Beige
                "ROSE": "#ff69b4",       # Rose/Pink
                "ZWART": "#2c3e50",      # Black
                "GEEL": "#f1c40f",       # Yellow
                "GROEN": "#27ae60",      # Green
                "GRIJS": "#95a5a6",      # Grey
                "CREME": "#f5e6d3",      # Cream
                "BLAUW": "#3498db",      # Blue
                "DIVERSEN": "#34495e",   # Various (dark grey)
                "ROOD": "#e74c3c",       # Red
                "WIT": "#ecf0f1",        # White
                "BRUIN": "#8b4513",      # Brown
                "Niet geregistreerd": "#2d3436",  # Not registered (black)
            }

            # Get car color
            eerste_kleur = car.get('eerste_kleur', 'Niet geregistreerd')
            car_icon_color = color_map.get(eerste_kleur, "#2d3436")  # Default to black

            # Organize data into categories
            categorized_details = organize_car_data(car)

            # Convert car row to dictionary for storage
            car_dict = car.to_dict()

            # Return comprehensive detailed view with car name, car data, and show widget
            return (
                html.Div(
                    [
                        # Car title section (simple text with icon, no colored box)
                        html.Div(
                            [
                                html.H2(
                                    [
                                        html.I(
                                            className="fas fa-car-side",
                                            style={
                                                "marginRight": "12px",
                                                "color": car_icon_color,
                                            },
                                        ),
                                        car_name,
                                    ],
                                    style={
                                        "textAlign": "center",
                                        "margin": "0",
                                        "padding": "20px 0",
                                        "color": "#2d3436",
                                        "fontWeight": "600",
                                        "fontSize": "2rem",
                                    },
                                )
                            ],
                            style={
                                "marginBottom": "30px",
                            },
                        ),
                        # Detailed information organized by categories
                        html.Div(
                            [
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
                ),
                car_dict,  # Store car data
                {"display": "block"}  # Show the AI assistant widget
            )

        except Exception as e:
            print(f"Error searching for kenteken: {e}")
            import traceback
            traceback.print_exc()
            return (
                html.Div(
                    f"Error searching for vehicle: {str(e)}",
                    style={"color": "#e74c3c", "padding": "20px", "textAlign": "center"}
                ),
                None,
                {"display": "none"}
            )

    # Toggle callbacks for car chat panel
    @app.callback(
        Output("car-chat-toggle-btn", "style"),
        [Input("car-assistant-container", "style")],
        prevent_initial_call=True
    )
    def update_toggle_button_visibility(container_style):
        """Update toggle button visibility based on container visibility."""
        if container_style.get("display") == "block":
            return {
                "position": "fixed",
                "bottom": "20px",
                "right": "20px",
                "width": "60px",
                "height": "60px",
                "borderRadius": "50%",
                "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                "border": "none",
                "color": "#ffffff",
                "cursor": "pointer",
                "boxShadow": "0 4px 12px rgba(224, 88, 66, 0.4)",
                "zIndex": "1001",
                "transition": "all 0.3s ease",
                "display": "block",
            }
        else:
            return {
                "display": "none"
            }

    @app.callback(
        Output("car-chat-panel", "style"),
        [
            Input("car-chat-toggle-btn", "n_clicks"),
            Input("car-chat-close-btn", "n_clicks"),
        ],
        State("car-chat-panel", "style"),
        prevent_initial_call=True,
    )
    def toggle_car_chat_panel(toggle_clicks, close_clicks, current_style):
        """Toggle the visibility of the car chat panel."""
        current_display = current_style.get("display", "none")
        new_display = "block" if current_display == "none" else "none"
        updated_style = current_style.copy()
        updated_style["display"] = new_display
        return updated_style

    @app.callback(
        [
            Output("car-chat-messages", "children"),
            Output("car-chat-input", "value"),
        ],
        [Input("send-car-chat-btn", "n_clicks")],
        [
            State("car-chat-input", "value"),
            State("car-chat-messages", "children"),
            State("car-data-store", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_car_chat(send_clicks, user_input, current_messages, car_data):
        """Handle car chat message processing."""
        if not send_clicks or not user_input or user_input.strip() == "":
            return current_messages, ""

        if not car_data:
            return current_messages + [
                html.Div(
                    [
                        html.I(
                            className="fas fa-robot",
                            style={"marginRight": "8px", "color": "#E05842"},
                        ),
                        html.Span("No car data available. Please search for a vehicle first."),
                    ],
                    style={
                        "padding": "15px",
                        "background": "linear-gradient(135deg, #E0584210 0%, #C7443010 100%)",
                        "borderRadius": "12px",
                        "marginBottom": "12px",
                        "fontSize": "14px",
                        "color": "#2d3436",
                        "border": "1px solid #E0584220",
                    },
                )
            ], ""

        # Import AICarChat here to avoid circular imports
        from ai_car_chat import AICarChat

        # Initialize AI car chat
        ai_car_chat = AICarChat()

        # Get AI response
        ai_response = ai_car_chat.ask_about_car(user_input, car_data)

        # Add user message to chat
        new_messages = current_messages + [
            html.Div(
                [
                    html.I(
                        className="fas fa-user",
                        style={"marginRight": "8px", "color": "#E05842"},
                    ),
                    html.Span(user_input),
                ],
                style={
                    "padding": "12px 15px",
                    "background": "linear-gradient(135deg, #E0584220 0%, #C7443020 100%)",
                    "borderRadius": "12px",
                    "marginBottom": "8px",
                    "textAlign": "right",
                    "fontSize": "14px",
                    "color": "#2d3436",
                    "border": "1px solid #E0584230",
                },
            ),
            html.Div(
                [
                    html.I(
                        className="fas fa-robot",
                        style={"marginRight": "8px", "color": "#E05842"},
                    ),
                    html.Span(ai_response),
                ],
                style={
                    "padding": "15px",
                    "background": "linear-gradient(135deg, #E0584210 0%, #C7443010 100%)",
                    "borderRadius": "12px",
                    "marginBottom": "12px",
                    "fontSize": "14px",
                    "color": "#2d3436",
                    "border": "1px solid #E0584220",
                },
            ),
        ]

        return new_messages, ""
