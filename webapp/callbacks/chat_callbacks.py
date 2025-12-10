"""Callbacks for AI chat functionality."""

import dash
from dash import Input, Output, State, html

from ai_chat import AIChat


def register_chat_callbacks(app: dash.Dash, ai_chat: AIChat):
    """Register all chat-related callbacks."""

    @app.callback(
        Output("chat-panel", "style"),
        [
            Input("chat-toggle-btn", "n_clicks"),
            Input("chat-close-btn", "n_clicks"),
        ],
        State("chat-panel", "style"),
        prevent_initial_call=True,
    )
    def toggle_chat_panel(toggle_clicks, close_clicks, current_style):
        """Toggle the visibility of the chat panel."""
        ctx = dash.callback_context
        if not ctx.triggered:
            return current_style

        # Determine which button was clicked
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        # Get current display state
        current_display = current_style.get("display", "none")

        # Toggle display
        if current_display == "none":
            new_display = "block"
        else:
            new_display = "none"

        # Update the style dict
        updated_style = current_style.copy()
        updated_style["display"] = new_display

        return updated_style

    @app.callback(
        [
            Output("chat-messages", "children"),
            Output("chat-input", "value"),
            Output("brand-filter", "value", allow_duplicate=True),
            Output("model-filter", "value", allow_duplicate=True),
            Output("vehicle-type-filter", "value", allow_duplicate=True),
            Output("color-filter", "value", allow_duplicate=True),
            Output("engine-size-slider", "value", allow_duplicate=True),
            Output("weight-slider", "value", allow_duplicate=True),
            Output("seats-filter", "value", allow_duplicate=True),
            Output("year-slider", "value", allow_duplicate=True),
        ],
        [Input("send-chat-btn", "n_clicks")],
        [
            State("chat-input", "value"),
            State("chat-messages", "children"),
            State("brand-filter", "value"),
            State("model-filter", "value"),
            State("vehicle-type-filter", "value"),
            State("color-filter", "value"),
            State("engine-size-slider", "value"),
            State("weight-slider", "value"),
            State("seats-filter", "value"),
            State("year-slider", "value"),
        ],
        prevent_initial_call=True,
    )
    def handle_chat(
        send_clicks,
        user_input,
        current_messages,
        current_brand,
        current_models,
        current_vehicle_types,
        current_colors,
        current_engine_range,
        current_weight_range,
        current_seats,
        current_year_range,
    ):
        """Handle chat message processing and filter updates."""
        if not send_clicks or not user_input or user_input.strip() == "":
            return (
                current_messages,
                "",
                current_brand,
                current_models,
                current_vehicle_types,
                current_colors,
                current_engine_range,
                current_weight_range,
                current_seats,
                current_year_range,
            )

        # Create current filter state
        current_filters = {
            "brands": [current_brand] if current_brand else [],
            "models": current_models or [],
            "vehicle_types": current_vehicle_types or [],
            "colors": current_colors or [],
            "engine_range": current_engine_range or [500, 8000],
            "weight_range": current_weight_range or [500, 5000],
            "seats": current_seats or [1, 7],
            "year_range": current_year_range or [2000, 2020],
        }

        # Process user message with AI
        ai_response, suggested_filters = ai_chat.process_user_prompt(
            user_input, current_filters
        )

        # Detect which filters have been changed
        filter_changes = _detect_filter_changes(current_filters, suggested_filters)

        # Create enhanced AI response with filter changes
        ai_message_content = [
            html.I(
                className="fas fa-robot", style={"marginRight": "8px", "color": "#E05842"}
            ),
            html.Span(ai_response),
        ]

        if filter_changes:
            ai_message_content.extend(
                [
                    html.Hr(
                        style={
                            "margin": "10px 0",
                            "border": "none",
                            "borderTop": "1px solid #e3e8ee",
                        }
                    ),
                    html.Div(
                        [
                            html.I(
                                className="fas fa-sliders-h",
                                style={
                                    "marginRight": "6px",
                                    "color": "#fd79a8",
                                    "fontSize": "12px",
                                },
                            ),
                            html.Small(
                                "Filters updated:",
                                style={"fontWeight": "600", "color": "#2d3436"},
                            ),
                        ],
                        style={"marginBottom": "5px"},
                    ),
                    html.Div(
                        [
                            html.Small(
                                f"• {change}",
                                style={
                                    "display": "block",
                                    "marginBottom": "2px",
                                    "color": "#636e72",
                                },
                            )
                            for change in filter_changes
                        ]
                    ),
                ]
            )

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
                ai_message_content,
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

        # Apply suggested filters
        new_brand = suggested_filters.get(
            "brands", [current_brand] if current_brand else []
        )
        new_brand = new_brand[0] if new_brand else None
        new_models = suggested_filters.get("models", current_models)
        new_vehicle_types = suggested_filters.get("vehicle_types", current_vehicle_types)
        new_colors = suggested_filters.get("colors", current_colors)
        new_engine_range = suggested_filters.get("engine_range", current_engine_range)
        new_weight_range = suggested_filters.get("weight_range", current_weight_range)
        new_seats = suggested_filters.get("seats", current_seats)
        new_year_range = suggested_filters.get("year_range", current_year_range)

        return (
            new_messages,
            "",
            new_brand,
            new_models,
            new_vehicle_types,
            new_colors,
            new_engine_range,
            new_weight_range,
            new_seats,
            new_year_range,
        )


def _detect_filter_changes(current: dict, suggested: dict) -> list[str]:
    """Detect which filters have been changed."""
    changes = []
    filter_names = {
        "brands": "Brand",
        "models": "Models",
        "vehicle_types": "Vehicle Types",
        "colors": "Colors",
        "engine_range": "Engine Size",
        "weight_range": "Vehicle Weight",
        "seats": "Number of Seats",
        "year_range": "Registration Year",
    }

    for key, display_name in filter_names.items():
        current_value = current.get(key, [])
        suggested_value = suggested.get(key, current_value)

        # Compare values (handling different data types)
        if key in ["engine_range", "weight_range", "year_range"]:
            # Range sliders
            if current_value != suggested_value:
                if suggested_value:
                    changes.append(
                        f"{display_name}: {suggested_value[0]}-{suggested_value[1]}"
                    )
        elif key == "seats":
            # Seats is now a range slider like engine and weight
            if current_value != suggested_value:
                if suggested_value:
                    max_val = (
                        f"{suggested_value[1]}+"
                        if suggested_value[1] == 7
                        else str(suggested_value[1])
                    )
                    changes.append(f"{display_name}: {suggested_value[0]}-{max_val}")
        else:
            # Lists (brands, models, vehicle_types, colors)
            current_set = set(current_value or [])
            suggested_set = set(suggested_value or [])
            if current_set != suggested_set:
                if suggested_value:
                    display_value = (
                        ", ".join(suggested_value)
                        if len(suggested_value) <= 3
                        else f"{len(suggested_value)} items selected"
                    )
                    changes.append(f"{display_name}: {display_value}")

    return changes