"""AI chat assistant component for the webapp."""

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_chat_section() -> html.Div:
    """
    Create a floating collapsible AI chat assistant.

    Returns:
        Floating chat widget with toggle button
    """
    return html.Div(
        [
            # Toggle button (always visible)
            html.Button(
                [
                    html.I(
                        className="fas fa-robot",
                        style={"fontSize": "24px"},
                    ),
                ],
                id="chat-toggle-btn",
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
                },
            ),
            # Chat panel (collapsible)
            html.Div(
                id="chat-panel",
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
                                        "AI Search Assistant",
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
                                id="chat-close-btn",
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
                        id="chat-messages",
                        children=[
                            html.Div(
                                [
                                    html.I(
                                        className="fas fa-robot",
                                        style={"marginRight": "8px", "color": "#E05842"},
                                    ),
                                    html.Span("Hi! I'm here to help you find cars."),
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
                                id="chat-input",
                                placeholder="Ask me about cars...",
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
                                id="send-chat-btn",
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
                    # Hidden div to store chat history
                    html.Div(id="chat-history", style={"display": "none"}),
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
        ]
    )