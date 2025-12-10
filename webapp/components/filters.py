"""Filter sidebar component for the webapp."""

from dash import dcc, html
import dash_bootstrap_components as dbc


def create_filters_sidebar(brands: list[str], vehicle_types: list[str], colors: list[str]) -> dbc.Col:
    """
    Create the left sidebar with all filter controls.

    Args:
        brands: List of available brands
        vehicle_types: List of available vehicle types
        colors: List of available colors

    Returns:
        Dash Bootstrap Column component with filters
    """
    return dbc.Col(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.I(
                                className="fas fa-filter",
                                style={
                                    "marginRight": "10px",
                                    "color": "#E05842",
                                    "fontSize": "24px",
                                },
                            ),
                            html.H2(
                                "Filters",
                                style={
                                    "display": "inline",
                                    "color": "#2d3436",
                                    "fontWeight": "600",
                                    "marginRight": "10px",
                                },
                            ),
                            html.Div(
                                id="filter-count-badge",
                                children=[],
                                style={"display": "inline-block"}
                            ),
                        ],
                        style={"display": "inline-block"}
                    ),
                ],
                style={
                    "marginBottom": "15px",
                    "paddingBottom": "15px",
                    "borderBottom": "2px solid #E05842",
                },
            ),
            # Clear filters button
            html.Div(
                [
                    dbc.Button(
                        [
                            html.I(
                                className="fas fa-times",
                                style={"marginRight": "6px"}
                            ),
                            "Clear All"
                        ],
                        id="clear-filters-btn",
                        color="outline-secondary",
                        size="sm",
                        style={
                            "fontSize": "12px",
                            "padding": "6px 12px",
                            "width": "100%",
                        }
                    ),
                ],
                style={"marginBottom": "20px"},
            ),
            # Brand filter
            _create_filter_section(
                "Brand",
                "tags",
                "#1976d2",
                dcc.Dropdown(
                    id="brand-filter",
                    options=[{"label": brand, "value": brand} for brand in brands],
                    value=brands[0] if brands else None,
                    multi=False,
                    placeholder="Select a brand...",
                    style={"fontSize": "13px"},
                ),
            ),
            # Model filter
            _create_filter_section(
                "Model",
                "car-alt",
                "#e17055",
                dcc.Loading(
                    id="loading-model-filter",
                    type="dot",
                    color="#e17055",
                    style={"minHeight": "36px"},
                    children=[
                        dcc.Dropdown(
                            id="model-filter",
                            options=[],
                            value=[],
                            multi=True,
                            placeholder="Select models...",
                            style={"fontSize": "13px"},
                        ),
                    ]
                ),
            ),
            # Vehicle type filter
            _create_filter_section(
                "Vehicle Type",
                "car",
                "#fd79a8",
                dcc.Dropdown(
                    id="vehicle-type-filter",
                    options=[{"label": vtype, "value": vtype} for vtype in vehicle_types],
                    value=[vehicle_types[0]] if vehicle_types else [],
                    multi=True,
                    placeholder="Select vehicle types...",
                    style={"fontSize": "13px"},
                ),
            ),
            # Color filter
            _create_filter_section(
                "Primary Color",
                "palette",
                "#00b894",
                dcc.Dropdown(
                    id="color-filter",
                    options=[{"label": color, "value": color} for color in colors],
                    value=[],
                    multi=True,
                    placeholder="Select colors...",
                    style={"fontSize": "13px"},
                ),
            ),
            # Engine size slider
            _create_range_slider_section(
                "Engine Size",
                "cogs",
                "#e17055",
                "engine-size-slider",
                "engine-size-display",
                "500-8000cc",
                min_val=500,
                max_val=8000,
                step=100,
                default_range=[500, 8000],
                marks={
                    500: {"label": "500cc", "style": {"fontSize": "11px"}},
                    2000: {"label": "2.0L", "style": {"fontSize": "11px"}},
                    4000: {"label": "4.0L", "style": {"fontSize": "11px"}},
                    6000: {"label": "6.0L", "style": {"fontSize": "11px"}},
                    8000: {"label": "8.0L", "style": {"fontSize": "11px"}},
                },
            ),
            # Weight slider
            _create_range_slider_section(
                "Vehicle Weight",
                "weight-hanging",
                "#2196f3",
                "weight-slider",
                "weight-display",
                "500-5000kg",
                min_val=500,
                max_val=5000,
                step=50,
                default_range=[500, 5000],
                marks={
                    500: {"label": "500kg", "style": {"fontSize": "11px"}},
                    1000: {"label": "1000kg", "style": {"fontSize": "11px"}},
                    2000: {"label": "2000kg", "style": {"fontSize": "11px"}},
                    3000: {"label": "3000kg", "style": {"fontSize": "11px"}},
                    5000: {"label": "5000kg", "style": {"fontSize": "11px"}},
                },
            ),
            # Seats slider
            _create_range_slider_section(
                "Number of Seats",
                "users",
                "#00cec9",
                "seats-filter",
                "seats-display",
                "1-7+ seats",
                min_val=1,
                max_val=7,
                step=1,
                default_range=[1, 7],
                marks={
                    1: {"label": "1", "style": {"fontSize": "11px"}},
                    2: {"label": "2", "style": {"fontSize": "11px"}},
                    3: {"label": "3", "style": {"fontSize": "11px"}},
                    4: {"label": "4", "style": {"fontSize": "11px"}},
                    5: {"label": "5", "style": {"fontSize": "11px"}},
                    6: {"label": "6", "style": {"fontSize": "11px"}},
                    7: {"label": "7+", "style": {"fontSize": "11px"}},
                },
            ),
            # Year range slider
            _create_range_slider_section(
                "Registration Year",
                "calendar-alt",
                "#fdcb6e",
                "year-slider",
                "year-display",
                "2000-2020",
                min_val=1980,
                max_val=2025,
                step=1,
                default_range=[2000, 2020],
                marks={
                    1980: {"label": "1980", "style": {"fontSize": "11px"}},
                    1990: {"label": "1990", "style": {"fontSize": "11px"}},
                    2000: {"label": "2000", "style": {"fontSize": "11px"}},
                    2010: {"label": "2010", "style": {"fontSize": "11px"}},
                    2020: {"label": "2020", "style": {"fontSize": "11px"}},
                    2025: {"label": "2025", "style": {"fontSize": "11px"}},
                },
            ),
        ],
        width=2,
        style={
            "background": "linear-gradient(180deg, #f8f9fc 0%, #e8eef7 100%)",
            "padding": "25px 20px",
            "height": "100vh",
            "overflow": "auto",
            "borderRight": "1px solid #e3e8ee",
        },
    )


def _create_filter_section(label: str, icon: str, color: str, component) -> html.Div:
    """Helper function to create a filter section with consistent styling."""
    return html.Div(
        [
            html.Label(
                [
                    html.I(
                        className=f"fas fa-{icon}",
                        style={"marginRight": "8px", "color": color},
                    ),
                    label,
                ],
                style={
                    "fontWeight": "600",
                    "marginBottom": "8px",
                    "color": "#2d3436",
                    "fontSize": "14px",
                },
            ),
            component,
        ],
        style={
            "marginBottom": "22px",
            "padding": "15px",
            "backgroundColor": "#ffffff",
            "borderRadius": "12px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
            "border": "1px solid #e3e8ee",
        },
    )


def _create_range_slider_section(
    label: str,
    icon: str,
    color: str,
    slider_id: str,
    display_id: str,
    default_display: str,
    min_val: int,
    max_val: int,
    step: int,
    default_range: list[int],
    marks: dict,
) -> html.Div:
    """Helper function to create a range slider section with consistent styling."""
    return html.Div(
        [
            html.Div(
                [
                    html.Label(
                        [
                            html.I(
                                className=f"fas fa-{icon}",
                                style={"marginRight": "8px", "color": color},
                            ),
                            label,
                        ],
                        style={
                            "fontWeight": "600",
                            "color": "#2d3436",
                            "fontSize": "14px",
                            "display": "inline-block",
                        },
                    ),
                    html.Div(
                        id=display_id,
                        children=default_display,
                        style={
                            "float": "right",
                            "fontSize": "12px",
                            "color": color,
                            "fontWeight": "600",
                            "padding": "2px 8px",
                            "backgroundColor": f"{color}15",
                            "borderRadius": "4px",
                        }
                    ),
                ],
                style={"marginBottom": "12px"}
            ),
            dcc.RangeSlider(
                id=slider_id,
                min=min_val,
                max=max_val,
                step=step,
                value=default_range,
                marks=marks,
                tooltip={"placement": "bottom", "always_visible": True},
            ),
        ],
        style={
            "marginBottom": "22px",
            "padding": "15px",
            "backgroundColor": "#ffffff",
            "borderRadius": "12px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.08)",
            "border": "1px solid #e3e8ee",
        },
    )