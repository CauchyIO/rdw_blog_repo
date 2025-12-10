"""Callbacks for results display."""

from urllib.parse import quote
import dash
from dash import Input, Output, State, dcc, html
import dash_bootstrap_components as dbc
import pandas as pd

from utils.helpers import format_kenteken


def register_results_callbacks(app: dash.Dash, df_pandas: pd.DataFrame):
    """Register all results-related callbacks."""

    @app.callback(
        [
            Output("view-mode-store", "data"),
            Output("list-view-btn", "outline"),
            Output("grid-view-btn", "outline"),
        ],
        [
            Input("list-view-btn", "n_clicks"),
            Input("grid-view-btn", "n_clicks"),
        ],
        [State("view-mode-store", "data")],
        prevent_initial_call=True,
    )
    def toggle_view_mode(list_clicks, grid_clicks, current_mode):
        """Toggle between list and grid view modes."""
        ctx = dash.callback_context
        if not ctx.triggered:
            return current_mode, False, True

        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if button_id == "list-view-btn":
            return "list", False, True  # list active, grid outline
        elif button_id == "grid-view-btn":
            return "grid", True, False  # list outline, grid active

        return current_mode, current_mode != "list", current_mode != "grid"

    @app.callback(
        Output("results-content", "children"),
        [
            Input("brand-filter", "value"),
            Input("model-filter", "value"),
            Input("vehicle-type-filter", "value"),
            Input("color-filter", "value"),
            Input("engine-size-slider", "value"),
            Input("weight-slider", "value"),
            Input("seats-filter", "value"),
            Input("year-slider", "value"),
            Input("view-mode-store", "data"),
        ],
    )
    def update_results(
        brand_selected,
        models_selected,
        vehicle_types_selected,
        colors_selected,
        engine_range,
        weight_range,
        seats_selected,
        year_range,
        view_mode,
    ):
        """Update results based on filter selections."""
        try:
            if df_pandas is None or len(df_pandas) == 0:
                return html.Div(
                    "No data available. Please check database connection.",
                    style={"padding": "20px", "color": "red"},
                )

            # Start with the in-memory Pandas DataFrame
            filtered_df = df_pandas.copy()

            # Brand filter (single selection)
            if brand_selected:
                filtered_df = filtered_df[filtered_df["merk"] == brand_selected]

            # Model filter
            if models_selected:
                filtered_df = filtered_df[filtered_df["handelsbenaming"].isin(models_selected)]

            # Vehicle type filter
            if vehicle_types_selected:
                filtered_df = filtered_df[filtered_df["voertuigsoort"].isin(vehicle_types_selected)]

            # Color filter
            if colors_selected:
                filtered_df = filtered_df[filtered_df["eerste_kleur"].isin(colors_selected)]

            # Engine size filter (only apply if not at default range)
            if engine_range and engine_range != [500, 8000]:
                filtered_df = filtered_df[
                    (filtered_df["cilinderinhoud"] >= engine_range[0])
                    & (filtered_df["cilinderinhoud"] <= engine_range[1])
                ]

            # Weight filter (only apply if not at default range)
            if weight_range and weight_range != [500, 5000]:
                filtered_df = filtered_df[
                    (filtered_df["massa_ledig_voertuig"] >= weight_range[0])
                    & (filtered_df["massa_ledig_voertuig"] <= weight_range[1])
                ]

            # Seats filter (only apply if not at default range)
            if seats_selected and seats_selected != [1, 7]:
                min_seats = seats_selected[0]
                max_seats = seats_selected[1]

                if max_seats == 7:
                    # Max is 7+, include all cars with 7 or more seats
                    filtered_df = filtered_df[filtered_df["aantal_zitplaatsen"] >= min_seats]
                else:
                    # Normal range filter
                    filtered_df = filtered_df[
                        (filtered_df["aantal_zitplaatsen"] >= min_seats)
                        & (filtered_df["aantal_zitplaatsen"] <= max_seats)
                    ]

            # Year filter (extract year from datum_eerste_toelating)
            if year_range:
                # Add year column - handle both string and date types
                filtered_df["year"] = pd.to_datetime(
                    filtered_df["datum_eerste_toelating"],
                    format="%Y%m%d",
                    errors="coerce"
                ).dt.year
                filtered_df = filtered_df[
                    (filtered_df["year"] >= year_range[0])
                    & (filtered_df["year"] <= year_range[1])
                ]

            # Only take the first 25 rows
            result_df = filtered_df.head(25)

            if len(result_df) == 0:
                return _create_no_results_message()

            # Create car items based on view mode
            car_items = []

            for _, row in result_df.iterrows():
                car_items.append(_create_car_item(row, view_mode))

            # Return the results with count and layout based on view mode
            results_container = (
                dbc.Row(car_items, className="g-3")
                if view_mode == "grid"
                else html.Div(car_items)
            )

            return html.Div(
                [
                    html.Div(
                        [
                            html.I(
                                className="fas fa-info-circle",
                                style={"marginRight": "8px", "color": "#E05842"},
                            ),
                            html.Span(
                                f"Found {len(filtered_df)} matches, showing {len(result_df)} results",
                                style={"fontWeight": "600"},
                            ),
                            html.Small(
                                " (limited to 25 entries)" if len(filtered_df) > 25 else "",
                                style={"color": "#b2bec3", "marginLeft": "5px"},
                            ),
                        ],
                        style={
                            "marginBottom": "25px",
                            "padding": "15px",
                            "backgroundColor": "#f8f9fc",
                            "border": "1px solid #e3e8ee",
                            "borderRadius": "8px",
                            "color": "#2d3436",
                            "fontSize": "14px",
                        },
                    ),
                    results_container,
                ]
            )

        except Exception as e:
            return html.Div(
                f"Error loading data: {str(e)}",
                style={"padding": "20px", "color": "red"},
            )


def _create_no_results_message() -> html.Div:
    """Create a message when no results are found."""
    return html.Div(
        [
            html.I(
                className="fas fa-search-minus",
                style={"fontSize": "48px", "color": "#ddd", "marginBottom": "15px"},
            ),
            html.H4(
                "No Results Found",
                style={"color": "#636e72", "marginBottom": "10px", "fontWeight": "600"},
            ),
            html.P(
                "Try adjusting your filters or use the AI assistant to refine your search",
                style={"color": "#b2bec3", "fontSize": "16px"},
            ),
        ],
        style={"textAlign": "center", "padding": "60px 20px", "color": "#636e72"},
    )


def _create_car_item(row: pd.Series, view_mode: str):
    """Create a car item card (list or grid view)."""
    # Create car name
    car_name = f"{row.get('merk', 'Unknown')} {row.get('handelsbenaming', 'Unknown Model')}"

    # Create description with key specs
    description_parts = []
    if pd.notna(row.get("voertuigsoort")):
        description_parts.append(f"Type: {row['voertuigsoort']}")
    if pd.notna(row.get("eerste_kleur")):
        description_parts.append(f"Color: {row['eerste_kleur']}")
    if pd.notna(row.get("cilinderinhoud")):
        try:
            engine_size = int(float(row["cilinderinhoud"]))
            if engine_size >= 1000:
                liters = engine_size / 1000
                if liters == int(liters):
                    description_parts.append(f"Engine: {int(liters)}L")
                else:
                    description_parts.append(f"Engine: {liters:.1f}L")
            else:
                description_parts.append(f"Engine: {engine_size}cc")
        except (ValueError, TypeError):
            description_parts.append(f"Engine: {row['cilinderinhoud']}")
    if pd.notna(row.get("massa_ledig_voertuig")):
        try:
            weight = int(float(row["massa_ledig_voertuig"]))
            description_parts.append(f"Weight: {weight}kg")
        except (ValueError, TypeError):
            description_parts.append(f"Weight: {row['massa_ledig_voertuig']}kg")
    if pd.notna(row.get("aantal_zitplaatsen")):
        try:
            seats = int(float(row["aantal_zitplaatsen"]))
            description_parts.append(f"Seats: {seats}")
        except (ValueError, TypeError):
            description_parts.append(f"Seats: {row['aantal_zitplaatsen']}")
    if pd.notna(row.get("datum_eerste_toelating")):
        try:
            year = int(pd.to_datetime(row["datum_eerste_toelating"]).year)
            description_parts.append(f"Year: {year}")
        except (ValueError, TypeError):
            pass

    description = " • ".join(description_parts)
    kenteken = row.get("kenteken", "Unknown")

    if view_mode == "grid":
        return _create_grid_card(car_name, description, kenteken)
    else:
        return _create_list_card(car_name, description, kenteken)


def _create_grid_card(car_name: str, description: str, kenteken: str) -> dbc.Col:
    """Create a grid view card."""
    return dbc.Col(
        [
            dcc.Link(
                href=f"/car/{quote(kenteken)}",
                children=[
                    dbc.Card(
                        [
                            dbc.CardBody(
                                [
                                    html.P(
                                        description,
                                        style={
                                            "marginBottom": "8px",
                                            "color": "#636e72",
                                            "fontSize": "12px",
                                            "textAlign": "center",
                                            "lineHeight": "1.3",
                                        },
                                    ),
                                ]
                            )
                        ],
                        style={
                            "marginBottom": "15px",
                            "backgroundColor": "#ffffff",
                            "border": "1px solid #e3e8ee",
                            "borderRadius": "12px",
                            "boxShadow": "0 4px 6px rgba(0,0,0,0.07)",
                            "transition": "all 0.2s ease",
                            "cursor": "pointer",
                            "height": "200px",
                        },
                        className="car-card",
                    )
                ],
                style={"textDecoration": "none", "color": "inherit"},
            )
        ],
        width=4,
    )  # 3 cards per row


def _create_list_card(car_name: str, description: str, kenteken: str) -> dcc.Link:
    """Create a list view card."""
    return dcc.Link(
        href=f"/car/{quote(kenteken)}",
        children=[
            dbc.Card(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    # Placeholder thumbnail
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-car-side",
                                                style={"fontSize": "32px", "color": "#E05842"},
                                            )
                                        ],
                                        style={
                                            "width": "70px",
                                            "height": "70px",
                                            "background": "linear-gradient(135deg, #E0584215 0%, #C7443015 100%)",
                                            "border": "2px solid #E0584230",
                                            "borderRadius": "12px",
                                            "display": "flex",
                                            "alignItems": "center",
                                            "justifyContent": "center",
                                        },
                                    )
                                ],
                                width=2,
                                style={"display": "flex", "alignItems": "center"},
                            ),
                            dbc.Col(
                                [
                                    html.H5(
                                        car_name,
                                        style={
                                            "marginBottom": "10px",
                                            "color": "#2d3436",
                                            "fontSize": "18px",
                                            "fontWeight": "600",
                                        },
                                    ),
                                    html.P(
                                        description,
                                        style={
                                            "marginBottom": "8px",
                                            "color": "#636e72",
                                            "fontSize": "14px",
                                            "lineHeight": "1.4",
                                        },
                                    ),
                                    # Click to view details hint
                                    html.Div(
                                        [
                                            html.I(
                                                className="fas fa-eye",
                                                style={
                                                    "marginRight": "6px",
                                                    "color": "#E05842",
                                                    "fontSize": "12px",
                                                },
                                            ),
                                            html.Small(
                                                "Click to view details",
                                                style={
                                                    "color": "#E05842",
                                                    "fontSize": "12px",
                                                    "fontStyle": "italic",
                                                },
                                            ),
                                        ],
                                        style={"marginTop": "5px"},
                                    ),
                                ],
                                width=10,
                            ),
                        ],
                        className="g-0",
                    )
                ],
                style={
                    "marginBottom": "15px",
                    "padding": "20px",
                    "backgroundColor": "#ffffff",
                    "border": "1px solid #e3e8ee",
                    "borderRadius": "12px",
                    "boxShadow": "0 4px 6px rgba(0,0,0,0.07)",
                    "transition": "all 0.2s ease",
                    "cursor": "pointer",
                },
                className="car-card",
            )
        ],
        style={"textDecoration": "none", "color": "inherit"},
    )