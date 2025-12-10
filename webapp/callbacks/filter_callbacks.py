"""Callbacks for filter controls."""

import dash
from dash import Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd


def register_filter_callbacks(app: dash.Dash, df_pandas: pd.DataFrame):
    """Register all filter-related callbacks."""

    @app.callback(
        Output("filter-count-badge", "children"),
        [
            Input("brand-filter", "value"),
            Input("model-filter", "value"),
            Input("vehicle-type-filter", "value"),
            Input("color-filter", "value"),
            Input("engine-size-slider", "value"),
            Input("weight-slider", "value"),
            Input("seats-filter", "value"),
            Input("year-slider", "value"),
        ],
    )
    def update_filter_count(
        brand, models, vehicle_types, colors, engine_range, weight_range, seats, year_range
    ):
        """Update the filter count badge."""
        count = 0

        # Count active filters
        if brand:
            count += 1
        if models:
            count += 1
        if vehicle_types:
            count += 1
        if colors:
            count += 1
        if engine_range and engine_range != [500, 8000]:
            count += 1
        if weight_range and weight_range != [500, 5000]:
            count += 1
        if seats and seats != [1, 7]:
            count += 1
        if year_range and year_range != [2000, 2020]:
            count += 1

        if count == 0:
            return []
        else:
            return dbc.Badge(
                f"{count} active", color="primary", pill=True, style={"fontSize": "11px"}
            )

    @app.callback(
        Output("engine-size-display", "children"), [Input("engine-size-slider", "value")]
    )
    def update_engine_display(value):
        """Update engine size display."""
        if not value:
            return "500-8000cc"
        return f"{value[0]}-{value[1]}cc"

    @app.callback(
        Output("weight-display", "children"), [Input("weight-slider", "value")]
    )
    def update_weight_display(value):
        """Update weight display."""
        if not value:
            return "500-5000kg"
        return f"{value[0]}-{value[1]}kg"

    @app.callback(Output("seats-display", "children"), [Input("seats-filter", "value")])
    def update_seats_display(value):
        """Update seats display."""
        if not value:
            return "1-7+ seats"
        max_val = f"{value[1]}+" if value[1] == 7 else str(value[1])
        return f"{value[0]}-{max_val} seats"

    @app.callback(Output("year-display", "children"), [Input("year-slider", "value")])
    def update_year_display(value):
        """Update year display."""
        if not value:
            return "2000-2020"
        return f"{value[0]}-{value[1]}"

    @app.callback(
        [Output("model-filter", "options"), Output("model-filter", "value")],
        [Input("brand-filter", "value")],
        prevent_initial_call=True,
    )
    def update_model_options(selected_brand):
        """Update model dropdown based on selected brand."""
        if not selected_brand or df_pandas is None:
            return [], []

        try:
            # Use in-memory Pandas DataFrame - filter by selected brand
            filtered_df = df_pandas[df_pandas["merk"] == selected_brand]

            # Get unique models (handelsbenaming) for the selected brand
            models = sorted(
                [
                    x
                    for x in filtered_df["handelsbenaming"].unique()
                    if x and x != "Niet geregistreerd" and str(x).strip() != "" and pd.notna(x)
                ]
            )

            model_options = [{"label": model, "value": model} for model in models]

            return model_options, []

        except Exception as e:
            print(f"Error updating model options: {e}")
            return [], []

    @app.callback(
        [
            Output("brand-filter", "value", allow_duplicate=True),
            Output("vehicle-type-filter", "value", allow_duplicate=True),
            Output("color-filter", "value", allow_duplicate=True),
            Output("engine-size-slider", "value", allow_duplicate=True),
            Output("weight-slider", "value", allow_duplicate=True),
            Output("seats-filter", "value", allow_duplicate=True),
            Output("year-slider", "value", allow_duplicate=True),
        ],
        [Input("clear-filters-btn", "n_clicks")],
        prevent_initial_call=True,
    )
    def clear_all_filters(n_clicks):
        """Clear all filter values."""
        if n_clicks:
            return None, [], [], [500, 8000], [500, 5000], [1, 7], [2000, 2020]
        return (
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )