"""Main layout components for the webapp."""

from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd

from components.filters import create_filters_sidebar
from components.results import create_results_section
from components.chat import create_chat_section
from tabs import create_license_plate_tab, create_prompt_feedback_tab
from utils.helpers import format_kenteken, format_value
from utils.styles import CATEGORY_STYLES


def create_header() -> dbc.Row:
    """Create the main header with gradient background."""
    return dbc.Row(
        [
            dbc.Col(
                [
                    html.Div(
                        [
                            html.Img(
                                src="/assets/cauchy.svg",
                                style={
                                    "height": "40px",
                                    "marginRight": "20px",
                                    "filter": "brightness(0) invert(1)",
                                },
                            ),
                            html.H1(
                                "Cars Dashboard",
                                style={
                                    "margin": "0",
                                    "color": "#ffffff",
                                    "fontWeight": "600",
                                    "fontSize": "2.5rem",
                                    "textShadow": "0 2px 4px rgba(0,0,0,0.3)",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "alignItems": "center",
                            "justifyContent": "center",
                            "padding": "25px 0",
                        },
                    )
                ],
                width=12,
            )
        ],
        style={
            "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
            "boxShadow": "0 4px 6px rgba(0,0,0,0.1)",
            "marginBottom": "0",
        },
    )


def create_overview_tab(
    brands: list[str], vehicle_types: list[str], colors: list[str]
) -> html.Div:
    """Create the overview tab content with filters, results, and floating chat."""
    return html.Div(
        [
            dbc.Row(
                [
                    create_filters_sidebar(brands, vehicle_types, colors),
                    create_results_section(),
                ],
                style={"margin": "0"},
            ),
            create_chat_section(),  # Floating chat widget
        ]
    )


def create_main_layout(
    brands: list[str], vehicle_types: list[str], colors: list[str]
) -> html.Div:
    """
    Create the main dashboard layout with tabs.

    Args:
        brands: List of available brands
        vehicle_types: List of available vehicle types
        colors: List of available colors

    Returns:
        Complete main layout
    """
    return html.Div(
        [
            # Header row with gradient background
            create_header(),
            # Tabs navigation
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Tabs(
                            [
                                dbc.Tab(label="Overview", tab_id="overview", label_style={"cursor": "pointer"}),
                                dbc.Tab(label="License Plate", tab_id="license-plate", label_style={"cursor": "pointer"}),
                                dbc.Tab(label="Prompt Feedback", tab_id="prompt-feedback", label_style={"cursor": "pointer"}),
                            ],
                            id="tabs",
                            active_tab="overview",
                            style={
                                "backgroundColor": "transparent",
                                "borderBottom": "none",
                            },
                        ),
                        width=12,
                    )
                ],
                style={
                    "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                    "marginBottom": "0",
                    "paddingLeft": "15px",
                    "paddingRight": "15px",
                },
            ),
            # Tab content
            html.Div(id="tabs-content"),
        ]
    )


def create_car_detail_layout(kenteken: str, df_pandas: pd.DataFrame) -> html.Div:
    """
    Create detailed car page layout.

    Args:
        kenteken: License plate number
        df_pandas: Pandas DataFrame with car data

    Returns:
        Detailed car information page
    """
    try:
        # Check if data is available
        if df_pandas is None or len(df_pandas) == 0:
            return _create_error_page(
                "Data Not Available",
                "No data available. Please check database connection.",
            )

        # Filter in-memory DataFrame for the specific car
        car_data = df_pandas[df_pandas["kenteken"] == kenteken]

        if car_data.empty:
            return _create_error_page(
                "Car Not Found",
                f"No car found with license plate: {kenteken}",
            )

        car = car_data.iloc[0]

        # Create car name
        car_name = f"{car.get('merk', 'Unknown')} {car.get('handelsbenaming', 'Unknown Model')}"

        # Define categories and their respective columns
        categories = {
            "General": [
                "merk", "handelsbenaming", "voertuigsoort", "variant", "uitvoering",
                "eerste_kleur", "tweede_kleur", "aantal_zitplaatsen", "aantal_deuren",
            ],
            "Historic": [
                "datum_eerste_toelating", "datum_eerste_afgifte_nederland",
                "datum_eerste_tenaamstelling_in_nederland", "vervaldatum_apk",
                "datum_tenaamstelling", "jaar_laatste_registratie_tellerstand",
                "jaar_laatste_wijziging_euro_norm",
            ],
            "Weights/Dimensions": [
                "massa_ledig_voertuig", "massa_rijklaar", "maximum_massa_samenstelling",
                "laadvermogen", "maximum_trekken_massa_geremd", "breedte", "lengte",
                "hoogte_voertuig", "wielbasis",
            ],
            "Engine/Emissions": [
                "cilinderinhoud", "aantal_cilinders", "nettomaximumvermogen",
                "brandstof_omschrijving", "brandstof_volgnummer", "co2_uitstoot_gecombineerd",
                "emissieklasse", "euro_norm",
            ],
            "Technical": [
                "aantal_wielen", "maximale_constructiesnelheid", "opgegeven_maximum_snelheid",
                "typegoedkeuringsnummer", "europese_voertuigcategorie",
                "europese_voertuigcategorie_toevoeging", "europese_uitvoeringcategorie_toevoeging",
                "plaats_chassisnummer", "technische_max_massa_voertuig",
            ],
        }

        # Organize data into categories
        categorized_details = {}
        for category, columns in categories.items():
            category_data = []
            for col in columns:
                if col in car.index and pd.notna(car[col]) and car[col] != "":
                    formatted_detail = format_value(col, car[col])
                    category_data.append(formatted_detail)
            if category_data:
                categorized_details[category] = category_data

        # Add any remaining columns that weren't categorized
        remaining_columns = []
        for col, value in car.items():
            if (
                col != "kenteken"
                and pd.notna(value)
                and value != ""
                and not any(col in cat_cols for cat_cols in categories.values())
            ):
                formatted_detail = format_value(col, value)
                remaining_columns.append(formatted_detail)

        if remaining_columns:
            categorized_details["Other"] = remaining_columns

        return html.Div(
            [
                # Header with back button
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                dbc.Button(
                                    [
                                        html.I(
                                            className="fas fa-arrow-left",
                                            style={"marginRight": "8px"},
                                        ),
                                        "Back",
                                    ],
                                    href="/",
                                    color="outline-primary",
                                    style={"marginBottom": "20px"},
                                )
                            ],
                            width=12,
                        )
                    ]
                ),
                # Car title section
                dbc.Row(
                    [
                        dbc.Col(
                            [
                                html.H1(
                                    [
                                        html.I(
                                            className="fas fa-car-side",
                                            style={"marginRight": "15px", "color": "#ffffff"},
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
                        "background": "linear-gradient(135deg, #E05842 0%, #C74430 100%)",
                        "boxShadow": "0 4px 6px rgba(0,0,0,0.1)",
                        "marginBottom": "30px",
                    },
                ),
                # License plate section
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
                                            format_kenteken(kenteken),
                                            style={
                                                "display": "inline",
                                                "color": "#00b894",
                                                "fontWeight": "600",
                                                "margin": "0",
                                            },
                                        ),
                                    ],
                                    style={"textAlign": "center", "marginBottom": "30px"},
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
                                _create_category_section(category, details)
                                for category, details in categorized_details.items()
                            ]
                        ),
                    ]
                ),
            ],
            style={"padding": "30px"},
        )

    except Exception as e:
        return _create_error_page(
            "Error Loading Car Details",
            f"Error: {str(e)}",
        )


def _create_error_page(title: str, message: str) -> html.Div:
    """Create an error page with back button."""
    return html.Div(
        [
            html.H1(title, style={"color": "#e74c3c", "textAlign": "center"}),
            html.P(message, style={"textAlign": "center"}),
            dbc.Button(
                "Back",
                href="/",
                color="primary",
                style={"margin": "20px auto", "display": "block"},
            ),
        ]
    )


def _create_category_section(category: str, details: list[tuple[str, str]]) -> html.Div:
    """Create a section for a specific category of car details."""
    category_icon, category_color = CATEGORY_STYLES.get(
        category, ("question-circle", "#636e72")
    )

    return html.Div(
        [
            # Category header
            html.H4(
                [
                    html.I(
                        className=f"fas fa-{category_icon}",
                        style={"marginRight": "10px", "color": category_color},
                    ),
                    category,
                ],
                style={
                    "color": "#2d3436",
                    "fontWeight": "600",
                    "marginBottom": "20px",
                    "paddingBottom": "10px",
                    "borderBottom": f"2px solid {category_color}",
                },
            ),
            # Category cards in a grid
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Card(
                                [
                                    dbc.CardBody(
                                        [
                                            html.H6(
                                                detail[0],
                                                style={
                                                    "color": category_color,
                                                    "fontWeight": "600",
                                                    "marginBottom": "8px",
                                                    "fontSize": "14px",
                                                },
                                            ),
                                            html.P(
                                                detail[1],
                                                style={
                                                    "color": "#2d3436",
                                                    "fontSize": "13px",
                                                    "marginBottom": "0",
                                                },
                                            ),
                                        ]
                                    )
                                ],
                                style={
                                    "border": f"1px solid {category_color}30",
                                    "borderRadius": "8px",
                                    "boxShadow": "0 2px 4px rgba(0,0,0,0.05)",
                                    "marginBottom": "10px",
                                },
                            )
                        ],
                        width=6,
                    )
                    for detail in details
                ]
            ),
        ],
        style={"marginBottom": "35px"},
    )