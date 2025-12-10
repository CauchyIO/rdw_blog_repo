"""Common utilities shared across all tabs."""
import re
import pandas as pd
from dash import html
import dash_bootstrap_components as dbc


def format_kenteken(kenteken):
    """Format kenteken (license plate) by adding dashes between letter/number sequences."""
    if not kenteken or kenteken == "Unknown":
        return kenteken

    # Split letter sequences into groups of 2, processing all consecutive letters
    def split_letters(match):
        letters = match.group(0)
        # Split into groups of 2 letters
        groups = [letters[i : i + 2] for i in range(0, len(letters), 2)]
        return "-".join(groups)

    # Find sequences of 4 or more consecutive letters and split them into groups of 2
    formatted = re.sub(r"[A-Za-z]{4,}", split_letters, kenteken)

    # Add dashes between transitions from letters to numbers and numbers to letters
    formatted = re.sub(r"([A-Za-z])(\d)", r"\1-\2", formatted)
    formatted = re.sub(r"(\d)([A-Za-z])", r"\1-\2", formatted)

    return formatted


def create_category_section(category, details):
    """Create a section for a specific category of car details."""
    # Define category styling
    category_styles = {
        "General": ("info-circle", "rgb(224, 88, 66)"),
        "Historic": ("history", "#fd79a8"),
        "Weights/Dimensions": ("weight-hanging", "rgb(224, 88, 66)"),
        "Engine/Emissions": ("cogs", "#e17055"),
        "Technical": ("tools", "#00cec9"),
        "External Data": ("external-link-alt", "#00b894"),
        "Other": ("question-circle", "#636e72"),
    }

    category_icon, category_color = category_styles.get(
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
                                                label,
                                                style={
                                                    "color": category_color,
                                                    "fontWeight": "600",
                                                    "marginBottom": "8px",
                                                    "fontSize": "14px",
                                                },
                                            ),
                                            html.P(
                                                value,
                                                style={
                                                    "color": "#2d3436",
                                                    "fontSize": "13px",
                                                    "marginBottom": "0",
                                                    "wordWrap": "break-word",
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
                    for label, value in details.items()
                ]
            ),
        ],
        style={"marginBottom": "35px"},
    )


def format_value(col, value):
    """Format a value based on its column type."""
    display_name = col.replace("_", " ").title()

    if col == "datum_eerste_toelating":
        try:
            formatted_date = pd.to_datetime(value).strftime("%B %d, %Y")
            return (display_name, formatted_date)
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col == "cilinderinhoud":
        try:
            engine_size = int(float(value))
            if engine_size >= 1000:
                liters = engine_size / 1000
                if liters == int(liters):
                    return (display_name, f"{int(liters)}L")
                else:
                    return (display_name, f"{liters:.1f}L")
            else:
                return (display_name, f"{engine_size}cc")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col in [
        "massa_ledig_voertuig",
        "massa_rijklaar",
        "maximum_massa_samenstelling",
        "laadvermogen",
        "maximum_trekken_massa_geremd",
    ]:
        try:
            int_value = int(float(value))
            return (display_name, f"{int_value}kg")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col in ["breedte", "lengte", "hoogte_voertuig", "wielbasis"]:
        try:
            int_value = int(float(value))
            return (display_name, f"{int_value}mm")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col in ["maximale_constructiesnelheid", "opgegeven_maximum_snelheid"]:
        try:
            int_value = int(float(value))
            return (display_name, f"{int_value}km/h")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col == "nettomaximumvermogen":
        try:
            int_value = int(float(value))
            return (display_name, f"{int_value}kW")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col == "co2_uitstoot_gecombineerd":
        try:
            int_value = int(float(value))
            return (display_name, f"{int_value}g/km")
        except (ValueError, TypeError):
            return (display_name, str(value))
    elif col in [
        "aantal_zitplaatsen",
        "aantal_deuren",
        "aantal_cilinders",
        "aantal_wielen",
    ]:
        try:
            int_value = int(float(value))
            return (display_name, str(int_value))
        except (ValueError, TypeError):
            return (display_name, str(value))
    else:
        # For any other numerical columns, try to convert to int
        try:
            float_value = float(value)
            if float_value == int(float_value):
                return (display_name, str(int(float_value)))
            else:
                return (display_name, str(int(round(float_value))))
        except (ValueError, TypeError):
            return (display_name, str(value))


# Define categories and their respective columns
CATEGORIES = {
    "General": [
        "merk",
        "handelsbenaming",
        "voertuigsoort",
        "variant",
        "uitvoering",
        "eerste_kleur",
        "tweede_kleur",
        "aantal_zitplaatsen",
        "aantal_deuren",
    ],
    "Historic": [
        "datum_eerste_toelating",
        "datum_eerste_afgifte_nederland",
        "datum_eerste_tenaamstelling_in_nederland",
        "vervaldatum_apk",
        "datum_tenaamstelling",
        "jaar_laatste_registratie_tellerstand",
        "jaar_laatste_wijziging_euro_norm",
    ],
    "Weights/Dimensions": [
        "massa_ledig_voertuig",
        "massa_rijklaar",
        "maximum_massa_samenstelling",
        "laadvermogen",
        "maximum_trekken_massa_geremd",
        "breedte",
        "lengte",
        "hoogte_voertuig",
        "wielbasis",
    ],
    "Engine/Emissions": [
        "cilinderinhoud",
        "aantal_cilinders",
        "nettomaximumvermogen",
        "brandstof_omschrijving",
        "brandstof_volgnummer",
        "co2_uitstoot_gecombineerd",
        "emissieklasse",
        "euro_norm",
    ],
    "Technical": [
        "aantal_wielen",
        "maximale_constructiesnelheid",
        "opgegeven_maximum_snelheid",
        "typegoedkeuringsnummer",
        "europese_voertuigcategorie",
        "europese_voertuigcategorie_toevoeging",
        "europese_uitvoeringcategorie_toevoeging",
        "plaats_chassisnummer",
        "technische_max_massa_voertuig",
    ],
}


def organize_car_data(car):
    """Organize car data into categorized details."""
    categorized_details = {}

    for category, columns in CATEGORIES.items():
        category_data = {}
        for col in columns:
            if col in car.index and pd.notna(car[col]) and car[col] != "":
                label, value = format_value(col, car[col])
                category_data[label] = value
        if category_data:
            categorized_details[category] = category_data

    # Add any remaining columns that weren't categorized
    remaining_columns = {}
    for col, value in car.items():
        if (
            col != "kenteken"
            and pd.notna(value)
            and value != ""
            and not any(col in cat_cols for cat_cols in CATEGORIES.values())
        ):
            label, formatted_value = format_value(col, value)
            remaining_columns[label] = formatted_value

    if remaining_columns:
        categorized_details["Other"] = remaining_columns

    return categorized_details
