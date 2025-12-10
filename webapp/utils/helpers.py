"""Helper utility functions for the webapp."""

import re
import pandas as pd


def format_kenteken(kenteken: str) -> str:
    """
    Format kenteken (license plate) by adding dashes between letter/number sequences.

    Args:
        kenteken: The license plate string to format

    Returns:
        Formatted license plate string
    """
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


def format_value(col: str, value) -> tuple[str, str]:
    """
    Format a value based on its column type for display.

    Args:
        col: Column name
        value: Value to format

    Returns:
        Tuple of (display_name, formatted_value)
    """
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