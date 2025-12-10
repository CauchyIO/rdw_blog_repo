"""Tab modules for the webapp."""
from .common import (
    format_kenteken,
    create_category_section,
    organize_car_data,
    format_value,
    CATEGORIES,
)
from .prompt_feedback import (
    create_prompt_feedback_tab,
    register_callbacks as register_prompt_feedback_callbacks,
)
from .license_plate import create_license_plate_tab, register_callbacks as register_license_plate_callbacks

__all__ = [
    "format_kenteken",
    "create_category_section",
    "organize_car_data",
    "format_value",
    "CATEGORIES",
    "create_prompt_feedback_tab",
    "register_prompt_feedback_callbacks",
    "create_license_plate_tab",
    "register_license_plate_callbacks",
]
