"""
Defines the variables.
"""

from pathlib import Path

# paths
VIEWS_DIR = Path(__file__).parent / "views"

# catalogs
bronze_catalog = "bronze_catalog"
silver_catalog = "silver_catalog"
gold_catalog = "gold_catalog"

# schemas
rdw_schema = "rdw_etl"
quarantine_schema = "rdw_etl_quarantine"

# volumes
ingestion_volume = "raw"

# gold cols
GOLD_COLS = [
    "license_plate",
    "vehicle_type",
    "make",
    "trade_name",
    "mot_expiry_date",
    "registration_date",
    "gross_bpm",
    "interior_layout",
    "number_of_seats",
    "primary_color",
    "secondary_color",
    "number_of_cylinders",
    "engine_displacement",
    "unladen_mass",
    "permitted_maximum_mass",
    "kerb_weight",
    "max_towing_mass_unbraked",
    "max_towing_mass_braked",
    "first_admission_date",
    "first_registration_date_nl",
    "awaiting_inspection",
    "catalog_price",
    "liability_insured",
    "max_design_speed",
    "number_of_doors",
    "number_of_wheels",
    "length",
    "width",
    "european_vehicle_category",
    "european_vehicle_category_addition",
    "european_variant_category_addition",
    "chassis_number_location",
    "technical_max_mass",
    "type",
    "type_approval_number",
    "variant",
    "version",
    "eu_type_approval_amendment_number",
    "power_to_weight_ratio",
    "wheelbase",
    "export_indicator",
    "open_recall_indicator",
    "max_combination_mass",
    "last_odometer_registration_year",
    "odometer_judgment",
    "odometer_judgment_explanation_code",
    "registration_possible",
    "fuel_efficiency_class",
    "approval_registration_date_bpm_depreciation"
]

GOLD_DEDUP_COLS = [
    "vehicle_type",
    "make",
    "trade_name",
    "gross_bpm",
    "interior_layout",
    "number_of_seats",
    "number_of_cylinders",
    "engine_displacement",
    "unladen_mass",
    "permitted_maximum_mass",
    "kerb_weight",
    "max_towing_mass_unbraked",
    "max_towing_mass_braked",
    "first_admission_date",
    "catalog_price",
    "max_design_speed",
    "number_of_doors",
    "number_of_wheels",
    "length",
    "width",
    "european_vehicle_category",
    "european_vehicle_category_addition",
    "european_variant_category_addition",
    "chassis_number_location",
    "technical_max_mass",
    "type",
    "type_approval_number",
    "variant",
    "version",
    "eu_type_approval_amendment_number",
    "power_to_weight_ratio",
    "wheelbase",
    "max_combination_mass",
    "fuel_efficiency_class"
]

# scd2 cols
SCD2_COLS = [
    "__row_hash",
    "__valid_from",
    "__valid_to",
    "__is_current",
    "__operation",
    "__processed_time",
    "__version"
]
