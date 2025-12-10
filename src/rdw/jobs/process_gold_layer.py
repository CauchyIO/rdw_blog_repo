#!/usr/bin/env python3
"""
RDW Gold Layer Processing Script
Processes silver layer data and creates gold delta tables and metric views.
"""

import argparse

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from rdw.conf import silver_catalog, gold_catalog, rdw_schema, SCD2_COLS, GOLD_COLS, GOLD_DEDUP_COLS, VIEWS_DIR
from rdw.utils.utils import save_delta_table, read_delta_table
from rdw.utils.logging_config import setup_logging


def load_silver_registered_vehicles(spark: SparkSession) -> DataFrame:
    """Load registered vehicles from silver layer.

    Args:
        spark: SparkSession instance

    Returns:
        DataFrame with current registered vehicles
    """
    return read_delta_table(
        f"{silver_catalog}.{rdw_schema}.registered_vehicles",
        spark
    )


def transform_licensed_vehicles(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Transform silver data to gold licensed vehicles tables.

    Applies the following filters:
    - record is current
    - not recently exported
    - passenger cars only
    - not a taxi
    - has WAM insurance

    Args:
        df: Input DataFrame with registered vehicles

    Returns:
        Tuple of (main_df, dedup_df) with selected GOLD_COLS and GOLD_DEDUP_COLS
    """
    df_filtered = df.filter(
        (F.col("__is_current") == True) &
        (F.col("export_indicator") == "Nee") &
        (F.col("vehicle_type") == "Personenauto") &
        (F.col("taxi_indicator") == "Nee") &
        (F.col("liability_insured") == "Ja")
    )

    df_main = df_filtered.drop(*SCD2_COLS).select(*GOLD_COLS)

    df_dedup = (
        df_filtered
        .dropDuplicates(["make", "trade_name"])
        .select(*GOLD_DEDUP_COLS)
    )

    return df_main, df_dedup


def load_metric_view_yaml(yaml_path) -> str:
    """Load metric view YAML definition from file.

    Args:
        yaml_path: Path to the YAML file

    Returns:
        YAML content as string (without comment lines starting with #)
    """
    with open(yaml_path, "r") as f:
        lines = f.readlines()

    # Filter out comment-only lines (lines starting with #)
    # Keep inline comments and YAML content
    filtered_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("#"):
            filtered_lines.append(line)

    return "".join(filtered_lines)


def create_metric_view(spark: SparkSession, view_name: str, yaml_content: str) -> None:
    """Create a metric view using SQL with embedded YAML.

    Args:
        spark: SparkSession instance
        view_name: Fully qualified view name (catalog.schema.view)
        yaml_content: YAML definition for the metric view
    """
    sql = f"""
    CREATE OR REPLACE VIEW {view_name}
    WITH METRICS
    LANGUAGE YAML
    AS $$
    {yaml_content}
    $$
    """
    spark.sql(sql)


def process_gold_layer():
    """Process gold layer: create licensed_vehicles table and metric view.

    Follows Load -> Transform -> Save pattern.
    """
    from rdw.utils.spark_session import get_spark_session

    logger = setup_logging(log_level="INFO", service_name="rdw_gold")
    spark = get_spark_session()

    logger.info("Processing gold layer")

    # Load
    logger.info("Loading silver registered_vehicles table")
    df = load_silver_registered_vehicles(spark)

    # Transform
    logger.info("Transforming to licensed vehicles")
    df_main, df_dedup = transform_licensed_vehicles(df)

    # Save tables
    logger.info(f"Saving output table")
    save_delta_table(df_main, f"{gold_catalog}.{rdw_schema}.registered_vehicles")

    logger.info(f"Saving deduplicated table")
    save_delta_table(df_dedup, f"{gold_catalog}.{rdw_schema}.registered_vehicles_dedup")

    # Create metric view
    logger.info("Creating metric view")
    yaml_path = VIEWS_DIR / "vehicle_fleet_metrics.yml"

    yaml_content = load_metric_view_yaml(yaml_path)
    metric_view_name = f"{gold_catalog}.{rdw_schema}.vehicle_fleet_metrics"

    create_metric_view(spark, metric_view_name, yaml_content)
    logger.info(f"Created metric view: {metric_view_name}")

    logger.info("Gold layer processing completed successfully")


def main():
    """Main entry point for gold layer processing."""
    parser = argparse.ArgumentParser(
        description="Process RDW gold layer from silver data"
    )
    parser.parse_args()

    process_gold_layer()


if __name__ == "__main__":
    main()
