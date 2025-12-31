#!/usr/bin/env python3
"""
RDW Silver Layer Processing Script
Processes bronze layer data and creates silver delta tables with SCD2 implementation.
"""
import logging
import argparse
from pyspark.sql import DataFrame

from rdw.definitions import get_table_by_name, rdw_tables, RDWTable
from rdw.scd2.processor import SCD2DeltaProcessor
from rdw.conf import bronze_catalog, silver_catalog, quarantine_schema
from rdw.utils.utils import read_delta_table, save_delta_table
from rdw.utils.logging_config import setup_logging
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.checks_storage import FileChecksStorageConfig
from databricks.sdk import WorkspaceClient


class TableNotFoundException(Exception):
    """Raised when requested table is not found in definitions."""
    pass


def apply_dqx_validation(df: DataFrame, table: RDWTable, logger: logging.Logger) -> DataFrame:
    """Apply DQX data quality checks and split into valid and quarantined data.

    Saves quarantined records to quarantine table and returns only valid records.

    Args:
        df: Input DataFrame from bronze layer
        table: RDWTable definition with dqx_checks_path
        logger: Logger instance

    Returns:
        DataFrame with valid records only
    """
    logger.info(f"Loading DQX checks from {table.dqx_checks_path}")

    # Initialize DQX engine
    dq_engine = DQEngine(WorkspaceClient())

    # Load checks from YAML file using FileChecksStorageConfig
    checks_config = FileChecksStorageConfig(location=table.dqx_checks_path)
    checks = dq_engine.load_checks(checks_config)
    logger.info(f"Loaded {len(checks)} DQX checks")

    # Apply checks and split into valid and quarantined DataFrames
    valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)

    # Log validation results
    quarantined_count = quarantined_df.count()

    logger.info(f"  Quarantined records: {quarantined_count}")

    # Save quarantined records
    quarantine_table_path = f"{bronze_catalog}.{quarantine_schema}.{table.name}"
    if quarantined_count > 0:
        logger.info(f"Saving {quarantined_count} quarantined records to {quarantine_table_path}")
        save_delta_table(quarantined_df, quarantine_table_path)
        logger.info(f"Successfully saved quarantined records to {quarantine_table_path}")
    else:
        logger.info("No quarantined records to save")

    return valid_df


def process_silver_layer(table_name: str):
    """Process silver layer with SCD2 for a specific table.

    Uses SCD2DeltaProcessor which writes directly to Delta table via MERGE.
    """
    from rdw.utils.spark_session import get_spark_session

    logger = setup_logging(log_level="INFO", service_name="rdw_silver")
    spark = get_spark_session()

    # Specify which table is processed
    table = get_table_by_name(table_name)
    if not table:
        raise TableNotFoundException(
            f"Table '{table_name}' not found. Available tables: {[t.name for t in rdw_tables]}"
        )

    # Add FK constraints if they don't exist yet
    for fk in table.alter_fk_statements(catalog=silver_catalog):
        check_query = table.check_fk_exists_query(fk["constraint_name"], catalog=silver_catalog)
        fk_exists = spark.sql(check_query).count() > 0

        if not fk_exists:
            logger.info(f"Adding FK constraint: {fk['constraint_name']}")
            spark.sql(fk["statement"])
        else:
            logger.info(f"FK constraint already exists: {fk['constraint_name']}")

    logger.info(f"Processing silver layer for table '{table.name}'")

    # 1. LOAD: Read bronze snapshot
    logger.info(f"Reading bronze layer data from {table.delta_bronze_path}")
    bronze_data = read_delta_table(table.delta_bronze_path, spark)

    # 2. VALIDATE: Apply DQX data quality checks (quarantine is saved internally)
    logger.info("Applying DQX data quality validation")
    new_snapshot = apply_dqx_validation(bronze_data, table, logger)

    # 3. TRANSFORM + SAVE

    if table.scd2_tracking:
        # Apply SCD2 via Delta MERGE (writes directly to table)
        logger.info("Applying SCD2 transformations via Delta MERGE")
        primary_key = table.primary_key_column or "license_plate"
        logger.info(f"Using primary key: {primary_key}")

        processor = SCD2DeltaProcessor(
            spark=spark,
            primary_keys=[primary_key],
            history_table=table.delta_silver_path,
            exclude_columns=["ingestion_timestamp", "_file_name"]
        )

        processor.process_snapshot(new_snapshot)
    else:
        # Simple overwrite - no SCD2 tracking or table doesn't exist yet
        logger.info("Overwriting silver table (SCD2 disabled or initial load)")
        save_delta_table(new_snapshot, table.delta_silver_path, mode="overwrite")

    logger.info(f"Successfully processed silver layer for '{table.name}'")


def main():
    """Main entry point for silver layer processing."""
    parser = argparse.ArgumentParser(
        description="Process RDW silver layer with SCD2 from bronze data"
    )
    parser.add_argument(
        "--table-name",
        help="Specific RDW table name to process"
    )

    args = parser.parse_args()

    process_silver_layer(args.table_name)


if __name__ == "__main__":
    main()