#!/usr/bin/env python3
"""
RDW Bronze Layer Processing Script
Processes downloaded CSV data and creates bronze delta tables.
"""

import argparse
from typing import Optional

from rdw.definitions import get_table_by_name, rdw_tables
from rdw.utils.utils import (
    read_volume_csv,
    save_delta_table,
    convert_columns_from_definition,
    filter_first_col_length,
    get_latest_timestamp_folder,
)
from rdw.utils.logging_config import setup_logging


class TableNotFoundException(Exception):
    """Raised when requested table is not found in definitions."""
    pass


def transform_bronze(df, table):
    """Apply bronze layer transformations to DataFrame.

    Args:
        df: Raw DataFrame from CSV
        table: RDWTable definition

    Returns:
        Transformed DataFrame
    """
    df = convert_columns_from_definition(df, table)
    return df


def process_bronze_layer(table_name: str, run_timestamp: Optional[str] = None):
    """Process bronze layer for a specific table.

    Follows Load -> Transform -> Save pattern.

    Args:
        table_name: Name of the RDW table to process
        run_timestamp: Optional timestamp to read from specific download folder.
                       If not provided, auto-detects the latest timestamp folder.
                       Format: 'YYYY-MM-DDTHH-MM'
    """
    logger = setup_logging(log_level="INFO", service_name="rdw_bronze")

    table = get_table_by_name(table_name)
    if not table:
        raise TableNotFoundException(
            f"Table '{table_name}' not found. Available tables: {[t.name for t in rdw_tables]}"
        )

    logger.info(f"Processing bronze layer for table '{table.name}'")

    # 1. LOAD: Read CSV from volume
    # Auto-detect latest timestamp folder if not provided
    if run_timestamp:
        timestamp = run_timestamp
        logger.info(f"Using provided timestamp: {timestamp}")
    else:
        timestamp = get_latest_timestamp_folder(table.volume_base_path)
        logger.info(f"Auto-detected latest timestamp folder: {timestamp}")

    source_path = table.get_timestamped_volume_path(timestamp)
    logger.info(f"Reading CSV from {source_path}")
    df = read_volume_csv(source_path)

    # 2. TRANSFORM: Apply bronze transformations
    logger.info("Applying transformations")
    df = transform_bronze(df, table)

    # 3. SAVE: Write to bronze layer
    logger.info(f"Saving bronze layer table to {table.delta_bronze_path}")
    save_delta_table(df, table.delta_bronze_path)

    logger.info(f"Successfully processed bronze layer for '{table.name}'")
    logger.info("Bronze layer processing completed successfully")


def main():
    """Main entry point for bronze layer processing."""
    parser = argparse.ArgumentParser(
        description="Process RDW bronze layer from CSV data"
    )
    parser.add_argument(
        "--table-name",
        required=True,
        help="Specific RDW table name to process"
    )
    parser.add_argument(
        "--run-timestamp",
        help="Timestamp of download folder to process (format: YYYY-MM-DDTHH-MM-SS). "
             "If not provided, auto-detects the latest timestamp folder."
    )

    args = parser.parse_args()

    process_bronze_layer(args.table_name, args.run_timestamp)


if __name__ == "__main__":
    main()
