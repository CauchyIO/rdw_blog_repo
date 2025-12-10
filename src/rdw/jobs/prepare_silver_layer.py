#!/usr/bin/env python3
"""
RDW Silver Layer Preparation Script
Creates silver layer tables with proper schema, constraints, and column descriptions.
"""
import logging
import argparse
from pyspark.sql import SparkSession

from rdw.definitions import get_table_by_name, rdw_tables, RDWTable
from rdw.conf import silver_catalog
from rdw.utils.logging_config import setup_logging


class TableNotFoundException(Exception):
    """Raised when requested table is not found in definitions."""
    pass


def create_silver_table_if_not_exists(table: RDWTable, spark: SparkSession, logger: logging.Logger):
    """Create silver layer table with constraints if it doesn't exist.

    Args:
        table: RDWTable definition
        spark: SparkSession instance
        logger: Logger instance
    """
    if not spark.catalog.tableExists(table.delta_silver_path):
        logger.info(f"Creating silver layer table: {table.delta_silver_path}")

        # Generate table with PKs and FKs
        create_statement = table.create_table_statement(catalog=silver_catalog, scd2_cols=True, foreign_keys=False)
        logger.info(f"Executing: {create_statement}")
        spark.sql(create_statement)

        # Add column descriptions
        for alter_statement in table.alter_comment_statements(catalog=silver_catalog, scd2_cols=True):
            logger.info(f"Executing: {alter_statement}")
            spark.sql(alter_statement)

        logger.info(f"Successfully created table: {table.delta_silver_path}")
    else:
        logger.info(f"Table already exists: {table.delta_silver_path}")


def prepare_silver_layer(table_name: str):
    """Prepare silver layer table for a specific table.

    Creates the table with proper schema if it doesn't exist.
    """
    from rdw.utils.spark_session import get_spark_session

    logger = setup_logging(log_level="INFO", service_name="rdw_silver_prepare")
    spark = get_spark_session()

    table = get_table_by_name(table_name)
    if not table:
        raise TableNotFoundException(
            f"Table '{table_name}' not found. Available tables: {[t.name for t in rdw_tables]}"
        )

    logger.info(f"Preparing silver layer for table '{table.name}'")

    create_silver_table_if_not_exists(table, spark, logger)

    logger.info("Silver layer preparation completed successfully")


def main():
    """Main entry point for silver layer preparation."""
    parser = argparse.ArgumentParser(
        description="Prepare RDW silver layer tables with schema and constraints"
    )
    parser.add_argument(
        "--table-name",
        help="Specific RDW table name to prepare"
    )

    args = parser.parse_args()

    prepare_silver_layer(args.table_name)


if __name__ == "__main__":
    main()