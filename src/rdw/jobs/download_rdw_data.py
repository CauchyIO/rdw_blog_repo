#!/usr/bin/env python3
"""
RDW Data Download Script
Downloads CSV data from RDW API and saves to volume storage.
"""

import argparse
from datetime import datetime
from typing import Optional

from rdw.definitions import get_table_by_name, rdw_tables
from rdw.utils.utils import chunked_download
from rdw.utils.logging_config import setup_logging


class TableNotFoundException(Exception):
    """Raised when requested table is not found in definitions."""
    pass


def download_table_data(table_name: str, run_timestamp: Optional[str] = None):
    """Download RDW data for a specific table.

    Args:
        table_name: Name of the RDW table to download
        run_timestamp: Optional timestamp for the download folder. If not provided,
                       generates a new timestamp. Format: 'YYYY-MM-DDTHH-MM'
    """
    logger = setup_logging(log_level="INFO", service_name="rdw_download")

    table = get_table_by_name(table_name)
    if not table:
        raise TableNotFoundException(
            f"Table '{table_name}' not found. Available tables: {[t.name for t in rdw_tables]}"
        )

    # Use provided timestamp or generate new one
    timestamp = run_timestamp or datetime.now().strftime("%Y-%m-%dT%H-%M")
    destination = table.get_timestamped_volume_path(timestamp)

    logger.info(f"Starting download for table '{table.name}'")
    logger.info(f"URL: {table.url}")
    logger.info(f"Destination: {destination}")

    chunked_download(table.url, destination)
    logger.info(f"Successfully downloaded '{table.name}' to {destination}")
    logger.info("Download process completed successfully")


def main():
    """Main entry point for RDW data download."""
    parser = argparse.ArgumentParser(
        description="Download RDW CSV data from API"
    )
    parser.add_argument(
        "--table-name",
        required=True,
        help="Specific RDW table name to download"
    )
    parser.add_argument(
        "--run-timestamp",
        help="Timestamp for download folder (format: YYYY-MM-DDTHH-MM-SS). "
             "If not provided, generates a new timestamp."
    )

    args = parser.parse_args()

    download_table_data(args.table_name, args.run_timestamp)


if __name__ == "__main__":
    main()