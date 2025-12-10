"""
SCD Type 2 (Slowly Changing Dimension) implementation module.

This module provides SCD2 functionality for tracking historical changes in data.
"""

from rdw.scd2.processor import (
    SCD2Processor,
    detect_changes,
    apply_scd2_logic,
    add_scd2_columns,
    create_history_records,
    get_current_records,
    generate_row_hash,
    merge_history_tables,
)

__all__ = [
    "SCD2Processor",
    "detect_changes",
    "apply_scd2_logic",
    "add_scd2_columns",
    "create_history_records",
    "get_current_records",
    "generate_row_hash",
    "merge_history_tables",
]
