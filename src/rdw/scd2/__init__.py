"""
SCD Type 2 (Slowly Changing Dimension) implementation module.

This module provides SCD2 functionality for tracking historical changes in data.

Two implementations are available:
- SCD2Processor: DataFrame-based, suitable for testing and smaller datasets
- SCD2DeltaProcessor: Delta MERGE-based, optimized for large-scale production (16M+ rows)
"""

from rdw.scd2.processor import (
    SCD2Processor,
    SCD2DeltaProcessor,
    detect_changes,
    apply_scd2_logic,
    add_scd2_columns,
    create_history_records,
    get_current_records,
    generate_row_hash,
    merge_history_tables,
)


__all__ = [
    # DataFrame-based processor
    "SCD2Processor",
    # Delta MERGE-based processor
    "SCD2DeltaProcessor",
    # Utility functions
    "detect_changes",
    "apply_scd2_logic",
    "add_scd2_columns",
    "create_history_records",
    "get_current_records",
    "generate_row_hash",
    "merge_history_tables",
]
