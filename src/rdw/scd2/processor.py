"""
SCD Type 2 (Slowly Changing Dimension) implementation for Spark DataFrames.

This module provides modular functions to implement SCD2 logic without DLT,
suitable for tracking historical changes in data.
"""

from typing import List, Optional, Tuple
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType


class SCD2Processor:
    """
    A processor class for implementing SCD Type 2 logic on Spark DataFrames.
    
    This class handles:
    - Change detection between snapshots
    - Operation identification (INSERT, UPDATE, DELETE)
    - History tracking with validity periods
    - Current record management
    """
    
    def __init__(
        self,
        spark: SparkSession,
        primary_keys: List[str],
        tracking_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """
        Initialize the SCD2 processor.
        
        Args:
            spark: SparkSession instance
            primary_keys: List of column names that form the primary key
            tracking_columns: Columns to track for changes (None = all columns)
            exclude_columns: Columns to exclude from change tracking
        """
        self.spark = spark
        self.primary_keys = primary_keys
        self.tracking_columns = tracking_columns
        self.exclude_columns = exclude_columns or []
        
        # SCD2 metadata columns
        self.scd2_columns = {
            "valid_from": "__valid_from",
            "valid_to": "__valid_to",
            "is_current": "__is_current",
            "operation": "__operation",
            "row_hash": "__row_hash",
            "version": "__version",
        }
    
    def process_snapshot(
        self,
        new_snapshot: DataFrame,
        existing_history: Optional[DataFrame] = None,
        timestamp: Optional[datetime] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Process a new snapshot and return updated current records and history.
        
        Args:
            new_snapshot: New data snapshot
            existing_history: Existing history table (optional)
            timestamp: Processing timestamp (defaults to current time)
            
        Returns:
            Tuple of (current_records, history_records)
        """
        timestamp = timestamp or datetime.now()
        
        # Detect changes
        changes_df = detect_changes(
            new_snapshot=new_snapshot,
            existing_snapshot=self._get_current_from_history(existing_history),
            primary_keys=self.primary_keys,
            comparison_columns=self._get_comparison_columns(new_snapshot),
        )
        
        # Apply SCD2 logic
        history_updates = apply_scd2_logic(
            changes_df=changes_df,
            existing_history=existing_history,
            primary_keys=self.primary_keys,
            timestamp=timestamp,
        )
        
        # Get current records
        current_records = get_current_records(history_updates)
        
        return current_records, history_updates
    
    def _get_current_from_history(
        self, history_df: Optional[DataFrame]
    ) -> Optional[DataFrame]:
        """Extract current records from history table."""
        if history_df is None or history_df.isEmpty():
            return None
        return history_df.filter(F.col(self.scd2_columns["is_current"]) == True)
    
    def _get_comparison_columns(self, df: DataFrame) -> List[str]:
        """Get columns to use for comparison."""
        all_columns = df.columns

        if self.tracking_columns:
            comparison_cols = self.tracking_columns
        else:
            comparison_cols = all_columns

        # Exclude specified columns, SCD2 metadata, and primary keys
        exclude_set = set(self.exclude_columns) | set(self.scd2_columns.values()) | set(self.primary_keys)
        return [col for col in comparison_cols if col not in exclude_set]


def generate_row_hash(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Generate a hash for specified columns to detect changes.
    
    Args:
        df: Input DataFrame
        columns: Columns to include in hash
        
    Returns:
        DataFrame with added __row_hash column
    """
    # Create hash from concatenated column values
    hash_expr = F.md5(
        F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns]
        )
    )
    
    return df.withColumn("__row_hash", hash_expr)


def detect_changes(
    new_snapshot: DataFrame,
    existing_snapshot: Optional[DataFrame],
    primary_keys: List[str],
    comparison_columns: List[str],
) -> DataFrame:
    """
    Detect changes between two snapshots.
    
    Args:
        new_snapshot: New data snapshot
        existing_snapshot: Existing data snapshot (can be None for initial load)
        primary_keys: Primary key columns
        comparison_columns: Columns to compare for changes
        
    Returns:
        DataFrame with operation column (INSERT, UPDATE, DELETE, NO_CHANGE)
    """
    # Add row hashes for comparison
    new_with_hash = generate_row_hash(new_snapshot, comparison_columns)
    
    if existing_snapshot is None:
        # Initial load - all records are INSERTs
        return new_with_hash.withColumn("__operation", F.lit("INSERT"))
    
    existing_with_hash = generate_row_hash(existing_snapshot, comparison_columns)

    # Full outer join to detect all changes
    joined = new_with_hash.alias("new").join(
        existing_with_hash.alias("old"),
        on=[F.col(f"new.{pk}") == F.col(f"old.{pk}") for pk in primary_keys],
        how="full_outer"
    )
    
    # Determine operation type
    result = joined.select(
        # coalesced primary keys
        *[F.coalesce(F.col(f"new.{pk}"), F.col(f"old.{pk}")).alias(pk) 
        for pk in primary_keys],

        # coalesced comparison columns
        *[F.coalesce(F.col(f"new.{col}"), F.col(f"old.{col}")).alias(col)
        for col in comparison_columns],

        # operation
        F.when(F.col("old.__row_hash").isNull(), F.lit("INSERT"))
        .when(F.col("new.__row_hash").isNull(), F.lit("DELETE"))
        .when(F.col("new.__row_hash") != F.col("old.__row_hash"), F.lit("UPDATE"))
        .otherwise(F.lit("NO_CHANGE"))
        .alias("__operation"),

        # row hash
        F.coalesce(F.col("new.__row_hash"), F.col("old.__row_hash")).alias("__row_hash")
    )
    
    # Filter out records with no changes
    return result.filter(F.col("__operation") != "NO_CHANGE")


def add_scd2_columns(
    df: DataFrame,
    timestamp: datetime,
    is_current: bool = True,
    version: int = 1,
    operation: str = "INSERT",
) -> DataFrame:
    """
    Add SCD2 tracking columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        timestamp: Validity start timestamp
        is_current: Whether records are current
        version: Version number
        operation: Operation type (INSERT, UPDATE, DELETE)
        
    Returns:
        DataFrame with SCD2 columns added
    """
    result = (
        df.withColumn("__valid_from", F.lit(timestamp))
        .withColumn("__valid_to", F.lit(None).cast(TimestampType()))
        .withColumn("__is_current", F.lit(is_current))
        .withColumn("__operation", F.lit(operation))
        .withColumn("__processed_time", F.current_timestamp())
    )
    
    # Only set version if not None (allows preserving existing version)
    if version is not None:
        result = result.withColumn("__version", F.lit(version))
    
    return result


def apply_scd2_logic(
    changes_df: DataFrame,
    existing_history: Optional[DataFrame],
    primary_keys: List[str],
    timestamp: datetime,
) -> DataFrame:
    """Apply SCD2 logic to process changes and update history."""
    
    # Separate operations
    inserts = changes_df.filter(F.col("__operation") == "INSERT")
    updates = changes_df.filter(F.col("__operation") == "UPDATE")
    deletes = changes_df.filter(F.col("__operation") == "DELETE")
    
    # Process INSERTs - new records
    new_records = add_scd2_columns(
        inserts.drop("__operation"),
        timestamp=timestamp,
        is_current=True,
        version=1,
        operation="INSERT"
    )
    
    if existing_history is None or existing_history.isEmpty():
        # Initial load (no history or empty history)
        return new_records

    # Start with existing history
    history = existing_history
    
    # Process UPDATEs - close old records and insert new versions
    if updates.count() > 0:
        update_keys = updates.select(*primary_keys).distinct()

        # Only close CURRENT records, not all historical versions
        closed_records = (
            existing_history
            .filter(F.col("__is_current") == True)
            .join(update_keys, on=primary_keys, how="inner")
            .withColumn("__valid_to", F.lit(timestamp))
            .withColumn("__is_current", F.lit(False))
        )

        # Create new versions for updates
        new_versions = (
            updates.drop("__operation")
            .join(
                existing_history.filter(F.col("__is_current") == True)
                .select(*primary_keys, F.col("__version")),
                on=primary_keys,
                how="left"
            )
            .withColumn("__version",
                       F.when(F.col("__version").isNotNull(),
                             F.col("__version") + 1)
                       .otherwise(1))
        )

        new_update_records = add_scd2_columns(
            new_versions,
            timestamp=timestamp,
            is_current=True,
            version=None,
            operation="UPDATE"
        )

        # Keep historical (non-current) records for keys being updated - they should NOT change
        historical_records_for_updated_keys = (
            existing_history
            .filter(F.col("__is_current") == False)
            .join(update_keys, on=primary_keys, how="inner")
        )

        # Records with keys NOT being updated remain unchanged (both current and historical)
        unchanged_records = (
            existing_history.join(update_keys, on=primary_keys, how="left_anti")
        )

        # Combine: unchanged + preserved historical + closed current + new versions
        history = unchanged_records.unionByName(
            historical_records_for_updated_keys, allowMissingColumns=True
        ).unionByName(
            closed_records, allowMissingColumns=True
        ).unionByName(
            new_update_records, allowMissingColumns=True
        )
    
    # Process DELETEs
    if deletes.count() > 0:
        delete_keys = deletes.select(*primary_keys).distinct()

        # Only mark CURRENT records as deleted, not all historical versions
        deleted_records = (
            history
            .filter(F.col("__is_current") == True)
            .join(delete_keys, on=primary_keys, how="inner")
            .withColumn("__valid_to", F.lit(timestamp))
            .withColumn("__is_current", F.lit(False))
            .withColumn("__operation", F.lit("DELETE"))
        )

        # Keep historical (non-current) records for deleted keys - they should NOT change
        historical_records_for_deleted_keys = (
            history
            .filter(F.col("__is_current") == False)
            .join(delete_keys, on=primary_keys, how="inner")
        )

        # Records with keys NOT being deleted remain unchanged
        remaining_records = (
            history.join(delete_keys, on=primary_keys, how="left_anti")
        )

        # Combine: remaining + preserved historical + deleted current
        history = remaining_records.unionByName(
            historical_records_for_deleted_keys, allowMissingColumns=True
        ).unionByName(
            deleted_records, allowMissingColumns=True
        )
    
    # Add ONLY truly new records (not already in history)
    if new_records.count() > 0:
        existing_keys = history.select(*primary_keys).distinct()
        truly_new_records = new_records.join(
            existing_keys,
            on=primary_keys,
            how="left_anti"  # Only records NOT in existing history
        )
        
        if truly_new_records.count() > 0:
            history = history.unionByName(truly_new_records, allowMissingColumns=True)
    
    return history


def create_history_records(
    df: DataFrame,
    operation: str,
    timestamp: datetime,
    version: int = 1,
) -> DataFrame:
    """
    Create history records with SCD2 columns for a specific operation.
    
    Args:
        df: Input DataFrame
        operation: Operation type (INSERT, UPDATE, DELETE)
        timestamp: Operation timestamp
        version: Record version
        
    Returns:
        DataFrame with history records
    """
    is_current = operation != "DELETE"
    valid_to = None if is_current else timestamp
    
    return (
        df.withColumn("__operation", F.lit(operation))
        .withColumn("__valid_from", F.lit(timestamp))
        .withColumn("__valid_to", F.lit(valid_to).cast(TimestampType()))
        .withColumn("__is_current", F.lit(is_current))
        .withColumn("__version", F.lit(version))
        .withColumn("__processed_time", F.current_timestamp())
    )


def get_current_records(history_df: DataFrame) -> DataFrame:
    """
    Extract current records from a history table.
    
    Args:
        history_df: History DataFrame with SCD2 columns
        
    Returns:
        DataFrame containing only current records
    """
    return (
        history_df.filter(F.col("__is_current") == True)
        .drop("__valid_from", "__valid_to", "__is_current", 
              "__version", "__operation", "__row_hash", "__processed_time")
    )


def merge_history_tables(
    existing_history: DataFrame,
    new_history: DataFrame,
    primary_keys: List[str],
) -> DataFrame:
    """
    Merge two history tables, maintaining SCD2 integrity.
    
    Args:
        existing_history: Existing history table
        new_history: New history records
        primary_keys: Primary key columns
        
    Returns:
        Merged history DataFrame
    """
    # Union the tables
    merged = existing_history.unionByName(new_history, allowMissingColumns=True)
    
    # Ensure only one current record per key
    from pyspark.sql.window import Window
    window_spec = (
        Window.partitionBy(*primary_keys)
        .orderBy(F.col("__valid_from").desc())
    )
    
    return (
        merged.withColumn("__row_num", F.row_number().over(window_spec))
        .withColumn("__is_current", 
                   F.when(F.col("__row_num") == 1, True).otherwise(False))
        .drop("__row_num")
    )


# Utility function for testing
def compare_snapshots_example(
    spark: SparkSession,
    old_snapshot_path: str,
    new_snapshot_path: str,
    primary_keys: List[str],
    output_path: str,
) -> None:
    """
    Example function showing how to use the SCD2 functions.
    
    Args:
        spark: SparkSession
        old_snapshot_path: Path to old snapshot
        new_snapshot_path: Path to new snapshot  
        primary_keys: Primary key columns
        output_path: Output path for results
    """
    # Read snapshots
    old_df = spark.read.parquet(old_snapshot_path) if old_snapshot_path else None
    new_df = spark.read.parquet(new_snapshot_path)
    
    # Initialize processor
    processor = SCD2Processor(
        spark=spark,
        primary_keys=primary_keys,
        exclude_columns=["ingestion_timestamp", "_file_name"]
    )
    
    # Process snapshot
    current, history = processor.process_snapshot(
        new_snapshot=new_df,
        existing_history=old_df
    )
    
    # Save results
    current.write.mode("overwrite").parquet(f"{output_path}/current")
    history.write.mode("overwrite").parquet(f"{output_path}/history")