"""
SCD Type 2 (Slowly Changing Dimension) implementation for Spark DataFrames.

This module provides modular functions to implement SCD2 logic without DLT,
suitable for tracking historical changes in data.

Two implementations are available:
- SCD2Processor: DataFrame-based, suitable for testing and smaller datasets
- SCD2DeltaProcessor: Delta MERGE-based, optimized for large-scale production
"""

from typing import List, Optional, Tuple
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

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
        if history_df is None:
            return None
        # Return filtered DF - if empty, downstream joins handle it gracefully
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


class SCD2DeltaProcessor:
    """
    Delta MERGE-based SCD2 processor optimized for large-scale production workloads.

    Uses Delta Lake MERGE operations to efficiently update only affected records,
    making it ideal for large tables (10M+ rows) with relatively few changes per batch.

    Key differences from SCD2Processor:
    - Operates directly on Delta tables (not DataFrames)
    - Only rewrites files containing changed records
    - Better performance when change ratio is low (<10% of records)
    - Requires Delta Lake
    """

    def __init__(
        self,
        spark: SparkSession,
        primary_keys: List[str],
        history_table: str,
        tracking_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
    ):
        """
        Initialize the Delta MERGE SCD2 processor.

        Args:
            spark: SparkSession instance
            primary_keys: List of column names that form the primary key
            history_table: Unity Catalog table name (e.g., 'catalog.schema.table')
            tracking_columns: Columns to track for changes (None = all columns)
            exclude_columns: Columns to exclude from change tracking
        """
        self.spark = spark
        self.primary_keys = primary_keys
        self.history_table = history_table
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

    def _table_exists(self) -> bool:
        """Check if the history table exists."""
        return self.spark.catalog.tableExists(self.history_table)

    def _get_delta_table(self) -> DeltaTable:
        """Get DeltaTable reference."""
        return DeltaTable.forName(self.spark, self.history_table)

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

    def process_snapshot(
        self,
        new_snapshot: DataFrame,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Process a new snapshot using Delta MERGE operations.

        Optimized for minimal Spark jobs:
        - No operation counting (avoids .collect())
        - Single INSERT for all new records (updates + inserts combined)
        - MERGE runs unconditionally (no-op if no matches)

        Args:
            new_snapshot: New data snapshot
            timestamp: Processing timestamp (defaults to current time)
        """
        timestamp = timestamp or datetime.now()
        comparison_columns = self._get_comparison_columns(new_snapshot)

        # Add row hash to new snapshot
        new_with_hash = generate_row_hash(new_snapshot, comparison_columns)

        if not self._table_exists():
            # Initial load - create table with all records as INSERTs
            initial_history = add_scd2_columns(
                new_with_hash,
                timestamp=timestamp,
                is_current=True,
                version=1,
                operation="INSERT"
            )
            initial_history.write.format("delta").saveAsTable(self.history_table)
            return

        # Get current records from history for comparison
        delta_table = self._get_delta_table()
        current_with_hash = generate_row_hash(
            delta_table.toDF()
            .filter(F.col("__is_current") == True)
            .drop("__row_hash"),
            comparison_columns
        )

        # Detect changes using full outer join (lazy - no action triggered)
        pk_condition = [F.col(f"new.{pk}") == F.col(f"old.{pk}") for pk in self.primary_keys]

        changes = new_with_hash.alias("new").join(
            current_with_hash.alias("old"),
            on=pk_condition,
            how="full_outer"
        ).select(
            *[F.coalesce(F.col(f"new.{pk}"), F.col(f"old.{pk}")).alias(pk)
              for pk in self.primary_keys],
            F.col("old.__version").alias("old_version"),
            F.when(F.col("old.__row_hash").isNull(), F.lit("INSERT"))
            .when(F.col("new.__row_hash").isNull(), F.lit("DELETE"))
            .when(F.col("new.__row_hash") != F.col("old.__row_hash"), F.lit("UPDATE"))
            .otherwise(F.lit("NO_CHANGE"))
            .alias("__operation")
        ).filter(F.col("__operation") != "NO_CHANGE")

        # CRITICAL: Use localCheckpoint to break lineage and prevent re-computation.
        # Regular cache() is insufficient - Spark can still re-compute from source
        # after MERGE modifies the Delta table, causing UPDATE records to be
        # misclassified as INSERT.
        changes = changes.localCheckpoint()

        # === MERGE: Close current records for updates and deletes ===
        # Runs unconditionally - MERGE is a no-op if source is empty
        keys_to_close = changes.filter(
            F.col("__operation").isin(["UPDATE", "DELETE"])
        ).select(*self.primary_keys, F.col("__operation").alias("change_op"))

        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in self.primary_keys
        ])

        delta_table.alias("target").merge(
            keys_to_close.alias("source"),
            f"{merge_condition} AND target.__is_current = true"
        ).whenMatchedUpdate(
            set={
                "__valid_to": F.lit(timestamp),
                "__is_current": F.lit(False),
                "__operation": F.when(
                    F.col("source.change_op") == "DELETE",
                    F.lit("DELETE")
                ).otherwise(F.col("target.__operation")),
                "__processed_time": F.current_timestamp()
            }
        ).execute()

        # === SINGLE INSERT: Combine updates and inserts ===
        # Updates: get version from old record + 1
        update_keys = changes.filter(F.col("__operation") == "UPDATE").select(
            *self.primary_keys,
            (F.coalesce(F.col("old_version"), F.lit(0)) + 1).alias("__new_version"),
            F.lit("UPDATE").alias("__new_operation")
        )

        # Inserts: version = 1
        insert_keys = changes.filter(F.col("__operation") == "INSERT").select(
            *self.primary_keys,
            F.lit(1).alias("__new_version"),
            F.lit("INSERT").alias("__new_operation")
        )

        # Union keys and join with new snapshot
        all_keys = update_keys.unionByName(insert_keys)

        new_records = (
            new_with_hash
            .join(all_keys, on=self.primary_keys, how="inner")
            .withColumn("__valid_from", F.lit(timestamp))
            .withColumn("__valid_to", F.lit(None).cast("timestamp"))
            .withColumn("__is_current", F.lit(True))
            .withColumn("__version", F.col("__new_version"))
            .withColumn("__operation", F.col("__new_operation"))
            .withColumn("__processed_time", F.current_timestamp())
            .drop("__new_version", "__new_operation")
        )

        new_records.write.format("delta").mode("append").saveAsTable(self.history_table)

    def get_current_records(self) -> DataFrame:
        """Get current records from the history table."""
        if not self._table_exists():
            raise ValueError(f"History table {self.history_table} does not exist")

        return (
            self._get_delta_table().toDF()
            .filter(F.col("__is_current") == True)
            .drop("__valid_from", "__valid_to", "__is_current",
                  "__version", "__operation", "__row_hash", "__processed_time")
        )

    def get_history(self) -> DataFrame:
        """Get full history from the table."""
        if not self._table_exists():
            raise ValueError(f"History table {self.history_table} does not exist")

        return self._get_delta_table().toDF()


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
    """Apply SCD2 logic to process changes and update history.

    Optimized single-pass implementation that:
    - Caches existing_history to prevent multiple reads
    - Uses a single join to categorize all records AND extract version info
    - Uses current records for insert filtering (no distinct on full history)
    - Builds output DataFrames independently then unions once at the end
    """
    from functools import reduce

    # Separate operations - these are lazy and don't trigger computation
    inserts = changes_df.filter(F.col("__operation") == "INSERT")
    updates = changes_df.filter(F.col("__operation") == "UPDATE")
    deletes = changes_df.filter(F.col("__operation") == "DELETE")

    # Process INSERTs - new records with version 1
    new_insert_records = add_scd2_columns(
        inserts.drop("__operation"),
        timestamp=timestamp,
        is_current=True,
        version=1,
        operation="INSERT"
    )

    if existing_history is None:
        # Initial load (no history)
        return new_insert_records

    # === CACHE existing_history to prevent multiple reads ===
    existing_history = existing_history.cache()
    existing_history.count()

    # === Build change lookup with operations ===
    # Combine update and delete keys with their operations
    update_keys_df = updates.select(
        *primary_keys,
        F.lit("UPDATE").alias("__change_op"),
        F.col("__row_hash").alias("__new_hash")
    )
    delete_keys_df = deletes.select(
        *primary_keys,
        F.lit("DELETE").alias("__change_op"),
        F.lit(None).cast("string").alias("__new_hash")
    )

    # Union all change keys (updates and deletes)
    all_change_keys = update_keys_df.unionByName(delete_keys_df)

    # Broadcast the change keys (typically small compared to history)
    change_keys_broadcast = F.broadcast(all_change_keys)

    # === SINGLE JOIN: Categorize all existing history records ===
    # Left join existing history with change keys
    # This tells us for each history record: is its key being updated/deleted?
    categorized_history = existing_history.join(
        change_keys_broadcast,
        on=primary_keys,
        how="left"
    )

    # Split into categories based on join result and current status
    # 1. Unchanged records: key not in changes (both current and historical)
    unchanged_records = (
        categorized_history
        .filter(F.col("__change_op").isNull())
        .drop("__change_op", "__new_hash")
    )

    # 2. Historical records for changed keys: keep as-is (immutable)
    historical_for_changed_keys = (
        categorized_history
        .filter(
            (F.col("__change_op").isNotNull()) &
            (F.col("__is_current") == False)
        )
        .drop("__change_op", "__new_hash")
    )

    # 3. Current records being updated: close them AND capture version for new records
    current_being_updated = categorized_history.filter(
        (F.col("__change_op") == "UPDATE") &
        (F.col("__is_current") == True)
    )

    closed_for_update = (
        current_being_updated
        .drop("__change_op", "__new_hash")
        .withColumn("__valid_to", F.lit(timestamp))
        .withColumn("__is_current", F.lit(False))
    )

    # Extract version info from current records being updated (reuses the same filter)
    version_info = current_being_updated.select(
        *primary_keys,
        F.col("__version").alias("__old_version")
    )

    # 4. Current records being deleted: close them with DELETE operation
    closed_for_delete = (
        categorized_history
        .filter(
            (F.col("__change_op") == "DELETE") &
            (F.col("__is_current") == True)
        )
        .drop("__change_op", "__new_hash")
        .withColumn("__valid_to", F.lit(timestamp))
        .withColumn("__is_current", F.lit(False))
        .withColumn("__operation", F.lit("DELETE"))
    )

    # === Create new version records for updates (reusing version_info from above) ===
    # Join updates with version_info (derived from same categorization)
    new_update_records = (
        updates.drop("__operation")
        .join(F.broadcast(version_info), on=primary_keys, how="left")
        .withColumn(
            "__version",
            F.when(F.col("__old_version").isNotNull(), F.col("__old_version") + 1)
            .otherwise(1)
        )
        .drop("__old_version")
    )

    new_update_records = add_scd2_columns(
        new_update_records,
        timestamp=timestamp,
        is_current=True,
        version=None,  # Already set above
        operation="UPDATE"
    )

    # === Filter truly new inserts using current records only (no distinct on full history) ===
    existing_current_keys = F.broadcast(
        existing_history
        .filter(F.col("__is_current") == True)
        .select(*primary_keys)
    )
    truly_new_inserts = new_insert_records.join(
        existing_current_keys,
        on=primary_keys,
        how="left_anti"
    )

    # === SINGLE UNION: Combine all output DataFrames ===
    all_outputs = [
        unchanged_records,
        historical_for_changed_keys,
        closed_for_update,
        closed_for_delete,
        new_update_records,
        truly_new_inserts,
    ]

    history = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        all_outputs
    )

    # Unpersist cached DataFrame
    existing_history.unpersist()

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