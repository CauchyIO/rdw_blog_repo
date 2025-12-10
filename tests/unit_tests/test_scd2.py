"""Unit tests for SCD2 implementation."""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

import os
import time
    
from rdw.scd2.processor import (
    SCD2Processor,
    generate_row_hash,
    detect_changes,
    add_scd2_columns,
    get_current_records,
    create_history_records,
    merge_history_tables
)

def test_scd2_update(spark_session: SparkSession):
    base_dir = os.path.dirname(os.path.dirname(__file__))  # goes from unit_tests -> tests

    # Initialize the SCD2 processor
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        exclude_columns=["ingestion_timestamp", "_rescued_data", "_file_name"]
    )
    assert processor is not None, "Error initializing SCD2Processor"

    # old snapshot
    sample = pd.read_csv(os.path.join(base_dir, 'test_data', 'scd2', 'old_sample.csv'), sep=",", engine="python", on_bad_lines="skip")
    sample = sample.where(sample.notna(), None)  
    old_df = spark_session.createDataFrame(sample)
    old_df.show()
    assert old_df is not None, "Error loading old_df"

    # new snapshot
    sample = pd.read_csv(os.path.join(base_dir, 'test_data', 'scd2', 'new_sample.csv'), sep=",", engine="python", on_bad_lines="skip")
    sample = sample.where(sample.notna(), None)  
    new_df = spark_session.createDataFrame(sample)
    new_df.show()
    assert new_df is not None, "Error loading new_df"

    # initial history
    initial_timestamp = datetime(2024, 1, 1, 0, 0, 0)

    # Process old snapshot as if it was the first load
    initial_current, initial_history = processor.process_snapshot(
        new_snapshot=old_df,
        existing_history=None,  # No existing history for first load
        timestamp=initial_timestamp
    )

    assert initial_current.count() == initial_history.count()

    # Process an update snapshot
    update_timestamp = datetime(2025, 1, 1, 0, 0, 0)

    current_records, updated_history = processor.process_snapshot(
        new_snapshot=new_df,
        existing_history=initial_history,
        timestamp=update_timestamp
    )
    updated_history.show()
    assert updated_history.filter((F.col("kenteken") == "LTFH51") & (F.col("__operation") == "DELETE")).count() > 0, "Row with kenteken 'LTFH51' and __operation 'DELETE' not found"


# Individual function tests
def test_generate_row_hash(spark_session):
    """Test row hash generation function."""
    
    # Create test data
    data = [("A123", "Toyota", "Camry"), ("B456", "Honda", "Civic")]
    columns = ["kenteken", "merk", "model"]
    df = spark_session.createDataFrame(data, columns)
    
    # Generate hash
    result_df = generate_row_hash(df, ["merk", "model"])
    
    assert "__row_hash" in result_df.columns
    assert result_df.count() == 2
    
    # Test hash consistency
    hashes = [row["__row_hash"] for row in result_df.collect()]
    assert len(set(hashes)) == 2  # Should have unique hashes


def test_detect_changes_initial_load(spark_session):
    """Test detect_changes function with initial load (no existing data)."""
    
    # Create new snapshot
    data = [("A123", "Toyota"), ("B456", "Honda")]
    columns = ["kenteken", "merk"]
    new_df = spark_session.createDataFrame(data, columns)
    
    result_df = detect_changes(
        new_snapshot=new_df,
        existing_snapshot=None,
        primary_keys=["kenteken"],
        comparison_columns=["merk"]
    )
    
    # All records should be INSERT operations
    operations = [row["__operation"] for row in result_df.collect()]
    assert all(op == "INSERT" for op in operations)
    assert len(operations) == 2


def test_detect_changes_with_updates(spark_session):
    """Test detect_changes function with various change types."""
    
    # Old snapshot
    old_data = [("A123", "Toyota"), ("B456", "Honda"), ("C789", "Ford")]
    columns = ["kenteken", "merk"]
    old_df = spark_session.createDataFrame(old_data, columns)
    
    # New snapshot - update A123, delete C789, insert D000
    new_data = [("A123", "Tesla"), ("B456", "Honda"), ("D000", "BMW")]
    new_df = spark_session.createDataFrame(new_data, columns)
    
    result_df = detect_changes(
        new_snapshot=new_df,
        existing_snapshot=old_df,
        primary_keys=["kenteken"],
        comparison_columns=["merk"]
    )
    
    # Collect results and check operations
    results = {row["kenteken"]: row["__operation"] for row in result_df.collect()}
    
    assert results["A123"] == "UPDATE"  # Toyota -> Tesla
    assert results["C789"] == "DELETE"  # Removed
    assert results["D000"] == "INSERT"  # New
    # B456 should not appear (no change)


def test_add_scd2_columns(spark_session):
    """Test adding SCD2 metadata columns."""
    
    data = [("A123", "Toyota")]
    columns = ["kenteken", "merk"]
    df = spark_session.createDataFrame(data, columns)
    
    test_timestamp = datetime(2024, 1, 1)
    result_df = add_scd2_columns(df, test_timestamp, is_current=True, version=2, operation="INSERT")
    
    # Check SCD2 columns are added
    scd2_cols = ["__valid_from", "__valid_to", "__is_current", "__version", "__operation", "__processed_time"]
    for col in scd2_cols:
        assert col in result_df.columns
    
    row = result_df.collect()[0]
    assert row["__valid_from"] == test_timestamp
    assert row["__valid_to"] is None
    assert row["__is_current"] == True
    assert row["__version"] == 2


def test_get_current_records(spark_session):
    """Test extracting current records from history."""
    
    # Create history data with current and non-current records
    data = [
        ("A123", "Toyota", False, 1, "INSERT"),
        ("A123", "Honda", True, 2, "UPDATE"),
        ("B456", "Ford", True, 1, "INSERT")
    ]
    columns = ["kenteken", "merk", "__is_current", "__version", "__operation"]
    history_df = spark_session.createDataFrame(data, columns)
    
    current_df = get_current_records(history_df)
    
    # Should only have current records
    assert current_df.count() == 2
    current_records = {row["kenteken"]: row["merk"] for row in current_df.collect()}
    assert current_records["A123"] == "Honda"  # Latest version
    assert current_records["B456"] == "Ford"
    
    # SCD2 columns should be removed
    scd2_cols = ["__valid_from", "__valid_to", "__is_current", "__version", "__operation"]
    for col in scd2_cols:
        assert col not in current_df.columns


@pytest.mark.parametrize("operation,expected_current", [
    ("INSERT", True),
    ("UPDATE", True),
    ("DELETE", False)
])
def test_create_history_records(spark_session, operation, expected_current):
    """Test creating history records for different operations."""
    
    data = [("A123", "Toyota")]
    columns = ["kenteken", "merk"]
    df = spark_session.createDataFrame(data, columns)
    
    test_timestamp = datetime(2024, 1, 1)
    result_df = create_history_records(df, operation, test_timestamp, version=1)
    
    row = result_df.collect()[0]
    assert row["__operation"] == operation
    assert row["__is_current"] == expected_current
    assert row["__valid_from"] == test_timestamp
    
    if operation == "DELETE":
        assert row["__valid_to"] == test_timestamp
    else:
        assert row["__valid_to"] is None


# Edge case tests
def test_empty_dataframes(spark_session):
    """Test SCD2 processor with empty DataFrames."""
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        exclude_columns=[]
    )
    
    # Create empty DataFrame with correct schema
    schema = StructType([
        StructField("kenteken", StringType(), True),
        StructField("merk", StringType(), True)
    ])
    empty_df = spark_session.createDataFrame([], schema)
    
    current, history = processor.process_snapshot(
        new_snapshot=empty_df,
        existing_history=None
    )
    
    assert current.count() == 0
    assert history.count() == 0


def test_null_values_handling(spark_session):
    """Test handling of null values in data."""
    
    # Create data with null values
    data = [("A123", None), ("B456", "Honda"), (None, "Toyota")]
    columns = ["kenteken", "merk"]
    df = spark_session.createDataFrame(data, columns)
    
    result_df = generate_row_hash(df, ["kenteken", "merk"])
    
    # Should handle nulls gracefully
    assert result_df.count() == 3
    assert "__row_hash" in result_df.columns


def test_processor_with_tracking_columns(spark_session):
    """Test SCD2 processor with specific tracking columns."""
    data = [("A123", "Toyota", "Red", "2020")]
    columns = ["kenteken", "merk", "kleur", "jaar"]
    df = spark_session.createDataFrame(data, columns)
    
    # Only track 'merk' changes, ignore 'kleur' and 'jaar'
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        tracking_columns=["merk"]
    )
    
    current, history = processor.process_snapshot(
        new_snapshot=df,
        existing_history=None
    )
    
    assert current.count() == 1
    assert history.count() == 1


def test_processor_with_exclude_columns(spark_session):
    """Test SCD2 processor with excluded columns."""
    data = [("A123", "Toyota", "metadata1", "metadata2")]
    columns = ["kenteken", "merk", "ignore_col1", "ignore_col2"]
    df = spark_session.createDataFrame(data, columns)
    
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        exclude_columns=["ignore_col1", "ignore_col2"]
    )
    
    # Get comparison columns to verify exclusion
    comparison_cols = processor._get_comparison_columns(df)
    
    assert "ignore_col1" not in comparison_cols
    assert "ignore_col2" not in comparison_cols
    assert "merk" in comparison_cols
    assert "kenteken" not in comparison_cols  # Primary keys are excluded from comparison


def test_scd2_columns_consistency(spark_session):
    """Test that SCD2 columns maintain consistency."""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        exclude_columns=[]
    )
    
    sample = pd.read_csv(os.path.join(base_dir, 'test_data', 'scd2', 'old_sample.csv'), sep=",", engine="python", on_bad_lines="skip")
    sample = sample.where(sample.notna(), None)
    df = spark_session.createDataFrame(sample)
    
    timestamp = datetime(2024, 1, 1)
    current, history = processor.process_snapshot(
        new_snapshot=df,
        existing_history=None,
        timestamp=timestamp
    )
    
    # Verify SCD2 column consistency
    for row in history.collect():
        # Current records should have valid_to = null
        if row["__is_current"]:
            assert row["__valid_to"] is None
        # Non-current records should have valid_to set
        else:
            assert row["__valid_to"] is not None
        
        # All records should have valid_from
        assert row["__valid_from"] is not None
        
        # Version should be >= 1
        assert row["__version"] >= 1


def test_delete_operations(spark_session):
    """Test specific DELETE operation behavior."""
    # Create initial data
    data = [("A123", "Toyota"), ("B456", "Honda")]
    columns = ["kenteken", "merk"]
    old_df = spark_session.createDataFrame(data, columns)
    
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )
    
    # Initial load
    initial_timestamp = datetime(2024, 1, 1)
    current1, history1 = processor.process_snapshot(
        new_snapshot=old_df,
        existing_history=None,
        timestamp=initial_timestamp
    )
    
    # New data with one record deleted
    new_data = [("A123", "Toyota")]  # B456 deleted
    new_df = spark_session.createDataFrame(new_data, columns)
    
    update_timestamp = datetime(2024, 2, 1)
    current2, history2 = processor.process_snapshot(
        new_snapshot=new_df,
        existing_history=history1,
        timestamp=update_timestamp
    )
    
    # Verify deleted record
    deleted_record = history2.filter(
        (F.col("kenteken") == "B456") & 
        (F.col("__operation") == "DELETE")
    ).collect()
    
    assert len(deleted_record) == 1
    assert deleted_record[0]["__is_current"] == False
    assert deleted_record[0]["__valid_to"] == update_timestamp
    
    # Current records should only have A123
    assert current2.count() == 1
    assert current2.collect()[0]["kenteken"] == "A123"


def test_merge_history_tables(spark_session):
    """Test merging two history tables while maintaining SCD2 integrity."""
    
    # Create first history table
    data1 = [("A123", "Toyota"), ("B456", "Honda")]
    columns = ["kenteken", "merk"]
    df1 = spark_session.createDataFrame(data1, columns)
    history1 = add_scd2_columns(df1, datetime(2024, 1, 1), is_current=True, version=1, operation="INSERT")
    
    # Create second history table with overlapping records
    data2 = [("A123", "Tesla"), ("C789", "Ford")]
    df2 = spark_session.createDataFrame(data2, columns)
    history2 = add_scd2_columns(df2, datetime(2024, 2, 1), is_current=True, version=2, operation="UPDATE")
    
    # Merge histories
    merged = merge_history_tables(history1, history2, primary_keys=["kenteken"])
    
    # Should have 4 records total
    assert merged.count() == 4
    
    # Verify only one current record per key
    current_a123 = merged.filter(
        (F.col("kenteken") == "A123") & 
        (F.col("__is_current") == True)
    )
    assert current_a123.count() == 1
    assert current_a123.collect()[0]["merk"] == "Tesla"  # Latest should be current


def test_all_operations_in_single_batch(spark_session):
    """Test INSERT, UPDATE, and DELETE operations in a single processing batch."""
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )
    
    # Initial data
    initial_data = [
        ("A123", "Toyota"),
        ("B456", "Honda"), 
        ("C789", "Ford")
    ]
    columns = ["kenteken", "merk"]
    old_df = spark_session.createDataFrame(initial_data, columns)
    
    # Initial load
    initial_timestamp = datetime(2024, 1, 1)
    current1, history1 = processor.process_snapshot(
        new_snapshot=old_df,
        existing_history=None,
        timestamp=initial_timestamp
    )
    
    # New data: UPDATE A123, DELETE C789, INSERT D000
    new_data = [
        ("A123", "Tesla"),    # UPDATE: Toyota -> Tesla
        ("B456", "Honda"),    # NO CHANGE
        ("D000", "BMW")       # INSERT: New record
        # C789 missing = DELETE
    ]
    new_df = spark_session.createDataFrame(new_data, columns)
    
    update_timestamp = datetime(2024, 2, 1)
    current2, history2 = processor.process_snapshot(
        new_snapshot=new_df,
        existing_history=history1,
        timestamp=update_timestamp
    )
    
    # Verify all operations occurred
    new_records = history2.filter(F.col("__valid_from") == update_timestamp).collect()
    operations = {row["kenteken"]: row["__operation"] for row in new_records}
    
    # Should have operations for changed records
    assert "D000" in operations and operations["D000"] == "INSERT"
    
    # Verify DELETE operation for C789
    deleted_c789 = history2.filter(
        (F.col("kenteken") == "C789") & 
        (F.col("__operation") == "DELETE")
    )
    assert deleted_c789.count() == 1
    
    # Verify current state
    current_records = {row["kenteken"]: row["merk"] for row in current2.collect()}
    expected_current = {"A123": "Tesla", "B456": "Honda", "D000": "BMW"}
    assert current_records == expected_current


def test_scd2_temporal_rules(spark_session):
    """Validate all SCD2 temporal integrity rules."""
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )
    
    # Create test scenario with multiple updates
    data1 = [("A123", "Toyota")]
    columns = ["kenteken", "merk"]
    df1 = spark_session.createDataFrame(data1, columns)
    
    # Multiple sequential updates
    timestamps = [
        datetime(2024, 1, 1),
        datetime(2024, 2, 1), 
        datetime(2024, 3, 1)
    ]
    
    history = None
    updates = ["Toyota", "Honda", "Tesla"]
    
    for i, (ts, brand) in enumerate(zip(timestamps, updates)):
        data = [("A123", brand)]
        df = spark_session.createDataFrame(data, columns)
        current, history = processor.process_snapshot(
            new_snapshot=df,
            existing_history=history,
            timestamp=ts
        )
    
    # Collect all history records for A123
    a123_history = history.filter(F.col("kenteken") == "A123").orderBy("__valid_from").collect()
    
    # Rule 1: Only one current record per primary key
    current_count = sum(1 for row in a123_history if row["__is_current"])
    assert current_count == 1, f"Expected 1 current record, got {current_count}"
    
    # Rule 2: valid_from < valid_to for closed records
    for row in a123_history:
        if not row["__is_current"] and row["__valid_to"] is not None:
            assert row["__valid_from"] < row["__valid_to"], \
                f"valid_from {row['__valid_from']} should be < valid_to {row['__valid_to']}"
    
    # Rule 3: Current record should have valid_to = null
    current_record = [row for row in a123_history if row["__is_current"]][0]
    assert current_record["__valid_to"] is None, "Current record should have valid_to = null"
    
    # Rule 4: Version numbers should increment
    versions = [row["__version"] for row in a123_history]
    assert versions == sorted(versions), f"Versions should increment: {versions}"


def test_multiple_updates_same_record(spark_session):
    """Test multiple changes to the same record across processing batches."""
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )
    
    # Track complete evolution of a single vehicle
    vehicle_evolution = [
        ("2024-01-01", "A123", "Toyota", "Red"),
        ("2024-02-01", "A123", "Toyota", "Blue"),     # Color change
        ("2024-03-01", "A123", "Honda", "Blue"),      # Brand change  
        ("2024-04-01", "A123", "Honda", "Green"),     # Color change again
    ]
    
    columns = ["kenteken", "merk", "kleur"]
    history = None
    
    for date_str, kenteken, merk, kleur in vehicle_evolution:
        timestamp = datetime.strptime(date_str, "%Y-%m-%d")
        data = [(kenteken, merk, kleur)]
        df = spark_session.createDataFrame(data, columns)
        
        current, history = processor.process_snapshot(
            new_snapshot=df,
            existing_history=history,
            timestamp=timestamp
        )
    
    # Should have 4 historical records for this vehicle
    a123_records = history.filter(F.col("kenteken") == "A123").collect()
    assert len(a123_records) == 4, f"Expected 4 records, got {len(a123_records)}"
    
    # Verify evolution timeline (order by valid_from to ensure correct sequence)
    sorted_records = sorted(a123_records, key=lambda x: x["__valid_from"])
    assert sorted_records[0]["merk"] == "Toyota" and sorted_records[0]["kleur"] == "Red"
    assert sorted_records[1]["merk"] == "Toyota" and sorted_records[1]["kleur"] == "Blue"
    assert sorted_records[2]["merk"] == "Honda" and sorted_records[2]["kleur"] == "Blue"  
    assert sorted_records[3]["merk"] == "Honda" and sorted_records[3]["kleur"] == "Green"
    
    # Only the latest should be current
    current_records = [row for row in a123_records if row["__is_current"]]
    assert len(current_records) == 1
    assert current_records[0]["__version"] == 4


def test_hash_stability(spark_session):
    """Test that hash generation is stable and consistent."""
    
    # Create identical data twice
    data = [("A123", "Toyota", "Red"), ("B456", "Honda", "Blue")]
    columns = ["kenteken", "merk", "kleur"]
    
    df1 = spark_session.createDataFrame(data, columns)
    df2 = spark_session.createDataFrame(data, columns)
    
    hash_df1 = generate_row_hash(df1, ["merk", "kleur"])
    hash_df2 = generate_row_hash(df2, ["merk", "kleur"])
    
    hashes1 = {row["kenteken"]: row["__row_hash"] for row in hash_df1.collect()}
    hashes2 = {row["kenteken"]: row["__row_hash"] for row in hash_df2.collect()}
    
    # Hashes should be identical for same data
    assert hashes1 == hashes2, "Hash should be stable for identical data"
    
    # Different data should produce different hashes
    data_different = [("A123", "Tesla", "Red")]  # Changed Toyota -> Tesla
    df3 = spark_session.createDataFrame(data_different, columns)
    hash_df3 = generate_row_hash(df3, ["merk", "kleur"])
    hash3 = hash_df3.collect()[0]["__row_hash"]
    
    assert hash3 != hashes1["A123"], "Different data should produce different hashes"


def test_complex_rdw_vehicle_scenario(spark_session):
    """Test complete RDW vehicle lifecycle with realistic scenarios."""
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"],
        tracking_columns=["eigenaar", "merk", "kleur", "vervaldatum_apk"]  # Track key vehicle attributes
    )
    
    # Vehicle lifecycle: registration -> ownership change -> technical update -> deregistration
    lifecycle_data = [
        # Initial registration
        {
            "timestamp": datetime(2020, 1, 15),
            "data": [("ABC123", "John Doe", "Toyota", "Red", "2022-01-15")]
        },
        # Ownership transfer
        {
            "timestamp": datetime(2021, 6, 10),
            "data": [("ABC123", "Jane Smith", "Toyota", "Red", "2022-01-15")]
        },
        # APK renewal with color change (respray)
        {
            "timestamp": datetime(2022, 1, 20),
            "data": [("ABC123", "Jane Smith", "Toyota", "Blue", "2024-01-20")]
        },
        # Vehicle sale to dealer
        {
            "timestamp": datetime(2023, 8, 5),
            "data": [("ABC123", "AutoBedrijf XYZ", "Toyota", "Blue", "2024-01-20")]
        }
        # Final step would be deregistration (removal from dataset)
    ]
    
    columns = ["kenteken", "eigenaar", "merk", "kleur", "vervaldatum_apk"]
    history = None
    
    for step in lifecycle_data:
        df = spark_session.createDataFrame(step["data"], columns)
        current, history = processor.process_snapshot(
            new_snapshot=df,
            existing_history=history,
            timestamp=step["timestamp"]
        )
    
    # Verify complete history is maintained
    vehicle_history = history.filter(F.col("kenteken") == "ABC123").orderBy("__version").collect()
    assert len(vehicle_history) == 4, f"Expected 4 lifecycle events, got {len(vehicle_history)}"
    
    # Verify ownership changes are tracked
    owners = [row["eigenaar"] for row in vehicle_history]
    expected_owners = ["John Doe", "Jane Smith", "Jane Smith", "AutoBedrijf XYZ"]
    assert owners == expected_owners, f"Expected owner progression {expected_owners}, got {owners}"
    
    # Verify technical changes are tracked
    colors = [row["kleur"] for row in vehicle_history]
    expected_colors = ["Red", "Red", "Blue", "Blue"]
    assert colors == expected_colors, f"Expected color progression {expected_colors}, got {colors}"
    
    # Current owner should be the dealer
    current_record = [row for row in vehicle_history if row["__is_current"]][0]
    assert current_record["eigenaar"] == "AutoBedrijf XYZ", "Current owner should be the dealer"


def test_historical_record_immutability(spark_session):
    """Test that historical records remain immutable when processing multiple updates.

    This is a regression test for a bug where historical records were being
    corrupted when the same record was updated multiple times. The bug caused
    all historical versions (not just current) to have their __valid_to timestamps
    updated, violating SCD2 immutability principles.

    Scenario:
    - V1: Insert kenteken "TEST123" with merk "Toyota" at T1
    - V2: Update merk to "Honda" at T2 (V1 should close with __valid_to=T2)
    - V3: Update merk to "Tesla" at T3 (V2 should close with __valid_to=T3, V1 should stay T2)
    - V4: Update merk to "BMW" at T4 (V3 should close with __valid_to=T4, V1/V2 should remain unchanged)

    Expected:
    - V1: __valid_to=T2 (should NEVER change after being set)
    - V2: __valid_to=T3 (should NEVER change after being set)
    - V3: __valid_to=T4 (should NEVER change after being set)
    - V4: __valid_to=None, __is_current=True
    """
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )

    columns = ["kenteken", "merk"]
    timestamps = [
        datetime(2024, 1, 1),   # T1
        datetime(2024, 2, 1),   # T2
        datetime(2024, 3, 1),   # T3
        datetime(2024, 4, 1),   # T4
    ]
    brands = ["Toyota", "Honda", "Tesla", "BMW"]

    history = None

    # Process each update
    for timestamp, brand in zip(timestamps, brands):
        data = [("TEST123", brand)]
        df = spark_session.createDataFrame(data, columns)

        current, history = processor.process_snapshot(
            new_snapshot=df,
            existing_history=history,
            timestamp=timestamp
        )

    # Verify we have 4 versions
    all_versions = history.filter(F.col("kenteken") == "TEST123").orderBy("__version").collect()
    assert len(all_versions) == 4, f"Expected 4 versions, got {len(all_versions)}"

    # Critical assertion: Verify historical records remain immutable
    v1, v2, v3, v4 = all_versions

    # V1 should be closed at T2 and never modified again
    assert v1["merk"] == "Toyota", "V1 should have original brand"
    assert v1["__version"] == 1, "V1 should have version 1"
    assert v1["__valid_to"] == timestamps[1], f"V1 __valid_to should be T2 ({timestamps[1]}), got {v1['__valid_to']}"
    assert v1["__is_current"] == False, "V1 should not be current"
    assert v1["__operation"] == "INSERT", "V1 should have INSERT operation"

    # V2 should be closed at T3 and never modified again
    assert v2["merk"] == "Honda", "V2 should have second brand"
    assert v2["__version"] == 2, "V2 should have version 2"
    assert v2["__valid_to"] == timestamps[2], f"V2 __valid_to should be T3 ({timestamps[2]}), got {v2['__valid_to']}"
    assert v2["__is_current"] == False, "V2 should not be current"
    assert v2["__operation"] == "UPDATE", "V2 should have UPDATE operation"

    # V3 should be closed at T4
    assert v3["merk"] == "Tesla", "V3 should have third brand"
    assert v3["__version"] == 3, "V3 should have version 3"
    assert v3["__valid_to"] == timestamps[3], f"V3 __valid_to should be T4 ({timestamps[3]}), got {v3['__valid_to']}"
    assert v3["__is_current"] == False, "V3 should not be current"
    assert v3["__operation"] == "UPDATE", "V3 should have UPDATE operation"

    # V4 should be current
    assert v4["merk"] == "BMW", "V4 should have latest brand"
    assert v4["__version"] == 4, "V4 should have version 4"
    assert v4["__valid_to"] is None, "V4 __valid_to should be None (current record)"
    assert v4["__is_current"] == True, "V4 should be current"
    assert v4["__operation"] == "UPDATE", "V4 should have UPDATE operation"

    print("✓ Historical record immutability verified - all closed records maintain original timestamps")


@pytest.mark.performance
def test_scd2_performance_baseline(spark_session):
    """Test SCD2 performance with moderately sized dataset."""
    
    # Generate synthetic test data (1000 records)
    num_records = 1000
    schema = StructType([
        StructField("kenteken", StringType(), True),
        StructField("merk", StringType(), True),
        StructField("model", StringType(), True),
        StructField("jaar", IntegerType(), True)
    ])
    
    # Generate data
    synthetic_data = []
    brands = ["Toyota", "Honda", "Ford", "BMW", "Mercedes"]
    models = ["ModelA", "ModelB", "ModelC", "ModelD", "ModelE"]
    
    for i in range(num_records):
        kenteken = f"TEST{i:04d}"
        merk = brands[i % len(brands)]
        model = models[i % len(models)]
        jaar = 2020 + (i % 5)
        synthetic_data.append((kenteken, merk, model, jaar))
    
    df = spark_session.createDataFrame(synthetic_data, schema)
    
    processor = SCD2Processor(
        spark=spark_session,
        primary_keys=["kenteken"]
    )
    
    # Measure initial load performance
    start_time = time.time()
    current, history = processor.process_snapshot(
        new_snapshot=df,
        existing_history=None,
        timestamp=datetime(2024, 1, 1)
    )
    initial_load_time = time.time() - start_time
    
    # Create updated data (change 10% of records)
    updated_data = []
    for i in range(num_records):
        kenteken = f"TEST{i:04d}"
        if i < num_records // 10:  # Update first 10%
            merk = "Tesla"  # Change brand to Tesla
            model = "ModelX"
        else:
            merk = brands[i % len(brands)]
            model = models[i % len(models)]
        jaar = 2020 + (i % 5)
        updated_data.append((kenteken, merk, model, jaar))
    
    updated_df = spark_session.createDataFrame(updated_data, schema)
    
    # Measure update performance
    start_time = time.time()
    current2, history2 = processor.process_snapshot(
        new_snapshot=updated_df,
        existing_history=history,
        timestamp=datetime(2024, 2, 1)
    )
    update_time = time.time() - start_time
    
    # Performance assertions (baseline benchmarks) - more lenient for local testing
    assert initial_load_time < 30.0, f"Initial load took {initial_load_time:.2f}s, should be < 30s"
    assert update_time < 45.0, f"Update took {update_time:.2f}s, should be < 45s"
    
    # Verify correct processing
    assert current2.count() == num_records, f"Expected {num_records} current records"
    
    # Verify updates were processed correctly
    tesla_records = current2.filter(F.col("merk") == "Tesla").count()
    expected_tesla_count = num_records // 10
    assert tesla_records == expected_tesla_count, \
        f"Expected {expected_tesla_count} Tesla records, got {tesla_records}"
    
    print(f"Performance baseline - Initial load: {initial_load_time:.2f}s, Update: {update_time:.2f}s")