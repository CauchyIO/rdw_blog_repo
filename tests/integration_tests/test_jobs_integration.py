import pytest
from pathlib import Path
from pyspark.sql import SparkSession
import shutil

from src.rdw.definitions import get_table_by_name
from src.rdw.utils.utils import chunked_download, read_volume_csv
from src.rdw.jobs.process_bronze_layer import transform_bronze
from src.rdw.jobs.process_silver_layer import transform_silver


# Shared test data directory and file paths
TEST_TABLE_NAME = "registered_vehicles"
TEST_DATA_DIR = Path("tests/test_data/integration")
TEST_CSV_PATH = TEST_DATA_DIR / f"{TEST_TABLE_NAME}.csv"
TEST_BRONZE_PARQUET = TEST_DATA_DIR / f"{TEST_TABLE_NAME}_bronze.parquet"


@pytest.mark.order(1)
def test_bronze_download():
    """Test 1: Download RDW CSV data from API."""
    TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    table = get_table_by_name(TEST_TABLE_NAME)

    # Download CSV
    chunked_download(table.url, str(TEST_CSV_PATH), max_chunks=2)

    # Verify download
    assert TEST_CSV_PATH.exists()
    assert TEST_CSV_PATH.stat().st_size > 0

    # Verify CSV has header
    with open(TEST_CSV_PATH, 'r') as f:
        header = f.readline()
        assert len(header) > 0
        assert "Kenteken" in header


@pytest.mark.order(2)
def test_bronze_transformation(spark_session: SparkSession):
    """Test 2: Test bronze layer transform_bronze() function and save results."""
    table = get_table_by_name(TEST_TABLE_NAME)

    # Load raw CSV
    df_raw = read_volume_csv(str(TEST_CSV_PATH), spark_session)
    raw_count = df_raw.count()

    # Apply transformation function
    df_transformed = transform_bronze(df_raw, table)

    # Verify transformations applied
    assert df_transformed.count() > 0
    assert df_transformed.count() <= raw_count  # May filter invalid rows
    assert "license_plate" in df_transformed.columns
    assert "make" in df_transformed.columns
    assert all(col.islower() or col.startswith("_") for col in df_transformed.columns)

    # Save bronze DataFrame for reuse in silver test
    df_transformed.write.mode("overwrite").parquet(str(TEST_BRONZE_PARQUET))


@pytest.mark.order(3)
def test_silver_transformation(spark_session: SparkSession):
    """Test 3: Test silver layer transform_silver() function using cached bronze data."""
    table = get_table_by_name(TEST_TABLE_NAME)

    # Load bronze DataFrame from saved parquet (created by test_bronze_transformation)
    df_bronze = spark_session.read.parquet(str(TEST_BRONZE_PARQUET))
    bronze_count = df_bronze.count()
    assert bronze_count > 0

    # Apply SCD2 transformation using the job's transform function (initial load)
    df_silver = transform_silver(
        new_snapshot=df_bronze,
        old_snapshot=None,  # Initial load
        spark=spark_session
    )

    # Verify SCD2 columns added
    assert "__valid_from" in df_silver.columns
    assert "__valid_to" in df_silver.columns
    assert "__is_current" in df_silver.columns
    assert "__operation" in df_silver.columns

    # Verify record count preserved
    assert df_silver.count() == bronze_count

    # All records should be current on initial load
    current_count = df_silver.filter("__is_current = true").count()
    assert current_count == bronze_count


# def test_full_pipeline_integration(bronze_dataframe, spark_session: SparkSession):
#     """Test 4: Complete pipeline from bronze to silver layer."""
#     table = get_table_by_name(TEST_TABLE_NAME)

#     # Start with bronze DataFrame
#     df_bronze = bronze_dataframe
#     bronze_count = df_bronze.count()

#     # Apply silver transformation using the job's transform function
#     df_silver = transform_silver(
#         new_snapshot=df_bronze,
#         old_snapshot=None,
#         spark=spark_session,
#         table=table
#     )

#     # Verify end-to-end data flow
#     assert bronze_count > 0
#     assert df_silver.count() == bronze_count

#     # Verify bronze columns preserved in silver
#     assert "kenteken" in df_silver.columns
#     assert "merk" in df_silver.columns

#     # Verify SCD2 metadata added
#     assert "__valid_from" in df_silver.columns
#     assert "__is_current" in df_silver.columns
#     assert "__operation" in df_silver.columns
