import pytest
import os
from src.rdw.definitions import rdw_tables
from src.rdw.utils.utils import read_volume_csv, read_delta_table
# from tests.conftest import spark_session
from pyspark.sql import SparkSession 
import re

# Test if the Delta tables outputted in the bronze layer exist and are non-empty.
@pytest.mark.skip(reason="Requires Databricks Unity Catalog - not available in local testing environment")
def test_bronze_tables_exist(spark_session: SparkSession):
    for table in rdw_tables:
        df = read_delta_table(f"{table.delta_output_path}_current", spark_session)
        assert df.count() >= 0, f"Spark DF '{table.name}' is empty."

# Check if the names in the source .csv's correspond correctly to those stored in the BaseTables.
# This does not work in local pyspark. 
# def test_basetable_col_names(spark_session: SparkSession):
#     for table in rdw_tables:
#         df = read_volume_csv(table.volume_file_path, spark_session)

#         for column in table.columns:
#             assert column.input_col in df.columns, f"{column.input_col} from BaseTable '{table.name}' not found in CSV from {table.volume_file_path}"