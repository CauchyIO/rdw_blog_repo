import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField
from datetime import date
from unittest.mock import Mock, patch, mock_open

from src.rdw.utils.utils import (
    to_snake_case,
    filter_first_col_length,
    convert_columns_from_definition,
    convert_illegal_column_characters,
    chunked_download
)
from src.rdw.utils.models import BaseTable, BaseColumn


def test_to_snake_case():
    """Test conversion of various column name formats to snake_case."""
    assert to_snake_case("Kenteken Nummer") == "kenteken_nummer"
    assert to_snake_case("VoertuigSoort") == "voertuigsoort"
    assert to_snake_case("CO2 Uitstoot (kg)") == "co2_uitstoot_kg"
    assert to_snake_case("Multiple   Spaces") == "multiple_spaces"
    assert to_snake_case("already_snake_case") == "already_snake_case"
    assert to_snake_case("_leading_underscore_") == "leading_underscore"
    assert to_snake_case("") == ""


def test_filter_first_col_length(spark_session: SparkSession):
    """Test filtering DataFrame rows by first column length."""
    data = [
        ("AB12CD",),    # Valid 6 chars
        ("SHORT",),     # 5 chars - invalid
        ("TOOLONG1",),  # 8 chars - invalid
        ("XY34ZW",),    # Valid 6 chars
    ]
    df = spark_session.createDataFrame(data, ["kenteken"])

    result = filter_first_col_length(df, length=6)

    assert result.count() == 2
    kentekens = [row.kenteken for row in result.collect()]
    assert "AB12CD" in kentekens
    assert "XY34ZW" in kentekens


def test_convert_illegal_column_characters(spark_session: SparkSession):
    """Test conversion of column names with spaces and special characters."""
    data = [("AB12CD", "Personenauto", 100)]
    df = spark_session.createDataFrame(
        data,
        ["Kenteken", "Voertuigsoort", "Aantal Zitplaatsen"]
    )

    result = convert_illegal_column_characters(df)

    assert result.columns == ["kenteken", "voertuigsoort", "aantal_zitplaatsen"]


def test_convert_columns_from_definition_string_and_int(spark_session: SparkSession):
    """Test column renaming and type casting for strings and integers."""
    data = [("AB-12-CD", "Toyota", "5", "4")]
    df = spark_session.createDataFrame(
        data,
        ["Kenteken", "Merk", "Aantal Zitplaatsen", "Aantal Deuren"]
    )

    table = BaseTable(
        name="test_table",
        description="Test table for unit tests",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType()),
            BaseColumn(input_col="Merk", output_col="merk",
                      original_type=str, output_data_type=StringType()),
            BaseColumn(input_col="Aantal Zitplaatsen", output_col="aantal_zitplaatsen",
                      original_type=int, output_data_type=IntegerType()),
            BaseColumn(input_col="Aantal Deuren", output_col="aantal_deuren",
                      original_type=int, output_data_type=IntegerType()),
        ]
    )

    result = convert_columns_from_definition(df, table)

    # Check column names
    assert set(result.columns) == {"kenteken", "merk", "aantal_zitplaatsen", "aantal_deuren"}

    # Check types
    schema_dict = {field.name: field.dataType for field in result.schema.fields}
    assert isinstance(schema_dict["kenteken"], StringType)
    assert isinstance(schema_dict["aantal_zitplaatsen"], IntegerType)

    # Check values
    row = result.collect()[0]
    assert row.kenteken == "AB-12-CD"
    assert row.aantal_zitplaatsen == 5


def test_convert_columns_from_definition_dates(spark_session: SparkSession):
    """Test date conversion from yyyyMMdd format."""
    data = [("AB-12-CD", "20230115", "20001231")]
    df = spark_session.createDataFrame(
        data,
        ["Kenteken", "Datum Tenaamstelling", "Datum Eerste Toelating"]
    )

    table = BaseTable(
        name="test_table",
        description="Test table for unit tests",
        columns=[
            BaseColumn(input_col="Kenteken", output_col="kenteken",
                      original_type=str, output_data_type=StringType()),
            BaseColumn(input_col="Datum Tenaamstelling", output_col="datum_tenaamstelling",
                      original_type=str, output_data_type=DateType()),
            BaseColumn(input_col="Datum Eerste Toelating", output_col="datum_eerste_toelating",
                      original_type=str, output_data_type=DateType()),
        ]
    )

    result = convert_columns_from_definition(df, table)

    # Check types
    schema_dict = {field.name: field.dataType for field in result.schema.fields}
    assert isinstance(schema_dict["datum_tenaamstelling"], DateType)

    # Check values
    row = result.collect()[0]
    assert row.datum_tenaamstelling == date(2023, 1, 15)
    assert row.datum_eerste_toelating == date(2000, 12, 31)


def test_chunked_download():
    """Test actual chunked download with max_chunks parameter."""
    import os
    from pathlib import Path

    # Create test directory
    test_dir = Path("tests/test_data/tmp")
    test_dir.mkdir(parents=True, exist_ok=True)
    output_path = test_dir / "test_download.csv"

    try:
        # Download with max_chunks limit (small download for testing)
        url = "https://opendata.rdw.nl/api/views/jqs4-4kvw/rows.csv?accessType=DOWNLOAD"
        chunked_download(url, str(output_path), max_chunks=2)

        # Verify file was created
        assert output_path.exists()

        # Verify file has content
        assert output_path.stat().st_size > 0

    finally:
        # Cleanup: delete the downloaded file
        if output_path.exists():
            output_path.unlink()